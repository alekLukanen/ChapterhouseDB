use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use super::message_registry::MessageRegistry;
use super::messages::Message;

#[derive(Error, Debug)]
pub enum MessengerError {
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
    #[error("buffer reached max size")]
    BufferReachedMaxSize,
    #[error("timed out waiting for connections to close")]
    TimedOutWaitingForConnectionsToClose,
}

pub struct MessageHandler {
    cancellation_token: CancellationToken,
    address: String,
    msg_reg: Arc<Box<MessageRegistry>>,
}

impl MessageHandler {
    pub fn new(cancellation_token: CancellationToken, address: String) -> MessageHandler {
        return MessageHandler {
            cancellation_token,
            address,
            msg_reg: Arc::new(Box::new(MessageRegistry::new())),
        };
    }

    pub async fn listen(&self) -> Result<()> {
        info!("Starting Messenger...");

        let tt = TaskTracker::new();
        let listener = TcpListener::bind(&self.address).await?;

        let (tx, rx) = mpsc::channel::<Message>(100);
        tokio::spawn(MessageHandler::task_route_message(rx));

        info!("Messenger listening on {}", self.address);

        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((socket, _)) => {
                            let mut connection =
                                InboundConnection::new(socket, tx.clone(), Arc::clone(&self.msg_reg));

                            // Spawn a new task to handle the connection
                            tt.spawn(async move {
                                if let Err(err) = connection.read_msgs().await {
                                    info!("error reading from tcp socket: {}", err);
                                }
                            });
                        },
                        Err(err) => {
                            return Err(err.into());
                        }
                    }
                }
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
            }
        }

        // wait for all existing connection to close
        tt.close();
        tokio::select! {
            _ = tt.wait() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                return Err(MessengerError::TimedOutWaitingForConnectionsToClose.into());
            }
        }

        Ok(())
    }

    async fn task_route_message(mut rx: mpsc::Receiver<Message>) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    info!("message: {:?}", msg);
                },
            }
        }
    }
}

struct InboundConnection {
    stream: TcpStream,
    msg_chan: mpsc::Sender<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,
    buf: BytesMut,
}

impl InboundConnection {
    fn new(
        stream: TcpStream,
        msg_chan: mpsc::Sender<Message>,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> InboundConnection {
        InboundConnection {
            stream,
            msg_chan,
            msg_reg,
            buf: BytesMut::with_capacity(4096),
        }
    }

    async fn read_msgs(&mut self) -> Result<()> {
        info!("new connection");

        loop {
            if let Ok(msg) = self.msg_reg.build_msg(&mut self.buf) {
                if let Some(msg) = msg {
                    info!("found message");
                    self.msg_chan.send(msg).await?;
                    self.stream.write_all("OK".as_bytes()).await?;
                }
                continue;
            }

            if self.buf.len() > 1024 * 1024 * 10 {
                return Err(MessengerError::BufferReachedMaxSize.into());
            }

            if self.stream.read_buf(&mut self.buf).await? == 0 {
                if self.buf.is_empty() {
                    return Ok(());
                } else {
                    return Err(MessengerError::ConnectionResetByPeer.into());
                }
            }
        }
    }
}
