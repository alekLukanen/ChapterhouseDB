use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
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

pub struct InboundConnectionPoolHandler {
    address: String,
    connect_to_addresses: Vec<String>,

    msg_reg: Arc<Box<MessageRegistry>>,
    outbound_sender: broadcast::Sender<Message>,
    outbound_receiver: broadcast::Receiver<Message>,
    inbound_receiver: mpsc::Receiver<Message>,
    inbound_sender: mpsc::Sender<Message>,
}

impl InboundConnectionPoolHandler {
    pub fn new(address: String, connect_to_addresses: Vec<String>) -> InboundConnectionPoolHandler {
        let (out_tx, out_rx) = broadcast::channel(1);
        let (in_tx, in_rx) = mpsc::channel(1);
        return InboundConnectionPoolHandler {
            address,
            connect_to_addresses,
            msg_reg: Arc::new(Box::new(MessageRegistry::new())),
            outbound_sender: out_tx,
            outbound_receiver: out_rx,
            inbound_receiver: in_rx,
            inbound_sender: in_tx,
        };
    }

    pub fn inbound_sender(&self) -> mpsc::Sender<Message> {
        self.inbound_sender.clone()
    }

    pub fn outbound_receiver(&mut self) -> broadcast::Receiver<Message> {
        self.outbound_sender.subscribe()
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        info!("Starting Messenger...");

        let tt = TaskTracker::new();
        let listener = TcpListener::bind(&self.address).await?;

        let (connection_tx, mut connection_rx) = mpsc::channel::<Message>(1);

        info!("Messenger listening on {}", self.address);

        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((socket, _)) => {
                            let mut connection =
                                InboundConnection::new(socket, connection_tx.clone(), Arc::clone(&self.msg_reg));

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
                Some(msg) = connection_rx.recv() => {
                    info!("message: {:?}", msg);
                    if let Err(err) = self.outbound_sender.send(msg) {
                        info!("error: {}", err);
                    }
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        info!("message handler closing...");

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
}

struct InboundConnection {
    stream: TcpStream,
    msg_sender: mpsc::Sender<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,
    buf: BytesMut,
}

impl InboundConnection {
    fn new(
        stream: TcpStream,
        msg_sender: mpsc::Sender<Message>,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> InboundConnection {
        InboundConnection {
            stream,
            msg_sender,
            msg_reg,
            buf: BytesMut::with_capacity(4096),
        }
    }

    async fn read_msgs(&mut self) -> Result<()> {
        info!("new connection");

        loop {
            if let Ok(msg) = self.msg_reg.build_msg(&mut self.buf) {
                if let Some(msg) = msg {
                    self.msg_sender.send(msg).await?;
                    self.stream.write_all("OK".as_bytes()).await?;
                }
                continue;
            }

            // end the conneciton if the other system has sent too much data
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
