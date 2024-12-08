use core::str;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::info;

use super::message_registry::MessageRegistry;
use super::messages::Message;

#[derive(Error, Debug)]
pub enum MessengerErrors {
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
}

pub struct Messenger {
    address: String,
    msg_reg: Arc<Box<MessageRegistry>>,
}

impl Messenger {
    pub fn new(address: String) -> Messenger {
        return Messenger {
            address,
            msg_reg: Arc::new(Box::new(MessageRegistry::new())),
        };
    }

    pub async fn listen(&self) -> Result<()> {
        info!("Starting Messenger...");

        let listener = TcpListener::bind(&self.address).await?;

        let (tx, mut rx) = mpsc::channel::<Message>(100);
        tokio::spawn(Messenger::task_route_message(rx));

        info!("Messenger listening on {}", self.address);

        loop {
            let (mut socket, _) = listener.accept().await?;
            info!("New connection established");

            let mut connection = Connection::new(socket, tx.clone(), Arc::clone(&self.msg_reg));

            // Spawn a new task to handle the connection
            tokio::spawn(async move {
                if let Err(err) = connection.read_msgs().await {
                    info!("error reading from tcp socket: {}", err);
                }
            });
        }
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

struct Connection {
    stream: TcpStream,
    msg_chan: mpsc::Sender<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,
    buf: BytesMut,
}

impl Connection {
    fn new(
        stream: TcpStream,
        msg_chan: mpsc::Sender<Message>,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> Connection {
        Connection {
            stream,
            msg_chan,
            msg_reg,
            buf: BytesMut::with_capacity(4096),
        }
    }

    async fn read_msgs(&mut self) -> Result<()> {
        loop {
            if let Some(msg) = self.msg_reg.build_msg(&mut self.buf)? {
                self.msg_chan.send(msg).await?;
                self.stream.write_all("OK".as_bytes()).await?;
                return Ok(());
            }

            if self.stream.read_buf(&mut self.buf).await? == 0 {
                if self.buf.is_empty() {
                    return Ok(());
                } else {
                    return Err(MessengerErrors::ConnectionResetByPeer.into());
                }
            }
        }
    }
}
