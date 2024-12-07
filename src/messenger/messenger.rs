use core::str;
use std::borrow::Borrow;

use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::info;

use super::messages::Message;

#[derive(Error, Debug)]
pub enum MessengerErrors {
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
}

pub struct Messenger {
    address: String,
}

impl Messenger {
    pub fn new(address: String) -> Messenger {
        return Messenger { address };
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

            // Spawn a new task to handle the connection
            tokio::spawn(Messenger::task_read_message(tx.clone(), socket));
        }
    }

    async fn task_read_message(
        message_chan: mpsc::Sender<Message>,
        mut stream: TcpStream,
    ) -> Result<()> {
        let mut buf = BytesMut::new();

        loop {
            if buf.len() > 0 {
                message_chan
                    .send(Message::Ping {
                        msg: str::from_utf8(&buf)?.to_string(),
                    })
                    .await?;
                stream.write_all("received!".as_bytes()).await?;
                return Ok(());
            }

            if stream.read_buf(&mut buf).await? == 0 {
                if buf.is_empty() {
                    return Ok(());
                } else {
                    return Err(MessengerErrors::ConnectionResetByPeer.into());
                }
            }
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
