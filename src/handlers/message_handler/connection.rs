use core::str;
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

use super::message_registry::MessageRegistry;
use super::messages::Message;
use super::Pipe;

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
    #[error("buffer reached max size")]
    BufferReachedMaxSize,
    #[error("timed out waiting for connections to close")]
    TimedOutWaitingForConnectionsToClose,
    #[error("connection responded with none ok response")]
    ConnectionRespondedWithNoneOkResponse,
}

pub struct ConnectionComm {
    pub stream_id: u128,
    pub sender: mpsc::Sender<Message>,
    pub connection_ct: CancellationToken,
}

impl ConnectionComm {
    pub fn new(
        connection_ct: CancellationToken,
        stream_id: u128,
        sender: mpsc::Sender<Message>,
    ) -> ConnectionComm {
        ConnectionComm {
            stream_id,
            sender,
            connection_ct,
        }
    }
}

pub struct Connection {
    pub stream_id: u128,
    stream: TcpStream,
    pipe: Pipe<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,
    buf: BytesMut,
    pub connection_ct: CancellationToken,
}

impl Connection {
    pub fn new(
        stream: TcpStream,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> (Connection, Pipe<Message>) {
        let (p1, p2) = Pipe::new(1);
        let conn = Connection {
            stream_id: Uuid::new_v4().as_u128(),
            stream,
            pipe: p1,
            msg_reg,
            buf: BytesMut::with_capacity(4096),
            connection_ct: CancellationToken::new(),
        };
        (conn, p2)
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        info!("new connection");

        loop {
            if let Ok(msg) = self.msg_reg.build_msg(&mut self.buf) {
                if let Some(mut msg) = msg {
                    msg = msg.set_inbound_stream_id(self.stream_id);
                    self.pipe.send(msg).await?;
                    self.stream.write_all("OK".as_bytes()).await?;
                }
                continue;
            }

            // end the conneciton if the other system has sent too much data
            if self.buf.len() > 1024 * 1024 * 10 {
                return Err(ConnectionError::BufferReachedMaxSize.into());
            }

            tokio::select! {
                read_res = self.stream.read_buf(&mut self.buf) => {
                    match read_res {
                        Ok(size) => {
                            if size == 0 {
                                if self.buf.is_empty() {
                                    return Ok(());
                                } else {
                                    return Err(ConnectionError::ConnectionResetByPeer.into());
                                }
                            }
                        },
                        Err(err) => return Err(err.into()),
                    }
                },
                Some(msg) = self.pipe.recv() => {
                    let msg_bytes = msg.to_bytes()?;
                    self.stream.write_all(&msg_bytes[..]).await?;

                    let mut resp = [0; 3];
                    let resp_size = self.stream.read(&mut resp).await?;

                    let resp_msg = str::from_utf8(&resp[..resp_size])?.to_string();
                    if resp_msg != "OK" {
                        return Err(ConnectionError::ConnectionRespondedWithNoneOkResponse.into());
                    }
                }
                _ = self.connection_ct.cancelled() => {
                    break;
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        info!("closing connection...");

        Ok(())
    }
}
