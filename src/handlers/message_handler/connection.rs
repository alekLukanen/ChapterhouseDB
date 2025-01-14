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

use crate::handlers::message_handler::SerializedMessageError;

use super::message_registry::MessageRegistry;
use super::messages::{Identify, Message};
use super::Pipe;

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
    #[error("buffer reached max size")]
    BufferReachedMaxSize,
    #[error("timed out waiting for connections to close")]
    TimedOutWaitingForConnectionsToClose,
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
    pub worker_id: u128,
    pub stream_id: u128,
    stream: TcpStream,
    pipe: Pipe,
    msg_reg: Arc<MessageRegistry>,
    buf: BytesMut,
    pub connection_ct: CancellationToken,
    send_identification_msg: bool,
    is_inbound: bool,
}

impl Connection {
    pub fn new(
        worker_id: u128,
        stream: TcpStream,
        sender: mpsc::Sender<Message>,
        msg_reg: Arc<MessageRegistry>,
        is_inbound: bool,
    ) -> (Connection, ConnectionComm) {
        let (pipe, sender_to_conn) = Pipe::new_with_existing_sender(sender, 1);
        let conn = Connection {
            worker_id,
            stream_id: Uuid::new_v4().as_u128(),
            stream,
            pipe,
            msg_reg,
            buf: BytesMut::with_capacity(4096),
            connection_ct: CancellationToken::new(),
            send_identification_msg: false,
            is_inbound,
        };
        let comm = ConnectionComm::new(conn.connection_ct.clone(), conn.stream_id, sender_to_conn);
        (conn, comm)
    }

    pub fn set_send_identification(&mut self) -> &Self {
        self.send_identification_msg = true;
        self
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        info!(
            stream_id = self.stream_id,
            ip = self.stream.peer_addr()?.to_string(),
            "new connection",
        );

        if self.send_identification_msg {
            let identity_msg = Message::new(Box::new(Identify::Worker {
                id: self.worker_id.clone(),
            }))
            .set_sent_from_worker_id(self.worker_id.clone());
            self.stream.write_all(&identity_msg.to_bytes()?[..]).await?;
        }

        loop {
            match self.msg_reg.build_msg(&mut self.buf) {
                Ok(msg) => {
                    if let Some(mut msg) = msg {
                        if self.is_inbound {
                            msg = msg.set_inbound_stream_id(self.stream_id);
                        } else {
                            msg = msg.set_outbound_stream(self.stream_id);
                        }
                        self.pipe.send(msg).await?;
                    }
                    continue;
                }
                Err(err) => {
                    let ser_msg_err = err.downcast_ref::<SerializedMessageError>();
                    match ser_msg_err {
                        Some(SerializedMessageError::Incomplete) => (),
                        _ => {
                            info!("error: {}", err);
                        }
                    }
                }
            }

            // end the conneciton if the other system has sent too much data
            if self.buf.len() > 1024 * 1024 * 10 {
                self.pipe.close_receiver();
                return Err(ConnectionError::BufferReachedMaxSize.into());
            }

            tokio::select! {
                read_res = self.stream.read_buf(&mut self.buf) => {
                    match read_res {
                        Ok(size) => {
                            if size == 0 {
                                self.pipe.close_receiver();
                                if self.buf.is_empty() {
                                    return Ok(());
                                } else {
                                    return Err(ConnectionError::ConnectionResetByPeer.into());
                                }
                            }
                        },
                        Err(err) => {
                            self.pipe.close_receiver();
                            return Err(err.into())
                        },
                    }
                },
                Some(msg) = self.pipe.recv() => {
                    let msg_bytes = msg.to_bytes()?;
                    self.stream.write_all(&msg_bytes[..]).await?;
                },
                _ = self.connection_ct.cancelled() => {
                    break;
                },
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        info!("closing connection...");
        self.pipe.close_receiver();
        tokio::select! {
            res = self.stream.shutdown() => {
                if let Err(err) = res {
                    return Err(err.into());
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                return Err(ConnectionError::TimedOutWaitingForConnectionsToClose.into());
            }
        }

        Ok(())
    }

    pub fn cleanup(&self) {
        self.connection_ct.cancel();
    }
}
