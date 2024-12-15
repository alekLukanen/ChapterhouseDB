use core::str;
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;
use uuid::Uuid;

use super::message_registry::MessageRegistry;
use super::messages::Message;
use super::Pipe;

#[derive(Error, Debug)]
pub enum InboundConnectionPoolError {
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
    #[error("buffer reached max size")]
    BufferReachedMaxSize,
    #[error("timed out waiting for connections to close")]
    TimedOutWaitingForConnectionsToClose,
    #[error("connection responded with none ok response")]
    ConnectionRespondedWithNoneOkResponse,
}

pub struct InboundConnectionPoolHandler {
    address: String,

    msg_reg: Arc<Box<MessageRegistry>>,
    pipe: Pipe<Message>,

    connections: Arc<Mutex<Vec<ConnectionComm>>>,
}

impl InboundConnectionPoolHandler {
    pub fn new(
        address: String,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> (InboundConnectionPoolHandler, Pipe<Message>) {
        let (p1, p2) = Pipe::new(1);
        let hndlr = InboundConnectionPoolHandler {
            address,
            msg_reg,
            pipe: p1,
            connections: Arc::new(Mutex::new(Vec::new())),
        };
        (hndlr, p2)
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
                            let (mut connection, pipe) =
                                InboundConnection::new(socket, Arc::clone(&self.msg_reg));

                            let (conn_send, conn_recv) = pipe.split();
                            let conn_comm = ConnectionComm::new(connection.connection_ct.clone(), connection.stream_id, conn_send);
                            self.connections.lock().await.push(conn_comm);

                            let ct1 = ct.clone();
                            let conn_ct = connection.connection_ct.clone();
                            let connection_tx = connection_tx.clone();
                            tt.spawn(async move {
                                if let Err(err) = Self::forward_msgs(ct1, conn_ct, conn_recv, connection_tx).await {
                                    info!("error forwarding messages: {}", err);
                                }
                            });

                            // Spawn a new task to handle the connection
                            let ct2 = ct.clone();
                            tt.spawn(async move {
                                if let Err(err) = connection.async_main(ct2).await {
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
                    if let Err(err) = self.pipe.send(msg).await {
                        info!("error: {}", err);
                    }
                }
                Some(msg) = self.pipe.recv() => {
                    info!("message: {:?}", msg);
                    if let Some(inbound_stream_id) = msg.inbound_stream_id {
                        for comm in self.connections.lock().await.iter() {
                            if comm.stream_id != inbound_stream_id {
                                continue;
                            }
                            if let Err(err) = comm.sender.send(msg.clone()).await {
                                // if there is an error close the connection
                                info!("error: {}", err);
                                comm.connection_ct.cancel();
                            };
                        }
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
                return Err(InboundConnectionPoolError::TimedOutWaitingForConnectionsToClose.into());
            }
        }

        Ok(())
    }

    async fn forward_msgs(
        ct: CancellationToken,
        connection_ct: CancellationToken,
        conn_recv: mpsc::Receiver<Message>,
        reducer: mpsc::Sender<Message>,
    ) -> Result<()> {
        let mut conn_recv = conn_recv;
        loop {
            tokio::select! {
                Some(msg) = conn_recv.recv() => {
                    reducer.send(msg).await?;
                },
                _ = ct.cancelled() => {
                    break;
                }
                _ = connection_ct.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }
}

pub struct ConnectionComm {
    stream_id: u128,
    sender: mpsc::Sender<Message>,
    connection_ct: CancellationToken,
}

impl ConnectionComm {
    fn new(
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

struct InboundConnection {
    stream_id: u128,
    stream: TcpStream,
    pipe: Pipe<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,
    buf: BytesMut,
    connection_ct: CancellationToken,
}

impl InboundConnection {
    fn new(
        stream: TcpStream,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> (InboundConnection, Pipe<Message>) {
        let (p1, p2) = Pipe::new(1);
        let conn = InboundConnection {
            stream_id: Uuid::new_v4().as_u128(),
            stream,
            pipe: p1,
            msg_reg,
            buf: BytesMut::with_capacity(4096),
            connection_ct: CancellationToken::new(),
        };
        (conn, p2)
    }

    async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
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
                return Err(InboundConnectionPoolError::BufferReachedMaxSize.into());
            }

            tokio::select! {
                read_res = self.stream.read_buf(&mut self.buf) => {
                    match read_res {
                        Ok(size) => {
                            if size == 0 {
                                if self.buf.is_empty() {
                                    return Ok(());
                                } else {
                                    return Err(InboundConnectionPoolError::ConnectionResetByPeer.into());
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
                        return Err(InboundConnectionPoolError::ConnectionRespondedWithNoneOkResponse.into());
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
