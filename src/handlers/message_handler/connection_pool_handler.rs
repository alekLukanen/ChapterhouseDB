use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use super::connection::{Connection, ConnectionComm};
use super::message_registry::MessageRegistry;
use super::messages::Message;
use super::Pipe;

#[derive(Error, Debug)]
pub enum ConnectionPoolError {
    #[error("timed out waiting for connections to close")]
    TimedOutWaitingForConnectionsToClose,
    #[error("timed out waiting for connection to worker {0}")]
    TimedOutWaitingForNewConnectionToWorker(String),
}

pub struct ConnectionPoolHandler {
    address: String,
    connect_to_addresses: Vec<String>,

    msg_reg: Arc<Box<MessageRegistry>>,
    pipe: Pipe<Message>,

    inbound_connections: Arc<Mutex<Vec<ConnectionComm>>>,
    outbound_connections: Arc<Mutex<Vec<ConnectionComm>>>,
}

impl ConnectionPoolHandler {
    pub fn new(
        address: String,
        connect_to_addresses: Vec<String>,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> (ConnectionPoolHandler, Pipe<Message>) {
        let (p1, p2) = Pipe::new(1);
        let hndlr = ConnectionPoolHandler {
            address,
            connect_to_addresses,
            msg_reg,
            pipe: p1,
            inbound_connections: Arc::new(Mutex::new(Vec::new())),
            outbound_connections: Arc::new(Mutex::new(Vec::new())),
        };
        (hndlr, p2)
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        info!("Starting Messenger...");

        let tt = TaskTracker::new();
        let listener = TcpListener::bind(&self.address).await?;

        let (connection_tx, mut connection_rx) = mpsc::channel::<Message>(1);
        let (stream_connect_tx, mut stream_connect_rx) = mpsc::channel::<TcpStream>(1);
        info!("Messenger listening on {}", self.address);

        info!("Attempting to connect to addresses");
        for cta in &self.connect_to_addresses {
            let ct = ct.clone();
            let stream_connect_tx = stream_connect_tx.clone();
            let cta = cta.clone();
            tt.spawn(async move {
                if let Err(err) = Self::connect_to_address(ct, stream_connect_tx, cta, 3, 1).await {
                    info!("error: {}", err);
                }
            });
        }

        // TODO: handle connections that close; need reconnect
        for address in &self.connect_to_addresses {
            let stream = if let Ok(stream) = TcpStream::connect(address.clone()).await {
                stream
            } else {
                continue;
            };
        }

        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((socket, _)) => {
                            let (mut connection, pipe) =
                                Connection::new(socket, Arc::clone(&self.msg_reg));

                            let (conn_send, conn_recv) = pipe.split();
                            let conn_comm = ConnectionComm::new(connection.connection_ct.clone(), connection.stream_id, conn_send);
                            self.inbound_connections.lock().await.push(conn_comm);

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
                        for comm in self.inbound_connections.lock().await.iter() {
                            if comm.stream_id != inbound_stream_id {
                                continue;
                            }
                            if let Err(err) = comm.sender.send(msg.clone()).await {
                                // if there is an error close the connection
                                info!("error: {}", err);
                                comm.connection_ct.cancel();
                            };
                        }
                    } else if let Some(outbound_stream_id) = msg.outbound_stream_id {
                        for comm in self.outbound_connections.lock().await.iter() {
                            if comm.stream_id != outbound_stream_id {
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
                Some(new_tcpstream_connection) = stream_connect_rx.recv() => {
                    let (mut connection, pipe) = Connection::new(new_tcpstream_connection, Arc::clone(&self.msg_reg));

                    let (conn_send, conn_recv) = pipe.split();
                    let conn_comm = ConnectionComm::new(
                        connection.connection_ct.clone(),
                        connection.stream_id,
                        conn_send,
                    );
                    self.outbound_connections.lock().await.push(conn_comm);

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
                return Err(ConnectionPoolError::TimedOutWaitingForConnectionsToClose.into());
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

    async fn connect_to_address(
        ct: CancellationToken,
        sender: Sender<TcpStream>,
        address: String,
        max_retries: u32,
        sleep_time: u64,
    ) -> Result<()> {
        let mut try_count = 0u32;
        loop {
            if try_count > max_retries {
                return Err(
                    ConnectionPoolError::TimedOutWaitingForNewConnectionToWorker(address.clone())
                        .into(),
                );
            }
            try_count += 1;

            tokio::select! {
                stream_resp = TcpStream::connect(address.clone()) => {
                    match stream_resp {
                        Ok(stream) => {
                            sender.send(stream).await?;
                        }
                        Err(err) => {
                            info!("error: {}", err);
                            tokio::time::sleep(std::time::Duration::from_secs(sleep_time)).await;
                        }
                    }
                },
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }
}
