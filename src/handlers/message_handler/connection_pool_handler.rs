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
    worker_id: u128,
    address: String,
    connect_to_addresses: Vec<String>,

    msg_reg: Arc<Box<MessageRegistry>>,
    pipe: Pipe<Message>,

    inbound_connections: Arc<Mutex<Vec<ConnectionComm>>>,
    outbound_connections: Arc<Mutex<Vec<ConnectionComm>>>,
}

impl ConnectionPoolHandler {
    pub fn new(
        worker_id: u128,
        address: String,
        connect_to_addresses: Vec<String>,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> (ConnectionPoolHandler, Pipe<Message>) {
        let (p1, p2) = Pipe::new(1);
        let hndlr = ConnectionPoolHandler {
            worker_id,
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
                if let Err(err) =
                    Self::connect_to_address(ct, stream_connect_tx, cta, 12 * 5, 1).await
                {
                    info!("error: {}", err);
                }
            });
        }

        // TODO: handle connections that close; need reconnect
        loop {
            tokio::select! {
                // connection handling
                res = listener.accept() => {
                    match res {
                        Ok((socket, _)) => {
                            let (mut connection, connection_comm) =
                                Connection::new(self.worker_id.clone(), socket, connection_tx.clone(), Arc::clone(&self.msg_reg), true);
                            self.inbound_connections.lock().await.push(connection_comm);

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
                Some(new_tcpstream_connection) = stream_connect_rx.recv() => {
                    let (mut connection, connection_comm) = Connection::new(self.worker_id, new_tcpstream_connection, connection_tx.clone(), Arc::clone(&self.msg_reg), false);
                    connection.set_send_identification();
                    self.outbound_connections.lock().await.push(connection_comm);

                    // Spawn a new task to handle the connection
                    let ct2 = ct.clone();
                    tt.spawn(async move {
                        if let Err(err) = connection.async_main(ct2).await {
                            info!("error reading from tcp socket: {}", err);
                        }
                        connection.cleanup();
                    });
                }
                // message routing
                Some(msg) = connection_rx.recv() => {
                    if let Err(err) = self.pipe.send(msg).await {
                        info!("error: {}", err);
                        info!("error on receive");
                    }
                }
                Some(msg) = self.pipe.recv() => {
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
                    } else {
                        info!("inbound or outbound stream id was not set");
                        info!("message: {:?}", msg);
                    }
                }
                // handle cancellationg token
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
                            break;
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
