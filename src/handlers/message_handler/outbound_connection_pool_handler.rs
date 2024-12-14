use core::str;
use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use super::{message_registry::MessageRegistry, Message};

#[derive(Error, Debug)]
pub enum ConnectionPoolError {
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
    #[error("connection responded with none ok response")]
    ConnectionRespondedWithNoneOkResponse,
    #[error("timed out waiting for connections to close")]
    TimedOutWaitingForConnectionsToClose,
}

pub struct OutboundConnectionPoolComm {
    sender: mpsc::Sender<Message>,
}

impl OutboundConnectionPoolComm {
    fn new(sender: mpsc::Sender<Message>) -> OutboundConnectionPoolComm {
        OutboundConnectionPoolComm { sender }
    }
    pub async fn send(&self, msg: Message) -> Result<()> {
        self.sender.send(msg).await?;
        Ok(())
    }
}

pub struct OutboundConnectionPoolHandler {
    connect_to_addresses: Vec<String>,
    outbound_connections: Arc<Mutex<Vec<OutboundConnection>>>,
    inbound_sender: mpsc::Sender<Message>,
    outbound_receiver: mpsc::Receiver<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,
}

impl OutboundConnectionPoolHandler {
    pub fn new(
        connect_to_addresses: Vec<String>,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> OutboundConnectionPoolHandler {
        let (send, recv) = mpsc::channel(1);
        OutboundConnectionPoolHandler {
            connect_to_addresses,
            outbound_connections: Arc::new(Mutex::new(Vec::new())),
            inbound_sender: send,
            outbound_receiver: recv,
            msg_reg,
        }
    }

    pub fn comm(&self) -> OutboundConnectionPoolComm {
        OutboundConnectionPoolComm::new(self.inbound_sender.clone())
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        let tt = TaskTracker::new();

        // make outbound connections
        // TODO: handle connections that close; need reconnect
        let mut outbound_connection_comms: Vec<OutboundConnectionComm> = Vec::new();
        for address in &self.connect_to_addresses {
            let mut connection =
                OutboundConnection::new(address.clone(), Arc::clone(&self.msg_reg));
            let comm = connection.comm();
            let ct = ct.clone();
            tt.spawn(async move {
                if let Err(err) = connection.async_main(ct).await {
                    info!("error: {}", err);
                }
            });
            outbound_connection_comms.push(comm);
        }

        loop {
            tokio::select! {
                Some(msg) = self.outbound_receiver.recv() => {
                    info!("message: {:?}", msg);
                    for comm in &outbound_connection_comms {
                        // TODO: add message routing based on address; the message router handler
                        // will know about addresses and relating them to workers. It will include
                        // the address on the message. For now send and the other worker will
                        // ignore the message.
                        if let Err(err) = comm.msg_sender.send(msg.clone()).await {
                            // if there is an error close the connection
                            info!("error: {}", err);
                            comm.connection_ct.cancel();
                        };
                    }
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        info!("connection pool closing...");

        tt.close();
        tokio::select! {
            _ = tt.wait() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                return Err(ConnectionPoolError::TimedOutWaitingForConnectionsToClose.into());
            }
        }

        Ok(())
    }
}

struct OutboundConnectionComm {
    address: String,
    msg_sender: mpsc::Sender<Message>,
    connection_ct: CancellationToken,
}

struct OutboundConnection {
    address: String,
    msg_receiver: mpsc::Receiver<Message>,
    msg_sender: mpsc::Sender<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,

    connection_ct: CancellationToken,
}

impl OutboundConnection {
    fn new(address: String, msg_reg: Arc<Box<MessageRegistry>>) -> OutboundConnection {
        let (msg_sender, msg_receiver) = mpsc::channel(1);
        OutboundConnection {
            address,
            msg_receiver,
            msg_sender,
            msg_reg,
            connection_ct: CancellationToken::new(),
        }
    }

    fn comm(&self) -> OutboundConnectionComm {
        OutboundConnectionComm {
            address: self.address.clone(),
            msg_sender: self.msg_sender.clone(),
            connection_ct: self.connection_ct.clone(),
        }
    }

    async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        let mut stream = TcpStream::connect(self.address.clone()).await?;

        loop {
            tokio::select! {
                Some(msg) = self.msg_receiver.recv() => {
                    let msg_bytes = msg.to_bytes()?;
                    stream.write_all(&msg_bytes[..]).await?;

                    let mut resp = [0; 3];
                    let resp_size = stream.read(&mut resp).await?;

                    let resp_msg = str::from_utf8(&resp[..resp_size])?.to_string();
                    if resp_msg != "OK" {
                        return Err(ConnectionPoolError::ConnectionRespondedWithNoneOkResponse.into());
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

        info!("closing outbound connection to {}", self.address);

        Ok(())
    }
}
