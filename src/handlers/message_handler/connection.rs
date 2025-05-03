use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error};
use uuid::Uuid;

use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::{Message, SerializedMessageError};

use super::message_registry::MessageRegistry;
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
    stream: Option<TcpStream>,
    pipe: Option<Pipe>,
    msg_reg: Arc<MessageRegistry>,
    pub connection_ct: CancellationToken,
    send_identification_msg: bool,
    is_inbound: bool,

    tt: TaskTracker,
}

impl Connection {
    pub fn new(
        ct: CancellationToken,
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
            stream: Some(stream),
            pipe: Some(pipe),
            msg_reg,
            connection_ct: ct.child_token(),
            send_identification_msg: false,
            is_inbound,
            tt: TaskTracker::new(),
        };
        let comm = ConnectionComm::new(conn.connection_ct.clone(), conn.stream_id, sender_to_conn);
        (conn, comm)
    }

    pub fn set_send_identification(&mut self) -> &Self {
        self.send_identification_msg = true;
        self
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        let pipe = self.pipe.take().expect("pipe");
        let (sender, receiver) = pipe.split();

        let stream = self.stream.take().expect("stream");
        let ip = stream.peer_addr()?.to_string();
        let (read_stream, write_stream) = tokio::io::split(stream);

        debug!(stream_id = self.stream_id, ip = ip, "new connection",);

        let read_ct = ct.clone();
        let mut connection_reader = ConnectionReader::new(
            self.stream_id.clone(),
            read_stream,
            sender,
            self.msg_reg.clone(),
            self.is_inbound.clone(),
        );

        let write_ct = ct.clone();
        let mut connection_writer = ConnectionWriter::new(
            self.worker_id.clone(),
            self.stream_id.clone(),
            write_stream,
            receiver,
            self.msg_reg.clone(),
            self.send_identification_msg,
        );

        self.tt.spawn(async move {
            if let Err(err) = connection_reader.async_main(read_ct).await {
                error!("{}", err);
            }
        });
        self.tt.spawn(async move {
            if let Err(err) = connection_writer.async_main(write_ct).await {
                error!("{}", err);
            }
        });

        self.tt.close();
        self.tt.wait().await;

        debug!("read and write closed for connection");

        Ok(())
    }

    pub fn cleanup(&self) {
        self.connection_ct.cancel();
    }
}

////////////////////////////////////////////////////////////
//

struct ConnectionWriter {
    worker_id: u128,
    stream_id: u128,
    stream: tokio::io::WriteHalf<TcpStream>,
    receiver: mpsc::Receiver<Message>,
    msg_reg: Arc<MessageRegistry>,
    send_identification_msg: bool,
}

impl ConnectionWriter {
    fn new(
        worker_id: u128,
        stream_id: u128,
        stream: tokio::io::WriteHalf<TcpStream>,
        receiver: mpsc::Receiver<Message>,
        msg_reg: Arc<MessageRegistry>,
        send_identification_msg: bool,
    ) -> ConnectionWriter {
        ConnectionWriter {
            worker_id,
            stream_id,
            stream,
            receiver,
            msg_reg,
            send_identification_msg,
        }
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        debug!(stream_id = self.stream_id, "new write connection",);

        let res = self.async_main_inner(ct.clone()).await;
        ct.cancel();

        debug!("closed write connection");

        res
    }

    pub async fn async_main_inner(&mut self, ct: CancellationToken) -> Result<()> {
        if self.send_identification_msg {
            let identity_msg = Message::new(Box::new(messages::common::Identify::Worker {
                id: self.worker_id.clone(),
            }))
            .set_sent_from_worker_id(self.worker_id.clone());
            self.stream.write_all(&identity_msg.to_bytes()?[..]).await?;
        }

        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    debug!("sending message to connection");
                    let msg_bytes = self.msg_reg.build_msg_bytes(msg).await?;
                    self.stream.write_all(&msg_bytes[..]).await?;
                },
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        debug!("closing write connection...");
        tokio::select! {
            _ = self.stream.shutdown() => {}
            _ = tokio::time::sleep(chrono::Duration::seconds(5).to_std()?) => {
                return Err(ConnectionError::TimedOutWaitingForConnectionsToClose.into());
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////
//

struct ConnectionReader {
    stream_id: u128,
    stream: tokio::io::ReadHalf<TcpStream>,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
    buf: BytesMut,
    is_inbound: bool,
}

impl ConnectionReader {
    fn new(
        stream_id: u128,
        stream: tokio::io::ReadHalf<TcpStream>,
        sender: mpsc::Sender<Message>,
        msg_reg: Arc<MessageRegistry>,
        is_inbound: bool,
    ) -> ConnectionReader {
        ConnectionReader {
            stream_id,
            stream,
            sender,
            msg_reg,
            buf: BytesMut::with_capacity(4096),
            is_inbound,
        }
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        debug!(stream_id = self.stream_id, "new read connection",);

        let res = self.async_main_inner(ct.clone()).await;
        ct.cancel();

        debug!("closed read connection");

        res
    }

    pub async fn async_main_inner(&mut self, ct: CancellationToken) -> Result<()> {
        loop {
            match self.msg_reg.build_msg(&mut self.buf).await {
                Ok(msg) => {
                    if let Some(mut msg) = msg {
                        if self.is_inbound {
                            msg = msg.set_inbound_stream_id(self.stream_id);
                        } else {
                            msg = msg.set_outbound_stream(self.stream_id);
                        }
                        debug!("received message from connection");
                        self.sender.send(msg).await?;
                    }
                    continue;
                }
                Err(err) => {
                    let ser_msg_err = err.downcast_ref::<SerializedMessageError>();
                    match ser_msg_err {
                        Some(SerializedMessageError::Incomplete) => (),
                        _ => {
                            error!("{:?}", err);
                        }
                    }
                }
            }

            // end the conneciton if the other system has sent too much data
            if self.buf.len() > 1024 * 1024 * 500 {
                return Err(ConnectionError::BufferReachedMaxSize.into());
            }

            tokio::select! {
                read_res = self.stream.read_buf(&mut self.buf) => {
                    match read_res {
                        Ok(size) => {
                            if size == 0 {
                                if self.buf.is_empty() {
                                    break;
                                } else {
                                    return Err(ConnectionError::ConnectionResetByPeer.into());
                                }
                            }
                        },
                        Err(err) => {
                            return Err(err.into())
                        },
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
