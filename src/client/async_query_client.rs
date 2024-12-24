use anyhow::{Context, Result};
use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

use crate::handlers::message_handler::{
    Identify, Message, MessageRegistry, RunQuery, RunQueryResp, SendableMessage,
};

#[derive(Debug, Error)]
pub enum AsyncQueryClientError {
    #[error("buffer reach max size")]
    BufferReachedMaxSize,
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
    #[error("expected message but received none")]
    ExpectedMessageButReceivedNone,
}

#[derive(Debug)]
pub struct AsyncQueryClient {
    address: String,
    msg_reg: MessageRegistry,
}

impl AsyncQueryClient {
    pub fn new(address: String) -> AsyncQueryClient {
        AsyncQueryClient {
            address,
            msg_reg: MessageRegistry::new(),
        }
    }

    pub async fn run_query(&self, query: String) -> Result<RunQueryResp> {
        let (ref mut stream, connection_id) = self
            .create_connection()
            .await
            .context("connection failed")?;

        let ref mut run_query = Message::new(Box::new(RunQuery::new(query)));
        self.send_msg(stream, run_query, connection_id)
            .await
            .context("failed to send query")?;

        let query_resp: RunQueryResp = self.expect_msg(stream).await?;
        Ok(query_resp)
    }

    async fn create_connection(&self) -> Result<(TcpStream, u128)> {
        let mut stream = TcpStream::connect(self.address.clone()).await?;
        let connection_id = Uuid::new_v4().as_u128();
        self.identify(&mut stream, connection_id.clone())
            .await
            .context("failed to identify with the worker")?;
        Ok((stream, connection_id))
    }

    async fn identify(&self, stream: &mut TcpStream, connection_id: u128) -> Result<()> {
        let ref mut identify = Message::new(Box::new(Identify::Connection { id: connection_id }));
        self.send_msg(stream, identify, connection_id).await?;

        let _: Identify = self
            .expect_msg(stream)
            .await
            .context("failed to receive response identification from the worker")?;

        Ok(())
    }

    async fn expect_msg<T: SendableMessage>(&self, stream: &mut TcpStream) -> Result<T> {
        let msg = self.read_msg(stream).await?;
        match msg {
            Some(msg) => Ok(self.msg_reg.try_cast_msg_owned(msg)?),
            None => Err(AsyncQueryClientError::ExpectedMessageButReceivedNone.into()),
        }
    }

    async fn read_msg(&self, stream: &mut TcpStream) -> Result<Option<Message>> {
        let ref mut buf = BytesMut::new();
        loop {
            if let Ok(msg) = self.msg_reg.build_msg(buf) {
                if let Some(msg) = msg {
                    return Ok(Some(msg));
                }
                continue;
            }

            // end the conneciton if the other system has sent too much data
            if buf.len() > 1024 * 1024 * 10 {
                return Err(AsyncQueryClientError::BufferReachedMaxSize.into());
            }

            match stream.read_buf(buf).await {
                Ok(size) => {
                    if size == 0 {
                        if buf.is_empty() {
                            return Ok(None);
                        } else {
                            return Err(AsyncQueryClientError::ConnectionResetByPeer.into());
                        }
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    async fn send_msg(
        &self,
        stream: &mut TcpStream,
        msg: &mut Message,
        connection_id: u128,
    ) -> Result<()> {
        msg.set_sent_from_connection_id(connection_id);
        stream.write_all(&msg.to_bytes()?[..]).await?;

        Ok(())
    }
}
