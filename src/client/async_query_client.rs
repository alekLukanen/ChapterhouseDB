use anyhow::{Context, Result};
use bytes::BytesMut;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::handlers::message_handler::messages::message::Message;
use crate::handlers::message_handler::{messages, MessageRegistry};

#[derive(Debug, Error)]
pub enum AsyncQueryClientError {
    #[error("buffer reach max size")]
    BufferReachedMaxSize,
    #[error("connection reset by peer")]
    ConnectionResetByPeer,
    #[error("expected message but received none")]
    ExpectedMessageButReceivedNone,
    #[error("received message with incorrect request id")]
    ReceivedMessageWithIncorrectRequestId,
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

    pub async fn run_query(&self, query: String) -> Result<messages::query::RunQueryResp> {
        let (ref mut stream, connection_id) = self
            .create_connection()
            .await
            .context("connection failed")?;

        let ref mut run_query =
            messages::message::Message::new(Box::new(messages::query::RunQuery::new(query)));
        self.send_msg(stream, run_query, connection_id)
            .await
            .context("failed to send query")?;

        let query_resp: messages::query::RunQueryResp =
            self.expect_msg(stream, run_query.request_id).await?;
        Ok(query_resp)
    }

    pub async fn get_query_status(
        &self,
        ct: CancellationToken,
        query_id: &u128,
    ) -> Result<messages::query::GetQueryStatusResp> {
        let (ref mut stream, connection_id) = self
            .create_connection()
            .await
            .context("connection failed")?;

        let ref mut get_query_status = Message::new(Box::new(messages::query::GetQueryStatus {
            query_id: query_id.clone(),
        }));
        self.send_msg(stream, get_query_status, connection_id)
            .await
            .context("failed to send message")?;

        let resp: messages::query::GetQueryStatusResp =
            self.expect_msg(stream, get_query_status.request_id).await?;
        Ok(resp)
    }

    pub async fn wait_for_query_to_finish(
        &self,
        ct: CancellationToken,
        query_id: &u128,
        max_wait: chrono::Duration,
    ) -> Result<messages::query::GetQueryStatusResp> {
        let ct_task = ct.clone();
        let dur = max_wait.to_std()?;
        tokio::spawn(async move {
            tokio::select! {
                _ = ct_task.cancelled() => {
                    return;
                }
                _ = tokio::time::sleep(dur) => {
                    ct_task.cancel();
                    return;
                }
            }
        });

        loop {
            let resp = self.get_query_status(ct.clone(), query_id).await?;
            match resp {
                messages::query::GetQueryStatusResp::QueryNotFound => {
                    return Ok(resp);
                }
                messages::query::GetQueryStatusResp::Status(_) => {
                    return Ok(resp);
                }
            }
        }
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
        let ref mut identify =
            messages::message::Message::new(Box::new(messages::common::Identify::Connection {
                id: connection_id,
            }));
        self.send_msg(stream, identify, connection_id).await?;

        let _: messages::common::Identify = self
            .expect_msg(stream, identify.request_id)
            .await
            .context("failed to receive response identification from the worker")?;

        Ok(())
    }

    async fn expect_msg<T: messages::message::SendableMessage>(
        &self,
        stream: &mut TcpStream,
        request_id: u128,
    ) -> Result<T> {
        let msg = self.read_msg(stream).await?;
        match msg {
            Some(msg) => {
                if msg.request_id != request_id {
                    Err(AsyncQueryClientError::ReceivedMessageWithIncorrectRequestId.into())
                } else {
                    Ok(self.msg_reg.try_cast_msg_owned(msg)?)
                }
            }
            None => Err(AsyncQueryClientError::ExpectedMessageButReceivedNone.into()),
        }
    }

    async fn read_msg(&self, stream: &mut TcpStream) -> Result<Option<messages::message::Message>> {
        let ref mut buf = BytesMut::new();
        loop {
            if let Ok(msg) = self.msg_reg.build_msg(buf).await {
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
        msg: &mut messages::message::Message,
        connection_id: u128,
    ) -> Result<()> {
        msg.set_sent_from_connection_id(connection_id);
        stream.write_all(&msg.to_bytes()?[..]).await?;

        Ok(())
    }
}
