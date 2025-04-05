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
    #[error("token cancelled")]
    TokenCancelled,
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

    pub async fn run_query(
        &self,
        ct: CancellationToken,
        query: String,
    ) -> Result<messages::query::RunQueryResp> {
        let (ref mut stream, connection_id) = self
            .create_connection(ct.clone())
            .await
            .context("connection failed")?;

        let ref mut run_query =
            messages::message::Message::new(Box::new(messages::query::RunQuery::new(query)));
        self.send_msg(ct.clone(), stream, run_query, connection_id)
            .await
            .context("failed to send query")?;

        let query_resp: messages::query::RunQueryResp = self
            .expect_msg(ct.clone(), stream, run_query.request_id)
            .await?;
        Ok(query_resp)
    }

    pub async fn get_query_status(
        &self,
        ct: CancellationToken,
        query_id: &u128,
    ) -> Result<messages::query::GetQueryStatusResp> {
        let (ref mut stream, connection_id) = self
            .create_connection(ct.clone())
            .await
            .context("connection failed")?;

        let ref mut get_query_status = Message::new(Box::new(messages::query::GetQueryStatus {
            query_id: query_id.clone(),
        }));
        self.send_msg(ct.clone(), stream, get_query_status, connection_id)
            .await
            .context("failed to send message")?;

        let resp: messages::query::GetQueryStatusResp = self
            .expect_msg(ct.clone(), stream, get_query_status.request_id)
            .await?;
        Ok(resp)
    }

    pub async fn wait_for_query_to_finish(
        &self,
        ct: CancellationToken,
        query_id: &u128,
        max_wait: chrono::Duration,
        poll_interval: chrono::Duration,
    ) -> Result<messages::query::GetQueryStatusResp> {
        let ct_task = CancellationToken::new();
        let poll_interval = poll_interval.to_std()?;

        Self::cancel_wait(ct.clone(), ct_task.clone(), max_wait)?;

        loop {
            let resp = self.get_query_status(ct_task.clone(), query_id).await?;
            match &resp {
                messages::query::GetQueryStatusResp::QueryNotFound => {
                    ct_task.cancel();
                    return Ok(resp);
                }
                messages::query::GetQueryStatusResp::Status(status) => {
                    if !status.terminal() {
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                    ct_task.cancel();
                    return Ok(resp);
                }
            }
        }
    }

    pub async fn get_query_data(
        &self,
        ct: CancellationToken,
        query_id: &u128,
        file_idx: u64,
        file_row_group_idx: u64,
        row_idx: u64,
        limit: u64,
        forward: bool,
        allow_overflow: bool,
        max_wait: chrono::Duration,
    ) -> Result<messages::query::GetQueryDataResp> {
        let ct_task = CancellationToken::new();
        Self::cancel_wait(ct.clone(), ct_task.clone(), max_wait)?;

        let (ref mut stream, connection_id) = self
            .create_connection(ct_task.clone())
            .await
            .context("connection failed")?;

        let ref mut msg = Message::new(Box::new(messages::query::GetQueryData {
            query_id: query_id.clone(),
            file_idx,
            file_row_group_idx,
            row_idx,
            limit,
            forward,
            allow_overflow,
        }));

        self.send_msg(ct_task.clone(), stream, msg, connection_id)
            .await
            .context("failed to send get query data message")?;

        let resp: messages::query::GetQueryDataResp = self
            .expect_msg(ct_task.clone(), stream, msg.request_id)
            .await?;
        Ok(resp)
    }

    fn cancel_wait(
        ct: CancellationToken,
        ct_task: CancellationToken,
        max_wait: chrono::Duration,
    ) -> Result<()> {
        let dur = max_wait.to_std()?;
        tokio::spawn(async move {
            tokio::select! {
                _ = ct_task.cancelled() => {}
                _ = ct.cancelled() => {}
                _ = tokio::time::sleep(dur) => {}
            }
            ct_task.cancel();
            return;
        });
        Ok(())
    }

    async fn create_connection(&self, ct: CancellationToken) -> Result<(TcpStream, u128)> {
        let mut stream = TcpStream::connect(self.address.clone()).await?;
        let connection_id = Uuid::new_v4().as_u128();
        self.identify(ct.clone(), &mut stream, connection_id.clone())
            .await
            .context("failed to identify with the worker")?;
        Ok((stream, connection_id))
    }

    async fn identify(
        &self,
        ct: CancellationToken,
        stream: &mut TcpStream,
        connection_id: u128,
    ) -> Result<()> {
        let ref mut identify =
            messages::message::Message::new(Box::new(messages::common::Identify::Connection {
                id: connection_id,
            }));
        self.send_msg(ct.clone(), stream, identify, connection_id)
            .await?;

        let _: messages::common::Identify = self
            .expect_msg(ct, stream, identify.request_id)
            .await
            .context("failed to receive response identification from the worker")?;

        Ok(())
    }

    async fn expect_msg<T: messages::message::SendableMessage>(
        &self,
        ct: CancellationToken,
        stream: &mut TcpStream,
        request_id: u128,
    ) -> Result<T> {
        tokio::select! {
            msg = self.read_msg(stream) => {
                match msg {
                    Ok(Some(msg)) => {
                        if msg.request_id != request_id {
                            Err(AsyncQueryClientError::ReceivedMessageWithIncorrectRequestId.into())
                        } else {
                            Ok(self.msg_reg.try_cast_msg_owned(msg)?)
                        }
                    }
                    Ok(None) => Err(AsyncQueryClientError::ExpectedMessageButReceivedNone.into()),
                    Err(err) => Err(err),
                }
            }
            _ = ct.cancelled() => {
                Err(AsyncQueryClientError::TokenCancelled.into())
            }
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
            // 1 Gib
            if buf.len() > 1024 * 1024 * 1024 {
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
        ct: CancellationToken,
        stream: &mut TcpStream,
        msg: &mut messages::message::Message,
        connection_id: u128,
    ) -> Result<()> {
        msg.set_sent_from_connection_id(connection_id);
        let data = msg.to_bytes()?;
        tokio::select! {
            res = stream.write_all(&data[..]) => {
                res?;
                Ok(())
            }
            _ = ct.cancelled() => {
                Err(AsyncQueryClientError::TokenCancelled.into())
            }
        }
    }
}
