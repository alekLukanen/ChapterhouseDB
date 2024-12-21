use core::str;

use anyhow::Result;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

use crate::handlers::message_handler::{Message, RunQuery};

#[derive(Debug, Error)]
pub enum AsyncQueryClientError {
    #[error("connection responded with none ok response")]
    ConnectionRespondedWithNoneOkResponse,
}

pub struct QueryTracker {
    query_id: u128,
}

#[derive(Debug)]
pub struct AsyncQueryClient {
    address: String,
}

impl AsyncQueryClient {
    pub fn new(address: String) -> AsyncQueryClient {
        AsyncQueryClient { address }
    }

    pub async fn run_query(&self, query: String) -> Result<QueryTracker> {
        let (ref mut stream, connection_id) = self.create_connection().await?;

        let ref mut run_query = Message::new(Box::new(RunQuery::new(query)));
        self.send_msg(stream, run_query, connection_id).await?;

        Ok(QueryTracker { query_id: 0u128 })
    }

    async fn create_connection(&self) -> Result<(TcpStream, u128)> {
        let stream = TcpStream::connect(self.address.clone()).await?;
        let connection_id = Uuid::new_v4().as_u128();
        Ok((stream, connection_id))
    }

    async fn identify(&self, stream: &mut TcpStream) -> Result<()> {
        let identify = Message::new()
    }

    async fn send_msg(&self, stream: &mut TcpStream, msg: &mut Message, connection_id: u128) -> Result<()> {

        msg.set_sent_from_connection_id(connection_id);

        stream.write_all(&msg.to_bytes()?[..]).await?;
        self.read_ok(stream).await?;
        Ok(())
    }

    async fn read_ok(&self, stream: &mut TcpStream) -> Result<()> {
        let mut resp = [0; 3];
        let resp_size = stream.read(&mut resp).await?;

        let resp_msg = str::from_utf8(&resp[..resp_size])?.to_string();
        if resp_msg != "OK" {
            return Err(AsyncQueryClientError::ConnectionRespondedWithNoneOkResponse.into());
        }

        Ok(())
    }
}
