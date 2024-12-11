use crate::handlers::message_handler::{Message, Ping, SendableMessage};
use anyhow::Result;
use core::str;
use std::{
    io::{Read, Write},
    net::TcpStream,
};
use tracing::info;
use uuid::Uuid;

pub struct QueryClient {
    client_id: u128,
    address: String,
    stream: TcpStream,
}

impl QueryClient {
    pub fn new(address: String) -> Result<QueryClient> {
        Ok(QueryClient {
            client_id: Uuid::new_v4().as_u128(),
            address: address.clone(),
            stream: TcpStream::connect(address)?,
        })
    }

    pub fn new_msg(&self, msg: Box<dyn SendableMessage>) -> Message {
        Message::new(msg).set_sent_from_client_id(self.client_id.clone())
    }

    pub fn get_client_id(&self) -> u128 {
        self.client_id
    }

    pub fn send_ping_message(&mut self, count: u8) -> Result<()> {
        info!("pinging address: {}", self.address);

        for _ in 0..count {
            info!("sending a message...");
            let ping = self.new_msg(Box::new(Ping::new("Hello".to_string())));
            let ping_data = ping.to_bytes()?;

            info!("ping_data.len(): {}", ping_data.len());

            self.stream.write(&ping_data[..])?;

            let ref mut read_buf = [0u8; 128];
            self.stream.read(&mut read_buf[..])?;

            let resp_msg = str::from_utf8(read_buf)?.to_string();
            info!("ping response: {}", resp_msg);
        }

        Ok(())
    }
}
