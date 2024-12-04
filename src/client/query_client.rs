use anyhow::Result;
use core::str;
use std::{
    io::{Read, Write},
    net::TcpStream,
};
use tracing::info;

pub struct QueryClient {
    address: String,
    stream: TcpStream,
}

impl QueryClient {
    pub fn new(address: String) -> Result<QueryClient> {
        Ok(QueryClient {
            address: address.clone(),
            stream: TcpStream::connect(address)?,
        })
    }

    pub fn send_ping_message(&mut self) -> Result<()> {
        info!("pinging address: {}", self.address);

        self.stream.write("ping!".as_bytes())?;

        let ref mut read_buf = [0u8; 128];
        self.stream.read(&mut read_buf[..])?;

        let resp_msg = str::from_utf8(read_buf)?.to_string();
        info!("ping response: {}", resp_msg);

        Ok(())
    }
}
