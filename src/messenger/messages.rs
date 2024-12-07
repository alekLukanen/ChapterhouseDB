use anyhow::Result;
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const HEADER_VERSION: u16 = 0;

trait SendableMessage {
    fn to_bytes(&self) -> Result<Vec<u8>>;
}

pub struct SerializedMessage {
    header_len: u32,
    header_version: u16,
    data_len: u64,
    reg_msg_id: u16,
    msg_id: u128,
    // determines which of the routing values
    // have been set
    // 0 - none (bits 0)
    // 1 - worker_id set (bit 0)
    // 2 - pipeline_id set (bit 1)
    // 4 - operation_id set (bit 2)
    routing_flags: u8,
    worker_id: u128,
    pipeline_id: u128,
    operation_id: u128,

    // the actual user-space message
    msg_data: Vec<u8>,
}

impl SerializedMessage {
    fn new(msg: &Message) -> Result<SerializedMessage> {
        let msg_data = msg.msg.to_bytes()?;

        let data_len: u64 = msg_data.len() as u64;
        let reg_msg_id = msg.reg_msg_id;
        let msg_id = msg.msg_id;
        let mut routing_flags: u8 = 0;

        let mut worker_id: u128 = 0;
        if let Some(_id) = msg.worker_id {
            worker_id = _id;
            routing_flags = routing_flags | 1;
        }

        let mut pipeline_id: u128 = 0;
        if let Some(_id) = msg.pipeline_id {
            pipeline_id = _id;
            routing_flags = routing_flags | (1 << 1);
        }

        let mut operation_id: u128 = 0;
        if let Some(_id) = msg.operation_id {
            operation_id = _id;
            routing_flags = routing_flags | (1 << 2);
        }

        let ser_msg = SerializedMessage {
            header_len: 16 + 64 + 16 + 128 + 8 + 128 + 128 + 128,
            header_version: HEADER_VERSION,
            data_len,
            reg_msg_id,
            msg_id,
            routing_flags,
            worker_id,
            pipeline_id,
            operation_id,
            msg_data,
        };
        Ok(ser_msg)
    }

    fn parse_from_bytes(data: &BytesMut) -> Result<SerializedMessage> {}

    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(
            32 + 16 + 64 + 16 + 128 + 8 + 128 + 128 + 128 + self.msg_data.len(),
        );

        buf.put_u32(self.header_len);
        buf.put_u16(self.header_version);
        buf.put_u64(self.data_len);
        buf.put_u16(self.reg_msg_id);
        buf.put_u128(self.msg_id);
        buf.put_u8(self.routing_flags);
        buf.put_u128(self.worker_id);
        buf.put_u128(self.pipeline_id);
        buf.put_u128(self.operation_id);
        buf.put(&self.msg_data[..]);

        buf.to_vec()
    }
}

pub struct Message {
    reg_msg_id: u16,
    msg_id: u128,
    msg: Box<dyn SendableMessage>,

    // routing
    worker_id: Option<u128>,
    pipeline_id: Option<u128>,
    operation_id: Option<u128>,
}

impl Message {
    pub fn new(
        reg_msg_id: u16,
        msg: Box<dyn SendableMessage>,
        worker_id: Option<u128>,
        pipeline_id: Option<u128>,
        operation_id: Option<u128>,
    ) -> Message {
        Message {
            reg_msg_id,
            msg_id: Uuid::new_v4().as_u128(),
            msg,
            worker_id,
            pipeline_id,
            operation_id,
        }
    }

    pub fn to_bytes(&self) -> Result<SerializedMessage> {
        Ok(SerializedMessage::new(&self)?)
    }
}

#[derive(Debug, Clone)]
pub struct RegisteredMessage {
    id: u16,
    build_msg: fn(&Vec<u8>) -> Result<Box<dyn SendableMessage>>,
}

impl RegisteredMessage {
    pub fn get_id(&self) -> u16 {
        self.id.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ping {
    msg: String,
}

impl Ping {
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let ping_msg: Ping = serde_json::from_slice(data)?;
        Ok(Box::new(ping_msg))
    }
}

impl SendableMessage for Ping {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
}
