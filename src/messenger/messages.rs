use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
}

pub struct Message {
    reg_msg_id: u128,
    msg: Box<dyn SendableMessage>,

    // routing
    worker_id: Option<u128>,
    pipeline_id: Option<u128>,
    operation_id: Option<u128>,
}

impl Message {
    pub fn new(
        msg: Box<dyn SendableMessage>,
        worker_id: Option<u128>,
        pipeline_id: Option<u128>,
        operation_id: Option<u128>,
    ) -> Message {
        Message {
            reg_msg_id: Uuid::new_v4().as_u128(),
            msg,
            worker_id,
            pipeline_id,
            operation_id,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let msg_data: Vec<u8> = self.msg.to_bytes()?;
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
