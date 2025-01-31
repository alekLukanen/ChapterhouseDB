use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::message::{GenericMessage, MessageName, SendableMessage};

///////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Identify {
    Worker { id: u128 },
    Connection { id: u128 },
}

impl GenericMessage for Identify {
    fn msg_name() -> MessageName {
        MessageName::Identify
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: Identify = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Ping {
    Ping,
    Pong,
}

impl GenericMessage for Ping {
    fn msg_name() -> MessageName {
        MessageName::Ping
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: Ping = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

/////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GenericResponse {
    Ok,
    Error(String),
}

impl GenericMessage for GenericResponse {
    fn msg_name() -> MessageName {
        MessageName::CommonGenericResponse
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: GenericResponse = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}
