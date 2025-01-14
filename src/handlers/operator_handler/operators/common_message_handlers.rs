use anyhow::Result;
use thiserror::Error;

use crate::handlers::message_handler::{Message, Ping};

#[derive(Debug, Error)]
pub enum HandlePingMessage {
    #[error("can not respond to a pong response")]
    CanNotResponseToAPongResponse,
}

pub fn handle_ping_Message(msg: &Message, ping_msg: &Ping) -> Result<Message> {
    match ping_msg {
        Ping::Ping => Ok(msg.reply(Box::new(Ping::Pong))),
        Ping::Pong => Err(HandlePingMessage::CanNotResponseToAPongResponse.into()),
    }
}
