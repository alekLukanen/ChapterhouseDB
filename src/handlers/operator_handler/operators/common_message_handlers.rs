use crate::handlers::message_handler::{Message, Ping};
use anyhow::Result;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum HandlePingMessage {
    #[error("can not respond to a pong response")]
    CanNotResponseToAPongResponse,
}

pub fn handle_ping_message(msg: &Message, ping_msg: &Ping) -> Result<Message> {
    match ping_msg {
        Ping::Ping => {
            debug!("responding to ping");
            Ok(msg.reply(Box::new(Ping::Pong)))
        }
        Ping::Pong => Err(HandlePingMessage::CanNotResponseToAPongResponse.into()),
    }
}
