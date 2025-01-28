use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::Message;
use anyhow::Result;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub enum HandlePingMessage {
    #[error("can not respond to a pong response")]
    CanNotResponseToAPongResponse,
}

pub fn handle_ping_message(msg: &Message, ping_msg: &messages::common::Ping) -> Result<Message> {
    match ping_msg {
        messages::common::Ping::Ping => {
            debug!("responding to ping");
            Ok(msg.reply(Box::new(messages::common::Ping::Pong)))
        }
        messages::common::Ping::Pong => {
            Err(HandlePingMessage::CanNotResponseToAPongResponse.into())
        }
    }
}
