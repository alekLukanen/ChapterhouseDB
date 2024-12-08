use crate::messenger::messages::{Message, Ping, SendableMessage};
use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum MessageRegistryError {
    #[error("unabled to convert data to message: registerd message id is {0}")]
    UnableToConvertDataToMessage(u16),
}

#[derive(Debug, Clone)]
pub struct RegisteredMessage {
    id: u16,
    build_msg: fn(&BytesMut) -> Result<Box<dyn SendableMessage>>,
}

impl RegisteredMessage {
    pub fn get_id(&self) -> u16 {
        self.id.clone()
    }
}

#[derive(Debug, Clone)]
pub struct MessageRegistry {
    msg_listing: Vec<RegisteredMessage>,
    msg_idx: u16,
}

impl MessageRegistry {
    pub fn new() -> MessageRegistry {
        let mut reg = MessageRegistry {
            msg_listing: Vec::new(),
            msg_idx: 0,
        };
        reg.register_messages();
        reg
    }

    fn register_messages(&mut self) {
        self.add(Ping::build_msg);
    }

    pub fn build_msg(&self, buf: &BytesMut) -> Result<Option<Message>> {}

    pub fn build_data(&self, msg: Message) -> Result<BytesMut> {}

    pub fn add(&mut self, build_msg: fn(&BytesMut) -> Result<Box<dyn SendableMessage>>) {
        self.msg_listing.push(RegisteredMessage {
            id: self.msg_idx,
            build_msg,
        });
    }

    fn find(&self, reg_msg_id: u16) -> Option<RegisteredMessage> {
        if let Some(reg_msg) = self
            .msg_listing
            .iter()
            .find(|&item| item.get_id() == reg_msg_id)
        {
            Some(reg_msg.clone())
        } else {
            None
        }
    }
}
