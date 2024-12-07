use crate::messenger::messages::{Message, RegisteredMessage};
use anyhow::Result;
use bytes::BytesMut;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum MessageRegistryError {
    #[error("unabled to convert data to message: registerd message id is {0}")]
    UnableToConvertDataToMessage(u16),
}

pub struct MessageRegistry {
    msg_listing: Vec<RegisteredMessage>,
}

impl MessageRegistry {
    pub fn new() -> MessageRegistry {
        MessageRegistry {
            msg_listing: Vec::new(),
        }
    }

    pub fn build_msg(&self, buf: &BytesMut) -> Result<Message> {}

    pub fn build_data(&self, msg: Message) -> Result<BytesMut> {}

    pub fn add(&mut self, reg_msg: RegisteredMessage) -> &Self {
        self.msg_listing.push(reg_msg);
        self
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
