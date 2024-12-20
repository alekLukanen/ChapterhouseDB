use anyhow::Result;
use core::fmt;

use crate::handlers::message_handler::{Message, Pipe};

pub trait Subscriber: fmt::Debug + Send + Sync {
    fn consumes_message(&self, msg: &Message) -> bool;
}

pub trait SubscriberSend: fmt::Debug + Send + Sync {
    fn send(&self, msg: &Message) -> Result<()>;
}

#[derive(Debug)]
pub struct InternalSubscriber {
    sub: Box<dyn Subscriber>,
    pipe: Pipe<Message>,
}

impl InternalSubscriber {
    pub fn new(sub: Box<dyn Subscriber>, pipe: Pipe<Message>) -> InternalSubscriber {
        InternalSubscriber { sub, pipe }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExternalSubscriber {
    InboundClientConnection {
        connection_id: u128,
        inbound_stream_id: u128,
    },
    OutboundWorker {
        worker_id: u128,
        outbound_stream_id: u128,
    },
}

impl Subscriber for ExternalSubscriber {
    fn consumes_message(&self, msg: &Message) -> bool {
        match self {
            Self::InboundClientConnection { connection_id, .. } => match msg.route_to_connection_id
            {
                Some(rid) => *connection_id == rid,
                _ => false,
            },
            Self::OutboundWorker { worker_id, .. } => {
                match (msg.route_to_worker_id, msg.route_to_connection_id) {
                    (Some(w_rid), None) => *worker_id == w_rid,
                    (None, None) => true,
                    _ => false,
                }
            }
        }
    }
}
