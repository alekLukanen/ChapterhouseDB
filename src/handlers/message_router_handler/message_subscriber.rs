use core::fmt;
use tokio::sync::mpsc::{self, Sender};

use crate::handlers::message_handler::messages::message::Message;

pub trait MessageConsumer: fmt::Debug + Send + Sync {
    fn consumes_message(&self, msg: &Message) -> bool;
}

pub trait MessageReceiver: fmt::Debug + Send + Sync {
    fn sender(&self) -> mpsc::Sender<Message>;
}

pub trait Subscriber: MessageConsumer + MessageReceiver {}

#[derive(Debug)]
pub struct InternalSubscriber {
    pub sub: Box<dyn Subscriber>,
    pub sender: Sender<Message>,
    pub operator_id: u128,
}

impl InternalSubscriber {
    pub fn new(
        sub: Box<dyn Subscriber>,
        sender: Sender<Message>,
        operator_id: u128,
    ) -> InternalSubscriber {
        InternalSubscriber {
            sub,
            sender,
            operator_id,
        }
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

impl MessageConsumer for ExternalSubscriber {
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

/////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone)]
pub struct MockSubscriber {
    sender: mpsc::Sender<Message>,
}

impl MockSubscriber {
    pub fn new(sender: mpsc::Sender<Message>) -> MockSubscriber {
        MockSubscriber { sender }
    }
}

impl Subscriber for MockSubscriber {}

impl MessageConsumer for MockSubscriber {
    fn consumes_message(&self, _: &Message) -> bool {
        true
    }
}

impl MessageReceiver for MockSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
