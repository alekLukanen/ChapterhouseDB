use core::fmt;

use tokio::sync::mpsc;

use crate::handlers::message_handler::{Message, Pipe};

pub trait Subscriber: fmt::Debug + Send + Sync {
    fn consumes_message(&self, msg: &Message) -> bool;
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

#[derive(Debug)]
pub struct WorkerSubscriber {
    address: String,
    worker_id: Option<u128>,
}

impl WorkerSubscriber {
    pub fn new(address: String) -> WorkerSubscriber {
        WorkerSubscriber {
            address,
            worker_id: None,
        }
    }
    pub fn set_worker_id(&mut self, worker_id: u128) {
        self.worker_id = Some(worker_id);
    }
    pub fn get_worker_id(&self) -> Option<u128> {
        self.worker_id.clone()
    }
}

impl Subscriber for WorkerSubscriber {
    fn consumes_message(&self, msg: &Message) -> bool {
        match (self.worker_id, msg.route_to_worker_id) {
            (Some(worker_id), Some(route_to_worker_id)) => worker_id == route_to_worker_id,
            _ => true,
        }
    }
}
