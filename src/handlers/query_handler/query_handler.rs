use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::handlers::message_handler::{Message, Pipe};
use crate::handlers::message_router_handler::{MessageConsumer, MessageReceiver, Subscriber};

#[derive(Debug)]
pub struct QueryHandler {
    router_pipe: Pipe<Message>,
    sender: mpsc::Sender<Message>,
    ct: CancellationToken,
}

impl QueryHandler {
    pub fn new(ct: CancellationToken, router_sender: mpsc::Sender<Message>) -> QueryHandler {
        let (pipe, sender) = Pipe::new_with_existing_sender(router_sender, 1);
        QueryHandler {
            router_pipe: pipe,
            sender,
            ct,
        }
    }
}

impl MessageConsumer for QueryHandler {
    fn consumes_message(&self, msg: &Message) -> bool {
        false
    }
}

impl MessageReceiver for QueryHandler {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
