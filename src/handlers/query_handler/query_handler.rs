use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::query_handler_state::{self, QueryHandlerState};
use crate::handlers::message_handler::{Message, MessageName, MessageRegistry, Pipe, RunQuery};
use crate::handlers::message_router_handler::{
    MessageConsumer, MessageReceiver, MessageRouterState, Subscriber,
};

#[derive(Debug)]
pub struct QueryHandler {
    state: QueryHandlerState,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe<Message>,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,

    ct: CancellationToken,
}

impl QueryHandler {
    pub async fn new(
        message_router_state: Arc<Mutex<MessageRouterState>>,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> QueryHandler {
        let router_sender = message_router_state.lock().await.sender();
        let (pipe, sender) = Pipe::new_with_existing_sender(router_sender, 0);

        let handler = QueryHandler {
            state: QueryHandlerState::new(),
            message_router_state,
            router_pipe: pipe,
            sender,
            msg_reg,
            ct: CancellationToken::new(),
        };

        handler
    }

    pub fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(QueryHandlerSubscriber {
            sender: self.sender.clone(),
            ct: self.ct.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        self.message_router_state
            .lock()
            .await
            .add_internal_subscriber(self.subscriber())?;

        loop {
            tokio::select! {
                Some(msg) = self.router_pipe.recv() => {
                    match msg.msg.msg_name() {
                        MessageName::RunQuery => {
                            self.run_query(&msg).await?;
                        },
                        _ => {
                            info!("unknown message received");
                        },
                    }
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        self.router_pipe.close_receiver();

        Ok(())
    }

    async fn run_query(&mut self, msg: &Message) -> Result<()> {
        let run_query: &RunQuery = self.msg_reg.cast_msg(&msg);
        let query = query_handler_state::Query {
            query: run_query.query.clone(),
        };

        self.state.add_query(query);

        Ok(())
    }
}

// the following struct defines the Subscriber that the
// message router will use to route messages to the QueryHandler
// struct. It needs to me it's own struct due to rust ownership
// rules.
#[derive(Debug)]
pub struct QueryHandlerSubscriber {
    sender: mpsc::Sender<Message>,
    ct: CancellationToken,
}

impl Subscriber for QueryHandlerSubscriber {}

impl MessageConsumer for QueryHandlerSubscriber {
    fn consumes_message(&self, msg: &Message) -> bool {
        false
    }
}

impl MessageReceiver for QueryHandlerSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
