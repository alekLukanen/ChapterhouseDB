use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::handlers::{
    message_handler::{
        Message, MessageName, MessageRegistry, OperatorInstanceAvailable,
        OperatorInstanceAvailableResponse, Pipe,
    },
    message_router_handler::{MessageConsumer, MessageReceiver, MessageRouterState, Subscriber},
};

use super::operator_handler_state::{OperatorHandlerState, TotalOperatorCompute};

pub struct OperatorHandler {
    state: OperatorHandlerState,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe<Message>,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,
}

impl OperatorHandler {
    pub async fn new(
        message_router_state: Arc<Mutex<MessageRouterState>>,
        msg_reg: Arc<Box<MessageRegistry>>,
        allowed_compute: TotalOperatorCompute,
    ) -> OperatorHandler {
        let router_sender = message_router_state.lock().await.sender();
        let (pipe, sender) = Pipe::new_with_existing_sender(router_sender, 1);

        let handler = OperatorHandler {
            state: OperatorHandlerState::new(allowed_compute),
            message_router_state,
            router_pipe: pipe,
            sender,
            msg_reg,
        };

        handler
    }

    pub fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(OperatorHandlerSubscriber {
            sender: self.sender.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        self.message_router_state
            .lock()
            .await
            .add_internal_subscriber(self.subscriber())
            .context("failed subscribing")?;

        loop {
            tokio::select! {
                Some(msg) = self.router_pipe.recv() => {
                    if let Err(err) = self.handle_message(msg).await {
                        info!("error: {:?}", err);
                    }
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        info!("closing the operator handler...");
        self.router_pipe.close_receiver();

        Ok(())
    }

    async fn handle_message(&self, msg: Message) -> Result<()> {
        match msg.msg.msg_name() {
            MessageName::OperatorInstanceAvailable => {}
            _ => (),
        }
        Ok(())
    }

    async fn handle_operator_instance_available(&self, msg: Message) -> Result<()> {
        let op_in_avail: &OperatorInstanceAvailable = self.msg_reg.try_cast_msg(&msg)?;

        let ref mut total_compute = self.state.total_operator_compute();
        total_compute.add(&TotalOperatorCompute {
            instances: 1,
            memory_in_mib: op_in_avail.compute.memory_in_mib,
            cpu_in_thousandths: op_in_avail.compute.cpu_in_thousandths,
        });

        let is_at_capacity = self
            .state
            .get_allowed_compute()
            .any_greater_than(total_compute);
        if is_at_capacity {
            return Ok(());
        }

        let resp = msg.reply(Box::new(OperatorInstanceAvailableResponse::new(
            op_in_avail.op_instance_id,
            true,
        )));
        self.router_pipe.send(resp).await?;

        Ok(())
    }
}

/////////////////////////////////////////////////
// Message subscriber for the operator handler
#[derive(Debug)]
pub struct OperatorHandlerSubscriber {
    sender: mpsc::Sender<Message>,
}

impl Subscriber for OperatorHandlerSubscriber {}

impl MessageConsumer for OperatorHandlerSubscriber {
    fn consumes_message(&self, msg: &Message) -> bool {
        match msg.msg.msg_name() {
            MessageName::OperatorInstanceAvailable => true,
            _ => false,
        }
    }
}

impl MessageReceiver for OperatorHandlerSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
