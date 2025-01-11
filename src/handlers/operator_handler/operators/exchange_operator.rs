use std::sync::Arc;

use anyhow::{Context, Result};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::handlers::message_handler::{Message, MessageRegistry, Pipe};
use crate::handlers::message_router_handler::{
    MessageConsumer, MessageReceiver, MessageRouterState, Subscriber,
};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;

#[derive(Debug, Error)]
pub enum ExchangeOperatorError {}

#[derive(Debug)]
pub struct ExchangeOperator {
    operator_instance_config: OperatorInstanceConfig,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe<Message>,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
}

impl ExchangeOperator {
    pub async fn new(
        op_in_config: OperatorInstanceConfig,
        message_router_state: Arc<Mutex<MessageRouterState>>,
        msg_reg: Arc<MessageRegistry>,
    ) -> ExchangeOperator {
        let router_sender = message_router_state.lock().await.sender();
        let (pipe, sender) = Pipe::new_with_existing_sender(router_sender, 1);

        ExchangeOperator {
            operator_instance_config: op_in_config,
            message_router_state,
            router_pipe: pipe,
            sender,
            msg_reg,
        }
    }

    fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(ExchangeOperatorSubscriber {
            sender: self.sender.clone(),
            msg_reg: self.msg_reg.clone(),
            operator_instance_id: self.operator_instance_config.id.clone(),
        })
    }

    async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        self.message_router_state
            .lock()
            .await
            .add_internal_subscriber(self.subscriber())
            .context("failed subscribing")?;

        info!(
            "started the exchange operator for instance {}",
            self.operator_instance_config.id
        );

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        info!(
            "closed exchange operator for instance {}",
            self.operator_instance_config.id
        );

        Ok(())
    }
}

//////////////////////////////////////////////////////
// Message Subscriber

#[derive(Debug, Clone)]
pub struct ExchangeOperatorSubscriber {
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
    operator_instance_id: u128,
}

impl Subscriber for ExchangeOperatorSubscriber {}

impl MessageConsumer for ExchangeOperatorSubscriber {
    fn consumes_message(&self, msg: &crate::handlers::message_handler::Message) -> bool {
        if let Some(route_to_operation_id) = msg.route_to_operation_id {
            if route_to_operation_id == self.operator_instance_id {
                return true;
            }
        } else {
            return false;
        }
        false
    }
}

impl MessageReceiver for ExchangeOperatorSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
