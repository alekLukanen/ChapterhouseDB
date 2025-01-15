use std::sync::Arc;

use anyhow::{Context, Result};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::handlers::message_handler::{
    Message, MessageName, MessageRegistry, Ping, Pipe, StoreRecordBatch,
};
use crate::handlers::message_router_handler::{
    MessageConsumer, MessageReceiver, MessageRouterState, Subscriber,
};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;
use crate::handlers::operator_handler::operators::common_message_handlers::handle_ping_message;

#[derive(Debug, Error)]
pub enum ExchangeOperatorError {
    #[error("received an unhandled message: {0}")]
    ReceivedAnUnhandledMessage(String),
}

#[derive(Debug)]
pub struct ExchangeOperator {
    operator_instance_config: OperatorInstanceConfig,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe,
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
        let (mut pipe, sender) = Pipe::new_with_existing_sender(router_sender, 1);
        pipe.set_sent_from_query_id(op_in_config.query_id.clone());
        pipe.set_sent_from_operation_id(op_in_config.id.clone());

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
            query_id: self.operator_instance_config.query_id.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
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
                Some(msg) = self.router_pipe.recv() => {
                    info!("received message: {}", msg.msg.msg_name());
                    match msg.msg.msg_name() {
                        MessageName::Ping => {
                            let ping_msg: &Ping = self.msg_reg.try_cast_msg(&msg)?;
                            handle_ping_message(&msg, ping_msg)?;
                        },
                        _ => {
                            return Err(ExchangeOperatorError::ReceivedAnUnhandledMessage(
                                msg.msg.msg_name().to_string()
                            ).into());
                        }
                    }
                }
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
    query_id: u128,
}

impl Subscriber for ExchangeOperatorSubscriber {}

impl MessageConsumer for ExchangeOperatorSubscriber {
    fn consumes_message(&self, msg: &crate::handlers::message_handler::Message) -> bool {
        if (msg.route_to_operation_id.is_some()
            && msg.route_to_operation_id != Some(self.operator_instance_id))
            || (msg.sent_from_query_id.is_some() && msg.sent_from_query_id != Some(self.query_id))
        {
            return false;
        }

        match msg.msg.msg_name() {
            MessageName::StoreRecordBatch => {
                match self.msg_reg.try_cast_msg::<StoreRecordBatch>(msg) {
                    Ok(StoreRecordBatch::RequestSendRecord { .. }) => true,
                    Ok(StoreRecordBatch::ResponseReceivedRecord { .. }) => false,
                    Err(err) => {
                        info!("error: {}", err);
                        false
                    }
                }
            }
            _ => false,
        }
    }
}

impl MessageReceiver for ExchangeOperatorSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
