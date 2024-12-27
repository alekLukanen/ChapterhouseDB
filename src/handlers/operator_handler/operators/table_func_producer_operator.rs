use std::sync::Arc;

use anyhow::{Context, Result};
use sqlparser::ast::FunctionArg;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::handlers::message_handler::{Message, MessageRegistry, Pipe};
use crate::handlers::message_router_handler::{
    MessageConsumer, MessageReceiver, MessageRouterState, Subscriber,
};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;

pub struct TableFuncConfig {
    pub alias: Option<String>,
    pub func_name: String,
    pub args: Vec<FunctionArg>,
    pub max_rows_per_batch: usize,

    pub outbound_exchange_id: String,
    pub inbound_exchange_ids: Vec<String>,
}

pub struct TableFuncProducerOperator {
    operator_instance_config: OperatorInstanceConfig,
    table_func_config: TableFuncConfig,

    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe<Message>,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,

    ct: CancellationToken,
}

impl TableFuncProducerOperator {
    pub async fn new(
        op_in_config: OperatorInstanceConfig,
        table_func_config: TableFuncConfig,
        message_router_state: Arc<Mutex<MessageRouterState>>,
        msg_reg: Arc<Box<MessageRegistry>>,
        ct: CancellationToken,
    ) -> TableFuncProducerOperator {
        let router_sender = message_router_state.lock().await.sender();
        let (pipe, sender) = Pipe::new_with_existing_sender(router_sender, 1);

        TableFuncProducerOperator {
            operator_instance_config: op_in_config,
            table_func_config,
            message_router_state,
            router_pipe: pipe,
            sender,
            msg_reg,
            ct,
        }
    }

    fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(TableFuncProducerOperatorSubscriber {
            sender: self.sender.clone(),
            msg_reg: self.msg_reg.clone(),
        })
    }

    pub async fn async_main(&self, ct: CancellationToken) -> Result<()> {
        self.message_router_state
            .lock()
            .await
            .add_internal_subscriber(self.subscriber())
            .context("failed subscribing")?;

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                _ = self.ct.cancelled() => {
                    break;
                }
            }
        }

        info!(
            "closing operator producer for instance {}",
            self.operator_instance_config.id
        );

        Ok(())
    }
}

//////////////////////////////////////////////////////
// Message Subscriber

#[derive(Debug, Clone)]
pub struct TableFuncProducerOperatorSubscriber {
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<Box<MessageRegistry>>,
}

impl Subscriber for TableFuncProducerOperatorSubscriber {}

impl MessageConsumer for TableFuncProducerOperatorSubscriber {
    fn consumes_message(&self, msg: &crate::handlers::message_handler::Message) -> bool {
        false
    }
}

impl MessageReceiver for TableFuncProducerOperatorSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
