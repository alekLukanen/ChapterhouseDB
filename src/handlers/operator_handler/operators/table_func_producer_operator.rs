use std::sync::Arc;

use anyhow::Result;
use sqlparser::ast::FunctionArg;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::operator_task_trackers::RestrictedOperatorTaskTracker;
use super::traits::OperatorTaskBuilder;
use crate::handlers::message_handler::{Message, MessageRegistry, Pipe};
use crate::handlers::message_router_handler::{MessageConsumer, MessageReceiver, Subscriber};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;

#[derive(Debug)]
pub struct TableFuncConfig {
    pub alias: Option<String>,
    pub func_name: String,
    pub args: Vec<FunctionArg>,
    pub max_rows_per_batch: usize,

    pub outbound_exchange_id: String,
    pub inbound_exchange_ids: Vec<String>,
}

#[derive(Debug)]
pub struct TableFuncProducerOperator {
    operator_instance_config: OperatorInstanceConfig,
    table_func_config: TableFuncConfig,

    router_pipe: Pipe<Message>,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
}

impl TableFuncProducerOperator {
    pub fn new(
        op_in_config: OperatorInstanceConfig,
        table_func_config: TableFuncConfig,
        message_router_sender: mpsc::Sender<Message>,
        msg_reg: Arc<MessageRegistry>,
    ) -> TableFuncProducerOperator {
        let (pipe, sender) = Pipe::new_with_existing_sender(message_router_sender, 1);

        TableFuncProducerOperator {
            operator_instance_config: op_in_config,
            table_func_config,
            router_pipe: pipe,
            sender,
            msg_reg,
        }
    }

    fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(TableFuncProducerOperatorSubscriber {
            sender: self.sender.clone(),
            msg_reg: self.msg_reg.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
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
// Table Func Producer Builder

#[derive(Debug, Clone)]
pub struct TableFuncProducerOperatorBuilder {}

impl OperatorTaskBuilder for TableFuncProducerOperatorBuilder {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        message_router_sender: mpsc::Sender<Message>,
        msg_reg: Arc<MessageRegistry>,
        tt: &RestrictedOperatorTaskTracker,
        ct: CancellationToken,
    ) -> Result<Box<dyn Subscriber>> {
        let table_func_config = TableFuncConfig::try_from(&op_in_config)?;
        let mut op = TableFuncProducerOperator::new(
            op_in_config,
            table_func_config,
            message_router_sender,
            msg_reg.clone(),
        );

        let subscriber = op.subscriber();

        tt.spawn(async move {
            if let Err(err) = op.async_main(ct).await {
                info!("error: {:?}", err);
            }
        });

        Ok(subscriber)
    }
}

//////////////////////////////////////////////////////
// Message Subscriber

#[derive(Debug, Clone)]
pub struct TableFuncProducerOperatorSubscriber {
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
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
