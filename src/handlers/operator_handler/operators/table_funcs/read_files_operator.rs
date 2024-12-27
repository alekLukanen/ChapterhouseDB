use std::sync::Arc;

use anyhow::Result;
use sqlparser::ast::FunctionArg;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::handlers::message_handler::{Message, MessageRegistry, Pipe};
use crate::handlers::message_router_handler::{MessageConsumer, MessageReceiver, Subscriber};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;
use crate::handlers::operator_handler::operators::operator_task_trackers::RestrictedOperatorTaskTracker;
use crate::handlers::operator_handler::operators::traits::TableFuncTaskBuilder;

use super::config::TableFuncConfig;

#[derive(Debug)]
pub struct ReadFilesOperator {
    operator_instance_config: OperatorInstanceConfig,
    table_func_config: TableFuncConfig,

    router_pipe: Pipe<Message>,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
}

impl ReadFilesOperator {
    pub fn new(
        op_in_config: OperatorInstanceConfig,
        table_func_config: TableFuncConfig,
        message_router_sender: mpsc::Sender<Message>,
        msg_reg: Arc<MessageRegistry>,
    ) -> ReadFilesOperator {
        let (pipe, sender) = Pipe::new_with_existing_sender(message_router_sender, 1);

        ReadFilesOperator {
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
pub struct ReadFilesOperatorBuilder {}

impl TableFuncTaskBuilder for ReadFilesOperator {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        table_func_config: TableFuncConfig,
        message_router_sender: mpsc::Sender<Message>,
        msg_reg: Arc<MessageRegistry>,
        tt: &RestrictedOperatorTaskTracker,
        ct: CancellationToken,
    ) -> Result<Box<dyn Subscriber>> {
        let mut op = ReadFilesOperator::new(
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

    fn implements_func_name(&self) -> String {
        "read_files".to_string()
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
