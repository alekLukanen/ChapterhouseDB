use std::sync::Arc;

use anyhow::Result;
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
pub struct ReadFilesTask {
    operator_instance_config: OperatorInstanceConfig,
    table_func_config: TableFuncConfig,

    operator_pipe: Pipe<Message>,
    msg_reg: Arc<MessageRegistry>,
}

impl ReadFilesTask {
    pub fn new(
        op_in_config: OperatorInstanceConfig,
        table_func_config: TableFuncConfig,
        operator_pipe: Pipe<Message>,
        msg_reg: Arc<MessageRegistry>,
    ) -> ReadFilesTask {
        ReadFilesTask {
            operator_instance_config: op_in_config,
            table_func_config,
            operator_pipe,
            msg_reg,
        }
    }

    pub fn subscriber(&self) -> Box<dyn MessageConsumer> {
        Box::new(ReadFilesConsumer {
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

impl TableFuncTaskBuilder for ReadFilesTask {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        table_func_config: TableFuncConfig,
        operator_pipe: Pipe<Message>,
        msg_reg: Arc<MessageRegistry>,
        tt: &RestrictedOperatorTaskTracker,
        ct: CancellationToken,
    ) -> Result<Box<dyn MessageConsumer>> {
        let mut op = ReadFilesTask::new(
            op_in_config,
            table_func_config,
            operator_pipe,
            msg_reg.clone(),
        );

        let consumer = op.subscriber();

        tt.spawn(async move {
            if let Err(err) = op.async_main(ct).await {
                info!("error: {:?}", err);
            }
        });

        Ok(consumer)
    }

    fn implements_func_name(&self) -> String {
        "read_files".to_string()
    }
}

//////////////////////////////////////////////////////
// Message Consumer

#[derive(Debug, Clone)]
pub struct ReadFilesConsumer {
    msg_reg: Arc<MessageRegistry>,
}

impl MessageConsumer for ReadFilesConsumer {
    fn consumes_message(&self, msg: &crate::handlers::message_handler::Message) -> bool {
        true
    }
}
