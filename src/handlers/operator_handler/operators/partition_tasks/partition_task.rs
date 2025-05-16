use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::handlers::exchange_handlers;
use crate::handlers::message_router_handler::MessageRouterState;
use crate::handlers::{
    message_handler::{
        messages::message::{Message, MessageName},
        MessageRegistry, Pipe,
    },
    message_router_handler::MessageConsumer,
    operator_handler::{
        operator_handler_state::OperatorInstanceConfig,
        operators::{
            operator_task_trackers::RestrictedOperatorTaskTracker, traits::TaskBuilder,
            ConnectionRegistry,
        },
    },
};

use super::config::PartitionConfig;

#[derive(Debug)]
struct PartitionTask {
    operator_instance_config: OperatorInstanceConfig,
    partition_config: PartitionConfig,

    operator_pipe: Pipe,
    msg_reg: Arc<MessageRegistry>,
    msg_router_state: Arc<Mutex<MessageRouterState>>,
}

impl PartitionTask {
    fn new(
        op_in_config: OperatorInstanceConfig,
        partition_config: PartitionConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        _: Arc<ConnectionRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> PartitionTask {
        PartitionTask {
            operator_instance_config: op_in_config,
            partition_config,
            operator_pipe,
            msg_reg,
            msg_router_state,
        }
    }

    fn consumer(&self) -> Box<dyn MessageConsumer> {
        Box::new(PartitionConsumer {
            msg_reg: self.msg_reg.clone(),
        })
    }

    async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        debug!(
            operator_task = self
                .operator_instance_config
                .operator
                .operator_type
                .task_name(),
            operator_id = self.operator_instance_config.operator.id,
            operator_instance_id = self.operator_instance_config.id,
            "started task",
        );

        let rec_handler = exchange_handlers::record_handler::RecordHandler::initiate(
            ct.child_token(),
            &self.operator_instance_config,
            &mut self.operator_pipe,
            self.msg_reg.clone(),
            self.msg_router_state.clone(),
        )
        .await?;

        for _ in 0..60 {
            debug!("waiting...");
            tokio::time::sleep(chrono::Duration::seconds(1).to_std()?).await;
        }

        if let Err(err) = rec_handler.close().await {
            error!("{}", err);
        }

        debug!(
            operator_task = self
                .operator_instance_config
                .operator
                .operator_type
                .task_name(),
            operator_id = self.operator_instance_config.operator.id,
            operator_instance_id = self.operator_instance_config.id,
            "closed task",
        );
        Ok(())
    }
}

///////////////////////////////////////////////////////
// Partition Producer Builder

#[derive(Debug, Clone)]
pub struct PartitionTaskBuilder {}

impl PartitionTaskBuilder {
    pub fn new() -> PartitionTaskBuilder {
        PartitionTaskBuilder {}
    }
}

impl TaskBuilder for PartitionTaskBuilder {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
        tt: &mut RestrictedOperatorTaskTracker,
        ct: tokio_util::sync::CancellationToken,
    ) -> Result<(
        tokio::sync::oneshot::Receiver<Option<anyhow::Error>>,
        Box<dyn MessageConsumer>,
    )> {
        let filter_files_config = PartitionConfig::try_from(&op_in_config)?;
        let mut op = PartitionTask::new(
            op_in_config,
            filter_files_config,
            operator_pipe,
            msg_reg.clone(),
            conn_reg.clone(),
            msg_router_state.clone(),
        );

        let consumer = op.consumer();

        let (tx, rx) = tokio::sync::oneshot::channel();
        tt.spawn(async move {
            if let Err(err) = op.async_main(ct).await {
                error!("{:?}", err);
                if let Err(err_send) = tx.send(Some(err)) {
                    error!("{:?}", err_send);
                }
            } else {
                if let Err(err_send) = tx.send(None) {
                    error!("{:?}", err_send);
                }
            }
        })?;

        Ok((rx, consumer))
    }
}

///////////////////////////////////////////////////////
// Message Consumer

#[derive(Debug, Clone)]
pub struct PartitionConsumer {
    msg_reg: Arc<MessageRegistry>,
}

impl MessageConsumer for PartitionConsumer {
    fn consumes_message(&self, msg: &Message) -> bool {
        match msg.msg.msg_name() {
            MessageName::Ping => true,
            MessageName::QueryHandlerRequests => true,
            MessageName::ExchangeRequests => true,
            _ => false,
        }
    }
}
