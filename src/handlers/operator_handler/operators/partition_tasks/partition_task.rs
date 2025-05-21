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
use crate::planner::PartitionRangeMethod;

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
        Box::new(PartitionConsumer {})
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

        // get the partition data
        for _ in 0..60 {
            debug!("waiting...");
            tokio::time::sleep(chrono::Duration::seconds(1).to_std()?).await;
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

    async fn compute_data_partitions(&mut self, ct: CancellationToken) -> Result<()> {
        let sampling_queue_name = match &self.partition_config.partition_range_method {
            PartitionRangeMethod::SampleDistribution {
                exchange_queue_name,
                ..
            } => exchange_queue_name.clone(),
        };

        let mut rec_handler = exchange_handlers::record_handler::RecordHandler::initiate(
            ct.child_token(),
            &self.operator_instance_config,
            sampling_queue_name,
            &mut self.operator_pipe,
            self.msg_reg.clone(),
            self.msg_router_state.clone(),
        )
        .await?;

        loop {
            let exchange_rec = rec_handler
                .next_record(ct.child_token(), &mut self.operator_pipe, None)
                .await?;

            match exchange_rec {
                Some(exchange_rec) => {
                    debug!(
                        record_id = exchange_rec.record_id,
                        record_num_rows = exchange_rec.record.num_rows(),
                        "received record"
                    );

                    // confirm processing of the record with the inbound exchange
                    rec_handler
                        .complete_record(&mut self.operator_pipe, exchange_rec)
                        .await?;
                }
                None => {
                    debug!("read all records from the exchange");
                    break;
                }
            }
        }

        if let Err(err) = rec_handler.close().await {
            error!("{}", err);
        }

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
        let config = PartitionConfig::try_from(&op_in_config)?;
        let mut op = PartitionTask::new(
            op_in_config,
            config,
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
pub struct PartitionConsumer {}

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
