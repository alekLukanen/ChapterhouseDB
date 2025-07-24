use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::handlers::exchange_handlers;
use crate::handlers::exchange_handlers::record_handler::ExchangeRecord;
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

use super::config::SortConfig;

#[derive(Debug)]
struct SortTask {
    operator_instance_config: OperatorInstanceConfig,
    sort_config: SortConfig,

    operator_pipe: Pipe,
    msg_reg: Arc<MessageRegistry>,
    msg_router_state: Arc<Mutex<MessageRouterState>>,
}

impl SortTask {
    fn new(
        op_in_config: OperatorInstanceConfig,
        sort_config: SortConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        _: Arc<ConnectionRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> SortTask {
        SortTask {
            operator_instance_config: op_in_config,
            sort_config,
            operator_pipe,
            msg_reg,
            msg_router_state,
        }
    }

    fn consumer(&self) -> Box<dyn MessageConsumer> {
        Box::new(SortConsumer {})
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

        let mut recs: Vec<ExchangeRecord> = Vec::new();

        // gather all of the records
        for part_idx in 0..self.sort_config.num_partitions {
            let mut rec_handler = exchange_handlers::record_handler::RecordHandler::initiate(
                ct.child_token(),
                &self.operator_instance_config,
                format!("part-{}", part_idx),
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
                        recs.push(exchange_rec);
                    }
                    None => {
                        info!("read all records from the exchange");
                        break;
                    }
                }
            }

            if let Err(err) = rec_handler.close().await {
                error!("{}", err);
            }
        }

        // sort the records

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
pub struct SortTaskBuilder {}

impl SortTaskBuilder {
    pub fn new() -> SortTaskBuilder {
        SortTaskBuilder {}
    }
}

impl TaskBuilder for SortTaskBuilder {
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
        let config = SortConfig::try_from(&op_in_config)?;
        let mut op = SortTask::new(
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
pub struct SortConsumer {}

impl MessageConsumer for SortConsumer {
    fn consumes_message(&self, msg: &Message) -> bool {
        match msg.msg.msg_name() {
            MessageName::Ping => true,
            MessageName::QueryHandlerRequests => true,
            MessageName::ExchangeRequests => true,
            _ => false,
        }
    }
}
