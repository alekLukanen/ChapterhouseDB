use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::compute::{SortColumn, SortOptions};
use arrow::row::SortField;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use super::config::PartitionConfig;
use crate::handlers::exchange_handlers;
use crate::handlers::message_router_handler::MessageRouterState;
use crate::handlers::operator_handler::operators::record_utils::{self, RecordPartitionHandler};
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
use crate::planner::{PartitionMethod, PartitionRangeMethod};

#[derive(Debug, Error)]
pub enum PartitionTaskError {
    #[error("sample data was empty")]
    SampleDataWasEmpty,
}

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

        let partition_handler = self.get_partition_handler(ct.child_token()).await?;
        let order_by_exprs = match &self.partition_config.partition_method {
            PartitionMethod::OrderByExprs { exprs } => exprs,
        };

        // create the transaction with the exchange. If the operator task restarts this
        // will get the existing transaction since transactions are keyed. All data
        // sent to the exchange during the transaction will be committed at the end
        // of this task. It's okay to ack the processing of records from the source
        // exchange because the outbound tranaction isn't ever deleted. The data
        // will remain there until it is committed.
        let mut transaction_rec_handler =
            exchange_handlers::record_handler::RecordHandler::initiate(
                ct.child_token(),
                &self.operator_instance_config,
                "default".to_string(),
                &mut self.operator_pipe,
                self.msg_reg.clone(),
                self.msg_router_state.clone(),
            )
            .await?
            .create_outbound_transaction(
                &mut self.operator_pipe,
                format!(
                    "partition-op-task-{}",
                    self.operator_instance_config.operator.id.clone()
                ),
            )
            .await?;

        loop {
            let exchange_rec = transaction_rec_handler
                .record_handler_inner
                .next_record(ct.child_token(), &mut self.operator_pipe, None)
                .await?;

            match exchange_rec {
                Some(exchange_rec) => {
                    debug!(
                        record_id = exchange_rec.record_id,
                        record_num_rows = exchange_rec.record.num_rows(),
                        "received record"
                    );

                    let sort_cols_rec = Arc::new(record_utils::compute_order_by_record(
                        order_by_exprs,
                        exchange_rec.record.clone(),
                        &exchange_rec.table_aliases,
                    )?);

                    // partition the record
                    let partitioned_recs =
                        partition_handler.partition(sort_cols_rec, exchange_rec.record.clone())?;

                    // send the records to the outbound exchange
                    for part_rec in partitioned_recs {
                        transaction_rec_handler
                            .insert_record(
                                &mut self.operator_pipe,
                                "default".to_string(),
                                exchange_rec.record_id.clone(),
                                part_rec.record,
                                exchange_rec.table_aliases.clone(),
                            )
                            .await?;
                    }

                    // confirm processing of the record with the inbound exchange
                    transaction_rec_handler
                        .record_handler_inner
                        .complete_record(&mut self.operator_pipe, exchange_rec)
                        .await?;
                }
                None => {
                    debug!("read all records from the exchange");
                    break;
                }
            }
        }

        let rec_handler = transaction_rec_handler
            .commit_transaction(&mut self.operator_pipe)
            .await?;

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

    async fn get_partition_handler(
        &mut self,
        ct: CancellationToken,
    ) -> Result<RecordPartitionHandler> {
        let (sampling_queue_name, num_partitions) =
            match &self.partition_config.partition_range_method {
                PartitionRangeMethod::SampleDistribution {
                    exchange_queue_name,
                    num_partitions,
                    ..
                } => (exchange_queue_name.clone(), num_partitions.clone()),
            };
        let order_by_exprs = match &self.partition_config.partition_method {
            PartitionMethod::OrderByExprs { exprs } => exprs,
        };

        let mut rec_handler = exchange_handlers::record_handler::RecordHandler::initiate(
            ct.child_token(),
            &self.operator_instance_config,
            sampling_queue_name,
            &mut self.operator_pipe,
            self.msg_reg.clone(),
            self.msg_router_state.clone(),
        )
        .await?
        .disable_record_heartbeat();

        // gather all records
        // these records only contain the computed fields used
        // in the order by
        let mut recs = Vec::new();
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

                    let computed_rec = record_utils::compute_order_by_record(
                        order_by_exprs,
                        exchange_rec.record,
                        &exchange_rec.table_aliases,
                    )?;

                    recs.push(computed_rec);
                }
                None => {
                    debug!("read all records from the exchange");
                    break;
                }
            }
        }

        let rec_schema = if let Some(rec) = recs.first() {
            rec.schema().clone()
        } else {
            return Err(PartitionTaskError::SampleDataWasEmpty.into());
        };

        assert_eq!(
            order_by_exprs.len(),
            recs.iter().map(|rec| rec.num_columns()).max().unwrap_or(0)
        );
        assert_eq!(
            order_by_exprs.len(),
            recs.iter().map(|rec| rec.num_columns()).min().unwrap_or(0)
        );

        // sort the records all together
        let merged_rec = arrow::compute::concat_batches(&rec_schema, recs.iter())?;

        let col_types = merged_rec
            .columns()
            .iter()
            .map(|arr| arr.data_type().clone())
            .collect::<Vec<_>>();
        let cols_to_sort = merged_rec
            .columns()
            .iter()
            .zip(order_by_exprs.iter())
            .map(|(arr, expr)| SortColumn {
                values: arr.clone(),
                options: Some(SortOptions {
                    descending: !expr.asc.unwrap_or(true),
                    nulls_first: expr.nulls_first.unwrap_or(true),
                }),
            })
            .collect::<Vec<_>>();
        let sorted_cols = arrow::compute::kernels::sort::lexsort(&cols_to_sort, None)?;
        let sorted_rec = Arc::new(RecordBatch::try_new(merged_rec.schema(), sorted_cols)?);

        let partitions_rec =
            record_utils::compute_record_partition_intervals(sorted_rec, num_partitions)?;

        // TODO: confirm processing of all records by comfirming the queue itself
        // not the individual records. You need to store the partitions in the downstream
        // exchange in a new "partitions" queue before confirming the records. Then
        // when the operator task starts check if that exists.
        // Maybe use a transaction?

        if let Err(err) = rec_handler.close().await {
            error!("{}", err);
        }

        let partition_handler = record_utils::RecordPartitionHandler::new(
            partitions_rec,
            col_types
                .iter()
                .zip(order_by_exprs.iter())
                .map(|(data_type, expr)| {
                    SortField::new_with_options(
                        data_type.clone(),
                        SortOptions {
                            descending: !expr.asc.unwrap_or(true),
                            nulls_first: expr.nulls_first.unwrap_or(true),
                        },
                    )
                })
                .collect::<Vec<_>>(),
        )?;

        Ok(partition_handler)
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
