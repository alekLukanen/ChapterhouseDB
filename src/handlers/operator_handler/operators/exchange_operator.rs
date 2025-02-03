use std::sync::Arc;
use std::u64;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::exchange::ExchangeRequests;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::message_handler::{MessageRegistry, Pipe};
use crate::handlers::message_router_handler::{
    MessageConsumer, MessageReceiver, MessageRouterState, Subscriber,
};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;
use crate::handlers::operator_handler::operators::common_message_handlers::handle_ping_message;
use crate::planner;

#[derive(Debug, Error)]
pub enum ExchangeOperatorError {
    #[error("invalid operator type: {0}")]
    InvalidOperatorType(String),
    #[error("operation instance id not set on message")]
    OperationInstanceIdNotSetOnMessage,
}

#[derive(Debug, PartialEq)]
enum Status {
    Complete,
    Running,
}

#[derive(Debug)]
struct ProducerOperatorStatus {
    operator_id: String,
    status: Status,
}

#[derive(Debug)]
pub struct ExchangeOperator {
    record_pool: RecordPool,
    inbound_producer_operator_states: Vec<ProducerOperatorStatus>,
    received_all_data_from_producers: bool,

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
    ) -> Result<ExchangeOperator> {
        let router_sender = message_router_state.lock().await.sender();
        let (mut pipe, sender) = Pipe::new_with_existing_sender(router_sender, 10);
        pipe.set_sent_from_query_id(op_in_config.query_id.clone());
        pipe.set_sent_from_operation_id(op_in_config.id.clone());

        let (outbound_producer_ids, inbound_producer_ids) =
            match &op_in_config.operator.operator_type {
                planner::OperatorType::Exchange {
                    outbound_producer_ids,
                    inbound_producer_ids,
                    ..
                } => (outbound_producer_ids.clone(), inbound_producer_ids.clone()),
                planner::OperatorType::Producer { .. } => {
                    return Err(ExchangeOperatorError::InvalidOperatorType(
                        op_in_config.operator.operator_type.name().to_string(),
                    )
                    .into());
                }
            };

        let record_pool = RecordPool::new(
            outbound_producer_ids,
            RecordPoolConfig {
                max_heartbeat_interval: chrono::Duration::seconds(10),
            },
        );

        let operator_statuses: Vec<ProducerOperatorStatus> = inbound_producer_ids
            .iter()
            .map(|producer_id| ProducerOperatorStatus {
                operator_id: producer_id.clone(),
                status: Status::Running,
            })
            .collect();

        Ok(ExchangeOperator {
            record_pool,
            inbound_producer_operator_states: operator_statuses,
            received_all_data_from_producers: false,
            operator_instance_config: op_in_config,
            message_router_state,
            router_pipe: pipe,
            sender,
            msg_reg,
        })
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
            .add_internal_subscriber(self.subscriber(), self.operator_instance_config.id.clone());

        debug!(
            operator_task = self
                .operator_instance_config
                .operator
                .operator_type
                .task_name(),
            operator_id = self.operator_instance_config.operator.id,
            operator_instance_id = self.operator_instance_config.id,
            "started exchange operator instance",
        );

        self.inner_async_main(ct).await?;

        self.message_router_state
            .lock()
            .await
            .remove_internal_subscriber(&self.operator_instance_config.id);

        debug!(
            operator_task = self
                .operator_instance_config
                .operator
                .operator_type
                .task_name(),
            operator_id = self.operator_instance_config.operator.id,
            operator_instance_id = self.operator_instance_config.id,
            "closed exchange operator instance",
        );

        Ok(())
    }

    async fn inner_async_main(&mut self, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = self.router_pipe.recv() => {
                    debug!("received message: {}", msg);
                    if msg.msg.msg_name() == MessageName::Ping {
                        let ping_msg: &messages::common::Ping = self.msg_reg.try_cast_msg(&msg)?;
                        if matches!(ping_msg, messages::common::Ping::Ping) {
                            let pong_msg = handle_ping_message(&msg, ping_msg)?;
                            self.router_pipe.send(pong_msg).await?;
                            continue;
                        }
                    } else if msg.msg.msg_name() == MessageName::OperatorShutdown {
                        debug!("exchange shutting down");
                        self.handle_operator_shutdown(&msg).await?;
                        break;
                    }

                    let routed = self.route_msg(&msg).await?;
                    if !routed {
                        debug!("message ignored: {}", msg);
                    }
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn route_msg(&mut self, msg: &Message) -> Result<bool> {
        let msg_reg = self.msg_reg.clone();
        match msg.msg.msg_name() {
            MessageName::ExchangeRequests => {
                let cast_msg: &messages::exchange::ExchangeRequests = msg_reg.try_cast_msg(msg)?;
                match cast_msg {
                    messages::exchange::ExchangeRequests::SendRecordRequest {
                        record_id,
                        record,
                        table_aliases,
                    } => {
                        self.handle_send_record_request(
                            msg,
                            record_id,
                            record.clone(),
                            table_aliases,
                        )
                        .await?;
                        Ok(true)
                    }
                    messages::exchange::ExchangeRequests::GetNextRecordRequest { operator_id } => {
                        self.handle_get_next_record_request(msg, operator_id)
                            .await?;
                        Ok(true)
                    }
                    messages::exchange::ExchangeRequests::OperatorCompletedRecordProcessingRequest {
                        operator_id,
                        record_id,
                    } => {
                        self.handle_operator_completed_record_processing_request(
                            msg,
                            operator_id,
                            record_id,
                        )
                        .await?;
                        Ok(true)
                    }
                    _ => Ok(false),
                }
            }
            MessageName::ExchangeOperatorStatusChange => {
                self.handle_operator_status_change(msg).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn handle_operator_shutdown(&mut self, msg: &Message) -> Result<()> {
        let _: &messages::operator::Shutdown = self.msg_reg.try_cast_msg(msg)?;

        // reply early
        let resp_msg = msg.reply(Box::new(messages::common::GenericResponse::Ok));
        self.router_pipe.send(resp_msg).await?;

        // TODO: do some cleanup here...
        Ok(())
    }

    async fn handle_operator_status_change(&mut self, msg: &Message) -> Result<()> {
        let cast_msg: &messages::exchange::OperatorStatusChange = self.msg_reg.try_cast_msg(msg)?;

        // reply early
        let resp_msg = msg.reply(Box::new(messages::common::GenericResponse::Ok));
        self.router_pipe.send(resp_msg).await?;

        match cast_msg {
            messages::exchange::OperatorStatusChange::Complete { operator_id } => {
                self.inbound_producer_operator_states
                    .iter_mut()
                    .filter(|item| item.operator_id == *operator_id)
                    .for_each(|item| item.status = Status::Complete);
                debug!(
                    operator_id = operator_id,
                    states = format!("{:?}", self.inbound_producer_operator_states),
                    "operator state updated",
                );
                if !self
                    .inbound_producer_operator_states
                    .iter()
                    .find(|item| item.status != Status::Complete)
                    .is_some()
                {
                    self.received_all_data_from_producers = true;
                }
            }
        }

        Ok(())
    }

    async fn handle_operator_completed_record_processing_request(
        &mut self,
        msg: &Message,
        operator_id: &String,
        record_id: &u64,
    ) -> Result<()> {
        self.record_pool
            .operator_completed_record_processing(operator_id, record_id)?;

        let resp_msg = msg.reply(Box::new(
            ExchangeRequests::OperatorCompletedRecordProcessingResponse,
        ));
        self.router_pipe.send(resp_msg).await?;

        Ok(())
    }

    async fn handle_send_record_request(
        &mut self,
        msg: &Message,
        record_id: &u64,
        record: Arc<arrow::array::RecordBatch>,
        table_aliases: &Vec<Vec<String>>,
    ) -> Result<()> {
        debug!(
            record_id = *record_id,
            num_rows = record.num_rows(),
            "received record",
        );
        self.record_pool
            .add_record(record_id.clone(), record.clone(), table_aliases.clone());

        let resp_msg = msg.reply(Box::new(
            messages::exchange::ExchangeRequests::SendRecordResponse {
                record_id: record_id.clone(),
            },
        ));
        self.router_pipe.send(resp_msg).await?;

        Ok(())
    }

    async fn handle_get_next_record_request(
        &mut self,
        msg: &Message,
        operator_id: &String,
    ) -> Result<()> {
        let op_in_id = if let Some(id) = &msg.sent_from_operation_id {
            id.clone()
        } else {
            return Err(ExchangeOperatorError::OperationInstanceIdNotSetOnMessage.into());
        };

        let rec_res = self.record_pool.get_next_record(operator_id, op_in_id)?;
        match rec_res {
            Some((record_id, record, table_aliases)) => {
                let resp_msg = msg.reply(Box::new(
                    messages::exchange::ExchangeRequests::GetNextRecordResponseRecord {
                        record_id,
                        record,
                        table_aliases,
                    },
                ));
                self.router_pipe.send(resp_msg).await?;
            }
            None => {
                if self.received_all_data_from_producers {
                    let resp_msg = msg.reply(Box::new(
                        messages::exchange::ExchangeRequests::GetNextRecordResponseNoneLeft,
                    ));
                    self.router_pipe.send(resp_msg).await?;
                } else {
                    let resp_msg = msg.reply(Box::new(
                        messages::exchange::ExchangeRequests::GetNextRecordResponseNoneAvailable,
                    ));
                    self.router_pipe.send(resp_msg).await?;
                }
            }
        }

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
    fn consumes_message(&self, msg: &Message) -> bool {
        if msg.route_to_operation_id.is_none() && msg.sent_from_query_id.is_none() {
            return false;
        } else if (msg.route_to_operation_id.is_some()
            && msg.route_to_operation_id != Some(self.operator_instance_id))
            || (msg.sent_from_query_id.is_some() && msg.sent_from_query_id != Some(self.query_id))
        {
            return false;
        }

        match msg.msg.msg_name() {
            MessageName::Ping => match self.msg_reg.try_cast_msg::<messages::common::Ping>(msg) {
                Ok(messages::common::Ping::Ping) => true,
                Ok(messages::common::Ping::Pong) => false,
                Err(err) => {
                    error!("{}", err);
                    false
                }
            },
            MessageName::ExchangeRequests => {
                match self
                    .msg_reg
                    .try_cast_msg::<messages::exchange::ExchangeRequests>(msg)
                {
                    Ok(messages::exchange::ExchangeRequests::SendRecordRequest { .. }) => true,
                    Ok(messages::exchange::ExchangeRequests::GetNextRecordRequest { .. }) => true,
                    Ok(messages::exchange::ExchangeRequests::OperatorCompletedRecordProcessingRequest { .. }) => true,
                    Err(err) => {
                        error!("{}", err);
                        false
                    }
                    _ => false,
                }
            }
            MessageName::ExchangeOperatorStatusChange => true,
            MessageName::OperatorShutdown => true,
            _ => false,
        }
    }
}

impl MessageReceiver for ExchangeOperatorSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}

//////////////////////////////////////////////////////
// Record Pool

#[derive(Debug, Error)]
pub enum RecordPoolError {
    #[error("operator does not exist: {0}")]
    OperatorDoesNotExist(String),
    #[error("record does not exist: {0}")]
    RecordDoesNotExist(u64),
    #[error("record {0} processing metrics does not exist for operator {1}")]
    RecordProcessingMetricsDoesNotExist(u64, String),
    #[error("reserved record instance missing for operator: {0}")]
    ReservedRecordInstanceMissingForOperator(String),
    #[error("record {0} already processed by operator {1}")]
    RecordAlreadyProcessedByOperator(u64, String),
}

#[derive(Debug)]
struct RecordRef {
    id: u64,
    record: Arc<arrow::array::RecordBatch>,
    table_aliases: Vec<Vec<String>>,
    processed_by_operators: Vec<String>,
}

#[derive(Debug)]
struct ReservedRecord {
    record_id: u64,
    operator_instance_id: u128,
    reserved_time: chrono::DateTime<chrono::Utc>,
    last_heartbeat_time: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug)]
struct RecordProcessingMetrics {
    failure_count: u32,
}

#[derive(Debug)]
struct OperatorRecordQueue {
    operator_id: String,
    records_to_process: std::collections::VecDeque<u64>,
    records_reserved_by_operator: std::collections::HashMap<u64, ReservedRecord>,
    record_processing_metrics: std::collections::HashMap<u64, RecordProcessingMetrics>,
}

#[derive(Debug)]
struct RecordPoolConfig {
    max_heartbeat_interval: chrono::Duration,
}

#[derive(Debug)]
struct RecordPool {
    records: std::collections::HashMap<u64, RecordRef>,
    operator_record_queues: Vec<OperatorRecordQueue>,
    operator_ids: Vec<String>,

    config: RecordPoolConfig,
}

impl RecordPool {
    fn new(mut operator_ids: Vec<String>, config: RecordPoolConfig) -> RecordPool {
        operator_ids.sort();
        RecordPool {
            records: std::collections::HashMap::new(),
            operator_record_queues: operator_ids
                .iter()
                .map(|item| OperatorRecordQueue {
                    operator_id: item.clone(),
                    records_to_process: std::collections::VecDeque::new(),
                    records_reserved_by_operator: std::collections::HashMap::new(),
                    record_processing_metrics: std::collections::HashMap::new(),
                })
                .collect(),
            operator_ids,
            config,
        }
    }

    fn add_record(
        &mut self,
        record_id: u64,
        record: Arc<arrow::array::RecordBatch>,
        table_aliases: Vec<Vec<String>>,
    ) {
        self.records.insert(
            record_id,
            RecordRef {
                id: record_id,
                record,
                table_aliases,
                processed_by_operators: Vec::new(),
            },
        );
        self.operator_record_queues.iter_mut().for_each(|item| {
            item.records_to_process.push_back(record_id.clone());
        })
    }

    fn get_next_record(
        &mut self,
        operator_id: &String,
        operator_instance_id: u128,
    ) -> Result<Option<(u64, Arc<arrow::array::RecordBatch>, Vec<Vec<String>>)>> {
        let op_queue = if let Some(queue) = self
            .operator_record_queues
            .iter_mut()
            .find(|item| item.operator_id == *operator_id)
        {
            queue
        } else {
            return Err(RecordPoolError::OperatorDoesNotExist(operator_id.clone()).into());
        };

        let record_id = op_queue.records_to_process.pop_front();
        match record_id {
            Some(record_id) => {
                let record = if let Some(rec) = self.records.get(&record_id) {
                    rec
                } else {
                    panic!("unable to find record in pool but it should exist");
                };

                // store a reserved record
                op_queue.records_reserved_by_operator.insert(
                    record_id,
                    ReservedRecord {
                        record_id,
                        operator_instance_id,
                        reserved_time: chrono::Utc::now(),
                        last_heartbeat_time: None,
                    },
                );
                op_queue
                    .record_processing_metrics
                    .insert(record_id, RecordProcessingMetrics { failure_count: 0 });

                Ok(Some((
                    record_id,
                    record.record.clone(),
                    record.table_aliases.clone(),
                )))
            }
            None => Ok(None),
        }
    }

    fn update_reserved_record_heartbeat(&mut self, operator_id: &String, record_id: u64) {
        self.operator_record_queues
            .iter_mut()
            .filter(|item| item.operator_id == *operator_id)
            .for_each(|item| {
                item.records_reserved_by_operator
                    .iter_mut()
                    .filter(|item2| item2.1.record_id == record_id)
                    .take(1)
                    .for_each(|item2| {
                        item2.1.last_heartbeat_time = Some(chrono::Utc::now());
                    });
            })
    }

    fn operator_completed_record_processing(
        &mut self,
        operator_id: &String,
        record_id: &u64,
    ) -> Result<()> {
        let op_queue = if let Some(queue) = self
            .operator_record_queues
            .iter_mut()
            .find(|item| item.operator_id == *operator_id)
        {
            queue
        } else {
            return Err(RecordPoolError::OperatorDoesNotExist(operator_id.clone()).into());
        };

        let reserved_record = op_queue.records_reserved_by_operator.remove(record_id);
        if reserved_record.is_none() {
            return Err(RecordPoolError::ReservedRecordInstanceMissingForOperator(
                operator_id.clone(),
            )
            .into());
        }
        op_queue.record_processing_metrics.remove(record_id);

        let rec_ref = self.records.get_mut(record_id);
        match rec_ref {
            Some(rec_ref) => {
                let already_finished = rec_ref
                    .processed_by_operators
                    .iter()
                    .find(|&item| *item == *operator_id)
                    .is_some();
                if already_finished {
                    // this should never be the case
                    return Err(RecordPoolError::RecordAlreadyProcessedByOperator(
                        record_id.clone(),
                        operator_id.clone(),
                    )
                    .into());
                }

                rec_ref.processed_by_operators.push(operator_id.clone());

                if rec_ref.processed_by_operators.len() == self.operator_ids.len() {
                    let mut pbo = rec_ref.processed_by_operators.clone();
                    pbo.sort();
                    if pbo == self.operator_ids {
                        self.records.remove(record_id);
                    }
                }

                Ok(())
            }
            None => Err(RecordPoolError::RecordDoesNotExist(record_id.clone()).into()),
        }
    }

    fn maintain(&mut self) -> Result<()> {
        self.requeue_reserved_records_with_stale_heartbeat()?;
        Ok(())
    }

    fn requeue_reserved_records_with_stale_heartbeat(&mut self) -> Result<()> {
        self.operator_record_queues.iter_mut().try_for_each(|q| {
            q.records_reserved_by_operator
                .iter()
                .filter(|res_rec| {
                    if let Some(last_heartbeat_time) = res_rec.1.last_heartbeat_time {
                        chrono::Utc::now().signed_duration_since(last_heartbeat_time)
                            > self.config.max_heartbeat_interval
                    } else {
                        false
                    }
                })
                .map(|res_rec| res_rec.0.clone())
                .collect::<Vec<u64>>()
                .iter()
                .try_for_each(|res_rec_id| {
                    q.records_reserved_by_operator.remove(res_rec_id);
                    q.records_to_process.push_front(res_rec_id.clone());
                    if let Some(metrics) = q.record_processing_metrics.get_mut(&res_rec_id) {
                        metrics.failure_count += 1;
                        Ok(())
                    } else {
                        Err(RecordPoolError::RecordProcessingMetricsDoesNotExist(
                            res_rec_id.clone(),
                            q.operator_id.clone(),
                        )
                        .into())
                    }
                })
        })
    }
}
