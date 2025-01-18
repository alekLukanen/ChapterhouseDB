use std::sync::Arc;
use std::u64;

use anyhow::{Context, Result};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

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

        debug!(
            "started the exchange operator for instance {}",
            self.operator_instance_config.id
        );

        loop {
            tokio::select! {
                Some(msg) = self.router_pipe.recv() => {
                    debug!("received message: {}", msg);
                    if msg.msg.msg_name() == MessageName::Ping {
                        let ping_msg: &Ping = self.msg_reg.try_cast_msg(&msg)?;
                        if matches!(ping_msg, Ping::Ping) {
                            let pong_msg = handle_ping_message(&msg, ping_msg)?;
                            self.router_pipe.send(pong_msg).await?;
                            continue;
                        }
                    }

                    match msg.msg.msg_name() {
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

        debug!(
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
            MessageName::Ping => match self.msg_reg.try_cast_msg::<Ping>(msg) {
                Ok(Ping::Ping) => true,
                Ok(Ping::Pong) => false,
                Err(err) => {
                    error!("{}", err);
                    false
                }
            },
            MessageName::StoreRecordBatch => {
                match self.msg_reg.try_cast_msg::<StoreRecordBatch>(msg) {
                    Ok(StoreRecordBatch::RequestSendRecord { .. }) => true,
                    Ok(StoreRecordBatch::ResponseReceivedRecord { .. }) => false,
                    Err(err) => {
                        error!("{}", err);
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

struct RecordRef {
    id: u64,
    record: arrow::array::RecordBatch,
    processed_by_operators: Vec<String>,
}

struct ReservedRecord {
    record_id: u64,
    operator_instance_id: u128,
    reserved_time: chrono::DateTime<chrono::Utc>,
    last_heartbeat_time: Option<chrono::DateTime<chrono::Utc>>,
}

struct RecordProcessingMetrics {
    failure_count: u32,
}

struct OperatorRecordQueue {
    operator_id: String,
    records_to_process: std::collections::VecDeque<u64>,
    records_reserved_by_operator: std::collections::HashMap<u64, ReservedRecord>,
    record_processing_metrics: std::collections::HashMap<u64, RecordProcessingMetrics>,
}

struct RecordPoolConfig {
    max_heartbeat_interval: chrono::Duration,
}

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

    fn add_record(&mut self, record_id: u64, record: arrow::array::RecordBatch) {
        self.records.insert(
            record_id,
            RecordRef {
                id: record_id,
                record,
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
    ) -> Result<Option<(u64, arrow::array::RecordBatch)>> {
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

                Ok(Some((record_id, record.record.clone())))
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
        record_id: u64,
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

        let reserved_record = op_queue.records_reserved_by_operator.remove(&record_id);
        if reserved_record.is_none() {
            return Err(RecordPoolError::ReservedRecordInstanceMissingForOperator(
                operator_id.clone(),
            )
            .into());
        }
        op_queue.record_processing_metrics.remove(&record_id);

        let rec_ref = self.records.get_mut(&record_id);
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
                        record_id,
                        operator_id.clone(),
                    )
                    .into());
                }

                rec_ref.processed_by_operators.push(operator_id.clone());

                if rec_ref.processed_by_operators.len() == self.operator_ids.len() {
                    let mut pbo = rec_ref.processed_by_operators.clone();
                    pbo.sort();
                    if pbo == self.operator_ids {
                        self.records.remove(&record_id);
                    }
                }

                Ok(())
            }
            None => Err(RecordPoolError::RecordDoesNotExist(record_id).into()),
        }
    }

    fn maintain(&mut self) -> Result<()> {
        self.requeue_reserved_records_with_stale_heartbeat()?;
        Ok(())
    }

    fn requeue_reserved_records_with_stale_heartbeat(&mut self) -> Result<()> {
        self.operator_record_queues.iter_mut().for_each(|q| {
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
                .for_each(|res_rec_id| {
                    q.records_reserved_by_operator.remove(res_rec_id);
                    q.records_to_process.push_front(res_rec_id.clone());
                    if let Some(metrics) = q.record_processing_metrics.get_mut(&res_rec_id) {
                        metrics.failure_count += 1;
                    }
                });
        });
        return Err(
            RecordPoolError::RecordProcessingMetricsDoesNotExist(res_rec_id.clone()).into(),
        );
    }
}
