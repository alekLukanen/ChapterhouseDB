use std::sync::Arc;
use std::u64;

use anyhow::Result;
use rand::{Rng, SeedableRng};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info};

use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::exchange::ExchangeRequests;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::message_handler::{MessageRegistry, Pipe};
use crate::handlers::message_router_handler::{
    MessageConsumer, MessageReceiver, MessageRouterState, Subscriber,
};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;
use crate::handlers::operator_handler::operators::common_message_handlers::handle_ping_message;
use crate::handlers::operator_handler::operators::requests;
use crate::planner;

#[derive(Debug, Error)]
pub enum ExchangeOperatorError {
    #[error("invalid operator type: {0}")]
    InvalidOperatorType(String),
    #[error("operation instance id not set on message")]
    OperationInstanceIdNotSetOnMessage,
    #[error("timed out waiting for task to close")]
    TimedOutWaitingForTaskToClose,
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
    record_pool: Arc<Mutex<RecordPool>>,
    inbound_producer_operator_states: Vec<ProducerOperatorStatus>,
    received_all_data_from_producers: bool,

    operator_instance_config: OperatorInstanceConfig,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,

    tt: TaskTracker,
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

        let (outbound_producer_ids, record_queue_configs, inbound_producer_ids) =
            match &op_in_config.operator.operator_type {
                planner::OperatorType::Exchange {
                    outbound_producer_ids,
                    inbound_producer_ids,
                    record_queue_configs,
                    ..
                } => (
                    outbound_producer_ids.clone(),
                    record_queue_configs.clone(),
                    inbound_producer_ids.clone(),
                ),
                planner::OperatorType::Producer { .. } => {
                    return Err(ExchangeOperatorError::InvalidOperatorType(
                        op_in_config.operator.operator_type.name().to_string(),
                    )
                    .into());
                }
            };

        let record_pool = RecordPool::new(
            outbound_producer_ids,
            record_queue_configs,
            RecordPoolConfig {
                max_heartbeat_interval: chrono::Duration::seconds(1),
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
            record_pool: Arc::new(Mutex::new(record_pool)),
            inbound_producer_operator_states: operator_statuses,
            received_all_data_from_producers: false,
            operator_instance_config: op_in_config,
            message_router_state,
            router_pipe: pipe,
            sender,
            msg_reg,
            tt: TaskTracker::new(),
        })
    }

    fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(ExchangeOperatorSubscriber {
            sender: self.sender.clone(),
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

        let exchange_ct = ct.child_token();

        let maintainer_ct = exchange_ct.clone();
        let maintainer_record_pool = self.record_pool.clone();
        self.tt.spawn(async move {
            let maintainer = RecordPoolMaintainer::new(
                maintainer_record_pool,
                chrono::Duration::milliseconds(100),
            );
            if let Err(err) = maintainer.async_main(maintainer_ct).await {
                error!("{}", err);
            }
        });

        let res = self.inner_async_main(exchange_ct.clone()).await;

        exchange_ct.cancel();

        let ref mut pipe = self.router_pipe;
        let status_change_res = match res {
            Ok(_) => {
                let req_res =
                    requests::operator::OperatorInstanceStatusChangeRequest::completed_request(
                        pipe,
                        self.msg_reg.clone(),
                    )
                    .await;
                req_res
            }
            Err(err) => {
                let req_res =
                    requests::operator::OperatorInstanceStatusChangeRequest::errored_request(
                        err.to_string(),
                        pipe,
                        self.msg_reg.clone(),
                    )
                    .await;
                req_res
            }
        };
        if let Err(err) = status_change_res {
            error!("{:?}", err);
        }

        self.message_router_state
            .lock()
            .await
            .remove_internal_subscriber(&self.operator_instance_config.id);

        self.tt.close();
        tokio::select! {
            _ = self.tt.wait() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                return Err(ExchangeOperatorError::TimedOutWaitingForTaskToClose.into());
            }
        }

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
                        queue_name,
                        record_id,
                        record,
                        table_aliases,
                    } => {
                        self.handle_send_record_request(
                            msg,
                            queue_name,
                            record_id,
                            record.clone(),
                            table_aliases,
                        )
                        .await?;
                        Ok(true)
                    }
                    messages::exchange::ExchangeRequests::GetNextRecordRequest { operator_id, queue_name } => {
                        self.handle_get_next_record_request(msg, operator_id, queue_name)
                            .await?;
                        Ok(true)
                    }
                    messages::exchange::ExchangeRequests::OperatorCompletedRecordProcessingRequest {
                        operator_id,
                        record_id,
                        queue_name,
                    } => {
                        self.handle_operator_completed_record_processing_request(
                            msg,
                            operator_id,
                            queue_name,
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
            MessageName::ExchangeRecordHeartbeat => {
                self.handle_exchange_record_heartbeat(msg).await?;
                Ok(true)
            }
            MessageName::ExchangeCreateTransaction => {
                self.handle_exchange_create_transaction(msg).await?;
                Ok(true)
            }
            MessageName::ExchangeTransactionHeartbeat => {
                self.handle_exchange_transaction_heartbeat(msg).await?;
                Ok(true)
            }
            MessageName::ExchangeInsertTransactionRecord => {
                self.handle_exchange_insert_transaction_record(msg).await?;
                Ok(true)
            }
            MessageName::ExchangeCommitTransaction => {
                self.handle_exchange_commit_transaction(msg).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn handle_exchange_create_transaction(&mut self, msg: &Message) -> Result<()> {
        let req_msg: &messages::exchange::CreateTransaction = self.msg_reg.try_cast_msg(msg)?;

        let transaction_id = self
            .record_pool
            .lock()
            .await
            .create_transaction(&req_msg.key);

        let resp_msg = msg.reply(Box::new(
            messages::exchange::CreateTransactionResponse::Ok { transaction_id },
        ));
        self.router_pipe.send(resp_msg).await?;

        Ok(())
    }

    async fn handle_exchange_transaction_heartbeat(&mut self, msg: &Message) -> Result<()> {
        let req_msg: &messages::exchange::TransactionHeartbeat = self.msg_reg.try_cast_msg(msg)?;

        match req_msg {
            messages::exchange::TransactionHeartbeat::Ping { transaction_id } => {
                let res = self
                    .record_pool
                    .lock()
                    .await
                    .update_transaction_heartbeat(transaction_id);

                let resp_msg = match res {
                    Ok(_) => msg.reply(Box::new(messages::common::GenericResponse::Ok)),
                    Err(err) => msg.reply(Box::new(messages::common::GenericResponse::Error(
                        err.to_string(),
                    ))),
                };
                self.router_pipe.send(resp_msg).await?;
            }
        }

        Ok(())
    }

    async fn handle_exchange_insert_transaction_record(&mut self, msg: &Message) -> Result<()> {
        let req_msg: &messages::exchange::InsertTransactionRecord =
            self.msg_reg.try_cast_msg(msg)?;

        let res = self.record_pool.lock().await.insert_transaction_record(
            &req_msg.transaction_id,
            req_msg.queue_name.clone(),
            req_msg.deduplication_key.clone(),
            req_msg.record.clone(),
            req_msg.table_aliases.clone(),
        );

        let resp_msg = match res {
            Ok(_) => msg.reply(Box::new(
                messages::exchange::InsertTransactionRecordResponse::Ok {
                    deduplication_key: req_msg.deduplication_key.clone(),
                },
            )),
            Err(err) => msg.reply(Box::new(
                messages::exchange::InsertTransactionRecordResponse::Err(err.to_string()),
            )),
        };
        self.router_pipe.send(resp_msg).await?;

        Ok(())
    }

    async fn handle_exchange_commit_transaction(&mut self, msg: &Message) -> Result<()> {
        let req_msg: &messages::exchange::CommitTransaction = self.msg_reg.try_cast_msg(msg)?;

        let res = self
            .record_pool
            .lock()
            .await
            .commit_transaction(&req_msg.transaction_id);

        let resp_msg = match res {
            Ok(_) => msg.reply(Box::new(messages::exchange::CommitTransactionResponse::Ok)),
            Err(err) => msg.reply(Box::new(
                messages::exchange::CommitTransactionResponse::Error(err.to_string()),
            )),
        };
        self.router_pipe.send(resp_msg).await?;

        Ok(())
    }

    async fn handle_operator_shutdown(&mut self, msg: &Message) -> Result<()> {
        let _: &messages::operator::Shutdown = self.msg_reg.try_cast_msg(msg)?;

        // reply early
        let resp_msg = msg.reply(Box::new(messages::common::GenericResponse::Ok));
        self.router_pipe.send(resp_msg).await?;

        // TODO: do some cleanup here...
        // for example, cleaning up data if any has been stored
        Ok(())
    }

    async fn handle_exchange_record_heartbeat(&mut self, msg: &Message) -> Result<()> {
        let cast_msg: &messages::exchange::RecordHeartbeat = self.msg_reg.try_cast_msg(msg)?;
        match cast_msg {
            messages::exchange::RecordHeartbeat::Ping {
                queue_name,
                operator_id,
                record_id,
            } => {
                self.record_pool
                    .lock()
                    .await
                    .update_reserved_record_heartbeat(queue_name, operator_id, record_id);

                self.router_pipe
                    .send(msg.reply(Box::new(messages::common::GenericResponse::Ok)))
                    .await?;
            }
        }
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
        queue_name: &String,
        record_id: &u64,
    ) -> Result<()> {
        self.record_pool
            .lock()
            .await
            .operator_completed_record_processing(operator_id, queue_name, record_id)?;

        let resp_msg = msg.reply(Box::new(
            ExchangeRequests::OperatorCompletedRecordProcessingResponse,
        ));
        self.router_pipe.send(resp_msg).await?;

        Ok(())
    }

    async fn handle_send_record_request(
        &mut self,
        msg: &Message,
        queue_name: &String,
        record_id: &u64,
        record: Arc<arrow::array::RecordBatch>,
        table_aliases: &Vec<Vec<String>>,
    ) -> Result<()> {
        debug!(
            record_id = *record_id,
            num_rows = record.num_rows(),
            "received record",
        );
        self.record_pool.lock().await.add_record(
            queue_name.clone(),
            record_id.clone(),
            record.clone(),
            table_aliases.clone(),
        );

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
        queue_name: &String,
    ) -> Result<()> {
        let op_in_id = if let Some(id) = &msg.sent_from_operation_id {
            id.clone()
        } else {
            return Err(ExchangeOperatorError::OperationInstanceIdNotSetOnMessage.into());
        };

        let rec_res =
            self.record_pool
                .lock()
                .await
                .get_next_record(operator_id, queue_name, op_in_id)?;

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
            MessageName::Ping => true,
            MessageName::ExchangeRequests => true,
            MessageName::ExchangeOperatorStatusChange => true,
            MessageName::OperatorShutdown => true,
            MessageName::CommonGenericResponse => true,
            MessageName::ExchangeRecordHeartbeat => true,
            MessageName::ExchangeCreateTransaction => true,
            MessageName::ExchangeInsertTransactionRecord => true,
            MessageName::ExchangeTransactionHeartbeat => true,
            MessageName::ExchangeCommitTransaction => true,
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
    #[error("transaction {0} does not exist")]
    TransactionDoesNotExist(u64),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
struct OperatorQueue {
    operator_id: String,
    queue_name: String,
}

#[derive(Debug)]
struct RecordRef {
    id: u64,
    record: Arc<arrow::array::RecordBatch>,
    table_aliases: Vec<Vec<String>>,
    processed_by_operator_queues: Vec<OperatorQueue>,
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
struct InsertRecord {
    queue_name: String,
    record: RecordRef,
}

#[derive(Debug)]
struct Transaction {
    key: String,

    inserts: std::collections::HashMap<u64, InsertRecord>,
    insert_deduplication_keys: std::collections::HashSet<String>,
    insert_queue_record_idxs: std::collections::HashMap<String, u64>,

    last_heartbeat_time: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug)]
struct OperatorRecordQueue {
    operator_id: String,
    queue_name: String,
    input_queue_names: Vec<String>,
    sampling_method: planner::ExchangeRecordQueueSamplingMethod,

    rows_inserted: usize,
    rows_ignored: usize,

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
    records: std::collections::HashMap<(String, u64), RecordRef>,
    operator_record_queues: Vec<OperatorRecordQueue>,
    operator_queues: Vec<OperatorQueue>,

    transactions: std::collections::HashMap<u64, Transaction>,
    transaction_idx: u64,

    config: RecordPoolConfig,
    rng: rand::rngs::StdRng,
}

impl RecordPool {
    fn new(
        mut operator_ids: Vec<String>,
        queue_configs: Vec<planner::ExchangeRecordQueueConfig>,
        config: RecordPoolConfig,
    ) -> RecordPool {
        operator_ids.sort();
        RecordPool {
            records: std::collections::HashMap::new(),
            operator_record_queues: queue_configs
                .iter()
                .map(|qc| OperatorRecordQueue {
                    operator_id: qc.producer_id.clone(),
                    queue_name: qc.queue_name.clone(),
                    input_queue_names: qc.input_queue_names.clone(),
                    sampling_method: qc.sampling_method.clone(),
                    rows_inserted: 0,
                    rows_ignored: 0,
                    records_to_process: std::collections::VecDeque::new(),
                    records_reserved_by_operator: std::collections::HashMap::new(),
                    record_processing_metrics: std::collections::HashMap::new(),
                })
                .collect(),
            operator_queues: queue_configs
                .iter()
                .map(|qc| OperatorQueue {
                    operator_id: qc.producer_id.clone(),
                    queue_name: qc.queue_name.clone(),
                })
                .collect(),
            transactions: std::collections::HashMap::new(),
            transaction_idx: 0,
            config,
            rng: rand::rngs::StdRng::from_entropy(),
        }
    }

    /// create a transaction by key
    fn create_transaction(&mut self, key: &String) -> u64 {
        let transaction = Transaction {
            key: key.clone(),
            inserts: std::collections::HashMap::new(),
            insert_deduplication_keys: std::collections::HashSet::new(),
            insert_queue_record_idxs: std::collections::HashMap::new(),
            last_heartbeat_time: None,
        };
        let transaction_idx = self.transaction_idx;

        self.transactions.insert(transaction_idx, transaction);
        self.transaction_idx += 1;

        transaction_idx
    }

    fn insert_transaction_record(
        &mut self,
        transaction_id: &u64,
        queue_name: String,
        deduplication_key: String,
        record: Arc<arrow::array::RecordBatch>,
        table_aliases: Vec<Vec<String>>,
    ) -> Result<()> {
        let transaction = if let Some(t) = self.transactions.get_mut(transaction_id) {
            t
        } else {
            return Err(RecordPoolError::TransactionDoesNotExist(*transaction_id).into());
        };

        let record_id = if let Some(idx) = transaction.insert_queue_record_idxs.get_mut(&queue_name)
        {
            let current_idx = idx.clone();
            *idx += 1;
            current_idx
        } else {
            transaction
                .insert_queue_record_idxs
                .insert(queue_name.clone(), 0);
            0
        };

        let insert = InsertRecord {
            queue_name,
            record: RecordRef {
                id: record_id,
                record,
                table_aliases,
                processed_by_operator_queues: Vec::new(),
            },
        };
        if !transaction
            .insert_deduplication_keys
            .contains(&deduplication_key)
        {
            transaction.inserts.insert(record_id.clone(), insert);
            transaction
                .insert_deduplication_keys
                .insert(deduplication_key);
        }

        Ok(())
    }

    fn commit_transaction(&mut self, transaction_id: &u64) -> Result<()> {
        let transaction = if let Some(t) = self.transactions.remove(transaction_id) {
            t
        } else {
            return Err(RecordPoolError::TransactionDoesNotExist(*transaction_id).into());
        };

        for (_, insert) in transaction.inserts.iter() {
            self.add_record(
                insert.queue_name.clone(),
                insert.record.id.clone(),
                insert.record.record.clone(),
                insert.record.table_aliases.clone(),
            );
        }

        Ok(())
    }

    fn update_transaction_heartbeat(&mut self, transaction_id: &u64) -> Result<()> {
        let transaction = if let Some(t) = self.transactions.get_mut(transaction_id) {
            t
        } else {
            return Err(RecordPoolError::TransactionDoesNotExist(*transaction_id).into());
        };

        transaction.last_heartbeat_time = Some(chrono::Utc::now());

        Ok(())
    }

    /// Add a record if it hasn't already been added. Records
    /// can only be added once in order to prevent duplication
    /// of work for the same record.
    fn add_record(
        &mut self,
        queue_name: String,
        record_id: u64,
        record: Arc<arrow::array::RecordBatch>,
        table_aliases: Vec<Vec<String>>,
    ) -> bool {
        let num_rows = record.num_rows();

        if !self
            .records
            .contains_key(&(queue_name.clone(), record_id.clone()))
        {
            for queue in &mut self.operator_record_queues {
                if !((queue.queue_name == queue_name)
                    || queue
                        .input_queue_names
                        .iter()
                        .find(|item| **item == queue_name)
                        .is_some())
                {
                    continue;
                }

                let insert = match &queue.sampling_method {
                    planner::ExchangeRecordQueueSamplingMethod::All => true,
                    planner::ExchangeRecordQueueSamplingMethod::PercentageWithReserve {
                        sample_rate,
                        min_rows,
                    } => {
                        if queue.rows_inserted > *min_rows {
                            if self.rng.gen::<f32>() < *sample_rate {
                                true
                            } else {
                                false
                            }
                        } else {
                            true
                        }
                    }
                };

                info!(
                    queue_name = queue.queue_name,
                    record_id = record_id,
                    insert = insert,
                    "adding record to sample.....",
                );

                if insert {
                    self.records.insert(
                        (queue.queue_name.clone(), record_id.clone()),
                        RecordRef {
                            id: record_id,
                            record: record.clone(),
                            table_aliases: table_aliases.clone(),
                            processed_by_operator_queues: Vec::new(),
                        },
                    );
                    queue.records_to_process.push_back(record_id.clone());
                    queue.rows_inserted += num_rows;
                } else {
                    queue.rows_ignored += num_rows;
                }
            }
            true
        } else {
            false
        }
    }

    fn get_next_record(
        &mut self,
        operator_id: &String,
        queue_name: &String,
        operator_instance_id: u128,
    ) -> Result<Option<(u64, Arc<arrow::array::RecordBatch>, Vec<Vec<String>>)>> {
        info!(
            operator_id = operator_id,
            queue_name = queue_name,
            operator_instance_id = operator_instance_id,
            "get_next_record"
        );

        let op_queue = if let Some(queue) = self
            .operator_record_queues
            .iter_mut()
            .find(|item| item.operator_id == *operator_id && item.queue_name == *queue_name)
        {
            queue
        } else {
            return Err(RecordPoolError::OperatorDoesNotExist(operator_id.clone()).into());
        };

        let record_id = op_queue.records_to_process.pop_front();

        info!(record_id = record_id, "popped front");
        match &record_id {
            Some(record_id) => {
                let record = if let Some(rec) = self.records.get(&(queue_name.clone(), *record_id))
                {
                    rec
                } else {
                    panic!("unable to find record in pool but it should exist");
                };

                // store a reserved record
                op_queue.records_reserved_by_operator.insert(
                    *record_id,
                    ReservedRecord {
                        record_id: *record_id,
                        operator_instance_id,
                        reserved_time: chrono::Utc::now(),
                        last_heartbeat_time: None,
                    },
                );
                op_queue
                    .record_processing_metrics
                    .insert(*record_id, RecordProcessingMetrics { failure_count: 0 });

                Ok(Some((
                    *record_id,
                    record.record.clone(),
                    record.table_aliases.clone(),
                )))
            }
            None => Ok(None),
        }
    }

    fn update_reserved_record_heartbeat(
        &mut self,
        queue_name: &String,
        operator_id: &String,
        record_id: &u64,
    ) {
        self.operator_record_queues
            .iter_mut()
            .filter(|item| item.queue_name == *queue_name || item.operator_id == *operator_id)
            .for_each(|item| {
                item.records_reserved_by_operator
                    .iter_mut()
                    .filter(|item2| item2.1.record_id == *record_id)
                    .take(1)
                    .for_each(|item2| {
                        item2.1.last_heartbeat_time = Some(chrono::Utc::now());
                    });
            })
    }

    fn operator_completed_record_processing(
        &mut self,
        operator_id: &String,
        queue_name: &String,
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

        let rec_ref = self.records.get_mut(&(queue_name.clone(), *record_id));
        match rec_ref {
            Some(rec_ref) => {
                let already_finished = rec_ref
                    .processed_by_operator_queues
                    .iter()
                    .find(|&item| {
                        item.operator_id == *operator_id && item.queue_name == *queue_name
                    })
                    .is_some();
                if already_finished {
                    // this should never be the case
                    return Err(RecordPoolError::RecordAlreadyProcessedByOperator(
                        record_id.clone(),
                        operator_id.clone(),
                    )
                    .into());
                }

                rec_ref.processed_by_operator_queues.push(OperatorQueue {
                    operator_id: operator_id.clone(),
                    queue_name: queue_name.clone(),
                });

                if rec_ref.processed_by_operator_queues.len() == self.operator_queues.len() {
                    let mut pbo = rec_ref.processed_by_operator_queues.clone();
                    pbo.sort();
                    if pbo == self.operator_queues {
                        self.records.remove(&(queue_name.clone(), *record_id));
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

////////////////////////////////////////////////
//

struct RecordPoolMaintainer {
    record_pool: Arc<Mutex<RecordPool>>,
    interval: chrono::TimeDelta,
}

impl RecordPoolMaintainer {
    fn new(
        record_pool: Arc<Mutex<RecordPool>>,
        interval: chrono::TimeDelta,
    ) -> RecordPoolMaintainer {
        RecordPoolMaintainer {
            record_pool,
            interval,
        }
    }

    async fn async_main(&self, exchange_ct: CancellationToken) -> Result<()> {
        debug!("started record pool maintainer");

        loop {
            tokio::select! {
                _ = exchange_ct.cancelled() => {
                    break;
                }
                _ = tokio::time::sleep(self.interval.to_std()?) => {
                    if let Err(err) = self.record_pool.lock().await.maintain() {
                        error!("{}", err);
                        exchange_ct.cancel();
                    }
                }
            }
        }

        debug!("stopped record pool maintainer");

        Ok(())
    }
}
