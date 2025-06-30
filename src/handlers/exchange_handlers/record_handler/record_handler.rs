use std::{collections::HashMap, sync, sync::Arc};

use anyhow::{anyhow, Result};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::handlers::{
    exchange_handlers::record_heartbeat_handler::RecordHeartbeatHandler,
    message_handler::{messages, MessageRegistry, Pipe},
    message_router_handler::MessageRouterState,
    operator_handler::{operators::requests, OperatorInstanceConfig},
};

use super::{
    drop_cancellation_token::DropCancellationToken,
    transaction_record_handler::TransactionRecordHandler,
};

#[derive(Debug, Error)]
pub enum RecordHandlerError {
    #[error("operator type not implemented: {0}")]
    OperatorTypeNotImplemented(String),
    #[error("inbound exchanges is empty")]
    InboundExhcnagesIsEmpty,
    #[error("outbound exchange is none")]
    OutboundExchangeIsNone,
    #[error("cancellation token cancelled")]
    CancellationTokenCancelled,
    #[error("unable to find tracked record {0}")]
    UnableToFindTrackedRecord(u64),
    #[error("unable to find exchange identity {0}")]
    UnableToFindExchangeIdentity(usize),
    #[error("timed out waiting for task to close")]
    TimedOutWaitingForTaskToClose,
    #[error("unable to complete record since heartbeat is not enabled")]
    UnableToCompleteRecordSinceHeartbeatIsNotEnabled,
    #[error("create transaction request returned error: {0}")]
    CreateTransactionRequestReturnedError(String),
}

#[derive(Debug, Clone)]
pub struct ExchangeIdentity {
    pub(crate) operator_id: String,
    pub(crate) worker_id: u128,
    pub(crate) operator_instance_id: u128,
}

#[derive(Debug, Clone)]
struct TrackedRecord {
    record_id: u64,
    exchange_identity_idx: usize,
    ct: CancellationToken,
}

pub struct ExchangeRecord {
    pub(crate) record_id: u64,
    pub(crate) record: Arc<arrow::array::RecordBatch>,
    pub(crate) table_aliases: Vec<Vec<String>>,
    pub(crate) queue_name: String,
}

impl ExchangeRecord {
    pub(crate) fn deduplication_key(&self) -> String {
        format!("{}-{}", self.queue_name.clone(), self.record_id.clone())
    }
}

pub(crate) struct RecordHandlerState {
    // exchanges can never be removed
    inbound_exchange_ids: Vec<String>,
    inbound_exchanges: Vec<ExchangeIdentity>,

    outbound_exchange_id: String,
    outbound_exchange: Option<ExchangeIdentity>,

    // tracking state ///////
    tracked_records: HashMap<u64, TrackedRecord>,
}

pub struct RecordHandler {
    pub(crate) query_handler_worker_id: u128,
    pub(crate) query_id: u128,
    pub(crate) operator_id: String,
    pub(crate) queue_name: String,
    pub(crate) create_heartbeat: bool,

    // config
    none_available_wait_time_in_ms: chrono::TimeDelta,

    msg_reg: Arc<MessageRegistry>,
    msg_router_state: Arc<Mutex<MessageRouterState>>,

    tt: tokio_util::task::TaskTracker,
    tracker_ct: DropCancellationToken,

    pub(crate) state: Arc<sync::Mutex<RecordHandlerState>>,
}

impl RecordHandler {
    pub async fn initiate(
        ct: CancellationToken,
        op_in_config: &OperatorInstanceConfig,
        queue_name: String,
        pipe: &mut Pipe,
        msg_reg: Arc<MessageRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> Result<RecordHandler> {
        let (inbound_exchange_ids, outbound_exchange_id) =
            match &op_in_config.operator.operator_type {
                crate::planner::OperatorType::Producer {
                    inbound_exchange_ids,
                    outbound_exchange_id,
                    ..
                } => (inbound_exchange_ids.clone(), outbound_exchange_id.clone()),
                crate::planner::OperatorType::Exchange { .. } => {
                    return Err(RecordHandlerError::OperatorTypeNotImplemented(format!(
                        "{:?}",
                        op_in_config.operator.operator_type
                    ))
                    .into());
                }
            };

        let mut rec_handler = RecordHandler {
            query_handler_worker_id: op_in_config.query_handler_worker_id.clone(),
            query_id: op_in_config.query_id.clone(),
            operator_id: op_in_config.operator.id.clone(),
            queue_name,
            create_heartbeat: true,
            none_available_wait_time_in_ms: chrono::Duration::milliseconds(50),
            msg_reg,
            msg_router_state,
            tt: tokio_util::task::TaskTracker::new(),
            tracker_ct: DropCancellationToken::new(ct.clone()),
            state: Arc::new(sync::Mutex::new(RecordHandlerState {
                inbound_exchange_ids,
                inbound_exchanges: Vec::new(),
                outbound_exchange_id,
                outbound_exchange: None,
                tracked_records: HashMap::new(),
            })),
        };
        rec_handler.find_inbound_exchanges(ct.clone(), pipe).await?;
        rec_handler.find_outbound_exchange(ct.clone(), pipe).await?;

        debug!("created the record handler");

        Ok(rec_handler)
    }

    pub fn disable_record_heartbeat(mut self) -> Self {
        self.create_heartbeat = false;
        self
    }

    pub(crate) fn get_outbound_exchange(&self) -> Result<ExchangeIdentity> {
        if let Some(exchange) = &self
            .state
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .outbound_exchange
        {
            Ok(exchange.clone())
        } else {
            Err(RecordHandlerError::OutboundExchangeIsNone.into())
        }
    }

    pub async fn create_outbound_transaction(
        self,
        pipe: &mut Pipe,
        key: String,
    ) -> Result<TransactionRecordHandler> {
        let outbound_exchange = self.get_outbound_exchange()?;

        let transaction_resp_msg =
            requests::exchange::CreateTransactionRequest::create_transaction_request(
                key,
                outbound_exchange.operator_instance_id.clone(),
                outbound_exchange.worker_id.clone(),
                pipe,
                self.msg_reg.clone(),
            )
            .await?;

        match transaction_resp_msg {
            messages::exchange::CreateTransactionResponse::Ok { transaction_id } => {
                let msg_reg = self.msg_reg.clone();
                let msg_router_state = self.msg_router_state.clone();
                let handler = TransactionRecordHandler::new(
                    self.tracker_ct.child_token(),
                    transaction_id,
                    self,
                    msg_reg,
                    msg_router_state,
                )
                .run_heartbeat()
                .await?;

                Ok(handler)
            }
            messages::exchange::CreateTransactionResponse::Err(err) => {
                Err(RecordHandlerError::CreateTransactionRequestReturnedError(err).into())
            }
        }
    }

    pub async fn next_record(
        &mut self,
        ct: CancellationToken,
        pipe: &mut Pipe,
        max_wait: Option<chrono::Duration>,
    ) -> Result<Option<ExchangeRecord>> {
        let (inbound_exchange_idx, inbound_exchange) = if let Some(id) = self
            .state
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .inbound_exchanges
            .first()
        {
            (0, id.clone())
        } else {
            return Err(RecordHandlerError::InboundExhcnagesIsEmpty.into());
        };

        let max_wait = if let Some(max_wait) = max_wait {
            max_wait
        } else {
            // use 100 years
            chrono::Duration::days(365 * 100)
        };

        let res = tokio::time::timeout(max_wait.to_std()?, async {
            loop {
                if ct.is_cancelled() {
                    return Err(RecordHandlerError::CancellationTokenCancelled.into());
                }
                let resp = requests::GetNextRecordRequest::get_next_record_request(
                    self.operator_id.clone(),
                    self.queue_name.clone(),
                    inbound_exchange.operator_instance_id.clone(),
                    inbound_exchange.worker_id,
                    pipe,
                    self.msg_reg.clone(),
                )
                .await?;

                match resp {
                    requests::GetNextRecordResponse::Record {
                        record_id,
                        record,
                        table_aliases,
                    } => {
                        if self.create_heartbeat {
                            let mut record_heartbeat_handler = RecordHeartbeatHandler::new(
                                self.query_id.clone(),
                                self.operator_id.clone(),
                                inbound_exchange.operator_instance_id.clone(),
                                inbound_exchange.worker_id.clone(),
                                record_id.clone(),
                                self.queue_name.clone(),
                                self.msg_reg.clone(),
                                self.msg_router_state.clone(),
                            )
                            .await;
                            let tt_ct = self.tracker_ct.child_token();
                            let tt_ct_clone = tt_ct.clone();
                            self.tt.spawn(async move {
                                if let Err(err) =
                                    record_heartbeat_handler.async_main(tt_ct_clone).await
                                {
                                    error!("{}", err);
                                }
                            });

                            self.state
                                .lock()
                                .map_err(|_| anyhow!("lock error"))?
                                .tracked_records
                                .insert(
                                    record_id.clone(),
                                    TrackedRecord {
                                        record_id,
                                        exchange_identity_idx: inbound_exchange_idx,
                                        ct: tt_ct,
                                    },
                                );
                        }

                        return Ok(Some(ExchangeRecord {
                            record_id,
                            record,
                            table_aliases,
                            queue_name: self.queue_name.clone(),
                        }));
                    }
                    requests::GetNextRecordResponse::NoneAvailable => {
                        tokio::time::sleep(self.none_available_wait_time_in_ms.to_std()?).await;
                        continue;
                    }
                    requests::GetNextRecordResponse::NoneLeft => {
                        return Ok(None);
                    }
                }
            }
        })
        .await?;

        res
    }

    pub async fn complete_record(&mut self, pipe: &mut Pipe, rec: ExchangeRecord) -> Result<()> {
        if !self.create_heartbeat {
            return Err(
                RecordHandlerError::UnableToCompleteRecordSinceHeartbeatIsNotEnabled.into(),
            );
        }

        let tracked_rec = if let Some(tracked_rec) = self
            .state
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .tracked_records
            .remove(&rec.record_id)
        {
            tracked_rec
        } else {
            return Err(
                RecordHandlerError::UnableToFindTrackedRecord(rec.record_id.clone()).into(),
            );
        };

        let exchange = if let Some(exchange) = self
            .state
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .inbound_exchanges
            .get(tracked_rec.exchange_identity_idx as usize)
        {
            exchange.clone()
        } else {
            return Err(RecordHandlerError::UnableToFindExchangeIdentity(
                tracked_rec.exchange_identity_idx.clone(),
            )
            .into());
        };

        requests::OperatorCompletedRecordProcessingRequest::request(
            self.operator_id.clone(),
            self.queue_name.clone(),
            rec.record_id.clone(),
            exchange.operator_instance_id.clone(),
            exchange.worker_id.clone(),
            pipe,
            self.msg_reg.clone(),
        )
        .await?;

        // cancel any heartbeat tasks
        tracked_rec.ct.cancel();

        Ok(())
    }

    pub async fn send_record_to_outbound_exchange(
        &mut self,
        pipe: &mut Pipe,
        queue_name: String,
        record_id: u64,
        record: arrow::array::RecordBatch,
        table_aliases: Vec<Vec<String>>,
    ) -> Result<()> {
        let outbound_exchange = if let Some(exchange) = &self
            .state
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .outbound_exchange
        {
            exchange.clone()
        } else {
            return Err(RecordHandlerError::OutboundExchangeIsNone.into());
        };

        requests::SendRecordRequest::send_record_request(
            queue_name,
            record_id.clone(),
            record,
            table_aliases.clone(),
            outbound_exchange.operator_instance_id.clone(),
            outbound_exchange.worker_id.clone(),
            pipe,
            self.msg_reg.clone(),
        )
        .await?;

        Ok(())
    }

    async fn find_inbound_exchanges(
        &mut self,
        ct: CancellationToken,
        pipe: &mut Pipe,
    ) -> Result<()> {
        if self
            .state
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .inbound_exchange_ids
            .len()
            == 0
        {
            return Ok(());
        }

        debug!("finding inbound exchanges");

        let req = requests::IdentifyExchangeRequest::request_inbound_exchanges(
            self.query_handler_worker_id.clone(),
            self.query_id.clone(),
            self.state
                .lock()
                .map_err(|_| anyhow!("lock error"))?
                .inbound_exchange_ids
                .clone(),
            pipe,
            self.msg_reg.clone(),
        );
        tokio::select! {
            resp = req => {
                match resp {
                    Ok(resp) => {
                        for exchange in resp {
                            self.state.lock().map_err(|_| anyhow!("lock error"))?.inbound_exchanges.push(
                                ExchangeIdentity {
                                    operator_id: exchange.exchange_id,
                                    worker_id: exchange.exchange_worker_id,
                                    operator_instance_id: exchange.exchange_operator_instance_id,
                                }
                            )
                        }
                        Ok(())
                    }
                    Err(err) => {
                        Err(err)
                    }
                }
            }
            _ = ct.cancelled() => {
                Ok(())
            }
        }
    }

    async fn find_outbound_exchange(
        &mut self,
        ct: CancellationToken,
        pipe: &mut Pipe,
    ) -> Result<()> {
        debug!("finding outbound exchange");

        let req = requests::IdentifyExchangeRequest::request_outbound_exchange(
            self.query_handler_worker_id.clone(),
            self.query_id.clone(),
            self.state
                .lock()
                .map_err(|_| anyhow!("lock error"))?
                .outbound_exchange_id
                .clone(),
            pipe,
            self.msg_reg.clone(),
        );

        let outbound_exchange_id = self
            .state
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .outbound_exchange_id
            .clone();

        tokio::select! {
            resp = req => {
                match resp {
                    Ok(resp) => {
                        self.state.lock().map_err(|_| anyhow!("lock error"))?.outbound_exchange = Some(ExchangeIdentity {
                            operator_id: outbound_exchange_id,
                            worker_id: resp.exchange_worker_id,
                            operator_instance_id: resp.exchange_operator_instance_id,
                        });
                        Ok(())
                    }
                    Err(err) => {
                        Err(err)
                    }
                }
            }
            _ = ct.cancelled() => {
                Ok(())
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        self.tt.close();
        self.tracker_ct.cancel();
        tokio::select! {
            _ = self.tt.wait() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                return Err(RecordHandlerError::TimedOutWaitingForTaskToClose.into());
            }
        }

        debug!(
            query_id = self.query_id,
            operator_id = self.operator_id,
            "closed record handler",
        );

        Ok(())
    }
}
