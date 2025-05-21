use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::handlers::{
    exchange_handlers::heartbeat_handler::RecordHeartbeatHandler,
    message_handler::{MessageRegistry, Pipe},
    message_router_handler::MessageRouterState,
    operator_handler::{operators::requests, OperatorInstanceConfig},
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
}

#[derive(Debug, Clone)]
struct ExchangeIdentity {
    operator_id: String,
    worker_id: u128,
    operator_instance_id: u128,
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
}

pub struct RecordHandler {
    query_handler_worker_id: u128,
    query_id: u128,
    operator_id: String,
    queue_name: String,

    // config
    none_available_wait_time_in_ms: chrono::TimeDelta,

    // exchanges can never be removed
    inbound_exchange_ids: Vec<String>,
    inbound_exchanges: Vec<ExchangeIdentity>,

    outbound_exchange_id: String,
    outbound_exchange: Option<ExchangeIdentity>,

    // tracking state ///////
    tracked_records: HashMap<u64, TrackedRecord>,
    tt: tokio_util::task::TaskTracker,
    tracker_ct: CancellationToken,
    /////////////////////////
    msg_reg: Arc<MessageRegistry>,
    msg_router_state: Arc<Mutex<MessageRouterState>>,
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
            none_available_wait_time_in_ms: chrono::Duration::milliseconds(50),
            inbound_exchange_ids,
            inbound_exchanges: Vec::new(),
            outbound_exchange_id,
            outbound_exchange: None,
            tracked_records: HashMap::new(),
            tt: tokio_util::task::TaskTracker::new(),
            tracker_ct: ct.child_token(),
            msg_reg,
            msg_router_state,
        };
        rec_handler
            .find_inbound_exchanges(ct.child_token(), pipe)
            .await?;
        rec_handler
            .find_outbound_exchange(ct.child_token(), pipe)
            .await?;

        Ok(rec_handler)
    }

    pub async fn next_record(
        &mut self,
        ct: CancellationToken,
        pipe: &mut Pipe,
        max_wait: Option<chrono::Duration>,
    ) -> Result<Option<ExchangeRecord>> {
        let (inbound_exchange_idx, inbound_exchange) =
            if let Some(id) = self.inbound_exchanges.first() {
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
                        let mut record_heartbeat_handler = RecordHeartbeatHandler::new(
                            self.query_id.clone(),
                            self.operator_id.clone(),
                            inbound_exchange.operator_instance_id.clone(),
                            inbound_exchange.worker_id.clone(),
                            record_id.clone(),
                            self.msg_reg.clone(),
                            self.msg_router_state.clone(),
                        )
                        .await;
                        let tt_ct = self.tracker_ct.child_token();
                        let tt_ct_clone = tt_ct.clone();
                        self.tt.spawn(async move {
                            if let Err(err) = record_heartbeat_handler.async_main(tt_ct_clone).await
                            {
                                error!("{}", err);
                            }
                        });

                        self.tracked_records.insert(
                            record_id.clone(),
                            TrackedRecord {
                                record_id,
                                exchange_identity_idx: inbound_exchange_idx,
                                ct: tt_ct,
                            },
                        );

                        return Ok(Some(ExchangeRecord {
                            record_id,
                            record,
                            table_aliases,
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
        let tracked_rec = if let Some(tracked_rec) = self.tracked_records.remove(&rec.record_id) {
            tracked_rec
        } else {
            return Err(
                RecordHandlerError::UnableToFindTrackedRecord(rec.record_id.clone()).into(),
            );
        };

        let inbound_exchange = if let Some(exchange) = self
            .inbound_exchanges
            .get(tracked_rec.exchange_identity_idx as usize)
        {
            exchange
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
            inbound_exchange.operator_instance_id.clone(),
            inbound_exchange.worker_id.clone(),
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
        record_id: u64,
        record: arrow::array::RecordBatch,
        table_aliases: Vec<Vec<String>>,
    ) -> Result<()> {
        let outbound_exchange = if let Some(id) = &self.outbound_exchange {
            id.clone()
        } else {
            return Err(RecordHandlerError::OutboundExchangeIsNone.into());
        };

        requests::SendRecordRequest::send_record_request(
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
        let req = requests::IdentifyExchangeRequest::request_inbound_exchanges(
            self.query_handler_worker_id.clone(),
            self.query_id.clone(),
            self.inbound_exchange_ids.clone(),
            pipe,
            self.msg_reg.clone(),
        );
        tokio::select! {
            resp = req => {
                match resp {
                    Ok(resp) => {
                        for exchange in resp {
                            self.inbound_exchanges.push(
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
        let req = requests::IdentifyExchangeRequest::request_outbound_exchange(
            self.query_handler_worker_id.clone(),
            self.query_id.clone(),
            self.outbound_exchange_id.clone(),
            pipe,
            self.msg_reg.clone(),
        );
        tokio::select! {
            resp = req => {
                match resp {
                    Ok(resp) => {
                        self.outbound_exchange = Some(ExchangeIdentity {
                            operator_id: self.outbound_exchange_id.clone(),
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
