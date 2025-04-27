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
    #[error("cancellation token cancelled")]
    CancellationTokenCancelled,
}

struct ExchangeIdentity {
    operator_id: String,
    worker_id: u128,
    operator_instance_id: u128,
}

struct TrackedRecord {
    record_id: u64,
    exchange_identity_idx: u64,
}

pub struct ExchangeRecord {
    record_id: u64,
    record: Arc<arrow::array::RecordBatch>,
    table_aliases: Vec<Vec<String>>,
}

pub struct RecordHandler<'a> {
    query_id: u128,
    operator_id: String,

    // exchanges can never be removed
    inbound_exchange_ids: Vec<String>,
    inbound_exchanges: Vec<ExchangeIdentity>,

    outbound_exchange_id: String,
    outbound_exchange: Option<ExchangeIdentity>,

    // tracking state ///////
    tracked_records: HashMap<u64, TrackedRecord>,
    task_tracker: tokio_util::task::TaskTracker,
    tracker_ct: CancellationToken,
    /////////////////////////
    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
    msg_router_state: Arc<Mutex<MessageRouterState>>,
}

impl<'a> RecordHandler<'a> {
    pub async fn initiate(
        ct: CancellationToken,
        op_in_config: &OperatorInstanceConfig,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> Result<RecordHandler<'a>> {
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
            query_id: op_in_config.query_id,
            operator_id: op_in_config.operator.id.clone(),
            inbound_exchange_ids,
            inbound_exchanges: Vec::new(),
            outbound_exchange_id,
            outbound_exchange: None,
            tracked_records: HashMap::new(),
            task_tracker: tokio_util::task::TaskTracker::new(),
            tracker_ct: ct.child_token(),
            pipe,
            msg_reg,
            msg_router_state,
        };
        rec_handler.find_inbound_exchanges(ct.child_token()).await?;
        rec_handler.find_outbound_exchange(ct.child_token()).await?;

        Ok(rec_handler)
    }

    pub async fn next_record(
        &mut self,
        ct: CancellationToken,
        max_wait: chrono::Duration,
    ) -> Result<Option<ExchangeRecord>> {
        let (inbound_exchange_idx, inbound_exchange) =
            if let Some(id) = self.inbound_exchanges.first() {
                (0, id.clone())
            } else {
                return Err(RecordHandlerError::InboundExhcnagesIsEmpty.into());
            };

        let res = tokio::time::timeout(max_wait.to_std()?, async {
            loop {
                if ct.is_cancelled() {
                    return Err(RecordHandlerError::CancellationTokenCancelled.into());
                }
                let resp = requests::GetNextRecordRequest::get_next_record_request(
                    self.operator_id.clone(),
                    inbound_exchange.operator_instance_id.clone(),
                    inbound_exchange.worker_id,
                    self.pipe,
                    self.msg_reg.clone(),
                )
                .await?;

                match resp {
                    requests::GetNextRecordResponse::Record {
                        record_id,
                        record,
                        table_aliases,
                    } => {
                        let exchange_rec = ExchangeRecord {
                            record_id,
                            record,
                            table_aliases,
                        };
                        let tracked_rec = TrackedRecord {
                            record_id,
                            exchange_identity_idx: inbound_exchange_idx,
                        };

                        let mut record_heartbeat_handler = RecordHeartbeatHandler::new(
                            self.operator_id.clone(),
                            inbound_exchange.operator_instance_id.clone(),
                            inbound_exchange.worker_id.clone(),
                            record_id.clone(),
                            self.pipe,
                            self.msg_reg.clone(),
                        );
                        let tt_ct = self.tracker_ct.child_token();
                        self.task_tracker.spawn(async move {
                            if let Err(err) = record_heartbeat_handler.async_main(tt_ct).await {
                                error!("{}", err);
                            }
                        });

                        self.tracked_records.insert(record_id.clone(), tracked_rec);
                        return Ok(Some(exchange_rec));
                    }
                    requests::GetNextRecordResponse::NoneAvailable => {
                        tokio::time::sleep(chrono::Duration::milliseconds(100).to_std()?).await;
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

    pub fn complete_record(&mut self, rec: ExchangeRecord) -> Result<()> {
        Ok(())
    }

    pub fn send_record(
        &mut self,
        record: arrow::array::RecordBatch,
        table_aliases: Vec<Vec<String>>,
    ) {
    }

    async fn find_inbound_exchanges(&mut self, ct: CancellationToken) -> Result<()> {
        let req = requests::IdentifyExchangeRequest::request_inbound_exchanges(
            self.query_id.clone(),
            self.inbound_exchange_ids.clone(),
            self.pipe,
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

    async fn find_outbound_exchange(&mut self, ct: CancellationToken) -> Result<()> {
        let ref mut operator_pipe = self.pipe;
        let req = requests::IdentifyExchangeRequest::request_outbound_exchange(
            self.query_id.clone(),
            self.outbound_exchange_id.clone(),
            operator_pipe,
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
}
