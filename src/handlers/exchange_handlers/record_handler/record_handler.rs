use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::handlers::{
    message_handler::{MessageRegistry, Pipe},
    operator_handler::{operators::requests, OperatorInstanceConfig},
};

#[derive(Debug, Error)]
pub enum RecordHandlerError {
    #[error("operator type not implemented: {0}")]
    OperatorTypeNotImplemented(String),
}

struct exchangeIdentity {
    operator_id: String,
    worker_id: u128,
    operator_instance_id: u128,
}

pub struct RecordHandler<'a> {
    query_id: u128,

    inbound_exchange_ids: Vec<String>,
    inbound_exchanges: Vec<exchangeIdentity>,

    outbound_exchange_id: String,
    outbound_exchange: Option<exchangeIdentity>,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> RecordHandler<'a> {
    pub fn new(
        op_in_config: &OperatorInstanceConfig,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
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

        Ok(RecordHandler {
            query_id: op_in_config.query_id,
            inbound_exchange_ids,
            inbound_exchanges: Vec::new(),
            outbound_exchange_id,
            outbound_exchange: None,
            pipe,
            msg_reg,
        })
    }

    pub fn next_record(&mut self) {}

    pub fn complete_record(&mut self) {}

    pub fn send_record(
        &mut self,
        record: arrow::array::RecordBatch,
        table_aliases: Vec<Vec<String>>,
    ) {
    }

    async fn find_inbound_exchange(&mut self, ct: CancellationToken) -> Result<()> {
        let ref mut operator_pipe = self.pipe;
        let req = requests::IdentifyExchangeRequest::request_inbound_exchanges(
            self.query_id.clone(),
            self.inbound_exchange_ids.clone(),
            operator_pipe,
            self.msg_reg.clone(),
        );
        tokio::select! {
            resp = req => {
                match resp {
                    Ok(resp) => {
                        for exchange in resp {
                            self.inbound_exchanges.push(
                                exchangeIdentity {
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
                        self.outbound_exchange = Some(exchangeIdentity {
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
