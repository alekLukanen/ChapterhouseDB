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
    worker_id: String,
    operator_instance_id: String,
}

pub struct RecordHandler<'a> {
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

    /*
    async fn find_inbound_exchange(&mut self, ct: CancellationToken) -> Result<()> {
        let ref mut operator_pipe = self.pipe;
        let req = requests::IdentifyExchangeRequest::request_inbound_exchanges(
            &self.operator_instance_config,
            operator_pipe,
            self.msg_reg.clone(),
        );
        tokio::select! {
            resp = req => {
                match resp {
                    Ok(resp) => {
                        if resp.len() != 1 {
                            return Err(FilterTaskError::MoreThanOneExchangeIsCurrentlyNotImplemented.into());
                        }
                        let resp = resp.get(0).unwrap();
                        self.inbound_exchange_operator_instance_id = Some(resp.exchange_operator_instance_id);
                        self.inbound_exchange_worker_id = Some(resp.exchange_worker_id);
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
        let ref mut operator_pipe = self.operator_pipe;
        let req = requests::IdentifyExchangeRequest::request_outbound_exchange(
            &self.operator_instance_config,
            operator_pipe,
            self.msg_reg.clone(),
        );
        tokio::select! {
            resp = req => {
                match resp {
                    Ok(resp) => {
                        self.outbound_exchange_operator_instance_id = Some(resp.exchange_operator_instance_id);
                        self.outbound_exchange_worker_id = Some(resp.exchange_worker_id);
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
    */
}
