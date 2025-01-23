use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;

use crate::handlers::{
    message_handler::{MessageRegistry, Pipe},
    operator_handler::operator_handler_state::OperatorInstanceConfig,
};

#[derive(Debug, Error)]
pub enum SendRecordRequestError {
    #[error("operator type not implemented: {0}")]
    OperatorTypeNotImplemented(String),
}

pub struct SendRecordRequest<'a> {
    exchange_operator_instance_id: u128,
    exchange_worker_id: u128,
    exchange_id: String,
    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> SendRecordRequest<'a> {
    pub async fn send_record_request(
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        op_in_config: &OperatorInstanceConfig,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<()> {
        let exchange_id = match &op_in_config.operator.operator_type {
            crate::planner::OperatorType::Producer {
                outbound_exchange_id,
                ..
            } => outbound_exchange_id.clone(),
            crate::planner::OperatorType::Exchange { .. } => {
                return Err(SendRecordRequestError::OperatorTypeNotImplemented(format!(
                    "{:?}",
                    op_in_config.operator.operator_type
                ))
                .into());
            }
        };

        let mut req = SendRecordRequest {
            exchange_operator_instance_id,
            exchange_worker_id,
            exchange_id,
            pipe,
            msg_reg,
        };
        req.send_record().await?;
        Ok(())
    }

    async fn send_record(&mut self) -> Result<()> {}
}
