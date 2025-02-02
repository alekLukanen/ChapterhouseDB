use anyhow::Result;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error};

use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::message_handler::{MessageRegistry, Pipe, Request};

use super::retry;

#[derive(Debug, Error)]
pub enum OperatorCompletedRecordProcessingRequestError {
    #[error("received the wrong message type: {0}")]
    ReceivedTheWrongMessageType(String),
}

pub struct OperatorCompletedRecordProcessingRequest<'a> {
    exchange_worker_id: u128,
    exchange_operator_instance_id: u128,
    operator_id: String,
    record_id: u64,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> OperatorCompletedRecordProcessingRequest<'a> {
    pub async fn request(
        operator_id: String,
        record_id: u64,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<()> {
        debug!(
            operator_id = operator_id,
            record_id = record_id,
            exchange_operator_instance_id = exchange_operator_instance_id,
            exchange_worker_id = exchange_worker_id,
            "request",
        );
        let mut req = OperatorCompletedRecordProcessingRequest {
            exchange_operator_instance_id,
            exchange_worker_id,
            operator_id,
            record_id,
            pipe,
            msg_reg,
        };
        req.inner_request().await?;
        Ok(())
    }

    async fn inner_request(&mut self) -> Result<()> {
        retry::retry_request!(self.operator_completed_record_processing(), 3, 10)
    }

    async fn operator_completed_record_processing(&mut self) -> Result<()> {
        let msg = Message::new(Box::new(
            messages::exchange::ExchangeRequests::OperatorCompletedRecordProcessingRequest {
                operator_id: self.operator_id.clone(),
                record_id: self.record_id.clone(),
            },
        ))
        .set_route_to_worker_id(self.exchange_worker_id.clone())
        .set_route_to_operation_id(self.exchange_operator_instance_id.clone());

        let resp_msg = self
            .pipe
            .send_request(Request {
                msg,
                expect_response_msg_name: MessageName::ExchangeRequests,
                timeout: chrono::Duration::seconds(10),
            })
            .await?;

        let resp_cast_msg: &messages::exchange::ExchangeRequests =
            self.msg_reg.try_cast_msg(&resp_msg)?;
        match resp_cast_msg {
            messages::exchange::ExchangeRequests::OperatorCompletedRecordProcessingResponse => {
                Ok(())
            }
            _ => Err(
                OperatorCompletedRecordProcessingRequestError::ReceivedTheWrongMessageType(
                    format!("{}", resp_msg),
                )
                .into(),
            ),
        }
    }
}
