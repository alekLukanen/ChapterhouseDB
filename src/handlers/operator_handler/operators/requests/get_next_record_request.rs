use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tracing::{debug, error};

use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::message_handler::{MessageRegistry, Pipe, Request};

use super::retry;

#[derive(Debug, Error)]
pub enum GetNextRecordRequestError {
    #[error("received the wrong message type")]
    ReceivedTheWrongMessageType,
}

#[derive(PartialEq)]
pub enum GetNextRecordResponse {
    Record {
        record_id: u64,
        record: Arc<arrow::array::RecordBatch>,
        table_aliases: Vec<Vec<String>>,
    },
    NoneLeft,
    NoneAvailable,
}

pub struct GetNextRecordRequest<'a> {
    operator_id: String,
    exchange_operator_instance_id: u128,
    exchange_worker_id: u128,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> GetNextRecordRequest<'a> {
    pub async fn get_next_record_request(
        operator_id: String,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<GetNextRecordResponse> {
        debug!(
            operator_id = operator_id,
            exchange_operator_instance_id = exchange_operator_instance_id,
            exchange_worker_id = exchange_worker_id,
            "request",
        );
        let mut req = GetNextRecordRequest {
            operator_id,
            exchange_operator_instance_id,
            exchange_worker_id,
            pipe,
            msg_reg,
        };
        req.process_request().await
    }

    async fn process_request(&mut self) -> Result<GetNextRecordResponse> {
        retry::retry_request!(self.get_next_record(), 3, 10)
    }

    async fn get_next_record(&mut self) -> Result<GetNextRecordResponse> {
        // sent the request message
        let get_next_msg = Message::new(Box::new(
            messages::exchange::ExchangeRequests::GetNextRecordRequest {
                operator_id: self.operator_id.clone(),
            },
        ))
        .set_route_to_worker_id(self.exchange_worker_id.clone())
        .set_route_to_operation_id(self.exchange_operator_instance_id.clone());

        let resp_msg = self
            .pipe
            .send_request(Request {
                msg: get_next_msg,
                expect_response_msg_name: MessageName::ExchangeRequests,
                timeout: chrono::Duration::seconds(10),
            })
            .await?;

        let resp_msg_cast: &messages::exchange::ExchangeRequests =
            self.msg_reg.try_cast_msg(&resp_msg)?;
        match resp_msg_cast {
            messages::exchange::ExchangeRequests::GetNextRecordResponseRecord {
                record_id,
                record,
                table_aliases,
            } => Ok(GetNextRecordResponse::Record {
                record_id: record_id.to_owned(),
                record: record.to_owned(),
                table_aliases: table_aliases.to_owned(),
            }),
            messages::exchange::ExchangeRequests::GetNextRecordResponseNoneLeft => {
                Ok(GetNextRecordResponse::NoneLeft)
            }
            messages::exchange::ExchangeRequests::GetNextRecordResponseNoneAvailable => {
                Ok(GetNextRecordResponse::NoneAvailable)
            }
            _ => Err(GetNextRecordRequestError::ReceivedTheWrongMessageType.into()),
        }
    }
}
