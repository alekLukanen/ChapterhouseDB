use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tracing::{debug, error};

use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::message_handler::{MessageRegistry, Pipe, Request};

use super::retry;

#[derive(Debug, Error)]
pub enum SendRecordRequestError {
    #[error("received the wrong message type")]
    ReceivedTheWrongMessageType,
    #[error("response record id {0} does not match the request record id {1}")]
    ResponseRecordIdDoesNotMatchTheRequestRecordId(u64, u64),
}

pub struct SendRecordRequest<'a> {
    queue_name: String,
    record_id: u64,
    record: Arc<arrow::array::RecordBatch>,
    table_aliases: Vec<Vec<String>>,

    exchange_operator_instance_id: u128,
    exchange_worker_id: u128,
    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> SendRecordRequest<'a> {
    pub async fn send_record_request(
        queue_name: String,
        record_id: u64,
        record: arrow::array::RecordBatch,
        table_aliases: Vec<Vec<String>>,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<()> {
        debug!(
            record_id = record_id,
            exchange_operator_instance_id = exchange_operator_instance_id,
            exchange_worker_id = exchange_worker_id,
            "request"
        );

        let mut req = SendRecordRequest {
            queue_name,
            record_id,
            record: Arc::new(record),
            table_aliases,
            exchange_operator_instance_id,
            exchange_worker_id,
            pipe,
            msg_reg,
        };
        req.process_request().await?;
        Ok(())
    }

    async fn process_request(&mut self) -> Result<()> {
        retry::retry_request!(self.send_record(), 3, 10)
    }

    async fn send_record(&mut self) -> Result<()> {
        let msg = Message::new(Box::new(
            messages::exchange::ExchangeRequests::SendRecordRequest {
                queue_name: self.queue_name.clone(),
                record_id: self.record_id.clone(),
                record: self.record.clone(),
                table_aliases: self.table_aliases.clone(),
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

        let send_record_resp: &messages::exchange::ExchangeRequests =
            self.msg_reg.try_cast_msg(&resp_msg)?;
        match send_record_resp {
            messages::exchange::ExchangeRequests::SendRecordResponse { record_id } => {
                if *record_id == self.record_id {
                    Ok(())
                } else {
                    Err(
                        SendRecordRequestError::ResponseRecordIdDoesNotMatchTheRequestRecordId(
                            record_id.clone(),
                            self.record_id.clone(),
                        )
                        .into(),
                    )
                }
            }
            _ => Err(SendRecordRequestError::ReceivedTheWrongMessageType.into()),
        }
    }
}
