use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tracing::debug;

use crate::handlers::message_handler::{
    ExchangeRequests, Message, MessageName, MessageRegistry, Pipe, Request,
};

#[derive(Debug, Error)]
pub enum SendRecordRequestError {
    #[error("operator type not implemented: {0}")]
    OperatorTypeNotImplemented(String),
    #[error("received the wrong message type")]
    ReceivedTheWrongMessageType,
    #[error("response record id {0} does not match the request record id {1}")]
    ResponseRecordIdDoesNotMatchTheRequestRecordId(u64, u64),
}

pub struct SendRecordRequest<'a> {
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
        record_id: u64,
        record: arrow::array::RecordBatch,
        table_aliases: Vec<Vec<String>>,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<()> {
        let mut req = SendRecordRequest {
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
        self.send_record_with_retry(10).await?;
        Ok(())
    }

    async fn send_record_with_retry(&mut self, num_retries: u8) -> Result<()> {
        let mut last_err: Option<anyhow::Error> = None;
        for retry_idx in 0..(num_retries + 1) {
            match self.send_record().await {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    last_err = Some(err);
                    debug!("failed sending record; retrying after wait period");
                    tokio::time::sleep(std::time::Duration::from_secs(std::cmp::min(
                        retry_idx as u64 + 1,
                        5,
                    )))
                    .await;
                    continue;
                }
            }
        }
        return Err(last_err
            .unwrap()
            .context("failed to get the exchange operator worker id"));
    }

    async fn send_record(&mut self) -> Result<()> {
        let msg = Message::new(Box::new(ExchangeRequests::SendRecordRequest {
            record_id: self.record_id.clone(),
            record: self.record.clone(),
            table_aliases: self.table_aliases.clone(),
        }))
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

        let send_record_resp: &ExchangeRequests = self.msg_reg.try_cast_msg(&resp_msg)?;
        match send_record_resp {
            ExchangeRequests::SendRecordResponse { record_id } => {
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
