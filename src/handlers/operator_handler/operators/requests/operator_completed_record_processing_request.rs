use anyhow::Result;
use std::sync::Arc;
use thiserror::Error;

use crate::handlers::message_handler::{
    ExchangeRequests, Message, MessageName, MessageRegistry, Pipe, Request,
};

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
        self.operator_completed_record_processing_with_retry(10)
            .await?;
        Ok(())
    }

    async fn operator_completed_record_processing_with_retry(
        &mut self,
        num_retries: u8,
    ) -> Result<()> {
        let mut last_err: Option<anyhow::Error> = None;
        for retry_idx in 0..(num_retries + 1) {
            match self.operator_completed_record_processing().await {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    last_err = Some(err);

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
            .context("failed to tell the exchange that the record has completed processing"));
    }

    async fn operator_completed_record_processing(&mut self) -> Result<()> {
        let msg = Message::new(Box::new(
            ExchangeRequests::OperatorCompletedRecordProcessingRequest {
                operator_id: self.operator_id.clone(),
                record_id: self.record_id.clone(),
            },
        ));
        let resp_msg = self
            .pipe
            .send_request(Request {
                msg,
                expect_response_msg_name: MessageName::ExchangeRequests,
                timeout: chrono::Duration::seconds(10),
            })
            .await?;

        let resp_cast_msg: &ExchangeRequests = self.msg_reg.try_cast_msg(&resp_msg)?;
        match resp_cast_msg {
            ExchangeRequests::OperatorCompletedRecordProcessingResponse => Ok(()),
            _ => Err(
                OperatorCompletedRecordProcessingRequestError::ReceivedTheWrongMessageType(
                    format!("{}", resp_msg),
                )
                .into(),
            ),
        }
    }
}
