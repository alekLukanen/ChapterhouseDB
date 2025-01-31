use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;

use crate::handlers::message_handler::{
    messages::{
        self,
        message::{Message, MessageName},
    },
    MessageRegistry, Pipe, Request,
};

#[derive(Debug, Error)]
pub enum OperatorInstanceStatusChangeRequestError {
    #[error("received error response: {0}")]
    ReceivedErrorResponse(String),
}

pub struct OperatorInstanceStatusChangeRequest<'a> {
    operator_instance_id: u128,
    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> OperatorInstanceStatusChangeRequest<'a> {
    pub async fn completed_request(
        operator_instance_id: u128,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<()> {
        let mut req = OperatorInstanceStatusChangeRequest {
            operator_instance_id,
            pipe,
            msg_reg,
        };
        req.inner_completed_request().await?;
        Ok(())
    }

    async fn inner_completed_request(&mut self) -> Result<()> {
        self.operator_instance_status_change_with_retry(
            messages::operator::OperatorInstanceStatusChange::Complete,
            3,
        )
        .await
    }

    async fn operator_instance_status_change_with_retry(
        &mut self,
        msg: messages::operator::OperatorInstanceStatusChange,
        num_retries: u8,
    ) -> Result<()> {
        let mut last_err: Option<anyhow::Error> = None;
        for retry_idx in 0..(num_retries + 1) {
            match self.operator_instance_status_change(msg.clone()).await {
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
            .context("failed to communicate status change to the operator handler"));
    }

    async fn operator_instance_status_change(
        &mut self,
        msg: messages::operator::OperatorInstanceStatusChange,
    ) -> Result<()> {
        let msg = Message::new(Box::new(msg));

        let resp_msg = self
            .pipe
            .send_request(Request {
                msg,
                expect_response_msg_name: MessageName::CommonGenericResponse,
                timeout: chrono::Duration::seconds(3),
            })
            .await?;

        let resp_cast_msg: &messages::common::GenericResponse =
            self.msg_reg.try_cast_msg(&resp_msg)?;
        match resp_cast_msg {
            messages::common::GenericResponse::Ok => Ok(()),
            messages::common::GenericResponse::Error(err) => Err(
                OperatorInstanceStatusChangeRequestError::ReceivedErrorResponse(err.clone()).into(),
            ),
        }
    }
}
