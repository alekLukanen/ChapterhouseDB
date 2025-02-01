use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tracing::{debug, error};

use crate::handlers::message_handler::{
    messages::{
        self,
        message::{Message, MessageName},
    },
    MessageRegistry, Pipe, Request,
};

#[derive(Debug, Error)]
pub enum OperatorStatusChangeRequestError {
    #[error("received error response: {0}")]
    ReceivedErrorResponse(String),
}

pub struct OperatorStatusChangeRequest<'a> {
    route_to_operator_instance_id: u128,
    operator_id: String,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> OperatorStatusChangeRequest<'a> {
    pub async fn completed_request(
        route_to_operator_instance_id: u128,
        operator_id: String,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<()> {
        debug!(
            route_to_operator_instance_id = route_to_operator_instance_id,
            operator_id = operator_id,
            "request"
        );
        let mut req = OperatorStatusChangeRequest {
            route_to_operator_instance_id,
            operator_id,
            pipe,
            msg_reg,
        };
        req.inner_completed_request().await?;
        Ok(())
    }

    async fn inner_completed_request(&mut self) -> Result<()> {
        self.operator_status_change_with_retry(
            messages::exchange::OperatorStatusChange::Complete {
                operator_id: self.operator_id.clone(),
            },
            3,
        )
        .await
    }

    async fn operator_status_change_with_retry(
        &mut self,
        msg: messages::exchange::OperatorStatusChange,
        num_retries: u8,
    ) -> Result<()> {
        let mut last_err: Option<anyhow::Error> = None;
        for retry_idx in 0..(num_retries + 1) {
            match self.operator_status_change(msg.clone()).await {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    error!("{:?}", err);
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

    async fn operator_status_change(
        &mut self,
        msg: messages::exchange::OperatorStatusChange,
    ) -> Result<()> {
        let msg = Message::new(Box::new(msg))
            .set_route_to_operation_id(self.route_to_operator_instance_id.clone());

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
            messages::common::GenericResponse::Error(err) => {
                Err(OperatorStatusChangeRequestError::ReceivedErrorResponse(err.clone()).into())
            }
        }
    }
}
