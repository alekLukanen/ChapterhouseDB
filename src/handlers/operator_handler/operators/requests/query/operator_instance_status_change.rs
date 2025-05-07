use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tracing::{debug, error};

use crate::handlers::{
    message_handler::{
        messages::{
            self,
            message::{Message, MessageName},
        },
        MessageRegistry, Pipe, Request,
    },
    operator_handler::operators::requests::retry,
};

#[derive(Debug, Error)]
pub enum OperatorInstanceStatusChangeRequestError {
    #[error("received error response: {0}")]
    ReceivedErrorResponse(String),
}

pub struct OperatorInstanceStatusChangeRequest<'a> {
    query_handler_worker_id: u128,
    query_id: u128,
    operator_instance_id: u128,
    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> OperatorInstanceStatusChangeRequest<'a> {
    pub async fn completed_request(
        query_handler_worker_id: u128,
        query_id: u128,
        operator_instance_id: u128,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<()> {
        debug!(
            query_id = query_id,
            operator_instance_id = operator_instance_id,
            "request",
        );
        let mut req = OperatorInstanceStatusChangeRequest {
            query_handler_worker_id,
            query_id,
            operator_instance_id,
            pipe,
            msg_reg,
        };
        req.inner_completed_request().await?;
        Ok(())
    }

    pub async fn errored_request(
        query_handler_worker_id: u128,
        query_id: u128,
        operator_instance_id: u128,
        err: String,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<()> {
        let mut req = OperatorInstanceStatusChangeRequest {
            query_handler_worker_id,
            query_id,
            operator_instance_id,
            pipe,
            msg_reg,
        };
        req.inner_errored_request(err).await?;
        Ok(())
    }

    async fn inner_completed_request(&mut self) -> Result<()> {
        let msg = messages::query::OperatorInstanceStatusChange::Complete {
            query_id: self.query_id,
            operator_instance_id: self.operator_instance_id,
        };
        retry::retry_request!(self.operator_instance_status_change(&msg), 3, 10)
    }

    async fn inner_errored_request(&mut self, err: String) -> Result<()> {
        let msg = messages::query::OperatorInstanceStatusChange::Error {
            query_id: self.query_id,
            operator_instance_id: self.operator_instance_id,
            error: err,
        };
        retry::retry_request!(self.operator_instance_status_change(&msg), 3, 10)
    }

    async fn operator_instance_status_change(
        &mut self,
        msg: &messages::query::OperatorInstanceStatusChange,
    ) -> Result<()> {
        let msg = Message::new(Box::new(msg.clone()))
            .set_route_to_worker_id(self.query_handler_worker_id.clone());

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
