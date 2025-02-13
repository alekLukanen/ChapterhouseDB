use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tracing::error;

use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::message_handler::{messages, MessageRegistry, Pipe, Request};
use crate::handlers::operator_handler::operators::requests::retry;

#[derive(Debug, Error)]
pub enum ShutdownRequestError {
    #[error("received error response: {0}")]
    ReceivedErrorResponse(String),
}

pub struct ShutdownRequest<'a> {
    route_to_operation_id: u128,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> ShutdownRequest<'a> {
    pub async fn shutdown_immediate_request(
        route_to_operation_id: u128,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<()> {
        let mut req = ShutdownRequest {
            route_to_operation_id,
            pipe,
            msg_reg,
        };
        req.inner_shutdown_request(messages::operator::Shutdown::Immediate)
            .await?;
        Ok(())
    }

    async fn inner_shutdown_request(&mut self, msg: messages::operator::Shutdown) -> Result<()> {
        let msg = &msg;
        retry::retry_request!(self.shutdown_request(msg), 3, 10)?
    }

    async fn shutdown_request(&mut self, msg: &messages::operator::Shutdown) -> Result<()> {
        let msg = Message::new(Box::new(msg.clone()))
            .set_route_to_operation_id(self.route_to_operation_id.clone());
        let resp_msg = self
            .pipe
            .send_request(Request {
                msg,
                expect_response_msg_name: MessageName::CommonGenericResponse,
                timeout: chrono::Duration::seconds(10),
            })
            .await?;

        let resp_msg_cast: &messages::common::GenericResponse =
            self.msg_reg.try_cast_msg(&resp_msg)?;
        match resp_msg_cast {
            messages::common::GenericResponse::Ok => Ok(()),
            messages::common::GenericResponse::Error(err) => {
                Err(ShutdownRequestError::ReceivedErrorResponse(err.clone()).into())
            }
        }
    }
}
