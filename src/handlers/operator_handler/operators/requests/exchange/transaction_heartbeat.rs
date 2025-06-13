use std::sync::Arc;

use anyhow::Result;
use tracing::debug;

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

pub struct TransactionHeartbeatRequest<'a> {
    operator_id: String,
    exchange_operator_instance_id: u128,
    exchange_worker_id: u128,
    transaction_id: u64,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> TransactionHeartbeatRequest<'a> {
    pub async fn transaction_heartbeat_request(
        operator_id: String,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        transaction_id: u64,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<messages::common::GenericResponse> {
        debug!(
            operator_id = operator_id,
            exchange_operator_instance_id = exchange_operator_instance_id,
            exchange_worker_id = exchange_worker_id,
            transaction_id = transaction_id,
            "request"
        );

        let mut req = TransactionHeartbeatRequest {
            operator_id,
            exchange_operator_instance_id,
            exchange_worker_id,
            transaction_id,
            pipe,
            msg_reg,
        };
        req.process_transaction_heartbeat_request().await
    }

    async fn process_transaction_heartbeat_request(
        &mut self,
    ) -> Result<messages::common::GenericResponse> {
        retry::retry_request!(self.transaction_heartbeat(), 3, 10)
    }

    async fn transaction_heartbeat(&mut self) -> Result<messages::common::GenericResponse> {
        let req_msg = Message::new(Box::new(messages::exchange::TransactionHeartbeat::Ping {
            transaction_id: self.transaction_id.clone(),
        }))
        .set_route_to_worker_id(self.exchange_worker_id.clone())
        .set_route_to_operation_id(self.exchange_operator_instance_id.clone());

        let resp_msg = self
            .pipe
            .send_request(Request {
                msg: req_msg,
                expect_response_msg_name: MessageName::CommonGenericResponse,
                timeout: chrono::Duration::milliseconds(250),
            })
            .await?;

        let resp_msg_cast: &messages::common::GenericResponse =
            self.msg_reg.try_cast_msg(&resp_msg)?;
        Ok(resp_msg_cast.clone())
    }
}
