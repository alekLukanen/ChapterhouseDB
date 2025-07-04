use std::sync::Arc;

use anyhow::{Context, Result};
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

pub struct CreateTransactionRequest<'a> {
    key: String,
    exchange_operator_instance_id: u128,
    exchange_worker_id: u128,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> CreateTransactionRequest<'a> {
    pub async fn create_transaction_request(
        key: String,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<messages::exchange::CreateTransactionResponse> {
        debug!(
            key = key,
            exchange_operator_instance_id = exchange_operator_instance_id,
            exchange_worker_id = exchange_worker_id,
            "request"
        );

        let mut req = CreateTransactionRequest {
            key,
            exchange_operator_instance_id,
            exchange_worker_id,
            pipe,
            msg_reg,
        };
        req.process_create_transaction_request()
            .await
            .context("failed making the exchange transaction request")
    }

    async fn process_create_transaction_request(
        &mut self,
    ) -> Result<messages::exchange::CreateTransactionResponse> {
        retry::retry_request!(self.create_transaction(), 3, 10)
    }

    async fn create_transaction(
        &mut self,
    ) -> Result<messages::exchange::CreateTransactionResponse> {
        let req_msg = Message::new(Box::new(messages::exchange::CreateTransaction {
            key: self.key.clone(),
        }))
        .set_route_to_worker_id(self.exchange_worker_id.clone())
        .set_route_to_operation_id(self.exchange_operator_instance_id.clone());

        let resp_msg = self
            .pipe
            .send_request(Request {
                msg: req_msg,
                expect_response_msg_name: MessageName::ExchangeCreateTransactionResponse,
                timeout: chrono::Duration::milliseconds(250),
            })
            .await?;

        let resp_msg_cast: &messages::exchange::CreateTransactionResponse =
            self.msg_reg.try_cast_msg(&resp_msg)?;

        Ok(resp_msg_cast.clone())
    }
}
