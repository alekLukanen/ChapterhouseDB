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

pub struct CommitTransactionRequest<'a> {
    operator_id: String,
    exchange_operator_instance_id: u128,
    exchange_worker_id: u128,
    transaction_id: u64,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> CommitTransactionRequest<'a> {
    pub async fn commit_transaction_request(
        operator_id: String,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        transaction_id: u64,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<messages::exchange::CommitTransactionResponse> {
        debug!(
            transaction_id = transaction_id,
            exchange_operator_instance_id = exchange_operator_instance_id,
            exchange_worker_id = exchange_worker_id,
            "request"
        );

        let mut req = CommitTransactionRequest {
            operator_id,
            exchange_operator_instance_id,
            exchange_worker_id,
            transaction_id,
            pipe,
            msg_reg,
        };
        retry::retry_request!(req.commit_transaction(), 3, 10)
    }

    async fn commit_transaction(
        &mut self,
    ) -> Result<messages::exchange::CommitTransactionResponse> {
        let req_msg = Message::new(Box::new(messages::exchange::CommitTransaction {
            transaction_id: self.transaction_id.clone(),
        }))
        .set_route_to_worker_id(self.exchange_worker_id.clone())
        .set_route_to_operation_id(self.exchange_operator_instance_id.clone());

        let resp_msg = self
            .pipe
            .send_request(Request {
                msg: req_msg,
                expect_response_msg_name: MessageName::ExchangeCommitTransactionResponse,
                timeout: chrono::Duration::milliseconds(250),
            })
            .await?;

        let resp_msg_cast: &messages::exchange::CommitTransactionResponse =
            self.msg_reg.try_cast_msg(&resp_msg)?;

        Ok(resp_msg_cast.clone())
    }
}
