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

pub struct InsertTransactionRecordRequest<'a> {
    operator_id: String,
    exchange_operator_instance_id: u128,
    exchange_worker_id: u128,

    transaction_id: u64,
    queue_name: String,
    record_id: u64,
    record: Arc<arrow::array::RecordBatch>,
    table_aliases: Vec<Vec<String>>,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> InsertTransactionRecordRequest<'a> {
    pub async fn insert_record_request(
        operator_id: String,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        transaction_id: u64,
        queue_name: String,
        record_id: u64,
        record: Arc<arrow::array::RecordBatch>,
        table_aliases: Vec<Vec<String>>,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<messages::exchange::InsertTransactionRecordResponse> {
        debug!(
            transaction_id = transaction_id,
            exchange_operator_instance_id = exchange_operator_instance_id,
            exchange_worker_id = exchange_worker_id,
            "request"
        );

        let mut req = InsertTransactionRecordRequest {
            operator_id,
            exchange_operator_instance_id,
            exchange_worker_id,
            transaction_id,
            queue_name,
            record_id,
            record,
            table_aliases,
            pipe,
            msg_reg,
        };
        retry::retry_request!(req.insert_record(), 3, 10)
    }

    async fn insert_record(
        &mut self,
    ) -> Result<messages::exchange::InsertTransactionRecordResponse> {
        let req_msg = Message::new(Box::new(messages::exchange::InsertTransactionRecord {
            transaction_id: self.transaction_id.clone(),
            queue_name: self.queue_name.clone(),
            record_id: self.record_id.clone(),
            record: self.record.clone(),
            table_aliases: self.table_aliases.clone(),
        }))
        .set_route_to_worker_id(self.exchange_worker_id.clone())
        .set_route_to_operation_id(self.exchange_operator_instance_id.clone());

        let resp_msg = self
            .pipe
            .send_request(Request {
                msg: req_msg,
                expect_response_msg_name: MessageName::ExchangeInsertTransactionRecordResponse,
                timeout: chrono::Duration::milliseconds(5_000),
            })
            .await?;

        let resp_msg_cast: &messages::exchange::InsertTransactionRecordResponse =
            self.msg_reg.try_cast_msg(&resp_msg)?;

        Ok(resp_msg_cast.clone())
    }
}
