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

#[derive(PartialEq)]
pub enum RecordHeartbeatResponse {
    Ok,
    Error(String),
}

pub struct RecordHeartbeatRequest<'a> {
    operator_id: String,
    exchange_operator_instance_id: u128,
    exchange_worker_id: u128,
    record_id: u64,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> RecordHeartbeatRequest<'a> {
    pub async fn record_heartbeat_request(
        operator_id: String,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        record_id: u64,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<RecordHeartbeatResponse> {
        debug!(
            operator_id = operator_id,
            exchange_operator_instance_id = exchange_operator_instance_id,
            exchange_worker_id = exchange_worker_id,
            record_id = record_id,
            "request"
        );

        let mut req = RecordHeartbeatRequest {
            operator_id,
            exchange_operator_instance_id,
            exchange_worker_id,
            record_id,
            pipe,
            msg_reg,
        };
        req.process_record_heartbeat_request().await
    }

    async fn process_record_heartbeat_request(&mut self) -> Result<RecordHeartbeatResponse> {
        retry::retry_request!(self.record_heartbeat(), 3, 10)
    }

    async fn record_heartbeat(&mut self) -> Result<RecordHeartbeatResponse> {
        let req_msg = Message::new(Box::new(messages::exchange::RecordHeartbeat::Ping {
            operator_id: self.operator_id.clone(),
            record_id: self.record_id.clone(),
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
        match resp_msg_cast {
            messages::common::GenericResponse::Ok => Ok(RecordHeartbeatResponse::Ok),
            messages::common::GenericResponse::Error(val) => {
                Ok(RecordHeartbeatResponse::Error(val.clone()))
            }
        }
    }
}
