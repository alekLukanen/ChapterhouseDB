use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::handlers::exchange_handlers;
use crate::handlers::message_router_handler::MessageRouterState;
use crate::handlers::operator_handler::operators::record_utils;
use crate::handlers::{
    message_handler::{
        messages::{
            self,
            message::{Message, MessageName},
        },
        MessageRegistry, Pipe,
    },
    message_router_handler::MessageConsumer,
    operator_handler::{
        operator_handler_state::OperatorInstanceConfig,
        operators::{
            operator_task_trackers::RestrictedOperatorTaskTracker, requests, traits::TaskBuilder,
            ConnectionRegistry,
        },
    },
};

use super::config::FilterConfig;

#[derive(Debug, Error)]
pub enum FilterTaskError {
    #[error("more than one exchange is currently not implement")]
    MoreThanOneExchangeIsCurrentlyNotImplemented,
}

#[derive(Debug)]
struct FilterTask {
    operator_instance_config: OperatorInstanceConfig,
    filter_config: FilterConfig,

    operator_pipe: Pipe,
    msg_reg: Arc<MessageRegistry>,
    conn_reg: Arc<ConnectionRegistry>,
    msg_router_state: Arc<Mutex<MessageRouterState>>,
}

impl FilterTask {
    fn new(
        op_in_config: OperatorInstanceConfig,
        filter_config: FilterConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> FilterTask {
        FilterTask {
            operator_instance_config: op_in_config,
            filter_config,
            operator_pipe,
            msg_reg,
            conn_reg,
            msg_router_state,
        }
    }

    fn consumer(&self) -> Box<dyn MessageConsumer> {
        Box::new(FilterConsumer {
            msg_reg: self.msg_reg.clone(),
        })
    }

    async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        debug!(
            operator_task = self
                .operator_instance_config
                .operator
                .operator_type
                .task_name(),
            operator_id = self.operator_instance_config.operator.id,
            operator_instance_id = self.operator_instance_config.id,
            "started task",
        );

        let mut rec_handler = exchange_handlers::record_handler::RecordHandler::initiate(
            ct.child_token(),
            &self.operator_instance_config,
            &mut self.operator_pipe,
            self.msg_reg.clone(),
            self.msg_router_state.clone(),
        )
        .await?;

        loop {
            let exchange_rec = rec_handler
                .next_record(ct.child_token(), &mut self.operator_pipe, None)
                .await?;

            match exchange_rec {
                Some(exchange_rec) => {
                    debug!(
                        record_id = exchange_rec.record_id,
                        record_num_rows = exchange_rec.record.num_rows(),
                        "received record"
                    );
                    // filter the record
                    let filtered_rec = record_utils::filter_record(
                        exchange_rec.record.clone(),
                        &exchange_rec.table_aliases,
                        &self.filter_config.expr,
                    )?;

                    // send the record to the outbound exchange
                    rec_handler
                        .send_record_to_outbound_exchange(
                            &mut self.operator_pipe,
                            exchange_rec.record_id.clone(),
                            filtered_rec,
                            exchange_rec.table_aliases.clone(),
                        )
                        .await?;

                    // confirm processing of the record with the inbound exchange
                    rec_handler
                        .complete_record(&mut self.operator_pipe, exchange_rec)
                        .await?;
                }
                None => {
                    debug!("read all records from the exchange");
                    break;
                }
            }
        }

        debug!(
            operator_task = self
                .operator_instance_config
                .operator
                .operator_type
                .task_name(),
            operator_id = self.operator_instance_config.operator.id,
            operator_instance_id = self.operator_instance_config.id,
            "closed task",
        );
        Ok(())
    }
}

///////////////////////////////////////////////////////
// Filter Producer Builder

#[derive(Debug, Clone)]
pub struct FilterTaskBuilder {}

impl FilterTaskBuilder {
    pub fn new() -> FilterTaskBuilder {
        FilterTaskBuilder {}
    }
}

impl TaskBuilder for FilterTaskBuilder {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
        tt: &mut RestrictedOperatorTaskTracker,
        ct: tokio_util::sync::CancellationToken,
    ) -> Result<(
        tokio::sync::oneshot::Receiver<Option<anyhow::Error>>,
        Box<dyn MessageConsumer>,
    )> {
        let filter_files_config = FilterConfig::try_from(&op_in_config)?;
        let mut op = FilterTask::new(
            op_in_config,
            filter_files_config,
            operator_pipe,
            msg_reg.clone(),
            conn_reg.clone(),
            msg_router_state.clone(),
        );

        let consumer = op.consumer();

        let (tx, rx) = tokio::sync::oneshot::channel();
        tt.spawn(async move {
            if let Err(err) = op.async_main(ct).await {
                error!("{:?}", err);
                if let Err(err_send) = tx.send(Some(err)) {
                    error!("{:?}", err_send);
                }
            } else {
                if let Err(err_send) = tx.send(None) {
                    error!("{:?}", err_send);
                }
            }
        })?;

        Ok((rx, consumer))
    }
}

///////////////////////////////////////////////////////
// Message Consumer

#[derive(Debug, Clone)]
pub struct FilterConsumer {
    msg_reg: Arc<MessageRegistry>,
}

impl MessageConsumer for FilterConsumer {
    fn consumes_message(&self, msg: &Message) -> bool {
        match msg.msg.msg_name() {
            MessageName::Ping => match self.msg_reg.try_cast_msg::<messages::common::Ping>(msg) {
                Ok(messages::common::Ping::Ping) => false,
                Ok(messages::common::Ping::Pong) => true,
                Err(err) => {
                    error!("{:?}", err);
                    false
                }
            },
            MessageName::QueryHandlerRequests => {
                match self
                    .msg_reg
                    .try_cast_msg::<messages::query::QueryHandlerRequests>(msg)
                {
                    Ok(messages::query::QueryHandlerRequests::ListOperatorInstancesResponse {
                        ..
                    }) => true,
                    Ok(messages::query::QueryHandlerRequests::ListOperatorInstancesRequest {
                        ..
                    }) => false,
                    Err(err) => {
                        error!("{:?}", err);
                        false
                    }
                }
            }
            MessageName::ExchangeRequests => {
                match self
                    .msg_reg
                    .try_cast_msg::<messages::exchange::ExchangeRequests>(msg)
                {
                    Ok(messages::exchange::ExchangeRequests::GetNextRecordResponseRecord {
                        ..
                    }) => true,
                    // outbound
                    Ok(messages::exchange::ExchangeRequests::SendRecordResponse { .. }) => true,
                    Ok(messages::exchange::ExchangeRequests::SendRecordRequest { .. }) => false,
                    // inbound
                    Ok(messages::exchange::ExchangeRequests::GetNextRecordResponseNoneLeft) => true,
                    Ok(messages::exchange::ExchangeRequests::GetNextRecordResponseNoneAvailable) => true,
                    Ok(messages::exchange::ExchangeRequests::OperatorCompletedRecordProcessingResponse) => true,
                    Err(err) => {
                        error!("{:?}", err);
                        false
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }
}
