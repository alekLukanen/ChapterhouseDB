use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tracing::debug;

use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::message_handler::{MessageRegistry, Pipe, Request};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;

#[derive(Debug, Error)]
pub enum IdentifyExchangeRequestError {
    #[error("operator type not implemented: {0}")]
    OperatorTypeNotImplemented(String),
    #[error("received the wrong message type")]
    ReceivedTheWrongMessageType,
    #[error("received multiple operator instances for the exchange")]
    ReceivedMultipleOperatorInstancesForTheExchange,
    #[error("exchange operator instance id not set")]
    ExchangeOperatorInstanceIdNotSet,
    #[error("received message without a worker id")]
    ReceivedMessageWithoutAWorkerId,
    #[error(
        "missing part of response: exchange_operator_instance_id={0:?}, exchange_worker_id={1:?}"
    )]
    MissingPartOfResponse(Option<u128>, Option<u128>),
    #[error("operator instance id could not be determined")]
    OperatorInstanceIdCouldNotBeDetermined,
}

pub struct IdentifyExchangeResponse {
    pub exchange_id: String,
    pub exchange_operator_instance_id: u128,
    pub exchange_worker_id: u128,
}

pub struct IdentifyExchangeRequest<'a> {
    exchange_operator_instance_id: Option<u128>,
    exchange_worker_id: Option<u128>,
    exchange_id: String,
    query_id: u128,
    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> IdentifyExchangeRequest<'a> {
    pub async fn request_outbound_exchange(
        query_id: u128,
        exchange_id: String,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<IdentifyExchangeResponse> {
        Self::request(query_id, exchange_id, pipe, msg_reg.clone()).await
    }

    pub async fn request_inbound_exchanges(
        query_id: u128,
        exchange_ids: Vec<String>,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<Vec<IdentifyExchangeResponse>> {
        debug!("request inbound exchange");

        let mut res: Vec<IdentifyExchangeResponse> = Vec::new();
        for exchange_id in exchange_ids {
            res.push(Self::request(query_id.clone(), exchange_id, pipe, msg_reg.clone()).await?);
        }

        Ok(res)
    }

    async fn request(
        query_id: u128,
        exchange_id: String,
        pipe: &mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> Result<IdentifyExchangeResponse> {
        debug!(query_id = query_id, exchange_id = exchange_id, "request");

        let mut res = IdentifyExchangeRequest {
            exchange_operator_instance_id: None,
            exchange_worker_id: None,
            exchange_id: exchange_id.clone(),
            query_id,
            pipe,
            msg_reg,
        };
        res.identify_exchange().await?;
        if res.exchange_operator_instance_id.is_some() && res.exchange_worker_id.is_some() {
            Ok(IdentifyExchangeResponse {
                exchange_id: exchange_id.clone(),
                exchange_operator_instance_id: res.exchange_operator_instance_id.unwrap(),
                exchange_worker_id: res.exchange_worker_id.unwrap(),
            })
        } else {
            Err(IdentifyExchangeRequestError::MissingPartOfResponse(
                res.exchange_operator_instance_id.clone(),
                res.exchange_worker_id.clone(),
            )
            .into())
        }
    }

    async fn identify_exchange(&mut self) -> Result<()> {
        self.exchange_operator_instance_id = Some(
            self.get_exchange_operator_instance_id_with_retry(10)
                .await?,
        );
        self.exchange_worker_id = Some(self.get_exchange_worker_id_with_retry(10).await?);
        Ok(())
    }

    async fn get_exchange_worker_id_with_retry(&mut self, num_retries: u8) -> Result<u128> {
        let mut last_err: Option<anyhow::Error> = None;
        for retry_idx in 0..(num_retries + 1) {
            match self.get_exchange_worker_id().await {
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
            .context("failed to get the exchange operator worker id"));
    }

    async fn get_exchange_worker_id(&mut self) -> Result<u128> {
        let mut msg = Message::new(Box::new(messages::common::Ping::Ping));
        if let Some(exchange_operator_instance_id) = self.exchange_operator_instance_id {
            msg = msg.set_route_to_operation_id(exchange_operator_instance_id);
        } else {
            return Err(IdentifyExchangeRequestError::ExchangeOperatorInstanceIdNotSet.into());
        }

        let resp_msg = self
            .pipe
            .send_request(Request {
                msg,
                expect_response_msg_name: MessageName::Ping,
                timeout: chrono::Duration::seconds(3),
            })
            .await?;

        let ping_msg: &messages::common::Ping = self.msg_reg.try_cast_msg(&resp_msg)?;
        match ping_msg {
            messages::common::Ping::Pong => {
                if let Some(worker_id) = resp_msg.sent_from_worker_id {
                    Ok(worker_id)
                } else {
                    Err(IdentifyExchangeRequestError::ReceivedMessageWithoutAWorkerId.into())
                }
            }
            messages::common::Ping::Ping => {
                Err(IdentifyExchangeRequestError::ReceivedTheWrongMessageType.into())
            }
        }
    }

    async fn get_exchange_operator_instance_id_with_retry(
        &mut self,
        num_retries: u8,
    ) -> Result<u128> {
        let mut last_err: Option<anyhow::Error> = None;
        for retry_idx in 0..(num_retries + 1) {
            let res = self.get_exchange_operator_instance_id().await;
            match res {
                Ok(Some(val)) => {
                    return Ok(val);
                }
                Ok(None) => {
                    debug!(
                        operator_id = self.exchange_id.clone(),
                        query_id = self.query_id.clone(),
                        "query handler didn't have an operator instance id for exchange operator; retrying after delay",
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(std::cmp::min(
                        retry_idx as u64 + 1,
                        5,
                    )))
                    .await;
                    continue;
                }
                Err(err) => {
                    debug!(
                        operator_id = self.exchange_id.clone(),
                        query_id = self.query_id.clone(),
                        "query handler request returned an error; retrying after delay"
                    );
                    if retry_idx == num_retries {
                        last_err = Some(err);
                    } else {
                        tokio::time::sleep(std::time::Duration::from_secs(std::cmp::min(
                            retry_idx as u64 + 1,
                            5,
                        )))
                        .await;
                        continue;
                    }
                }
            }
        }
        if let Some(err) = last_err {
            Err(err.context("unable to get operator instance id"))
        } else {
            Err(IdentifyExchangeRequestError::OperatorInstanceIdCouldNotBeDetermined.into())
        }
    }

    async fn get_exchange_operator_instance_id(&mut self) -> Result<Option<u128>> {
        // find the worker with the exchange
        let list_msg = Message::new(Box::new(
            messages::query::QueryHandlerRequests::ListOperatorInstancesRequest {
                query_id: self.query_id.clone(),
                operator_id: self.exchange_id.clone(),
            },
        ));

        let resp_msg = self
            .pipe
            .send_request(Request {
                msg: list_msg,
                expect_response_msg_name: MessageName::QueryHandlerRequests,
                timeout: chrono::Duration::seconds(10),
            })
            .await?;

        let resp_msg: &messages::query::QueryHandlerRequests =
            self.msg_reg.try_cast_msg(&resp_msg)?;
        match resp_msg {
            messages::query::QueryHandlerRequests::ListOperatorInstancesResponse {
                op_instance_ids,
            } => {
                if op_instance_ids.len() == 1 {
                    Ok(Some(op_instance_ids.get(0).unwrap().clone()))
                } else if op_instance_ids.len() == 0 {
                    return Ok(None);
                } else {
                    return Err(
                        IdentifyExchangeRequestError::ReceivedMultipleOperatorInstancesForTheExchange
                            .into(),
                        );
                }
            }
            _ => {
                return Err(IdentifyExchangeRequestError::ReceivedTheWrongMessageType.into());
            }
        }
    }
}
