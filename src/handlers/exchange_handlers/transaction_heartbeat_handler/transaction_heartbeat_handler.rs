use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::handlers::{
    message_handler::{
        messages::{
            self,
            message::{Message, MessageName},
        },
        MessageRegistry, Pipe,
    },
    message_router_handler::{MessageConsumer, MessageReceiver, MessageRouterState, Subscriber},
    operator_handler::operators::requests,
};

#[derive(Debug, Error)]
pub enum TransactionbeatHandlerError {
    #[error("reached maximum number of request runtime errors allowed: {0}")]
    ReachedMaximumNumberOfRequestRuntimeErrorsAllowed(u32),
}

pub struct TransactionHeartbeatHandler {
    // working on behalf of
    query_id: u128,
    operator_id: String,
    // communicating with
    exchange_operator_instance_id: u128,
    exchange_worker_id: u128,
    transaction_id: u64,

    last_heartbeat_confirmation: Option<chrono::DateTime<chrono::Utc>>,
    max_heartbeat_interval: chrono::Duration,

    heartbeat_interval_in_ms: chrono::TimeDelta,
    error_delay_in_msg: chrono::TimeDelta,
    request_runtime_errors: u32,
    max_request_runtime_errors: u32,

    subscriber_operator_id: u128,
    router_pipe: Pipe,
    msg_reg: Arc<MessageRegistry>,
    msg_router_state: Arc<Mutex<MessageRouterState>>,
}

impl TransactionHeartbeatHandler {
    pub async fn new(
        query_id: u128,
        operator_id: String,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        transaction_id: u64,
        msg_reg: Arc<MessageRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> TransactionHeartbeatHandler {
        let subscriber_operator_id = Uuid::new_v4().as_u128();

        let router_sender = msg_router_state.lock().await.sender();
        let (mut pipe, sender) = Pipe::new_with_existing_sender(router_sender, 10);
        pipe.set_sent_from_query_id(query_id.clone());
        pipe.set_sent_from_operation_id(subscriber_operator_id.clone());

        msg_router_state.lock().await.add_internal_subscriber(
            Box::new(TransactionHeartbeatHandlerSubscriber { sender }),
            subscriber_operator_id.clone(),
        );

        TransactionHeartbeatHandler {
            query_id,
            operator_id,
            exchange_operator_instance_id,
            exchange_worker_id,
            transaction_id,
            subscriber_operator_id,
            last_heartbeat_confirmation: None,
            max_heartbeat_interval: chrono::Duration::seconds(1),
            heartbeat_interval_in_ms: chrono::Duration::milliseconds(100),
            error_delay_in_msg: chrono::Duration::milliseconds(100),
            request_runtime_errors: 0,
            max_request_runtime_errors: 25,
            router_pipe: pipe,
            msg_reg,
            msg_router_state,
        }
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        debug!(
            operator_id = self.operator_id,
            transaction_id = self.transaction_id,
            "started transaction heartbeat"
        );

        let inner_ct = ct.child_token();

        // propogate the error up to the creator
        self.inner_async_main(inner_ct.clone()).await?;

        self.msg_router_state
            .lock()
            .await
            .remove_internal_subscriber(&self.subscriber_operator_id);

        debug!(
            operator_id = self.operator_id,
            transaction_id = self.transaction_id,
            "completed transaction heartbeat"
        );

        inner_ct.cancel();

        Ok(())
    }

    pub async fn inner_async_main(&mut self, ct: CancellationToken) -> Result<()> {
        let start_time = chrono::Utc::now();
        loop {
            if self.request_runtime_errors >= self.max_request_runtime_errors {
                return Err(
                    TransactionbeatHandlerError::ReachedMaximumNumberOfRequestRuntimeErrorsAllowed(
                        self.request_runtime_errors.clone(),
                    )
                    .into(),
                );
            }
            if ct.is_cancelled() {
                return Ok(());
            }

            let ref mut pipe = self.router_pipe;
            let resp =
                requests::exchange::TransactionHeartbeatRequest::transaction_heartbeat_request(
                    self.operator_id.clone(),
                    self.exchange_operator_instance_id.clone(),
                    self.exchange_worker_id.clone(),
                    self.transaction_id.clone(),
                    pipe,
                    self.msg_reg.clone(),
                )
                .await;
            match resp {
                Ok(resp) => match resp {
                    messages::common::GenericResponse::Ok => {
                        debug!("heartbeat ok");
                    }
                    messages::common::GenericResponse::Error(err) => {
                        error!("error from exchange: {}", err);
                        break;
                    }
                },
                Err(err) => {
                    if ct.is_cancelled() {
                        break;
                    }
                    self.request_runtime_errors += 1;
                    error!("{}", err);

                    tokio::time::sleep(self.error_delay_in_msg.to_std()?).await;
                }
            }

            if let Some(last_heartbeat_confirmation) = self.last_heartbeat_confirmation {
                if chrono::Utc::now().signed_duration_since(last_heartbeat_confirmation)
                    > self.max_heartbeat_interval
                {
                    warn!(
                        query_id = self.query_id.clone(),
                        operator_id = self.operator_id,
                        transaction_id = self.transaction_id,
                        "transaction heartbeat passed allowed interval"
                    );
                    break;
                }
            } else if chrono::Utc::now().signed_duration_since(start_time)
                > self.max_heartbeat_interval
            {
                warn!(
                    query_id = self.query_id.clone(),
                    operator_id = self.operator_id,
                    transaction_id = self.transaction_id,
                    "transaction hearbeat never delivered"
                );
            }

            tokio::time::sleep(self.heartbeat_interval_in_ms.to_std()?).await;
        }
        Ok(())
    }
}

//////////////////////////////////////////////////////
// Message Subscriber

#[derive(Debug, Clone)]
pub struct TransactionHeartbeatHandlerSubscriber {
    sender: mpsc::Sender<Message>,
}

impl Subscriber for TransactionHeartbeatHandlerSubscriber {}

impl MessageConsumer for TransactionHeartbeatHandlerSubscriber {
    fn consumes_message(&self, msg: &Message) -> bool {
        match msg.msg.msg_name() {
            MessageName::CommonGenericResponse => true,
            _ => false,
        }
    }
}

impl MessageReceiver for TransactionHeartbeatHandlerSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
