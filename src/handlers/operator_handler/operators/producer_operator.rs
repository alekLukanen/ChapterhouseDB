use std::sync::Arc;

use anyhow::{Context, Error, Result};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error};

use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::message_handler::{MessageRegistry, Pipe};
use crate::handlers::message_router_handler::{
    MessageConsumer, MessageReceiver, MessageRouterState, Subscriber,
};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;
use crate::handlers::operator_handler::operators::common_message_handlers::handle_ping_message;
use crate::handlers::operator_handler::operators::requests;

use super::operator_task_trackers::RestrictedOperatorTaskTracker;

#[derive(Debug, Error)]
pub enum ProducerOperatorError {
    #[error("timed out waiting for task to close")]
    TimedOutWaitingForTaskToClose,
}

#[derive(Debug)]
pub struct ProducerOperator {
    operator_instance_config: OperatorInstanceConfig,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe,
    task_pipe: Pipe,
    task_msg_consumer: Option<Box<dyn MessageConsumer>>,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
    tt: TaskTracker,
    task_ct: CancellationToken,
}

impl ProducerOperator {
    pub async fn new(
        op_in_config: OperatorInstanceConfig,
        message_router_state: Arc<Mutex<MessageRouterState>>,
        task_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> ProducerOperator {
        let router_sender = message_router_state.lock().await.sender();
        let (mut pipe, sender) = Pipe::new_with_existing_sender(router_sender, 10);
        pipe.set_sent_from_query_id(op_in_config.query_id.clone());
        pipe.set_sent_from_operation_id(op_in_config.id.clone());

        ProducerOperator {
            operator_instance_config: op_in_config,
            message_router_state,
            router_pipe: pipe,
            task_pipe,
            task_msg_consumer: None,
            sender,
            msg_reg,
            tt: TaskTracker::new(),
            task_ct: CancellationToken::new(),
        }
    }

    fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(ProducerOperatorSubscriber {
            sender: self.sender.clone(),
            operator_instance_id: self.operator_instance_config.id.clone(),
            query_id: self.operator_instance_config.query_id.clone(),
        })
    }

    pub fn get_task_ct(&self) -> CancellationToken {
        self.task_ct.clone()
    }

    pub fn set_task_msg_consumer(mut self, task_msg_consumer: Box<dyn MessageConsumer>) -> Self {
        self.task_msg_consumer = Some(task_msg_consumer);
        self
    }

    pub fn restricted_tt(&self) -> RestrictedOperatorTaskTracker {
        RestrictedOperatorTaskTracker::new(&self.tt, 1)
    }

    pub async fn async_main(
        &mut self,
        ct: CancellationToken,
        mut task_res: tokio::sync::oneshot::Receiver<Option<Error>>,
    ) -> Result<()> {
        self.message_router_state
            .lock()
            .await
            .add_internal_subscriber(self.subscriber())
            .context("failed subscribing")?;

        debug!(
            operator_task = self
                .operator_instance_config
                .operator
                .operator_type
                .task_name(),
            operator_id = self.operator_instance_config.operator.id,
            operator_instance_id = self.operator_instance_config.id,
            "started producer operator instance",
        );

        loop {
            tokio::select! {
                Some(msg) = self.router_pipe.recv() => {
                    debug!("received message: {}", msg);
                    if msg.msg.msg_name() == MessageName::Ping {
                        let ping_msg: &messages::common::Ping = self.msg_reg.try_cast_msg(&msg)?;
                        if matches!(ping_msg, messages::common::Ping::Ping) {
                            let pong_msg = handle_ping_message(&msg, ping_msg)?;
                            self.router_pipe.send(pong_msg).await?;
                        }
                    }
                    if let Some(task_msg_consumer) = &self.task_msg_consumer {
                        if task_msg_consumer.consumes_message(&msg) {
                            self.task_pipe.send(msg).await?;
                        } else {
                            debug!("message not consumed by producer operator: {}", msg);
                        }
                    } else {
                        self.task_pipe.send(msg).await?;
                    }
                }
                Some(msg) = self.task_pipe.recv() => {
                    self.router_pipe.send(msg).await?;
                }
                res_err = &mut task_res => {
                    debug!("task future terminated");
                    let ref mut pipe = self.router_pipe;
                    match res_err {
                        Ok(None) => {
                            requests::operator::OperatorInstanceStatusChangeRequest::completed_request(
                                pipe, self.msg_reg.clone()
                            ).await?;
                        }
                        Ok(Some(res_err)) => {
                            requests::operator::OperatorInstanceStatusChangeRequest::errored_request(
                                res_err.to_string(), pipe, self.msg_reg.clone()
                            ).await?;
                        }
                        Err(err) => {
                            error!("{:?}", err);
                        }
                    }
                    break;
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        debug!("closing the task tracker");

        self.task_ct.cancel();
        self.tt.close();
        tokio::select! {
            _ = self.tt.wait() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                return Err(ProducerOperatorError::TimedOutWaitingForTaskToClose.into());
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
            "closed producer operator instance",
        );

        Ok(())
    }
}

//////////////////////////////////////////////////////
// Message Subscriber

#[derive(Debug, Clone)]
pub struct ProducerOperatorSubscriber {
    sender: mpsc::Sender<Message>,
    operator_instance_id: u128,
    query_id: u128,
}

impl Subscriber for ProducerOperatorSubscriber {}

impl MessageConsumer for ProducerOperatorSubscriber {
    fn consumes_message(&self, msg: &Message) -> bool {
        if msg.route_to_operation_id.is_none() && msg.sent_from_query_id.is_none() {
            false
        } else if (msg.route_to_operation_id.is_some()
            && msg.route_to_operation_id != Some(self.operator_instance_id))
            || (msg.sent_from_query_id.is_some() && msg.sent_from_query_id != Some(self.query_id))
        {
            false
        } else {
            true
        }
    }
}

impl MessageReceiver for ProducerOperatorSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
