use std::sync::Arc;

use anyhow::{Context, Result};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::handlers::message_handler::{Message, MessageRegistry, Pipe};
use crate::handlers::message_router_handler::{
    MessageConsumer, MessageReceiver, MessageRouterState, Subscriber,
};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;

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
    router_pipe: Pipe<Message>,
    task_pipe: Pipe<Message>,
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
        task_pipe: Pipe<Message>,
        msg_reg: Arc<MessageRegistry>,
    ) -> ProducerOperator {
        let router_sender = message_router_state.lock().await.sender();
        let (pipe, sender) = Pipe::new_with_existing_sender(router_sender, 1);

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
            msg_reg: self.msg_reg.clone(),
            operator_instance_id: self.operator_instance_config.id.clone(),
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
        task_fut: tokio::task::JoinHandle<()>,
    ) -> Result<()> {
        self.message_router_state
            .lock()
            .await
            .add_internal_subscriber(self.subscriber())
            .context("failed subscribing")?;

        info!(
            "started the producer operator for instance {}",
            self.operator_instance_config.id
        );

        loop {
            tokio::select! {
                _ = task_fut => {
                    info!("task future terminated");
                    break;
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        self.task_ct.cancel();
        tokio::select! {
            _ = self.tt.wait() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                return Err(ProducerOperatorError::TimedOutWaitingForTaskToClose.into());
            }
        }

        info!(
            "closed producer operator for instance {}",
            self.operator_instance_config.id
        );

        Ok(())
    }
}

//////////////////////////////////////////////////////
// Message Subscriber

#[derive(Debug, Clone)]
pub struct ProducerOperatorSubscriber {
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
    operator_instance_id: u128,
}

impl Subscriber for ProducerOperatorSubscriber {}

impl MessageConsumer for ProducerOperatorSubscriber {
    fn consumes_message(&self, msg: &crate::handlers::message_handler::Message) -> bool {
        if let Some(route_to_operation_id) = msg.route_to_operation_id {
            if route_to_operation_id == self.operator_instance_id {
                return true;
            }
        } else {
            return false;
        }
        false
    }
}

impl MessageReceiver for ProducerOperatorSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
