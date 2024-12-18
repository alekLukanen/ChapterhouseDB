use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::handlers::message_handler::{Message, MessageName, Pipe};

use super::message_subscriber::{ExternalSubscriber, InternalSubscriber, Subscriber};

#[derive(Debug, Error)]
pub enum MessageRouterError {
    #[error("timed out waiting for the tasks to close")]
    TimedOutWaitingForConnectionsToClose,
    #[error("routing rule not implemented for message {0}")]
    RoutingRuleNotImplementedForMessage(String),
}

pub struct MessageRouterHandler {
    worker_id: u128,
    task_tracker: TaskTracker,
    connection_pipe: Pipe<Message>,
    internal_subscribers: Arc<Mutex<Vec<InternalSubscriber>>>,
    external_subscribers: Arc<Mutex<Vec<ExternalSubscriber>>>,
}

impl MessageRouterHandler {
    pub fn new(worker_id: u128, connection_pipe: Pipe<Message>) -> MessageRouterHandler {
        MessageRouterHandler {
            worker_id,
            task_tracker: TaskTracker::new(),
            connection_pipe,
            internal_subscribers: Arc::new(Mutex::new(Vec::new())),
            external_subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = self.connection_pipe.recv() => {
                    info!("message: {:?}", msg);
                    self.route_msg(msg);
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        info!("message router handler closing...");

        self.task_tracker.close();
        tokio::select! {
            _ = self.task_tracker.wait() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                return Err(MessageRouterError::TimedOutWaitingForConnectionsToClose.into());
            }
        }

        Ok(())
    }

    async fn add_internal_subscriber(&mut self, sub: Box<dyn Subscriber>) -> Result<Pipe<Message>> {
        let (p1, p2) = Pipe::new(1);
        let msg_sub = InternalSubscriber::new(sub, p1);
        self.internal_subscribers.lock().await.push(msg_sub);
        Ok(p2)
    }

    async fn add_external_subscriber(&mut self, sub: ExternalSubscriber) -> Result<()> {
        self.external_subscribers.lock().await.push(sub);
        Ok(())
    }

    async fn identify_worker(&mut self, msg: Message) -> Result<()> {
        Ok(())
    }

    async fn route_msg(&mut self, msg: Message) -> Result<()> {
        match msg.msg.msg_name() {
            MessageName::Identify => self.identify_worker(msg).await,
            val => {
                Err(MessageRouterError::RoutingRuleNotImplementedForMessage(val.to_string()).into())
            }
        }
    }
}
