use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::handlers::message_handler::{Message, Pipe};

use super::message_subscriber::{InternalSubscriber, Subscriber, WorkerSubscriber};

#[derive(Debug, Error)]
pub enum MessageRouterError {
    #[error("timed out waiting for the tasks to close")]
    TimedOutWaitingForConnectionsToClose,
}

pub struct MessageRouterHandler {
    worker_id: u128,
    connect_to_addresses: Vec<String>,
    task_tracker: TaskTracker,
    connection_pipe: Pipe<Message>,
    internal_subscribers: Arc<Mutex<Vec<InternalSubscriber>>>,
    worker_subscribers: Arc<Mutex<Vec<WorkerSubscriber>>>,
}

impl MessageRouterHandler {
    pub fn new(
        worker_id: u128,
        connect_to_addresses: Vec<String>,
        connection_pipe: Pipe<Message>,
    ) -> MessageRouterHandler {
        MessageRouterHandler {
            worker_id,
            connect_to_addresses,
            task_tracker: TaskTracker::new(),
            connection_pipe,
            internal_subscribers: Arc::new(Mutex::new(Vec::new())),
            worker_subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = self.connection_pipe.recv() => {
                    info!("message: {:?}", msg);
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

    pub async fn add_internal_subscriber(
        &mut self,
        sub: Box<dyn Subscriber>,
    ) -> Result<Pipe<Message>> {
        let (p1, p2) = Pipe::new(1);
        let msg_sub = InternalSubscriber::new(sub, p1);
        self.internal_subscribers.lock().await.push(msg_sub);
        Ok(p2)
    }

    fn route_msg(&self, msg: Message) -> bool {
        false
    }
}
