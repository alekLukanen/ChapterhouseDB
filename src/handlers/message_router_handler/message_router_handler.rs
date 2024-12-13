use core::fmt;

use anyhow::Result;
use thiserror::Error;
use tokio::{
    select,
    sync::{mpsc, Mutex},
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::handlers::message_handler::{Message, MessageName};

#[derive(Debug, Error)]
pub enum MessageRouterError {
    #[error("timed out waiting for the tasks to close")]
    TimedOutWaitingForConnectionsToClose,
}

pub trait Subscriber: fmt::Debug + Send + Sync {
    fn consumes_message(&self, msg: &Message) -> bool;
}

#[derive(Debug)]
struct MessageSubscriber {
    sub: Box<dyn Subscriber>,
    inbound: mpsc::Receiver<Message>,
    outbound: mpsc::Sender<Message>,
}

impl MessageSubscriber {
    fn new(
        sub: Box<dyn Subscriber>,
        inbound: mpsc::Receiver<Message>,
        outbound: mpsc::Sender<Message>,
    ) -> MessageSubscriber {
        MessageSubscriber {
            sub,
            inbound,
            outbound,
        }
    }

    async fn async_main(&mut self, consumer_ct: CancellationToken) -> Result<()> {
        loop {
            select! {
                Some(msg) = self.inbound.recv() => {
                    info!("message: {:?}", msg);
                },
                _ = consumer_ct.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }
}

pub struct MessageRouterHandler {
    task_tracker: TaskTracker,
    subscribers: Mutex<Vec<MessageSubscriber>>,
}

impl MessageRouterHandler {
    pub fn new() -> MessageRouterHandler {
        MessageRouterHandler {
            task_tracker: TaskTracker::new(),
            subscribers: Mutex::new(Vec::new()),
        }
    }

    pub async fn async_main(&self, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        self.task_tracker.close();
        tokio::select! {
            _ = self.task_tracker.wait() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                return Err(MessageRouterError::TimedOutWaitingForConnectionsToClose.into());
            }
        }

        Ok(())
    }

    pub async fn add_subscriber(
        &mut self,
        ct: CancellationToken,
        sub: Box<dyn Subscriber>,
    ) -> Result<(mpsc::Sender<Message>, mpsc::Receiver<Message>)> {
        let (router_tx, sub_rx) = mpsc::channel(1);
        let (sub_tx, router_rx) = mpsc::channel(1);

        let mut msg_sub = MessageSubscriber::new(sub, router_rx, router_tx);
        self.task_tracker.spawn(async move {
            if let Err(err) = msg_sub.async_main(ct.clone()).await {
                info!("error: {}", err);
            }
        });

        Ok((sub_tx.clone(), sub_rx))
    }
}
