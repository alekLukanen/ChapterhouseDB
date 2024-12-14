use core::fmt;

use anyhow::Result;
use thiserror::Error;
use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::handlers::message_handler::Message;

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
    messenger_sender: mpsc::Sender<Message>,
    messenger_receiver: broadcast::Receiver<Message>,
}

impl MessageRouterHandler {
    pub fn new(
        messenger_sender: mpsc::Sender<Message>,
        messenger_receiver: broadcast::Receiver<Message>,
    ) -> MessageRouterHandler {
        MessageRouterHandler {
            task_tracker: TaskTracker::new(),
            messenger_sender,
            messenger_receiver,
        }
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                msg = self.messenger_receiver.recv() => {
                    match msg {
                        Ok(msg) => info!("message: {:?}", msg),
                        Err(err) => info!("error: {}", err)
                    }
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
