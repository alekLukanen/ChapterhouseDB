use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::handlers::message_handler::{Identify, Message, MessageName, MessageRegistry, Pipe};

use super::message_subscriber::{
    ExternalSubscriber, InternalSubscriber, MessageConsumer, Subscriber,
};

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
    external_subscribers: Arc<Mutex<Vec<ExternalSubscriber>>>,

    internal_subscribers: Arc<Mutex<Vec<InternalSubscriber>>>,
    internal_sub_sender: Sender<Message>,
    internal_sub_receiver: Receiver<Message>,

    msg_reg: Arc<Box<MessageRegistry>>,
}

impl MessageRouterHandler {
    pub fn new(
        worker_id: u128,
        connection_pipe: Pipe<Message>,
        msg_reg: Arc<Box<MessageRegistry>>,
    ) -> MessageRouterHandler {
        let (sender, receiver) = mpsc::channel(1);
        MessageRouterHandler {
            worker_id,
            task_tracker: TaskTracker::new(),
            connection_pipe,
            external_subscribers: Arc::new(Mutex::new(Vec::new())),
            internal_subscribers: Arc::new(Mutex::new(Vec::new())),
            internal_sub_sender: sender,
            internal_sub_receiver: receiver,
            msg_reg,
        }
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = self.connection_pipe.recv() => {
                    // info!("message: {:?}", msg);
                    self.route_msg(msg).await?;
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

    async fn add_internal_subscriber(&mut self, sub: Box<dyn Subscriber>) -> Result<()> {
        let sub_sender = sub.sender();
        let msg_sub = InternalSubscriber::new(sub, sub_sender);
        self.internal_subscribers.lock().await.push(msg_sub);
        Ok(())
    }

    async fn add_external_subscriber(&mut self, sub: ExternalSubscriber) {
        let mut es = self.external_subscribers.lock().await;
        es.retain(|item| *item != sub);
        es.push(sub);
    }

    async fn identify_external_subscriber(&mut self, msg: Message) -> Result<()> {
        let identify_msg: &Identify = self.msg_reg.cast_msg(&msg);
        match identify_msg {
            Identify::Worker { id } => {
                if let Some(inbound_stream_id) = msg.inbound_stream_id {
                    let identify_back = Message::new(Box::new(Identify::Worker {
                        id: self.worker_id.clone(),
                    }))
                    .set_sent_from_worker_id(self.worker_id.clone())
                    .set_inbound_stream_id(inbound_stream_id);
                    self.connection_pipe.send(identify_back).await?;
                } else if let Some(outbound_stream_id) = msg.outbound_stream_id {
                    let worker_id = id.clone();
                    let sub = ExternalSubscriber::OutboundWorker {
                        worker_id: worker_id.clone(),
                        outbound_stream_id,
                    };
                    self.add_external_subscriber(sub).await;
                    info!(
                        "added new external worker subscriber: {}",
                        worker_id.clone()
                    );
                } else {
                    info!("message ignored: {:?}", msg);
                }
            }
            Identify::Connection { id } => {
                if let Some(inbound_stream_id) = msg.inbound_stream_id {
                    let sub = ExternalSubscriber::InboundClientConnection {
                        connection_id: id.clone(),
                        inbound_stream_id,
                    };
                    self.add_external_subscriber(sub).await;

                    let identify_back = Message::new(Box::new(Identify::Worker {
                        id: self.worker_id.clone(),
                    }));
                    self.connection_pipe.send(identify_back).await?;
                } else {
                    info!("message ignored: {:?}", msg);
                }
            }
        }

        Ok(())
    }

    async fn route_msg(&mut self, msg: Message) -> Result<()> {
        match msg.msg.msg_name() {
            MessageName::Identify => self.identify_external_subscriber(msg).await,
            val => {
                Err(MessageRouterError::RoutingRuleNotImplementedForMessage(val.to_string()).into())
            }
        }
    }
}
