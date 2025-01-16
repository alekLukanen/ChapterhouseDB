use std::{sync::Arc, u128};

use anyhow::Result;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info};

use crate::handlers::message_handler::{Identify, Message, MessageName, MessageRegistry, Pipe};

use super::message_subscriber::{ExternalSubscriber, InternalSubscriber, Subscriber};

#[derive(Debug, Error)]
pub enum MessageRouterError {
    #[error("timed out waiting for the tasks to close")]
    TimedOutWaitingForConnectionsToClose,
}

#[derive(Debug)]
pub struct MessageRouterState {
    external_subscribers: Vec<ExternalSubscriber>,
    internal_subscribers: Vec<InternalSubscriber>,
    internal_sub_sender: Sender<Message>,
}

impl MessageRouterState {
    pub fn new(internal_sub_sender: Sender<Message>) -> MessageRouterState {
        MessageRouterState {
            external_subscribers: Vec::new(),
            internal_subscribers: Vec::new(),
            internal_sub_sender,
        }
    }

    pub fn add_internal_subscriber(&mut self, sub: Box<dyn Subscriber>) -> Result<()> {
        let sub_sender = sub.sender();
        let msg_sub = InternalSubscriber::new(sub, sub_sender);
        self.internal_subscribers.push(msg_sub);
        Ok(())
    }

    pub fn add_external_subscriber(&mut self, sub: ExternalSubscriber) -> Result<()> {
        self.external_subscribers.retain(|item| *item != sub);
        self.external_subscribers.push(sub);
        Ok(())
    }

    pub fn get_all_outbound_streams(&self) -> Vec<u128> {
        let mut outbound_stream_ids = Vec::new();
        for sub in &self.external_subscribers {
            match sub {
                ExternalSubscriber::OutboundWorker {
                    outbound_stream_id, ..
                } => {
                    outbound_stream_ids.push(outbound_stream_id.clone());
                }
                _ => (),
            }
        }
        outbound_stream_ids
    }

    pub fn get_worker_outbound_stream(&self, w_id: u128) -> Option<u128> {
        for sub in &self.external_subscribers {
            match sub {
                ExternalSubscriber::OutboundWorker {
                    worker_id,
                    outbound_stream_id,
                } => {
                    if w_id == *worker_id {
                        return Some(outbound_stream_id.clone());
                    }
                }
                _ => (),
            }
        }
        return None;
    }

    pub fn sender(&self) -> Sender<Message> {
        self.internal_sub_sender.clone()
    }
}

pub struct MessageRouterHandler {
    state: Arc<Mutex<MessageRouterState>>,

    worker_id: u128,
    msg_reg: Arc<MessageRegistry>,
    task_tracker: TaskTracker,

    connection_pipe: Pipe,
    internal_sub_receiver: Receiver<Message>,
}

impl MessageRouterHandler {
    pub fn new(
        worker_id: u128,
        connection_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> (MessageRouterHandler, Arc<Mutex<MessageRouterState>>) {
        let (sender, receiver) = mpsc::channel(1);
        let state = Arc::new(Mutex::new(MessageRouterState::new(sender.clone())));
        let handler = MessageRouterHandler {
            worker_id,
            task_tracker: TaskTracker::new(),
            connection_pipe,
            internal_sub_receiver: receiver,
            state: state.clone(),
            msg_reg,
        };
        (handler, state)
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = self.connection_pipe.recv() => {
                    let routed = self.route_msg(&msg).await?;
                    if !routed {
                        info!("message ignored: {}", msg);
                    }
                }
                Some(mut msg) = self.internal_sub_receiver.recv() => {
                    debug!("route message: {}", msg);
                    msg = msg.set_sent_from_worker_id(self.worker_id.clone());

                    if msg.inbound_stream_id.is_some() || msg.outbound_stream_id.is_some() {
                        // route to known stream
                        self.connection_pipe.send(msg).await?;

                    } else if msg.route_to_operation_id.is_some() {
                        // route to internal operation subscriber
                        debug!("route message to operation: {}", msg.msg.msg_name());
                        if let Some(route_to_worker_id) = msg.route_to_worker_id {
                            if route_to_worker_id != self.worker_id {
                                let outbound_stream_id = self.state.lock().await.get_worker_outbound_stream(route_to_worker_id);
                                if let Some(outbound_stream_id) = outbound_stream_id {
                                    msg = msg.set_outbound_stream(outbound_stream_id);
                                    self.connection_pipe.send(msg).await?;
                                }
                            } else {
                                let routed = self.route_msg(&msg).await?;
                                if !routed {
                                    debug!("message not routed");
                                }
                            }
                        } else {
                            let routed = self.route_msg(&msg).await?;
                            if !routed {
                                debug!("message not routed internally; sending to outbound streams/workers");
                                let outbound_stream_ids = self.state.lock().await.get_all_outbound_streams();
                                for out_id in outbound_stream_ids {
                                    let msg = msg.clone().set_outbound_stream(out_id);
                                    self.connection_pipe.send(msg).await?;
                                }
                            }
                        }

                    } else if let Some(route_to_worker_id) = msg.route_to_worker_id {
                        // route to specific outbound worker
                        debug!("route message to specific worker: {}", msg);
                        if route_to_worker_id != self.worker_id {
                            let outbound_stream_id = self.state.lock().await.get_worker_outbound_stream(route_to_worker_id);
                            if let Some(outbound_stream_id) = outbound_stream_id {
                                msg = msg.set_outbound_stream(outbound_stream_id);
                                self.connection_pipe.send(msg).await?;
                            }
                        } else {
                            let routed = self.route_msg(&msg).await?;
                            if !routed {
                                debug!("message not routed");
                            }
                        }

                    } else if let Some(_) = msg.route_to_connection_id {
                        // route to inbound connection
                        info!("route to connection not implemented: {}", msg);
                        panic!("route to connection not implemented");

                    } else if msg.route_to_worker_id.is_none() && msg.route_to_operation_id.is_none() && msg.route_to_connection_id.is_none() {
                        // broadcast to any subscriber
                        debug!("broadcasting message: {}", msg);
                        let routed = self.route_msg(&msg).await?;
                        if !routed {
                            debug!("message not routed internally; sending to outbound streams/workers");
                            let outbound_stream_ids = self.state.lock().await.get_all_outbound_streams();
                            for out_id in outbound_stream_ids {
                                let msg = msg.clone().set_outbound_stream(out_id);
                                self.connection_pipe.send(msg).await?;
                            }
                        }

                    } else {
                        info!("unable to route message from internal sub: {}", msg.msg.msg_name());
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

    async fn identify_external_subscriber(&mut self, msg: &Message) -> Result<bool> {
        let identify_msg: &Identify = self.msg_reg.cast_msg(msg);
        match identify_msg {
            Identify::Worker { id } => {
                if let Some(inbound_stream_id) = msg.inbound_stream_id {
                    let identify_back = Message::new(Box::new(Identify::Worker {
                        id: self.worker_id.clone(),
                    }))
                    .set_sent_from_worker_id(self.worker_id.clone())
                    .set_route_to_worker_id(id.clone())
                    .set_inbound_stream_id(inbound_stream_id);
                    self.connection_pipe.send(identify_back).await?;
                } else if let Some(outbound_stream_id) = msg.outbound_stream_id {
                    let worker_id = id.clone();
                    let sub = ExternalSubscriber::OutboundWorker {
                        worker_id: worker_id.clone(),
                        outbound_stream_id,
                    };
                    self.state.lock().await.add_external_subscriber(sub)?;
                    info!(
                        "added new external worker subscriber: {}",
                        worker_id.clone()
                    );
                } else {
                    return Ok(false);
                }
            }
            Identify::Connection { id } => {
                if let Some(inbound_stream_id) = msg.inbound_stream_id {
                    let sub = ExternalSubscriber::InboundClientConnection {
                        connection_id: id.clone(),
                        inbound_stream_id,
                    };
                    self.state.lock().await.add_external_subscriber(sub)?;

                    let identify_back = Message::new(Box::new(Identify::Worker {
                        id: self.worker_id.clone(),
                    }))
                    .set_sent_from_worker_id(self.worker_id.clone())
                    .set_route_to_connection_id(id.clone())
                    .set_inbound_stream_id(inbound_stream_id);
                    self.connection_pipe.send(identify_back).await?;
                } else {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    async fn route_to_internal_subscriber(&mut self, msg: &Message) -> Result<bool> {
        if let Some(route_to_worker_id) = msg.route_to_worker_id {
            if route_to_worker_id != self.worker_id {
                return Ok(false);
            }
        }

        let state = self.state.lock().await;
        let subs = state
            .internal_subscribers
            .iter()
            .filter(|&item| item.sub.consumes_message(&msg));

        let mut sent = false;
        for sub in subs {
            if let Err(err) = sub.sender.send(msg.clone()).await {
                info!("unable to send to subscriber; received error: {}", err);
            }
            sent = true;
        }

        Ok(sent)
    }

    async fn route_msg(&mut self, msg: &Message) -> Result<bool> {
        match msg.msg.msg_name() {
            MessageName::Identify => self.identify_external_subscriber(msg).await,
            _ => self.route_to_internal_subscriber(msg).await,
        }
    }
}
