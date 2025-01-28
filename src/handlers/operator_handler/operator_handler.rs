use std::sync::Arc;

use anyhow::{Context, Result};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::operator_handler_state::{OperatorHandlerState, OperatorInstance, TotalOperatorCompute};
use super::operators;
use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::{
    message_handler::{MessageRegistry, Pipe},
    message_router_handler::{MessageConsumer, MessageReceiver, MessageRouterState, Subscriber},
};

#[derive(Debug, Error)]
pub enum OperatorHandlerError {
    #[error("incorrect message: {0}")]
    IncorrectMessage(String),
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
    #[error("timed out waiting for task to close")]
    TimedOutWaitingForTaskToClose,
}

pub struct OperatorHandler {
    state: OperatorHandlerState,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe,
    sender: mpsc::Sender<Message>,

    msg_reg: Arc<MessageRegistry>,
    conn_reg: Arc<operators::ConnectionRegistry>,
    op_builder: operators::OperatorBuilder,

    tt: tokio_util::task::TaskTracker,
}

impl OperatorHandler {
    pub async fn new(
        message_router_state: Arc<Mutex<MessageRouterState>>,
        msg_reg: Arc<MessageRegistry>,
        op_reg: Arc<operators::OperatorTaskRegistry>,
        conn_reg: Arc<operators::ConnectionRegistry>,
        allowed_compute: TotalOperatorCompute,
    ) -> OperatorHandler {
        let router_sender = message_router_state.lock().await.sender();
        let (pipe, sender) = Pipe::new_with_existing_sender(router_sender, 1);
        let op_builder = operators::OperatorBuilder::new(
            op_reg.clone(),
            msg_reg.clone(),
            conn_reg.clone(),
            message_router_state.clone(),
        );

        let handler = OperatorHandler {
            state: OperatorHandlerState::new(allowed_compute),
            message_router_state,
            router_pipe: pipe,
            sender,
            msg_reg,
            conn_reg,
            op_builder,
            tt: tokio_util::task::TaskTracker::new(),
        };

        handler
    }

    pub fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(OperatorHandlerSubscriber {
            sender: self.sender.clone(),
            msg_reg: self.msg_reg.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        self.message_router_state
            .lock()
            .await
            .add_internal_subscriber(self.subscriber())
            .context("failed subscribing")?;

        loop {
            tokio::select! {
                Some(msg) = self.router_pipe.recv() => {
                    info!("operator handler received message");
                    if let Err(err) = self.handle_message(msg).await {
                        info!("error: {:?}", err);
                    }
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        self.state.close()?;
        self.tt.close();
        tokio::select! {
            _ = self.tt.wait() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                return Err(OperatorHandlerError::TimedOutWaitingForTaskToClose.into());
            }
        }

        info!("closing the operator handler...");
        self.router_pipe.close_receiver();

        Ok(())
    }

    async fn handle_message(&mut self, msg: Message) -> Result<()> {
        match msg.msg.msg_name() {
            MessageName::OperatorInstanceAvailable => self
                .handle_operator_instance_available(msg)
                .await
                .context("failed handling operator instance available message")?,
            MessageName::OperatorInstanceAssignment => self
                .handle_operator_instance_assignment(msg)
                .await
                .context("failed handling operator instance assignment message")?,
            _ => {
                info!("unknown message received: {:?}", msg);
            }
        }
        Ok(())
    }

    async fn handle_operator_instance_assignment(&mut self, msg: Message) -> Result<()> {
        let assignment: &messages::query::OperatorInstanceAssignment =
            self.msg_reg.try_cast_msg(&msg)?;
        let op_in: OperatorInstance = OperatorInstance::try_from(assignment)?;

        match self.op_builder.build_operator(&op_in, &self.tt).await {
            Ok(_) => {
                self.state.add_operator_instance(op_in)?;

                let resp_msg = msg.reply(Box::new(
                    messages::query::OperatorInstanceAssignment::AssignAcceptedResponse {
                        query_id: assignment.get_query_id(),
                        op_instance_id: assignment.get_op_instance_id(),
                        pipeline_id: assignment.get_pipeline_id(),
                    },
                ));
                self.router_pipe.send(resp_msg).await?;
            }
            Err(err) => {
                info!("error: {}", err);

                let resp_msg = msg.reply(Box::new(
                    messages::query::OperatorInstanceAssignment::AssignRejectedResponse {
                        query_id: assignment.get_query_id(),
                        op_instance_id: assignment.get_op_instance_id(),
                        pipeline_id: assignment.get_pipeline_id(),
                        error: err.to_string(),
                    },
                ));
                self.router_pipe.send(resp_msg).await?;
            }
        }

        Ok(())
    }

    async fn handle_operator_instance_available(&self, msg: Message) -> Result<()> {
        let op_in_avail: &messages::query::OperatorInstanceAvailable =
            self.msg_reg.try_cast_msg(&msg)?;
        match op_in_avail {
            messages::query::OperatorInstanceAvailable::Notification => (),
            _ => {
                return Err(
                    OperatorHandlerError::IncorrectMessage(format!("{:?}", op_in_avail)).into(),
                );
            }
        }

        let ref mut allowed_compute = self.state.get_allowed_compute();
        allowed_compute.subtract(&self.state.total_operator_compute());

        info!("allowed_compute: {:?}", allowed_compute);
        if allowed_compute.instances <= 0
            || allowed_compute.memory_in_mib <= 0
            || allowed_compute.cpu_in_thousandths <= 0
        {
            return Ok(());
        }

        let resp = msg.reply(Box::new(
            messages::query::OperatorInstanceAvailable::NotificationResponse {
                can_accept_up_to: allowed_compute.clone(),
            },
        ));
        self.router_pipe.send(resp).await?;

        Ok(())
    }
}

/////////////////////////////////////////////////
// Message subscriber for the operator handler
#[derive(Debug)]
pub struct OperatorHandlerSubscriber {
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
}

impl Subscriber for OperatorHandlerSubscriber {}

impl MessageConsumer for OperatorHandlerSubscriber {
    fn consumes_message(&self, msg: &Message) -> bool {
        match msg.msg.msg_name() {
            MessageName::OperatorInstanceAvailable => {
                match self
                    .msg_reg
                    .try_cast_msg::<messages::query::OperatorInstanceAvailable>(msg)
                {
                    Ok(messages::query::OperatorInstanceAvailable::Notification { .. }) => true,
                    _ => false,
                }
            }
            MessageName::OperatorInstanceAssignment => {
                match self
                    .msg_reg
                    .try_cast_msg::<messages::query::OperatorInstanceAssignment>(msg)
                {
                    Ok(messages::query::OperatorInstanceAssignment::Assign { .. }) => true,
                    _ => false,
                }
            }
            _ => false,
        }
    }
}

impl MessageReceiver for OperatorHandlerSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
