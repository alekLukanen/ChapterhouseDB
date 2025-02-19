use std::sync::Arc;

use anyhow::{Context, Result};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use uuid::Uuid;

use super::operator_handler_state::{OperatorHandlerState, OperatorInstance, TotalOperatorCompute};
use super::operators;
use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::{
    message_handler::{MessageRegistry, Pipe},
    message_router_handler::{MessageConsumer, MessageReceiver, MessageRouterState, Subscriber},
};
use operators::requests;

#[derive(Debug, Error)]
pub enum OperatorHandlerError {
    #[error("incorrect message: {0}")]
    IncorrectMessage(String),
    #[error("timed out waiting for task to close")]
    TimedOutWaitingForTaskToClose,
    #[error("message missing operator instance id")]
    MessageMissingOperatorInstanceId,
    #[error("message missing query id")]
    MessageMissingQueryId,
}

pub struct OperatorHandler {
    operator_id: u128,

    state: OperatorHandlerState,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe,
    sender: mpsc::Sender<Message>,

    msg_reg: Arc<MessageRegistry>,
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
        let operator_id = Uuid::new_v4().as_u128();

        let router_sender = message_router_state.lock().await.sender();
        let (mut pipe, sender) = Pipe::new_with_existing_sender(router_sender, 10);
        pipe.set_sent_from_operation_id(operator_id);

        let op_builder = operators::OperatorBuilder::new(
            op_reg.clone(),
            msg_reg.clone(),
            conn_reg.clone(),
            message_router_state.clone(),
        );

        let handler = OperatorHandler {
            operator_id,
            state: OperatorHandlerState::new(allowed_compute),
            message_router_state,
            router_pipe: pipe,
            sender,
            msg_reg,
            op_builder,
            tt: tokio_util::task::TaskTracker::new(),
        };

        handler
    }

    pub fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(OperatorHandlerSubscriber {
            operator_id: self.operator_id.clone(),
            sender: self.sender.clone(),
            msg_reg: self.msg_reg.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        self.message_router_state
            .lock()
            .await
            .add_internal_subscriber(self.subscriber(), self.operator_id);

        loop {
            tokio::select! {
                Some(msg) = self.router_pipe.recv() => {
                    debug!("received message: {}", msg);
                    if let Err(err) = self.handle_message(msg).await {
                        error!("{:?}", err);
                    }
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        self.message_router_state
            .lock()
            .await
            .remove_internal_subscriber(&self.operator_id);

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
            MessageName::OperatorOperatorInstanceStatusChange => self
                .handle_operator_instance_status_change(msg)
                .await
                .context("failed handling operator instance status change")?,
            _ => {
                debug!("unknown message received: {:?}", msg);
            }
        }
        Ok(())
    }

    async fn handle_operator_instance_status_change(&mut self, msg: Message) -> Result<()> {
        let op_in_id = if let Some(id) = &msg.sent_from_operation_id {
            id
        } else {
            return Err(OperatorHandlerError::MessageMissingOperatorInstanceId.into());
        };

        let op_query_id = if let Some(id) = &msg.sent_from_query_id {
            id
        } else {
            return Err(OperatorHandlerError::MessageMissingQueryId.into());
        };

        let status_change: &messages::operator::OperatorInstanceStatusChange =
            self.msg_reg.try_cast_msg(&msg)?;

        // update the operator state
        match status_change {
            messages::operator::OperatorInstanceStatusChange::Complete => {
                self.state.operator_instance_complete(op_in_id)?;
            }
            messages::operator::OperatorInstanceStatusChange::Error(err_msg) => {
                self.state
                    .operator_instance_error(op_in_id, err_msg.clone())?;
            }
        }

        // response for the operator instance
        let resp_msg = msg.reply(Box::new(messages::common::GenericResponse::Ok));
        self.router_pipe.send(resp_msg).await?;

        // request to the query handler
        let ref mut pipe = self.router_pipe;
        match status_change {
            messages::operator::OperatorInstanceStatusChange::Complete => {
                requests::query::OperatorInstanceStatusChangeRequest::completed_request(
                    op_query_id.clone(),
                    op_in_id.clone(),
                    pipe,
                    self.msg_reg.clone(),
                )
                .await?;
            }
            messages::operator::OperatorInstanceStatusChange::Error(err) => {
                requests::query::OperatorInstanceStatusChangeRequest::errored_request(
                    op_query_id.clone(),
                    op_in_id.clone(),
                    err.clone(),
                    pipe,
                    self.msg_reg.clone(),
                )
                .await?;
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
                error!("error: {}", err);

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

        let comp_avail = self.state.compute_available();

        if comp_avail.instances <= 0
            || comp_avail.memory_in_mib <= 0
            || comp_avail.cpu_in_thousandths <= 0
        {
            return Ok(());
        }

        let resp = msg.reply(Box::new(
            messages::query::OperatorInstanceAvailable::NotificationResponse {
                can_accept_up_to: comp_avail,
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
    operator_id: u128,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
}

impl Subscriber for OperatorHandlerSubscriber {}

impl MessageConsumer for OperatorHandlerSubscriber {
    fn consumes_message(&self, msg: &Message) -> bool {
        // accept these messages from anywhere
        match msg.msg.msg_name() {
            MessageName::OperatorInstanceAvailable => {
                match self
                    .msg_reg
                    .try_cast_msg::<messages::query::OperatorInstanceAvailable>(msg)
                {
                    Ok(messages::query::OperatorInstanceAvailable::Notification { .. }) => {
                        return true;
                    }
                    _ => return false,
                }
            }
            MessageName::OperatorInstanceAssignment => {
                match self
                    .msg_reg
                    .try_cast_msg::<messages::query::OperatorInstanceAssignment>(msg)
                {
                    Ok(messages::query::OperatorInstanceAssignment::Assign { .. }) => return true,
                    _ => return false,
                }
            }
            MessageName::OperatorOperatorInstanceStatusChange => return true,
            _ => (),
        }

        // only accpet other messages intended for this operator
        if msg.route_to_connection_id.is_some()
            || msg.route_to_operation_id != Some(self.operator_id)
        {
            return false;
        }

        match msg.msg.msg_name() {
            MessageName::CommonGenericResponse => true,
            _ => false,
        }
    }
}

impl MessageReceiver for OperatorHandlerSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
