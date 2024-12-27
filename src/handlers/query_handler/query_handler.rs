use std::sync::Arc;

use anyhow::{Context, Result};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::query_handler_state::{self, QueryHandlerState};
use crate::handlers::message_handler::{
    Message, MessageName, MessageRegistry, OperatorInstanceAssignment, OperatorInstanceAvailable,
    Pipe, RunQuery, RunQueryResp,
};
use crate::handlers::message_router_handler::{
    MessageConsumer, MessageReceiver, MessageRouterState, Subscriber,
};
use crate::planner::{LogicalPlanner, PhysicalPlanner};

#[derive(Debug, Error)]
pub enum QueryHandlerError {
    #[error("incorrect message: {0}")]
    IncorrectMessage(String),
}

#[derive(Debug)]
pub struct QueryHandler {
    state: QueryHandlerState,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe<Message>,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
}

impl QueryHandler {
    pub async fn new(
        message_router_state: Arc<Mutex<MessageRouterState>>,
        msg_reg: Arc<MessageRegistry>,
    ) -> QueryHandler {
        let router_sender = message_router_state.lock().await.sender();
        let (pipe, sender) = Pipe::new_with_existing_sender(router_sender, 1);

        let handler = QueryHandler {
            state: QueryHandlerState::new(),
            message_router_state,
            router_pipe: pipe,
            sender,
            msg_reg,
        };

        handler
    }

    pub fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(QueryHandlerSubscriber {
            sender: self.sender.clone(),
            msg_reg: self.msg_reg.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        self.message_router_state
            .lock()
            .await
            .add_internal_subscriber(self.subscriber())?;

        loop {
            tokio::select! {
                Some(msg) = self.router_pipe.recv() => {
                    self.handle_message(msg).await?;
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        info!("closing the query handler...");
        self.router_pipe.close_receiver();

        Ok(())
    }

    async fn handle_message(&mut self, msg: Message) -> Result<()> {
        match msg.msg.msg_name() {
            MessageName::RunQuery => self
                .handle_run_query(&msg)
                .await
                .context("failed handling the run query message")?,
            MessageName::OperatorInstanceAvailable => self
                .handle_operator_instance_response(&msg)
                .await
                .context("failed handling the operator instance available response message")?,
            _ => {
                info!("unknown message received: {:?}", msg);
            }
        }
        Ok(())
    }

    async fn handle_operator_instance_response(&mut self, msg: &Message) -> Result<()> {
        let op_avail_resp: &OperatorInstanceAvailable = self.msg_reg.try_cast_msg(msg)?;
        let can_accept_up_to = match op_avail_resp {
            OperatorInstanceAvailable::NotificationResponse { can_accept_up_to } => {
                can_accept_up_to
            }
            _ => {
                return Err(
                    QueryHandlerError::IncorrectMessage(format!("{:?}", op_avail_resp)).into(),
                );
            }
        };

        let operator_instances = self
            .state
            .claim_operator_instances_up_to_compute_available(can_accept_up_to);

        let msgs = operator_instances
            .iter()
            .map(|item| {
                msg.reply(Box::new(OperatorInstanceAssignment::Assign {
                    op_instance_id: item.1.id,
                    query_id: item.0,
                    pipeline_id: item.1.pipeline_id.clone(),
                    operator: item.2.clone(),
                }))
            })
            .collect();
        self.router_pipe.send_all(msgs).await?;

        info!("state: {:?}", self.state);

        Ok(())
    }

    async fn handle_run_query(&mut self, msg: &Message) -> Result<()> {
        let run_query: &RunQuery = self.msg_reg.try_cast_msg(&msg)?;

        let logical_plan = match LogicalPlanner::new(run_query.query.clone()).build() {
            Ok(plan) => plan,
            Err(err) => {
                info!("error: {}", err);
                let not_created_resp = msg.reply(Box::new(RunQueryResp::NotCreated));
                self.router_pipe.send(not_created_resp).await?;
                return Ok(());
            }
        };
        let physical_plan = match PhysicalPlanner::new(logical_plan).build() {
            Ok(plan) => plan,
            Err(err) => {
                info!("error: {}", err);
                let not_created_resp = msg.reply(Box::new(RunQueryResp::NotCreated));
                self.router_pipe.send(not_created_resp).await?;
                return Ok(());
            }
        };

        let mut query = query_handler_state::Query::new(run_query.query.clone(), physical_plan);
        query.init();

        let run_query_resp = msg.reply(Box::new(RunQueryResp::Created {
            query_id: query.id.clone(),
        }));

        self.state.add_query(query);
        self.router_pipe.send(run_query_resp).await?;

        info!("added a new query");

        let in_avail_msg = Message::new(Box::new(OperatorInstanceAvailable::Notification));
        self.router_pipe.send(in_avail_msg).await?;

        Ok(())
    }
}

/////////////////////////////////////////////////
// Message subscriber for the query handler
#[derive(Debug)]
pub struct QueryHandlerSubscriber {
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
}

impl Subscriber for QueryHandlerSubscriber {}

impl MessageConsumer for QueryHandlerSubscriber {
    fn consumes_message(&self, msg: &Message) -> bool {
        match msg.msg.msg_name() {
            MessageName::RunQuery => true,
            MessageName::OperatorInstanceAvailable => {
                match self.msg_reg.try_cast_msg::<OperatorInstanceAvailable>(msg) {
                    Ok(OperatorInstanceAvailable::NotificationResponse { .. }) => true,
                    _ => false,
                }
            }
            MessageName::OperatorInstanceAssignment => {
                match self.msg_reg.try_cast_msg::<OperatorInstanceAssignment>(msg) {
                    Ok(OperatorInstanceAssignment::AssignAcceptedResponse) => true,
                    Ok(OperatorInstanceAssignment::AssignRejectedResponse) => true,
                    _ => false,
                }
            }
            _ => false,
        }
    }
}

impl MessageReceiver for QueryHandlerSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
