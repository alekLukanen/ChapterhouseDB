use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::handlers::{
    message_handler::MessageRegistry, message_router_handler::MessageRouterState,
    operator_handler::operator_handler_state::OperatorInstance,
};
use crate::planner;

use super::traits::OperatorTaskBuilder;
use super::{table_func_producer_operator::TableFuncConfig, TableFuncProducerOperator};

#[derive(Debug, Error)]
pub enum BuildOperatorError {
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

pub struct OperatorRegistry {
    tt: TaskTracker,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    table_func_operator_task_builders: Vec<Box<dyn OperatorTaskBuilder>>,
}

impl OperatorRegistry {
    pub fn new(message_router_state: Arc<Mutex<MessageRouterState>>) -> OperatorRegistry {
        OperatorRegistry {
            tt: TaskTracker::new(),
            message_router_state,
            table_func_operator_task_builders: Vec::new(),
        }
    }
}

pub async fn build_operator(
    op_in: &OperatorInstance,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    msg_reg: Arc<MessageRegistry>,
    tt: &TaskTracker,
) -> Result<()> {
    match &op_in.config.operator.operator_task {
        planner::OperatorTask::Producer { typ, .. } => match typ {
            planner::TaskType::TableFunc { .. } => {
                let mut operator = TableFuncProducerOperator::new(
                    op_in.config.clone(),
                    TableFuncConfig::try_from(&op_in.config)?,
                    message_router_state,
                    msg_reg,
                )
                .await;
                let ct = op_in.ct.clone();
                tt.spawn(async move {
                    if let Err(err) = operator.async_main(ct).await {
                        info!("error: {:?}", err);
                    }
                });
            }
            planner::TaskType::Table { .. } => {
                return Err(BuildOperatorError::NotImplemented(
                    "table producer operator not implemented",
                )
                .into())
            }
            planner::TaskType::Filter { .. } => {
                return Err(BuildOperatorError::NotImplemented(
                    "filter producer operator not implemented",
                )
                .into())
            }
            planner::TaskType::Materialize { .. } => {
                return Err(BuildOperatorError::NotImplemented(
                    "materialize producer operator not implemented",
                )
                .into())
            }
        },
        planner::OperatorTask::Exchange { .. } => {
            return Err(
                BuildOperatorError::NotImplemented("exchange operator not implemented").into(),
            )
        }
    }

    Ok(())
}
