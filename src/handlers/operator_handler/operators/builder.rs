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

use super::operator_task_trackers::RestrictedOperatorTaskTracker;
use super::traits::TableFuncTaskBuilder;

#[derive(Debug, Error)]
pub enum BuildOperatorError {
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

pub struct OperatorRegistry {
    table_func_task_builders: Vec<Box<dyn TableFuncTaskBuilder>>,
}

impl OperatorRegistry {
    pub fn new() -> OperatorRegistry {
        OperatorRegistry {
            table_func_task_builders: Vec::new(),
        }
    }

    pub fn add_table_func_task_builder(mut self, bldr: Box<dyn TableFuncTaskBuilder>) -> Self {
        self.table_func_task_builders.push(bldr);
        self
    }

    pub fn get_table_func_task_builders(&self) -> &Vec<Box<dyn TableFuncTaskBuilder>> {
        &self.table_func_task_builders
    }
}

pub struct OperatorBuilder {
    op_reg: Arc<OperatorRegistry>,
    msg_reg: Arc<MessageRegistry>,
    message_router_state: Arc<Mutex<MessageRouterState>>,
}

impl OperatorBuilder {
    pub fn new(
        op_reg: Arc<OperatorRegistry>,
        msg_reg: Arc<MessageRegistry>,
        message_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> OperatorBuilder {
        OperatorBuilder {
            op_reg,
            msg_reg,
            message_router_state,
        }
    }

    pub fn build_operator(&self, op_in: &OperatorInstance, tt: &TaskTracker) -> Result<()> {
        let regstricted_tt = RestrictedOperatorTaskTracker::new(tt, 1);

        Ok(())
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
