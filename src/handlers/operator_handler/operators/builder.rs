use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::handlers::message_handler::Pipe;
use crate::handlers::{
    message_handler::MessageRegistry, message_router_handler::MessageRouterState,
    operator_handler::operator_handler_state::OperatorInstance,
};
use crate::planner;

use super::operator_task_trackers::RestrictedOperatorTaskTracker;
use super::table_funcs::TableFuncConfig;
use super::traits::TableFuncTaskBuilder;

#[derive(Debug, Error)]
pub enum OperatorBuilderError {
    #[error("not implemented: {0}")]
    NotImplemented(String),
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
        let restricted_tt = RestrictedOperatorTaskTracker::new(tt, 1);

        match &op_in.config.operator.operator_type {
            planner::OperatorType::Producer { task, .. } => match task {
                planner::OperatorTask::TableFunc { func_name, .. } => {
                    let bldr = if let Some(bldr) = self
                        .op_reg
                        .get_table_func_task_builders()
                        .iter()
                        .find(|item| item.implements_func_name() == *func_name)
                    {
                        bldr
                    } else {
                        return Err(OperatorBuilderError::NotImplemented(format!(
                            "table func: {}",
                            func_name
                        ))
                        .into());
                    };

                    let table_func_config = TableFuncConfig::try_from(&op_in.config)?;

                    let (pipe1, pipe2) = Pipe::new(1);

                    let msg_consumer = bldr.build(
                        op_in.config.clone(),
                        table_func_config,
                        pipe2,
                        self.msg_reg.clone(),
                        &restricted_tt,
                        op_in.ct.clone(),
                    )?;
                }
                planner::OperatorTask::Table { .. } => {
                    return Err(OperatorBuilderError::NotImplemented(
                        "table operator task".to_string(),
                    )
                    .into())
                }
                planner::OperatorTask::Filter { .. } => {
                    return Err(OperatorBuilderError::NotImplemented(
                        "filter operator task".to_string(),
                    )
                    .into())
                }
                planner::OperatorTask::Materialize { .. } => {
                    return Err(OperatorBuilderError::NotImplemented(
                        "materialize operator task".to_string(),
                    )
                    .into())
                }
            },
            planner::OperatorType::Exchange { .. } => {
                return Err(OperatorBuilderError::NotImplemented(
                    "exhange operator type".to_string(),
                )
                .into())
            }
        }

        Ok(())
    }
}
