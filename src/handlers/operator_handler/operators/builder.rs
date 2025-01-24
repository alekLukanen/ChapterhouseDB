use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;
use tracing::error;

use crate::handlers::message_handler::Pipe;
use crate::handlers::{
    message_handler::MessageRegistry, message_router_handler::MessageRouterState,
    operator_handler::operator_handler_state::OperatorInstance,
};
use crate::planner;

use super::exchange_operator::ExchangeOperator;
use super::operator_task_registry::OperatorTaskRegistry;
use super::producer_operator::ProducerOperator;
use super::ConnectionRegistry;

#[derive(Debug, Error)]
pub enum OperatorBuilderError {
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("operator task builder not found: {0}")]
    OperatorTaskBuilderNotFound(String),
}

pub struct OperatorBuilder {
    op_reg: Arc<OperatorTaskRegistry>,
    msg_reg: Arc<MessageRegistry>,
    conn_reg: Arc<ConnectionRegistry>,
    message_router_state: Arc<Mutex<MessageRouterState>>,
}

impl OperatorBuilder {
    pub fn new(
        op_reg: Arc<OperatorTaskRegistry>,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
        message_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> OperatorBuilder {
        OperatorBuilder {
            op_reg,
            msg_reg,
            conn_reg,
            message_router_state,
        }
    }

    pub async fn build_operator(&self, op_in: &OperatorInstance, tt: &TaskTracker) -> Result<()> {
        match &op_in.config.operator.operator_type {
            planner::OperatorType::Producer { task, .. } => match task {
                planner::OperatorTask::TableFunc { func_name, .. } => {
                    match self.build_producer_operator(op_in, tt, task).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(err) => {
                            return Err(err.context(format!(
                                "failed building producer operator for func_name: {}",
                                func_name
                            )));
                        }
                    }
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
                planner::OperatorTask::MaterializeFiles { data_format, .. } => {
                    match self.build_producer_operator(op_in, tt, task).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(err) => {
                            return Err(err.context(format!(
                                "failed building producer operator for data format: {}",
                                data_format
                            )));
                        }
                    }
                }
            },
            planner::OperatorType::Exchange { .. } => {
                let mut ex_op = ExchangeOperator::new(
                    op_in.config.clone(),
                    self.message_router_state.clone(),
                    self.msg_reg.clone(),
                )
                .await?;
                let ct = op_in.ct.clone();
                tt.spawn(async move {
                    if let Err(err) = ex_op.async_main(ct).await {
                        error!("{:?}", err);
                    }
                });
            }
        }

        Ok(())
    }

    async fn build_producer_operator(
        &self,
        op_in: &OperatorInstance,
        tt: &TaskTracker,
        task: &planner::OperatorTask,
    ) -> Result<()> {
        let bldr = match self.op_reg.find_task_builder(task) {
            Ok(Some(bldr)) => bldr,
            Ok(None) => {
                return Err(OperatorBuilderError::OperatorTaskBuilderNotFound(
                    task.name().to_string(),
                )
                .into());
            }
            Err(err) => {
                return Err(err.context("failed trying to find the task builder"));
            }
        };

        let (pipe1, pipe2) = Pipe::new(1);
        let mut producer_operator = ProducerOperator::new(
            op_in.config.clone(),
            self.message_router_state.clone(),
            pipe1,
            self.msg_reg.clone(),
        )
        .await;

        let mut restricted_tt = producer_operator.restricted_tt();
        let task_ct = producer_operator.get_task_ct();
        let (oneshot_res, msg_consumer) = bldr.build(
            op_in.config.clone(),
            pipe2,
            self.msg_reg.clone(),
            self.conn_reg.clone(),
            &mut restricted_tt,
            task_ct,
        )?;

        producer_operator = producer_operator.set_task_msg_consumer(msg_consumer);
        let ct = op_in.ct.clone();
        tt.spawn(async move {
            if let Err(err) = producer_operator.async_main(ct, oneshot_res).await {
                error!("{:?}", err);
            }
        });

        Ok(())
    }
}
