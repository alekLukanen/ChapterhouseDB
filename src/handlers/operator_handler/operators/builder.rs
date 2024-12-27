use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::info;

use crate::{
    handlers::{
        message_handler::MessageRegistry, message_router_handler::MessageRouterState,
        operator_handler::operator_handler_state::OperatorInstance,
    },
    planner::{OperatorTask, TaskType},
};

use super::{table_func_producer_operator::TableFuncConfig, TableFuncProducerOperator};

#[derive(Debug, Error)]
pub enum BuildOperatorError {
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

pub async fn build_operator(
    ct: CancellationToken,
    op_in: &OperatorInstance,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    msg_reg: Arc<Box<MessageRegistry>>,
    tt: &TaskTracker,
) -> Result<()> {
    match &op_in.config.operator.operator_task {
        OperatorTask::Producer { typ, .. } => match typ {
            TaskType::TableFunc { .. } => {
                let operator = TableFuncProducerOperator::new(
                    op_in.config.clone(),
                    TableFuncConfig::try_from(&op_in.config)?,
                    message_router_state,
                    msg_reg,
                    op_in.ct.clone(),
                )
                .await;
                tt.spawn(async move {
                    if let Err(err) = operator.async_main(ct).await {
                        info!("error: {:?}", err);
                    }
                });
            }
            TaskType::Table { .. } => {
                return Err(BuildOperatorError::NotImplemented(
                    "table producer operator not implemented",
                )
                .into())
            }
            TaskType::Filter { .. } => {
                return Err(BuildOperatorError::NotImplemented(
                    "filter producer operator not implemented",
                )
                .into())
            }
            TaskType::Materialize { .. } => {
                return Err(BuildOperatorError::NotImplemented(
                    "materialize producer operator not implemented",
                )
                .into())
            }
        },
        OperatorTask::Exchange { .. } => {
            return Err(
                BuildOperatorError::NotImplemented("exchange operator not implemented").into(),
            )
        }
    }

    Ok(())
}
