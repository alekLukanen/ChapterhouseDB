use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::operator_handler_state::{OperatorInstance, OperatorInstanceConfig, Status};
use crate::handlers::message_handler::messages;

#[derive(Debug, Error)]
pub enum TryFromOperatorInstanceError {
    #[error("unable to convert message to operator instance")]
    UnableToConvertMessageToOperatorInstance,
}

impl TryFrom<&messages::query::OperatorInstanceAssignment> for OperatorInstance {
    type Error = TryFromOperatorInstanceError;

    fn try_from(
        op_in_assign: &messages::query::OperatorInstanceAssignment,
    ) -> Result<OperatorInstance, Self::Error> {
        match op_in_assign {
            messages::query::OperatorInstanceAssignment::Assign {
                query_handler_worker_id,
                query_id,
                op_instance_id,
                pipeline_id,
                operator,
            } => Ok(OperatorInstance {
                status: Status::Running,
                ct: CancellationToken::new(),
                config: OperatorInstanceConfig {
                    id: op_instance_id.clone(),
                    query_handler_worker_id: query_handler_worker_id.clone(),
                    query_id: query_id.clone(),
                    pipeline_id: pipeline_id.clone(),
                    operator: operator.clone(),
                },
            }),
            _ => Err(TryFromOperatorInstanceError::UnableToConvertMessageToOperatorInstance),
        }
    }
}
