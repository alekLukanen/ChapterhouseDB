use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::operator_handler_state::{OperatorInstance, OperatorInstanceConfig, Status};
use crate::handlers::message_handler::OperatorInstanceAssignment;

#[derive(Debug, Error)]
pub enum TryFromOperatorInstanceError {
    #[error("unable to convert message to operator instance")]
    UnableToConvertMessageToOperatorInstance,
}

impl TryFrom<&OperatorInstanceAssignment> for OperatorInstance {
    type Error = TryFromOperatorInstanceError;

    fn try_from(
        op_in_assign: &OperatorInstanceAssignment,
    ) -> Result<OperatorInstance, Self::Error> {
        match op_in_assign {
            OperatorInstanceAssignment::Assign {
                query_id,
                op_instance_id,
                pipeline_id,
                operator,
            } => Ok(OperatorInstance {
                status: Status::Queued,
                ct: CancellationToken::new(),
                config: OperatorInstanceConfig {
                    id: op_instance_id.clone(),
                    query_id: query_id.clone(),
                    pipeline_id: pipeline_id.clone(),
                    operator: operator.clone(),
                },
            }),
            _ => Err(TryFromOperatorInstanceError::UnableToConvertMessageToOperatorInstance),
        }
    }
}
