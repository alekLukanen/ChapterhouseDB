use thiserror::Error;

use crate::{
    handlers::operator_handler::operator_handler_state::OperatorInstanceConfig,
    planner::{OperatorTask, OperatorType},
};

use super::config::PartitionConfig;

#[derive(Debug, Error)]
pub enum TryFromPartitionConfigError {
    #[error("unable to convert")]
    UnableToConvert,
}

impl TryFrom<&OperatorInstanceConfig> for PartitionConfig {
    type Error = TryFromPartitionConfigError;

    fn try_from(op_in_config: &OperatorInstanceConfig) -> Result<PartitionConfig, Self::Error> {
        match &op_in_config.operator.operator_type {
            OperatorType::Producer {
                task,
                outbound_exchange_id,
                inbound_exchange_ids,
            } => match task {
                OperatorTask::Partition {
                    partition_method,
                    partition_range_method,
                } => Ok(PartitionConfig {
                    partition_method: partition_method.clone(),
                    partition_range_method: partition_range_method.clone(),
                    outbound_exchange_id: outbound_exchange_id.clone(),
                    inbound_exchange_ids: inbound_exchange_ids.clone(),
                }),
                _ => Err(TryFromPartitionConfigError::UnableToConvert.into()),
            },
            OperatorType::Exchange { .. } => {
                Err(TryFromPartitionConfigError::UnableToConvert.into())
            }
        }
    }
}
