use thiserror::Error;

use crate::{
    handlers::operator_handler::operator_handler_state::OperatorInstanceConfig,
    planner::{OperatorTask, OperatorType},
};

use super::config::SortConfig;

#[derive(Debug, Error)]
pub enum TryFromSortConfigError {
    #[error("unable to convert")]
    UnableToConvert,
}

impl TryFrom<&OperatorInstanceConfig> for SortConfig {
    type Error = TryFromSortConfigError;

    fn try_from(op_in_config: &OperatorInstanceConfig) -> Result<SortConfig, Self::Error> {
        match &op_in_config.operator.operator_type {
            OperatorType::Producer {
                task,
                outbound_exchange_id,
                inbound_exchange_ids,
            } => match task {
                OperatorTask::Sort {
                    exprs,
                    num_partitions,
                } => Ok(SortConfig {
                    exprs: exprs.clone(),
                    num_partitions: num_partitions.clone(),
                    outbound_exchange_id: outbound_exchange_id.clone(),
                    inbound_exchange_ids: inbound_exchange_ids.clone(),
                }),
                _ => Err(TryFromSortConfigError::UnableToConvert.into()),
            },
            OperatorType::Exchange { .. } => Err(TryFromSortConfigError::UnableToConvert.into()),
        }
    }
}
