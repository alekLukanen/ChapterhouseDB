use thiserror::Error;

use crate::{
    handlers::operator_handler::operator_handler_state::OperatorInstanceConfig,
    planner::{OperatorTask, OperatorType},
};

use super::config::FilterConfig;

#[derive(Debug, Error)]
pub enum TryFromFilterConfigError {
    #[error("unable to convert")]
    UnableToConvert,
}

impl TryFrom<&OperatorInstanceConfig> for FilterConfig {
    type Error = TryFromFilterConfigError;

    fn try_from(op_in_config: &OperatorInstanceConfig) -> Result<FilterConfig, Self::Error> {
        match &op_in_config.operator.operator_type {
            OperatorType::Producer {
                task,
                outbound_exchange_id,
                inbound_exchange_ids,
            } => match task {
                OperatorTask::Filter { expr } => Ok(FilterConfig {
                    expr: expr.clone(),
                    outbound_exchange_id: outbound_exchange_id.clone(),
                    inbound_exchange_ids: inbound_exchange_ids.clone(),
                }),
                _ => Err(TryFromFilterConfigError::UnableToConvert.into()),
            },
            OperatorType::Exchange { .. } => Err(TryFromFilterConfigError::UnableToConvert.into()),
        }
    }
}
