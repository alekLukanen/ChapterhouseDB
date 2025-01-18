use anyhow::Result;
use thiserror::Error;

use crate::{
    handlers::operator_handler::operator_handler_state::OperatorInstanceConfig,
    planner::{OperatorTask, OperatorType},
};

use super::config::MaterializeFileConfig;

#[derive(Debug, Error)]
pub enum TryFromMaterializeFileConfigError {
    #[error("unable to convert")]
    UnableToConvert,
}

impl TryFrom<&OperatorInstanceConfig> for MaterializeFileConfig {
    type Error = TryFromMaterializeFileConfigError;

    fn try_from(
        op_in_config: &OperatorInstanceConfig,
    ) -> Result<MaterializeFileConfig, Self::Error> {
        match &op_in_config.operator.operator_type {
            OperatorType::Producer {
                task,
                outbound_exchange_id,
                inbound_exchange_ids,
            } => match task {
                OperatorTask::MaterializeFile {
                    data_format,
                    fields,
                } => Ok(MaterializeFileConfig {
                    data_format: data_format.clone(),
                    fields: fields.clone(),
                    outbound_exchange_id: outbound_exchange_id.clone(),
                    inbound_exchange_ids: inbound_exchange_ids.clone(),
                }),
                _ => Err(TryFromMaterializeFileConfigError::UnableToConvert.into()),
            },
            OperatorType::Exchange { .. } => {
                Err(TryFromMaterializeFileConfigError::UnableToConvert.into())
            }
        }
    }
}
