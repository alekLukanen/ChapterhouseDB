use anyhow::Result;
use thiserror::Error;

use crate::{
    handlers::operator_handler::operator_handler_state::OperatorInstanceConfig,
    planner::{OperatorTask, OperatorType},
};

use super::config::MaterializeFilesConfig;

#[derive(Debug, Error)]
pub enum TryFromMaterializeFilesConfigError {
    #[error("unable to convert")]
    UnableToConvert,
}

impl TryFrom<&OperatorInstanceConfig> for MaterializeFilesConfig {
    type Error = TryFromMaterializeFilesConfigError;

    fn try_from(
        op_in_config: &OperatorInstanceConfig,
    ) -> Result<MaterializeFilesConfig, Self::Error> {
        match &op_in_config.operator.operator_type {
            OperatorType::Producer {
                task,
                outbound_exchange_id,
                inbound_exchange_ids,
            } => match task {
                OperatorTask::MaterializeFiles {
                    data_format,
                    fields,
                } => Ok(MaterializeFilesConfig {
                    data_format: data_format.clone(),
                    fields: fields.clone(),
                    outbound_exchange_id: outbound_exchange_id.clone(),
                    inbound_exchange_ids: inbound_exchange_ids.clone(),
                }),
                _ => Err(TryFromMaterializeFilesConfigError::UnableToConvert.into()),
            },
            OperatorType::Exchange { .. } => {
                Err(TryFromMaterializeFilesConfigError::UnableToConvert.into())
            }
        }
    }
}
