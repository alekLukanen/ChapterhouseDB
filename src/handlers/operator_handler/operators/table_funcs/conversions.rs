use anyhow::Result;
use thiserror::Error;

use crate::{
    handlers::operator_handler::operator_handler_state::OperatorInstanceConfig,
    planner::{OperatorTask, TaskType},
};

use super::config::TableFuncConfig;

#[derive(Debug, Error)]
pub enum TryFromTableFuncConfigError {
    #[error("unable to convert")]
    UnableToConvert,
}

impl TryFrom<&OperatorInstanceConfig> for TableFuncConfig {
    type Error = TryFromTableFuncConfigError;

    fn try_from(op_in_config: &OperatorInstanceConfig) -> Result<TableFuncConfig, Self::Error> {
        match &op_in_config.operator.operator_task {
            OperatorTask::Producer {
                typ,
                outbound_exchange_id,
                inbound_exchange_ids,
            } => match typ {
                TaskType::TableFunc {
                    alias,
                    func_name,
                    args,
                    max_rows_per_batch,
                } => Ok(TableFuncConfig {
                    alias: alias.clone(),
                    func_name: func_name.clone(),
                    args: args.clone(),
                    max_rows_per_batch: max_rows_per_batch.clone(),
                    outbound_exchange_id: outbound_exchange_id.clone(),
                    inbound_exchange_ids: inbound_exchange_ids.clone(),
                }),
                _ => Err(TryFromTableFuncConfigError::UnableToConvert.into()),
            },
            OperatorTask::Exchange { .. } => {
                Err(TryFromTableFuncConfigError::UnableToConvert.into())
            }
        }
    }
}
