use anyhow::Result;
use thiserror::Error;

use crate::planner::{self, OperatorTask};

#[derive(Debug, Error)]
pub enum GetRecordTableAliasesError {
    #[error("operator task type does not have an alias field: {0}")]
    OperatorTaskTypeDoesNotHaveAnAliasField(String),
}

pub fn get_record_table_aliases(
    op_type: &planner::OperatorType,
    record: &arrow::array::RecordBatch,
) -> Result<Vec<Vec<String>>> {
    let task = match op_type {
        planner::OperatorType::Producer { task, .. } => task,
        planner::OperatorType::Exchange { task, .. } => task,
    };
    let alias = match task {
        planner::OperatorTask::TableFunc { alias, .. } => alias,
        planner::OperatorTask::Table { alias, .. } => alias,
        planner::OperatorTask::Filter { .. } => {
            return Err(
                GetRecordTableAliasesError::OperatorTaskTypeDoesNotHaveAnAliasField(format!(
                    "{}",
                    task
                ))
                .into(),
            );
        }
        planner::OperatorTask::MaterializeFiles { .. } => {
            return Err(
                GetRecordTableAliasesError::OperatorTaskTypeDoesNotHaveAnAliasField(format!(
                    "{}",
                    task
                ))
                .into(),
            );
        }
    };

    match alias {
        Some(alias) => {
            let mut res: Vec<Vec<String>> = Vec::new();
            for _ in 0..record.num_columns() {
                res.push(vec![alias.clone()]);
            }
            Ok(res)
        }
        None => {
            let mut res: Vec<Vec<String>> = Vec::new();
            for _ in 0..record.num_columns() {
                res.push(Vec::new());
            }
            Ok(res)
        }
    }
}
