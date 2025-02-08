use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum FilterRecordError {
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

pub fn filter_record(
    rec: Arc<RecordBatch>,
    expr: &sqlparser::ast::Expr,
    table_aliases: &Vec<Vec<String>>,
) -> Result<RecordBatch> {
    Err(FilterRecordError::NotImplemented("func".to_string()).into())
}
