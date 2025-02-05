use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, RecordBatch};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FilterRecordError {
    #[error("not implemented")]
    NotImplemented,
}

pub fn filter_record(
    rec: Arc<RecordBatch>,
    expr: &sqlparser::ast::Expr,
    table_aliases: &Vec<Vec<String>>,
) -> Result<RecordBatch> {
    Err(FilterRecordError::NotImplemented.into())
}

fn compute_value(
    rec: Arc<RecordBatch>,
    temp_rec: ArrayRef,
    expr: &sqlparser::ast::Expr,
) -> Result<ArrayRef> {
    Err(FilterRecordError::NotImplemented.into())
}
