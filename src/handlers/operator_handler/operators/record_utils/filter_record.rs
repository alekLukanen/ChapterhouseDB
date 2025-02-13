use std::sync::Arc;

use anyhow::Result;
use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute;

use thiserror::Error;

use super::compute_value::compute_value;

#[derive(Debug, Error)]
pub enum FilterRecordError {
    #[error("cast to boolean array failed for array type: {0}")]
    CastToBooleanArrayFailedForArrayType(String),
}

/// Applies filtering based on the provided expression. It is
/// possible that an empty record can be returned.
/// The column structure is not effected.
///
pub fn filter_record(
    rec: Arc<RecordBatch>,
    table_aliases: &Vec<Vec<String>>,
    expr: &sqlparser::ast::Expr,
) -> Result<RecordBatch> {
    let res = compute_value(rec.clone(), table_aliases, expr)?;
    let bool_arr = match res.array.as_any().downcast_ref::<BooleanArray>() {
        Some(arr) => arr,
        _ => {
            return Err(FilterRecordError::CastToBooleanArrayFailedForArrayType(
                res.array.data_type().to_string(),
            )
            .into())
        }
    };

    let filtered_rec = compute::filter_record_batch(&*rec, bool_arr)?;
    Ok(filtered_rec)
}
