use anyhow::Result;
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use sqlparser::ast::SelectItem;
use std::sync::Arc;
use thiserror::Error;

use super::compute_value;

#[derive(Debug, Error)]
pub enum ProjectRecordError {
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

pub fn project_record(
    fields: &Vec<SelectItem>,
    record: Arc<RecordBatch>,
    table_aliases: &Vec<Vec<String>>,
) -> Result<RecordBatch> {
    let mut unnamed_idx: usize = 0;
    let mut proj_fields: Vec<Field> = Vec::new();
    let mut proj_arrays: Vec<ArrayRef> = Vec::new();

    for field in fields {
        match field {
            SelectItem::Wildcard(_) => {
                for (idx, field) in record.schema().fields().iter().enumerate() {
                    proj_fields.push((**field).clone());
                    proj_arrays.push(record.column(idx).clone());
                }
            }
            SelectItem::QualifiedWildcard(_, _) => {
                return Err(ProjectRecordError::NotImplemented(
                    "SelectItem::QualifiedWildcard".to_string(),
                )
                .into());
            }
            SelectItem::UnnamedExpr(expr) => {
                let res = compute_value::compute_value(record.clone(), table_aliases, expr)?;
                proj_fields.push(Field::new(
                    format!("unnamed_{}", unnamed_idx),
                    res.array.data_type().clone(),
                    res.array.is_nullable(),
                ));
                proj_arrays.push(res.array);

                unnamed_idx += 1;
            }
            SelectItem::ExprWithAlias { alias, expr } => {
                let res = compute_value::compute_value(record.clone(), table_aliases, expr)?;
                proj_fields.push(Field::new(
                    alias.value.clone(),
                    res.array.data_type().clone(),
                    res.array.is_nullable(),
                ));
                proj_arrays.push(res.array);
            }
        }
    }

    let proj_schema = Arc::new(Schema::new(proj_fields));
    let proj_record = RecordBatch::try_new(proj_schema, proj_arrays)?;

    Ok(proj_record)
}
