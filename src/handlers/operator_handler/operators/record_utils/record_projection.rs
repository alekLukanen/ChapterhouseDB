use anyhow::Result;
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{Field, Schema};
use sqlparser::ast::SelectItem;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProjectRecordError {
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

pub fn project_record(
    fields: Vec<SelectItem>,
    record: Arc<RecordBatch>,
    table_aliases: Vec<Vec<String>>,
) -> Result<RecordBatch> {
    let proj_fields: Vec<Field> = Vec::new();
    let proj_arrays: Vec<Arc<dyn Array>> = Vec::new();

    for field in fields {
        match field {
            SelectItem::Wildcard(_) => {}
            SelectItem::QualifiedWildcard(_, _) => {
                return Err(ProjectRecordError::NotImplemented(
                    "SelectItem::QualifiedWildcard".to_string(),
                )
                .into());
            }
            SelectItem::UnnamedExpr(_) => {
                return Err(ProjectRecordError::NotImplemented(
                    "SelectItem::UnnamedExpr".to_string(),
                )
                .into());
            }
            SelectItem::ExprWithAlias { .. } => {
                return Err(ProjectRecordError::NotImplemented(
                    "SelectItem::ExprWithAlias".to_string(),
                )
                .into());
            }
        }
    }

    let proj_schema = Arc::new(Schema::new(proj_fields));
    let proj_record = RecordBatch::try_new(proj_schema, proj_arrays)?;

    Ok(proj_record)
}
