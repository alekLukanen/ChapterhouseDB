use std::sync::Arc;

use anyhow::Result;
use arrow::{
    array::RecordBatch,
    compute::{SortColumn, SortOptions},
    datatypes::{Field, Schema},
};

use super::compute_value::compute_value;

pub fn compute_order_by_record(
    exprs: &Vec<sqlparser::ast::OrderByExpr>,
    rec: Arc<RecordBatch>,
    table_aliases: &Vec<Vec<String>>,
) -> Result<RecordBatch> {
    let idx = 0usize;
    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<SortColumn> = Vec::new();

    // compute the values
    for expr in exprs {
        let res = compute_value(rec.clone(), table_aliases, &expr.expr)?;
        fields.push(Field::new(
            format!("col_{}", idx),
            res.array.data_type().clone(),
            res.array.is_nullable(),
        ));
        columns.push(SortColumn {
            values: res.array,
            options: Some(SortOptions {
                descending: !expr.asc.unwrap_or(true),
                nulls_first: expr.nulls_first.unwrap_or(true),
            }),
        });
    }

    // sort the columns
    let sorted_arrays = arrow::compute::kernels::sort::lexsort(&columns, None)?;

    let schema = Arc::new(Schema::new(fields));
    let final_rec = RecordBatch::try_new(schema, sorted_arrays)?;

    Ok(final_rec)
}
