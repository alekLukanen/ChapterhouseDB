use std::sync::Arc;

use anyhow::Result;
use arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};

use super::filter_record::filter_record;

#[test]
fn test_filter_simple_record() -> Result<()> {
    let id_array = Int32Array::from(vec![10, 20, 30, 40, 50]);
    let schema = Schema::new(vec![Field::new("cost", DataType::Int32, false)]);

    let rec = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(id_array)])?;
    let expr = sqlparser::ast::Expr::BinaryOp {
        op: sqlparser::ast::BinaryOperator::Lt,
        left: Box::new(sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident {
            value: "cost".to_string(),
            quote_style: None,
        })),
        right: Box::new(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
            "30".to_string(),
            false,
        ))),
    };
    let table_aliases = vec![vec![]];

    let res = filter_record(Arc::new(rec), &table_aliases, &expr)?;
    let expected_res = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![10, 20]))],
    )?;

    assert!(res.eq(&expected_res));

    Ok(())
}
