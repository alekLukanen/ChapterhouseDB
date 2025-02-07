use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Array, ArrayRef, BooleanArray, Datum, RecordBatch};
use arrow::compute;
use arrow::datatypes::DataType;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FilterRecordError {
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

#[derive(Debug, Error)]
pub enum ComputeValueError {
    #[error("value type not implemented: {0}")]
    ValueTypeNotImplemented(String),
    #[error("expression type not implemented: {0}")]
    ExpressionTypeNotImplemented(String),
    #[error("binary operator not implemented: {0}")]
    BinaryOperatorNotImplemented(String),
    #[error("cast failed")]
    CastFailed,
}

pub fn filter_record(
    rec: Arc<RecordBatch>,
    expr: &sqlparser::ast::Expr,
    table_aliases: &Vec<Vec<String>>,
) -> Result<RecordBatch> {
    Err(FilterRecordError::NotImplemented("func".to_string()).into())
}

fn compute_value(rec: Arc<RecordBatch>, expr: Box<sqlparser::ast::Expr>) -> Result<ArrayRef> {
    match *expr {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            let left_array = compute_value(rec.clone(), left)?;
            let right_array = compute_value(rec.clone(), right)?;

            match op {
                sqlparser::ast::BinaryOperator::And => {
                    let left_array_bool = if let Some(arr) =
                        compute::cast(&*left_array, &DataType::Boolean)?
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                    {
                        arr
                    } else {
                        return Err(ComputeValueError::CastFailed.into());
                    };
                    let right_array_bool = if let Some(arr) =
                        compute::cast(&*right_array, &DataType::Boolean)?
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                    {
                        arr
                    } else {
                        return Err(ComputeValueError::CastFailed.into());
                    };

                    let res = compute::and(left_array_bool, right_array_bool)?;
                    Ok(Arc::new(res))
                }
                sqlparser::ast::BinaryOperator::Or => {
                    let left_array_bool = if let Some(arr) =
                        compute::cast(&*left_array, &DataType::Boolean)?
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                    {
                        arr
                    } else {
                        return Err(ComputeValueError::CastFailed.into());
                    };
                    let right_array_bool = if let Some(arr) =
                        compute::cast(&*right_array, &DataType::Boolean)?
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                    {
                        arr
                    } else {
                        return Err(ComputeValueError::CastFailed.into());
                    };

                    let res = compute::or(left_array_bool, right_array_bool)?;
                    Ok(Arc::new(res))
                }
                sqlparser::ast::BinaryOperator::Plus => {
                    let res = compute::kernels::numeric::add(&*right_array, &*left_array)?;
                    Ok(res)
                }
                _ => {
                    return Err(ComputeValueError::BinaryOperatorNotImplemented(format!(
                        "{:?}",
                        op
                    ))
                    .into());
                }
            }
        }
        sqlparser::ast::Expr::Value(val) => match val {
            sqlparser::ast::Value::Number(num_val, num_bool) => {}
            sqlparser::ast::Value::SingleQuotedString(str_val) => {}
            _ => {
                return Err(
                    ComputeValueError::ValueTypeNotImplemented(format!("{:?}", val)).into(),
                );
            }
        },
        _ => {
            return Err(
                ComputeValueError::ExpressionTypeNotImplemented(format!("{:?}", *expr)).into(),
            );
        }
    }
}
