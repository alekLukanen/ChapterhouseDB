use std::sync::Arc;

use anyhow::Result;
use arrow::array::{
    Array, BooleanArray, Datum, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
};
use arrow::compute;
use arrow::datatypes::DataType;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ComputeValueError {
    #[error("value type not implemented: {0}")]
    ValueTypeNotImplemented(String),
    #[error("expression type not implemented: {0}")]
    ExpressionTypeNotImplemented(String),
    #[error("binary operator not implemented: {0}")]
    BinaryOperatorNotImplemented(String),
    #[error("data type not supported: {0}")]
    DataTypeNotSupported(String),
    #[error("binary operation cast failed; left_array type={0}, right_array type={1}")]
    BinaryOperatinCastFailed(String, String),
    #[error("failed to parse {0} as an integer")]
    FailedToParseAsAnInteger(String),
    #[error("failed to parse {0} as a float")]
    FailedToParseAsAFloat(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("cast failed")]
    CastFailed,
}

struct ArrayDatum {
    array: Arc<dyn Array>,
    is_scalar: bool,
}

impl ArrayDatum {
    fn new(array: Arc<dyn Array>, is_scalar: bool) -> ArrayDatum {
        ArrayDatum { array, is_scalar }
    }
    fn new_binary_op(left: &dyn Datum, right: &dyn Datum, array: Arc<dyn Array>) -> ArrayDatum {
        ArrayDatum {
            array,
            is_scalar: left.get().1 && right.get().1,
        }
    }
}

impl Datum for ArrayDatum {
    fn get(&self) -> (&dyn Array, bool) {
        (&*self.array, self.is_scalar)
    }
}

pub fn compute_value(
    rec: Arc<RecordBatch>,
    expr: Box<sqlparser::ast::Expr>,
) -> Result<Arc<dyn Datum>> {
    match *expr {
        sqlparser::ast::Expr::Nested(expr_val) => compute_value(rec.clone(), expr_val),
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            let left_array = compute_value(rec.clone(), left)?;
            let right_array = compute_value(rec.clone(), right)?;

            match op {
                sqlparser::ast::BinaryOperator::And => {
                    let left_array_bool = compute::cast(left_array.get().0, &DataType::Boolean)?;
                    let right_array_bool = compute::cast(right_array.get().0, &DataType::Boolean)?;

                    match (
                        left_array_bool.as_any().downcast_ref::<BooleanArray>(),
                        right_array_bool.as_any().downcast_ref::<BooleanArray>(),
                    ) {
                        (Some(left), Some(right)) => {
                            let res = compute::and(left, right)?;
                            Ok(Arc::new(res))
                        }
                        _ => Err(ComputeValueError::BinaryOperatinCastFailed(
                            left_array.get().0.data_type().to_string(),
                            right_array.get().0.data_type().to_string(),
                        )
                        .into()),
                    }
                }
                sqlparser::ast::BinaryOperator::Or => {
                    let left_array_bool = compute::cast(left_array.get().0, &DataType::Boolean)?;
                    let right_array_bool = compute::cast(right_array.get().0, &DataType::Boolean)?;

                    match (
                        left_array_bool.as_any().downcast_ref::<BooleanArray>(),
                        right_array_bool.as_any().downcast_ref::<BooleanArray>(),
                    ) {
                        (Some(left), Some(right)) => {
                            let res = compute::or(left, right)?;
                            Ok(Arc::new(res))
                        }
                        _ => Err(ComputeValueError::BinaryOperatinCastFailed(
                            left_array.get().0.data_type().to_string(),
                            right_array.get().0.data_type().to_string(),
                        )
                        .into()),
                    }
                }
                sqlparser::ast::BinaryOperator::Plus => {
                    let left_array = &*left_array;
                    let right_array = &*right_array;
                    let res_array = compute::kernels::numeric::add(left_array, right_array)?;
                    Ok(Arc::new(ArrayDatum::new_binary_op(
                        left_array,
                        right_array,
                        res_array,
                    )))
                }
                sqlparser::ast::BinaryOperator::Divide => {
                    let left_array = &*left_array;
                    let right_array = &*right_array;
                    let res_array = compute::kernels::numeric::div(left_array, right_array)?;
                    Ok(Arc::new(ArrayDatum::new_binary_op(
                        left_array,
                        right_array,
                        res_array,
                    )))
                }
                sqlparser::ast::BinaryOperator::Multiply => {
                    let left_array = &*left_array;
                    let right_array = &*right_array;
                    let res_array = compute::kernels::numeric::mul(left_array, right_array)?;
                    Ok(Arc::new(ArrayDatum::new_binary_op(
                        left_array,
                        right_array,
                        res_array,
                    )))
                }
                sqlparser::ast::BinaryOperator::Eq => {
                    let left_array = &*left_array;
                    let right_array = &*right_array;
                    let res_array = compute::kernels::cmp::eq(left_array, right_array)?;
                    Ok(Arc::new(ArrayDatum::new_binary_op(
                        left_array,
                        right_array,
                        Arc::new(res_array),
                    )))
                }
                sqlparser::ast::BinaryOperator::NotEq => {
                    let left_array = &*left_array;
                    let right_array = &*right_array;
                    let res_array = compute::kernels::cmp::neq(left_array, right_array)?;
                    Ok(Arc::new(ArrayDatum::new_binary_op(
                        left_array,
                        right_array,
                        Arc::new(res_array),
                    )))
                }
                sqlparser::ast::BinaryOperator::Gt => {
                    let left_array = &*left_array;
                    let right_array = &*right_array;
                    let res_array = compute::kernels::cmp::gt(left_array, right_array)?;
                    Ok(Arc::new(ArrayDatum::new_binary_op(
                        left_array,
                        right_array,
                        Arc::new(res_array),
                    )))
                }
                sqlparser::ast::BinaryOperator::GtEq => {
                    let left_array = &*left_array;
                    let right_array = &*right_array;
                    let res_array = compute::kernels::cmp::gt_eq(left_array, right_array)?;
                    Ok(Arc::new(ArrayDatum::new_binary_op(
                        left_array,
                        right_array,
                        Arc::new(res_array),
                    )))
                }
                sqlparser::ast::BinaryOperator::Lt => {
                    let left_array = &*left_array;
                    let right_array = &*right_array;
                    let res_array = compute::kernels::cmp::lt(left_array, right_array)?;
                    Ok(Arc::new(ArrayDatum::new_binary_op(
                        left_array,
                        right_array,
                        Arc::new(res_array),
                    )))
                }
                sqlparser::ast::BinaryOperator::LtEq => {
                    let left_array = &*left_array;
                    let right_array = &*right_array;
                    let res_array = compute::kernels::cmp::lt_eq(left_array, right_array)?;
                    Ok(Arc::new(ArrayDatum::new_binary_op(
                        left_array,
                        right_array,
                        Arc::new(res_array),
                    )))
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
        sqlparser::ast::Expr::Value(val) => match &val {
            sqlparser::ast::Value::Number(num_val, is_long) => {
                if *is_long {
                    return Err(
                        ComputeValueError::ValueTypeNotImplemented(format!("{:?}", val)).into(),
                    );
                }
                if num_val.contains(".") {
                    if let Ok(f) = num_val.parse::<f32>() {
                        let res = Float32Array::new_scalar(f);
                        return Ok(Arc::new(res));
                    } else if let Ok(f) = num_val.parse::<f64>() {
                        let res = Float64Array::new_scalar(f);
                        return Ok(Arc::new(res));
                    } else {
                        return Err(
                            ComputeValueError::FailedToParseAsAFloat(num_val.clone()).into()
                        );
                    }
                } else {
                    if let Ok(i) = num_val.parse::<i32>() {
                        let res = Int32Array::new_scalar(i);
                        return Ok(Arc::new(res));
                    } else if let Ok(i) = num_val.parse::<i64>() {
                        let res = Int64Array::new_scalar(i);
                        return Ok(Arc::new(res));
                    } else {
                        return Err(
                            ComputeValueError::FailedToParseAsAnInteger(num_val.clone()).into()
                        );
                    }
                }
            }
            sqlparser::ast::Value::Boolean(bool_val) => {
                let res = BooleanArray::new_scalar(*bool_val);
                Ok(Arc::new(res))
            }
            sqlparser::ast::Value::SingleQuotedString(str_val) => {
                return Err(
                    ComputeValueError::ValueTypeNotImplemented(format!("{:?}", val)).into(),
                );
            }
            _ => {
                return Err(
                    ComputeValueError::ValueTypeNotImplemented(format!("{:?}", val)).into(),
                );
            }
        },
        sqlparser::ast::Expr::Identifier(ident) => {
            let col_array = rec.column_by_name(&ident.value);
            if let Some(col_array) = col_array {
                let col_array = col_array.clone();
                Ok(Arc::new(ArrayDatum::new(col_array, false)))
            } else {
                Err(ComputeValueError::ColumnNotFound(ident.value.clone()).into())
            }
        }
        _ => {
            return Err(
                ComputeValueError::ExpressionTypeNotImplemented(format!("{:?}", *expr)).into(),
            );
        }
    }
}
