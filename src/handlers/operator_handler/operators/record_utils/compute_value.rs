use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Datum, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
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
    #[error("identifier not found: {0}")]
    IdentifierNotFound(String),
    #[error("unsupported type coersion for operation between types {0} and {0}")]
    UnsupportedTypeCoersionForOperationBetweenTypes(DataType, DataType),
}

pub struct ArrayDatum {
    pub array: ArrayRef,
    pub is_scalar: bool,
}

impl ArrayDatum {
    fn new(array: ArrayRef, is_scalar: bool) -> ArrayDatum {
        ArrayDatum { array, is_scalar }
    }
    fn new_binary_op(left: &dyn Datum, right: &dyn Datum, array: ArrayRef) -> ArrayDatum {
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
    table_aliases: &Vec<Vec<String>>,
    expr: &sqlparser::ast::Expr,
) -> Result<ArrayDatum> {
    match expr {
        sqlparser::ast::Expr::Nested(expr_val) => {
            compute_value(rec.clone(), table_aliases, expr_val.as_ref())
        }
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            let left_array = compute_value(rec.clone(), table_aliases, left.as_ref())?;
            let right_array = compute_value(rec.clone(), table_aliases, right.as_ref())?;

            match op {
                sqlparser::ast::BinaryOperator::And => {
                    let left_array_bool = compute::cast(left_array.get().0, &DataType::Boolean)?;
                    let right_array_bool = compute::cast(right_array.get().0, &DataType::Boolean)?;

                    match (
                        left_array_bool.as_any().downcast_ref::<BooleanArray>(),
                        right_array_bool.as_any().downcast_ref::<BooleanArray>(),
                    ) {
                        (Some(left_array), Some(right_array)) => {
                            let res = compute::and(left_array, right_array)?;
                            Ok(ArrayDatum::new_binary_op(
                                left_array,
                                right_array,
                                Arc::new(res),
                            ))
                        }
                        _ => Err(ComputeValueError::BinaryOperatinCastFailed(
                            left_array.array.data_type().to_string(),
                            right_array.array.data_type().to_string(),
                        )
                        .into()),
                    }
                }
                sqlparser::ast::BinaryOperator::Or => {
                    let left_array_bool = compute::cast(&left_array.array, &DataType::Boolean)?;
                    let right_array_bool = compute::cast(&right_array.array, &DataType::Boolean)?;

                    match (
                        left_array_bool.as_any().downcast_ref::<BooleanArray>(),
                        right_array_bool.as_any().downcast_ref::<BooleanArray>(),
                    ) {
                        (Some(left_array), Some(right_array)) => {
                            let res = compute::or(left_array, right_array)?;
                            Ok(ArrayDatum::new_binary_op(
                                left_array,
                                right_array,
                                Arc::new(res),
                            ))
                        }
                        _ => Err(ComputeValueError::BinaryOperatinCastFailed(
                            left_array.array.data_type().to_string(),
                            right_array.array.data_type().to_string(),
                        )
                        .into()),
                    }
                }
                sqlparser::ast::BinaryOperator::Plus => {
                    let (left_array, right_array) = cast_to_common_type(&left_array, &right_array)?;
                    let res_array = compute::kernels::numeric::add(&left_array, &right_array)?;
                    Ok(ArrayDatum::new_binary_op(
                        &left_array,
                        &right_array,
                        Arc::new(res_array),
                    ))
                }
                sqlparser::ast::BinaryOperator::Divide => {
                    let (left_array, right_array) = cast_to_common_type(&left_array, &right_array)?;
                    let res_array = compute::kernels::numeric::div(&left_array, &right_array)?;
                    Ok(ArrayDatum::new_binary_op(
                        &left_array,
                        &right_array,
                        Arc::new(res_array),
                    ))
                }
                sqlparser::ast::BinaryOperator::Multiply => {
                    let (left_array, right_array) = cast_to_common_type(&left_array, &right_array)?;
                    let res_array = compute::kernels::numeric::mul(&left_array, &right_array)?;
                    Ok(ArrayDatum::new_binary_op(
                        &left_array,
                        &right_array,
                        Arc::new(res_array),
                    ))
                }
                sqlparser::ast::BinaryOperator::Eq => {
                    let (left_array, right_array) = cast_to_common_type(&left_array, &right_array)?;
                    let res_array = Arc::new(compute::kernels::cmp::eq(&left_array, &right_array)?);
                    Ok(ArrayDatum::new_binary_op(
                        &left_array,
                        &right_array,
                        res_array,
                    ))
                }
                sqlparser::ast::BinaryOperator::NotEq => {
                    let (left_array, right_array) = cast_to_common_type(&left_array, &right_array)?;
                    let res_array =
                        Arc::new(compute::kernels::cmp::neq(&left_array, &right_array)?);
                    Ok(ArrayDatum::new_binary_op(
                        &left_array,
                        &right_array,
                        res_array,
                    ))
                }
                sqlparser::ast::BinaryOperator::Gt => {
                    let (left_array, right_array) = cast_to_common_type(&left_array, &right_array)?;
                    let res_array = Arc::new(compute::kernels::cmp::gt(&left_array, &right_array)?);
                    Ok(ArrayDatum::new_binary_op(
                        &left_array,
                        &right_array,
                        res_array,
                    ))
                }
                sqlparser::ast::BinaryOperator::GtEq => {
                    let (left_array, right_array) = cast_to_common_type(&left_array, &right_array)?;
                    let res_array =
                        Arc::new(compute::kernels::cmp::gt_eq(&left_array, &right_array)?);
                    Ok(ArrayDatum::new_binary_op(
                        &left_array,
                        &right_array,
                        res_array,
                    ))
                }
                sqlparser::ast::BinaryOperator::Lt => {
                    let (left_array, right_array) = cast_to_common_type(&left_array, &right_array)?;
                    let res_array = Arc::new(compute::kernels::cmp::lt(&left_array, &right_array)?);
                    Ok(ArrayDatum::new_binary_op(
                        &left_array,
                        &right_array,
                        res_array,
                    ))
                }
                sqlparser::ast::BinaryOperator::LtEq => {
                    let (left_array, right_array) = cast_to_common_type(&left_array, &right_array)?;
                    let res_array =
                        Arc::new(compute::kernels::cmp::lt_eq(&left_array, &right_array)?);
                    Ok(ArrayDatum::new_binary_op(
                        &left_array,
                        &right_array,
                        res_array,
                    ))
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
                        let res = Arc::new(Float32Array::from(vec![f]));
                        return Ok(ArrayDatum::new(res, true));
                    } else if let Ok(f) = num_val.parse::<f64>() {
                        let res = Arc::new(Float64Array::from(vec![f]));
                        return Ok(ArrayDatum::new(res, true));
                    } else {
                        return Err(
                            ComputeValueError::FailedToParseAsAFloat(num_val.clone()).into()
                        );
                    }
                } else {
                    if let Ok(i) = num_val.parse::<i32>() {
                        let res = Arc::new(Int32Array::from(vec![i]));
                        return Ok(ArrayDatum::new(res, true));
                    } else if let Ok(i) = num_val.parse::<i64>() {
                        let res = Arc::new(Int64Array::from(vec![i]));
                        return Ok(ArrayDatum::new(res, true));
                    } else {
                        return Err(
                            ComputeValueError::FailedToParseAsAnInteger(num_val.clone()).into()
                        );
                    }
                }
            }
            sqlparser::ast::Value::Boolean(bool_val) => {
                let res = Arc::new(BooleanArray::from(vec![*bool_val]));
                Ok(ArrayDatum::new(res.clone(), true))
            }
            sqlparser::ast::Value::SingleQuotedString(str_val) => {
                let res = Arc::new(StringArray::from(vec![str_val.clone()]));
                Ok(ArrayDatum::new(res.clone(), true))
            }
            _ => {
                return Err(
                    ComputeValueError::ValueTypeNotImplemented(format!("{:?}", val)).into(),
                );
            }
        },
        sqlparser::ast::Expr::Identifier(col_ident) => {
            let col_array = rec.column_by_name(&col_ident.value);
            if let Some(col_array) = col_array {
                let col_array = col_array.clone();
                Ok(ArrayDatum::new(col_array, false))
            } else {
                Err(ComputeValueError::ColumnNotFound(col_ident.value.clone()).into())
            }
        }
        sqlparser::ast::Expr::CompoundIdentifier(idents) => {
            if idents.len() == 1 {
                let col_ident = idents.get(0).expect("expected zero index to exist");
                let col_array = rec.column_by_name(&col_ident.value);
                if let Some(col_array) = col_array {
                    let col_array = col_array.clone();
                    Ok(ArrayDatum::new(col_array, false))
                } else {
                    Err(ComputeValueError::ColumnNotFound(col_ident.value.clone()).into())
                }
            } else if idents.len() == 2 {
                let alias_ident = idents.get(0).expect("expected 0 index to exist");
                let col_ident = idents.get(1).expect("expected 1 index to exist");

                let col_idx = rec
                    .clone()
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(idx, field)| {
                        *field.name() == col_ident.value
                            && table_aliases
                                .get(idx.clone())
                                .expect("table aliases vec has incorrect length")
                                .iter()
                                .find(|table_alias| **table_alias == alias_ident.value)
                                .is_some()
                    })
                    .map(|(idx, _)| idx)
                    .find(|_| true);
                let col_idx = if let Some(col_idx) = col_idx {
                    col_idx
                } else {
                    return Err(ComputeValueError::IdentifierNotFound(format!(
                        "{:?}",
                        idents
                            .iter()
                            .map(|item| item.value.clone())
                            .collect::<Vec<String>>()
                            .join(".")
                    ))
                    .into());
                };

                if col_idx < rec.num_columns() {
                    let col_array = rec.column(col_idx);
                    Ok(ArrayDatum::new(col_array.clone(), false))
                } else {
                    Err(ComputeValueError::ColumnNotFound(col_ident.value.clone()).into())
                }
            } else {
                return Err(ComputeValueError::IdentifierNotFound(format!(
                    "{:?}",
                    idents
                        .iter()
                        .map(|item| item.value.clone())
                        .collect::<Vec<String>>()
                        .join(".")
                ))
                .into());
            }
        }
        _ => {
            return Err(
                ComputeValueError::ExpressionTypeNotImplemented(format!("{:?}", *expr)).into(),
            );
        }
    }
}

////////////////////////////////////////////////////////////////
// Type coersion for common data types

/// Determines the common type for two arrays
fn get_common_type(left: &DataType, right: &DataType) -> Result<DataType> {
    use DataType::*;

    match (left, right) {
        // Same types remain unchanged
        _ if left == right => Ok(left.clone()),

        // Integer widening conversions
        (Int8, Int16) | (Int16, Int8) => Ok(Int16),
        (Int8, Int32) | (Int32, Int8) | (Int16, Int32) | (Int32, Int16) => Ok(Int32),
        (Int8, Int64)
        | (Int64, Int8)
        | (Int16, Int64)
        | (Int64, Int16)
        | (Int32, Int64)
        | (Int64, Int32) => Ok(Int64),

        (UInt8, UInt16) | (UInt16, UInt8) => Ok(UInt16),
        (UInt8, UInt32) | (UInt32, UInt8) | (UInt16, UInt32) | (UInt32, UInt16) => Ok(UInt32),
        (UInt8, UInt64)
        | (UInt64, UInt8)
        | (UInt16, UInt64)
        | (UInt64, UInt16)
        | (UInt32, UInt64)
        | (UInt64, UInt32) => Ok(UInt64),

        // Mixed signed/unsigned conversions (promote to larger signed type)
        (UInt8, Int16) | (Int16, UInt8) => Ok(Int16),
        (UInt8, Int32) | (Int32, UInt8) | (UInt16, Int32) | (Int32, UInt16) => Ok(Int32),
        (UInt8, Int64)
        | (Int64, UInt8)
        | (UInt16, Int64)
        | (Int64, UInt16)
        | (UInt32, Int64)
        | (Int64, UInt32) => Ok(Int64),

        // Float widening
        (Float16, Float32) | (Float32, Float16) => Ok(Float32),
        (Float16, Float64) | (Float64, Float16) | (Float32, Float64) | (Float64, Float32) => {
            Ok(Float64)
        }

        // Integer to float conversions (always upcast to float)
        (Int8, Float32)
        | (Float32, Int8)
        | (Int16, Float32)
        | (Float32, Int16)
        | (UInt8, Float32)
        | (Float32, UInt8)
        | (UInt16, Float32)
        | (Float32, UInt16)
        | (Int32, Float32)
        | (Float32, Int32)
        | (UInt32, Float32)
        | (Float32, UInt32) => Ok(Float32),

        (Int8, Float64)
        | (Float64, Int8)
        | (Int16, Float64)
        | (Float64, Int16)
        | (UInt8, Float64)
        | (Float64, UInt8)
        | (UInt16, Float64)
        | (Float64, UInt16)
        | (Int32, Float64)
        | (Float64, Int32)
        | (UInt32, Float64)
        | (Float64, UInt32)
        | (Int64, Float64)
        | (Float64, Int64)
        | (UInt64, Float64)
        | (Float64, UInt64) => Ok(Float64),

        _ => Err(
            ComputeValueError::UnsupportedTypeCoersionForOperationBetweenTypes(
                left.clone(),
                right.clone(),
            )
            .into(),
        ),
    }
}

fn cast_to_common_type(left: &ArrayDatum, right: &ArrayDatum) -> Result<(ArrayDatum, ArrayDatum)> {
    let left_type = left.array.data_type();
    let right_type = right.array.data_type();

    let common_type = get_common_type(left_type, right_type)?;

    let left_casted = if *left_type != common_type {
        compute::cast(&left.array, &common_type)?
    } else {
        left.array.clone()
    };

    let right_casted = if *right_type != common_type {
        compute::cast(&right.array, &common_type)?
    } else {
        right.array.clone()
    };

    Ok((
        ArrayDatum {
            array: left_casted,
            is_scalar: left.is_scalar,
        },
        ArrayDatum {
            array: right_casted,
            is_scalar: right.is_scalar,
        },
    ))
}
