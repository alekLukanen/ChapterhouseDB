use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, Datum, Int32Array, RecordBatch, StringArray};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};

#[test]
fn test_scalar_plus_scalar_returns_an_array_of_length_one_which_is_not_scalar() -> Result<()> {
    let left = Int32Array::new_scalar(1);
    let right = Int32Array::new_scalar(2);
    let res = compute::kernels::numeric::add(&left, &right)?;

    let res_int32_array = res.as_any().downcast_ref::<Int32Array>().unwrap();

    assert_eq!(left.get().1, true);
    assert_eq!(right.get().1, true);
    assert_eq!(res.len(), 1);
    assert_eq!(res_int32_array.value(0), 3);

    // !!! NOT SCALAR !!!
    assert_eq!(res.get().1, false);

    Ok(())
}

#[test]
fn test_scalar_plus_array() -> Result<()> {
    let left = Int32Array::new_scalar(5);
    let right = Int32Array::new(vec![5, 4, 3, 2, 1, 0].into(), None);
    let res = compute::kernels::numeric::add(&left, &right)?;
    let res_int32_array = res.as_any().downcast_ref::<Int32Array>().unwrap();

    let expected_res = Int32Array::new(vec![10, 9, 8, 7, 6, 5].into(), None);

    assert!(res_int32_array.eq(&expected_res));
    assert_eq!(res.get().1, false);
    Ok(())
}

#[test]
fn test_arrow_record_duplicate_columns() -> Result<()> {
    let text_array1: ArrayRef = Arc::new(StringArray::from(vec!["hello", ", ", "world", "!"]));
    let text_array2: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let schema = Schema::new(vec![
        Field::new("table_a.text", DataType::Utf8, false),
        Field::new("table_a.text", DataType::Utf8, false),
    ]);

    let rec = RecordBatch::try_new(
        Arc::new(schema),
        vec![text_array1.clone(), text_array2.clone()],
    )
    .unwrap();

    assert_eq!(rec.num_columns(), 2);

    let col = rec
        .column_by_name("table_a.text")
        .expect("expected column to exist");
    assert!(col.eq(&text_array1));

    let col0 = rec.column(0);
    let col1 = rec.column(1);
    assert!(col0.eq(&text_array1));
    assert!(col1.eq(&text_array2));

    Ok(())
}
