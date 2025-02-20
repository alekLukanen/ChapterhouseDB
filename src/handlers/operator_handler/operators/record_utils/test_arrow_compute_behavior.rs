use std::sync::Arc;

use anyhow::Result;
use arrow::array::{
    ArrayRef, Datum, Float32Array, Int32Array, RecordBatch, StringArray, UInt16Array, UInt32Array,
    UInt8Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};

#[test]
fn test_pretty_print_record() -> Result<()> {
    let text_array1: ArrayRef = Arc::new(StringArray::from(vec!["hello", ", ", "world", "!"]));
    let text_array2: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let int_array1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let schema = Schema::new(vec![
        Field::new("table_a.id", DataType::Int32, false),
        Field::new("table_a.text", DataType::Utf8, false),
        Field::new("table_b.text", DataType::Utf8, false),
    ]);

    let rec = RecordBatch::try_new(
        Arc::new(schema),
        vec![int_array1, text_array1.clone(), text_array2.clone()],
    )
    .unwrap();

    let recs = vec![rec];
    let form_rec = arrow::util::pretty::pretty_format_batches(&recs)?;

    println!("{}", form_rec);

    let expected_form_rec = "+------------+--------------+--------------+
| table_a.id | table_a.text | table_b.text |
+------------+--------------+--------------+
| 1          | hello        | a            |
| 2          | ,            | b            |
| 3          | world        | c            |
| 4          | !            | d            |
+------------+--------------+--------------+";

    assert_eq!(form_rec.to_string(), expected_form_rec.to_string());

    Ok(())
}

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

#[test]
fn test_array_type_conversion_roundoff_uint32_to_float32() -> Result<()> {
    let arr = UInt32Array::from(vec![1, 1000, 16_777_216, 16_777_217, 4_294_967_295]);

    let cast_arr = compute::cast(&arr, &DataType::Float32)?;
    let expected_arr: ArrayRef = Arc::new(Float32Array::from(vec![
        1.0,
        1000.0,
        16777216.0,
        16777216.0,
        4294967300.0,
    ]));

    assert!(cast_arr.eq(&expected_arr));

    Ok(())
}

#[test]
fn test_array_type_conversion_roundoff_uint16_to_uint8() -> Result<()> {
    let arr = UInt16Array::from(vec![1, 2, 3, 2 ^ 8 - 1, 2 ^ 8, 2 ^ 8 + 1]);

    let cast_arr = compute::cast(&arr, &DataType::UInt8)?;
    let expected_arr: ArrayRef = Arc::new(UInt8Array::from(vec![1, 2, 3, 5, 10, 11]));

    assert!(cast_arr.eq(&expected_arr));

    Ok(())
}
