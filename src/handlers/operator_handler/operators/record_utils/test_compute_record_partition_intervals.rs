use std::sync::Arc;

use anyhow::Result;
use arrow::{
    array::{Int32Builder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
};

use super::compute_record_partition_intervals;

#[test]
fn test_compute_record_partition_intervals() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("table_a.text", DataType::Utf8, true),
        Field::new("table_a.id", DataType::Int32, true),
    ]));

    // input data //////////////////////////////////////////
    // 1, 1, 1, 1, 2, 1
    let mut text_array_builder = StringBuilder::new();
    text_array_builder.append_null();
    text_array_builder.append_null();
    // ---
    text_array_builder.append_null();
    text_array_builder.append_value("a");
    // ---
    text_array_builder.append_value("a");
    text_array_builder.append_value("a");
    text_array_builder.append_value("c");

    let mut int_array_builder = Int32Builder::new();
    int_array_builder.append_null();
    int_array_builder.append_value(0);
    // ---
    int_array_builder.append_value(1);
    int_array_builder.append_null();
    // ---
    int_array_builder.append_value(0);
    int_array_builder.append_value(0);
    int_array_builder.append_value(0);

    let rec = Arc::new(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(text_array_builder.finish()),
            Arc::new(int_array_builder.finish()),
        ],
    )?);

    // expected result ////////////////////////////////////
    let mut expected_text_array_builder = StringBuilder::new();
    expected_text_array_builder.append_null();
    expected_text_array_builder.append_value("a");

    let mut expected_int_array_builder = Int32Builder::new();
    expected_int_array_builder.append_value(1);
    expected_int_array_builder.append_value(0);

    let expected_intervals_rec = Arc::new(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(expected_text_array_builder.finish()),
            Arc::new(expected_int_array_builder.finish()),
        ],
    )?);

    ///////////////////////////////////////////////////////

    let partition_intervals = compute_record_partition_intervals(rec.clone(), 3)?;

    assert_eq!(expected_intervals_rec, partition_intervals);

    Ok(())
}
