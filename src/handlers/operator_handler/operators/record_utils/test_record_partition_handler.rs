use std::sync::Arc;

use anyhow::Result;
use arrow::{
    array::{Int32Builder, RecordBatch, StringBuilder},
    compute::SortOptions,
    datatypes::{DataType, Field, Schema},
    row::SortField,
};

use super::RecordPartitionHandler;

#[test]
fn test_record_partition_handler() -> Result<()> {
    // build test data ///////////////////////////////
    let schema = Arc::new(Schema::new(vec![
        Field::new("table_a.text", DataType::Utf8, true),
        Field::new("table_a.id", DataType::Int32, true),
    ]));

    let mut part_text_array_builder = StringBuilder::new();
    part_text_array_builder.append_null();
    part_text_array_builder.append_value("a");

    let mut part_int_array_builder = Int32Builder::new();
    part_int_array_builder.append_value(1);
    part_int_array_builder.append_value(0);

    let part_intervals_rec = Arc::new(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(part_text_array_builder.finish()),
            Arc::new(part_int_array_builder.finish()),
        ],
    )?);

    let sort_fields = vec![
        SortField::new_with_options(DataType::Utf8, SortOptions::default()),
        SortField::new_with_options(DataType::Int32, SortOptions::default()),
    ];

    let mut part_rec_text_array_builder = StringBuilder::new();
    part_rec_text_array_builder.append_null();
    part_rec_text_array_builder.append_null();
    part_rec_text_array_builder.append_value("b");

    let mut part_rec_int_array_builder = Int32Builder::new();
    part_rec_int_array_builder.append_value(0);
    part_rec_int_array_builder.append_value(2);
    part_rec_int_array_builder.append_value(0);

    let part_rec = Arc::new(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(part_rec_text_array_builder.finish()),
            Arc::new(part_rec_int_array_builder.finish()),
        ],
    )?);

    // create the handler //////////////////////////
    let part_handler = RecordPartitionHandler::new(part_intervals_rec, sort_fields)?;
    let part_recs = part_handler.partition(part_rec.clone(), part_rec.clone())?;

    // validate the partitioned data ///////////////
    assert_eq!(3, part_recs.len());

    let part_0 = part_recs.get(0).unwrap();
    let part_1 = part_recs.get(1).unwrap();
    let part_2 = part_recs.get(2).unwrap();

    let part_0_rec_indices = arrow::array::UInt64Array::from(vec![0u64]);
    let expected_part_0_rec = arrow::compute::take_record_batch(&*part_rec, &part_0_rec_indices)?;
    assert_eq!(0, part_0.partition_idx);
    assert_eq!(expected_part_0_rec, part_0.record);

    let part_1_rec_indices = arrow::array::UInt64Array::from(vec![1u64]);
    let expected_part_1_rec = arrow::compute::take_record_batch(&*part_rec, &part_1_rec_indices)?;
    assert_eq!(1, part_1.partition_idx);
    assert_eq!(expected_part_1_rec, part_1.record);

    let part_2_rec_indices = arrow::array::UInt64Array::from(vec![2u64]);
    let expected_part_2_rec = arrow::compute::take_record_batch(&*part_rec, &part_2_rec_indices)?;
    assert_eq!(2, part_2.partition_idx);
    assert_eq!(expected_part_2_rec, part_2.record);

    Ok(())
}
