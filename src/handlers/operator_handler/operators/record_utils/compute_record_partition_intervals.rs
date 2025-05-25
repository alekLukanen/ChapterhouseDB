use std::sync::Arc;

use anyhow::Result;
use arrow::array::{RecordBatch, UInt64Array};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ComputeRecordPartitionIntervalsError {
    #[error("number of partitions can not be zero")]
    NumberOfPartitionsCanNotBeZero,
}

/// Partition the RecordBatch using the columns in the
/// order they are in the record. The record is assumed
/// to be in sorted order. The output record contains
/// the partition intervals which try to keep the partitions
/// of equal size. The number of partitions is not always
/// guaranteed to be equal to the num_partitions value.
pub fn compute_record_partition_intervals(
    rec: Arc<RecordBatch>,
    num_partitions: usize,
) -> Result<Arc<RecordBatch>> {
    if num_partitions == 0 {
        return Err(ComputeRecordPartitionIntervalsError::NumberOfPartitionsCanNotBeZero.into());
    }
    if rec.num_rows() == 0 {
        return Ok(Arc::new(RecordBatch::new_empty(rec.schema())));
    }
    if num_partitions == 1 {
        return Ok(Arc::new(RecordBatch::new_empty(rec.schema())));
    }

    let ranges = arrow::compute::partition(rec.columns())?.ranges();

    assert_ne!(ranges.len(), 0);
    assert_eq!(ranges.first().unwrap().start, 0);

    let rows_per_partition = rec.num_rows() / num_partitions;
    let mut compacted_starts = Vec::new();
    let mut range_start_idx: usize = 0;
    let mut range_len: usize = 0;
    for range in ranges {
        if range_len + range.len() > rows_per_partition && range_len > 0 {
            if range_start_idx != 0 {
                compacted_starts.push(range_start_idx as u64);
            }
            range_start_idx = range_start_idx + range_len;
            range_len = 0;
        }
        range_len += range.len();
    }

    let range_starts_array = UInt64Array::from(compacted_starts);
    let partition_intervals = arrow::compute::take_record_batch(&(*rec), &range_starts_array)?;

    Ok(Arc::new(partition_intervals))
}
