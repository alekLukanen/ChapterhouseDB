use std::sync::Arc;

use anyhow::Result;
use arrow::{
    array::{RecordBatch, UInt64Array},
    row::{RowConverter, Rows, SortField},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RecordPartitionHandlerError {
    #[error("partition record has incorrect number of columns: {0}")]
    PartitionRecordHasIncorrectNumberOfColumns(usize),
}

#[derive(Debug)]
pub struct PartitionRecord {
    pub partition_idx: usize,
    pub record: RecordBatch,
}

#[derive(Debug)]
pub struct RecordPartitionHandler {
    partitions: Rows,
    row_converter: RowConverter,
    fields: Vec<SortField>,
}

impl RecordPartitionHandler {
    pub fn new(
        partitions_rec: Arc<RecordBatch>,
        fields: Vec<SortField>,
    ) -> Result<RecordPartitionHandler> {
        assert_eq!(partitions_rec.num_columns(), fields.len());

        let row_converter = RowConverter::new(fields.clone())?;
        let partitions = row_converter.convert_columns(&partitions_rec.columns())?;

        Ok(RecordPartitionHandler {
            partitions,
            row_converter,
            fields,
        })
    }

    pub fn partition(
        &self,
        part_rec: Arc<RecordBatch>,
        data_rec: Arc<RecordBatch>,
    ) -> Result<Vec<PartitionRecord>> {
        if part_rec.num_columns() != self.fields.len() {
            return Err(
                RecordPartitionHandlerError::PartitionRecordHasIncorrectNumberOfColumns(
                    part_rec.num_columns(),
                )
                .into(),
            );
        }

        if part_rec.num_rows() == 0 {
            return Ok(Vec::new());
        }
        if self.partitions.num_rows() == 0 {
            return Ok(vec![PartitionRecord {
                partition_idx: 0,
                record: (*data_rec).clone(),
            }]);
        }

        let part_rows = self.row_converter.convert_columns(part_rec.columns())?;

        let mut part_idxs = (0..self.partitions.num_rows() + 1)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<u64>>>();

        'outer: for (row_idx, row) in part_rows.iter().enumerate() {
            // lowest partition
            if row < self.partitions.row(0) {
                part_idxs[0].push(row_idx as u64);
                continue;
            }

            // middle partitions
            for part_idx in 1..self.partitions.num_rows() {
                if row >= self.partitions.row(part_idx - 1) && row < self.partitions.row(part_idx) {
                    part_idxs[part_idx].push(row_idx as u64);
                    continue 'outer;
                }
            }

            // highest partition
            if row >= self.partitions.row(self.partitions.num_rows() - 1) {
                part_idxs[self.partitions.num_rows()].push(row_idx as u64);
                continue;
            }
        }

        let mut part_recs = Vec::new();
        let mut part_idx = 0;
        for part_row_idxs in part_idxs {
            let range_starts_array = UInt64Array::from(part_row_idxs);
            let partition_intervals =
                arrow::compute::take_record_batch(&(*data_rec), &range_starts_array)?;

            part_recs.push(PartitionRecord {
                partition_idx: part_idx,
                record: partition_intervals,
            });
            part_idx += 1;
        }

        Ok(part_recs)
    }
}
