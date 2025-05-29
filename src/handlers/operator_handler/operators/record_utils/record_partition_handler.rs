use std::sync::Arc;

use anyhow::Result;
use arrow::{
    array::RecordBatch,
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
    pub record: Arc<RecordBatch>,
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
                record: data_rec.clone(),
            }]);
        }

        let part_rows = self.row_converter.convert_columns(part_rec.columns())?;

        let mut part_idxs = Vec::new();

        'outer: for row in part_rows.iter() {
            // lowest partition
            if row < self.partitions.row(0) {
                part_idxs.push(0);
                continue;
            }

            // middle partitions
            for part_idx in 1..self.partitions.num_rows() {
                if row >= self.partitions.row(part_idx - 1) && row < self.partitions.row(part_idx) {
                    part_idxs.push(part_idx);
                    break 'outer;
                }
            }

            // highest partition
            if row >= self.partitions.row(self.partitions.num_rows() - 1) {
                part_idxs.push(self.partitions.num_rows() - 1);
                continue;
            }
        }

        Ok(vec![])
    }
}
