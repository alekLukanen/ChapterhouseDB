#![allow(dead_code)]

use std::collections;
use std::sync::{atomic, Arc, Mutex};

use anyhow::{anyhow, Result};

pub struct Record {
    rec: Arc<arrow::array::RecordBatch>,
    record_id: usize,
    mem_hndlr_rec_state: Arc<Mutex<MemoryHandlerRecordState>>,
}

struct TrackedRecord {
    rec: Arc<arrow::array::RecordBatch>,
}

pub struct MemoryHandlerRecordState {
    records: collections::HashMap<usize, TrackedRecord>,
}

pub struct MemoryHandler {
    record_state: Arc<Mutex<MemoryHandlerRecordState>>,
    record_idx: atomic::AtomicUsize,

    // size in mebibytes
    max_allowed_memory: usize,
    max_allowed_disk: usize,
}

impl MemoryHandler {
    pub fn new(max_allowed_memory: usize, max_allowed_disk: usize) -> MemoryHandler {
        MemoryHandler {
            record_state: Arc::new(Mutex::new(MemoryHandlerRecordState {
                records: collections::HashMap::new(),
            })),
            record_idx: atomic::AtomicUsize::new(0),
            max_allowed_memory,
            max_allowed_disk,
        }
    }

    pub async fn add(&mut self, rec: arrow::array::RecordBatch) -> Result<Record> {
        let rec = Arc::new(rec);

        let record_id = self.record_idx.fetch_add(1, atomic::Ordering::SeqCst);
        let tracked_rec = TrackedRecord { rec: rec.clone() };

        self.record_state
            .lock()
            .map_err(|_| anyhow!("lock error"))?
            .records
            .insert(record_id, tracked_rec);

        let res_rec = Record {
            rec,
            record_id,
            mem_hndlr_rec_state: self.record_state.clone(),
        };
        Ok(res_rec)
    }
}
