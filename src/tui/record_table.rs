use std::sync::Arc;

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    style::Color,
    widgets::StatefulWidget,
};

#[derive(Debug, Clone)]
struct TableRecord {
    order: usize,
    record: Arc<arrow::array::RecordBatch>,
}

#[derive(Debug, Default)]
pub struct RecordTableState {
    records: Vec<TableRecord>,
    offset: (usize, usize), // the index in the records vec to start presenting rows
    selected: Option<usize>, // the selected row accouting for the offset
    max_rows_to_display: usize,
    desired_rows_to_buffer: usize,
}

impl RecordTableState {
    pub fn select(&mut self, val: Option<usize>) {
        self.selected = val;
        if val.is_none() {
            self.offset = (0, 0);
        }
        self.remove_unused_records();
    }
    pub fn need_next_record(&self) -> bool {
        let current_record_rows_remaining = self
            .records
            .get(self.offset.0)
            .expect("missing table record")
            .record
            .num_rows()
            - (self.offset.1 + 1);
        let next_records_rows_remaining: usize = (self.offset.0..self.records.len())
            .map(|idx| {
                self.records
                    .get(idx)
                    .expect("missing table record")
                    .record
                    .num_rows()
            })
            .sum();
        return current_record_rows_remaining + next_records_rows_remaining
            < self.max_rows_to_display;
    }
    pub fn remove_unused_records(&mut self) {
        let mut new_records: Vec<TableRecord> = Vec::new();
        let mut rows_seen: usize = 0;
        for (idx, rec) in self.records.iter().enumerate() {
            if idx >= self.offset.0 && rows_seen < self.desired_rows_to_buffer {
                new_records.push(rec.clone());
                rows_seen += rec.record.num_rows();
            }
        }
        self.offset = (0, self.offset.1);
        self.records = new_records;
    }
    pub fn add_record(&mut self, order: usize, record: Arc<arrow::array::RecordBatch>) {
        let table_record = TableRecord { order, record };
        for (idx, rec) in self.records.iter().enumerate() {
            if order < rec.order {
                self.records.insert(idx, table_record);
                return;
            }
        }
        self.records.push(table_record);
    }
}

#[derive(Debug)]
pub struct RecordTable {
    max_text_chars: usize,
    wrap_text: bool,
    selected_color: Color,
}

impl RecordTable {
    fn build_grid_layout(&self, state: &RecordTableState) -> Layout {
        Layout::horizontal([Constraint::Length(5)])
    }
}

impl Default for RecordTable {
    fn default() -> Self {
        RecordTable {
            max_text_chars: 100,
            wrap_text: false,
            selected_color: Color::Blue,
        }
    }
}

impl StatefulWidget for RecordTable {
    type State = RecordTableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        self.build_grid_layout(state);
    }
}
