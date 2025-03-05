use std::{cmp::max, cmp::min, sync::Arc};

use anyhow::Result;
use arrow::{
    array::Datum,
    error::ArrowError,
    util::display::{ArrayFormatter, FormatOptions},
};
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::Color,
    text::Text,
    widgets::{Paragraph, StatefulWidget},
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
    max_column_width: usize,
    wrap_text: bool,
    grid_spacing: u16,
    selected_color: Color,
}

impl RecordTable {
    fn build_row_layout(&self, max_column_widths: &Vec<usize>) -> Layout {
        Layout::default()
            .direction(Direction::Horizontal)
            .spacing(self.grid_spacing)
            .constraints(
                max_column_widths
                    .iter()
                    .map(|item| Constraint::Length(item.clone() as u16)),
            )
    }

    fn render_grid(
        &self,
        area: Rect,
        buf: &mut Buffer,
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        state: &mut RecordTableState,
    ) {
        for row in &rows {
            assert_eq!(columns.len(), row.len());
        }

        let mut max_column_widths = Vec::new();
        for idx in 0..columns.len() {
            let column_name_width = columns.get(idx).expect("column value").len();
            let max_column_width_for_rows = rows
                .iter()
                .map(|row| row.get(idx).expect("row value").len())
                .max();

            let desired_column_width =
                max(column_name_width, max_column_width_for_rows.unwrap_or(0));
            let column_width = min(desired_column_width, self.max_column_width);

            max_column_widths.push(column_width);
        }

        let row_heights = Vec::new();

        // define the grid needed for the table
        let row_layout = self.build_row_layout(&max_column_widths);
    }

    fn render_error(&self, area: Rect, buf: &mut Buffer, msg: String) {}
}

impl Default for RecordTable {
    fn default() -> Self {
        RecordTable {
            max_text_chars: 100,
            max_column_width: 25,
            wrap_text: false,
            grid_spacing: 1,
            selected_color: Color::Blue,
        }
    }
}

impl StatefulWidget for RecordTable {
    type State = RecordTableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        if state.offset.0 >= state.records.len() {
            return;
        }

        let first_rec = if let Some(rec) = state.records.first() {
            rec
        } else {
            return;
        };
        let columns: Vec<String> = first_rec
            .record
            .schema()
            .fields
            .iter()
            .map(|item| item.name().clone())
            .collect();

        let num_cols = columns.len();

        let formatter_options = FormatOptions::default();

        // stringify the arrow data
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut current_offset = state.offset.clone();
        let initial_rec = state.records.get(state.offset.0).cloned();
        let mut formatters = if let Some(initial_rec) = &initial_rec {
            initial_rec
                .record
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &formatter_options))
                .collect::<Result<Vec<_>, ArrowError>>()
                .unwrap_or_else(|_| {
                    self.render_error(
                        area,
                        buf,
                        "unable to create formatters for arrow record data types".to_string(),
                    );
                    vec![]
                })
        } else {
            return self.render_error(
                area,
                buf,
                "unable to create formatters for arrow record data types".to_string(),
            );
        };

        for _ in 0..state.max_rows_to_display {
            let rec = if let Some(rec) = state.records.get(current_offset.0) {
                rec
            } else {
                break;
            };

            let mut row = Vec::with_capacity(num_cols);
            for formatter in &formatters {
                let value = formatter.value(current_offset.1);
                match value.try_to_string() {
                    Ok(res) => row.push(res),
                    Err(_) => row.push("Unable to Display".to_string()),
                }
            }
            rows.push(row);

            if current_offset.1 >= rec.record.num_rows() {
                current_offset = (current_offset.0 + 1, 0);
                let next_rec = state.records.get(current_offset.0).cloned();
                if let Some(next_rec) = &next_rec {
                    formatters = if let Ok(f) = next_rec
                        .record
                        .columns()
                        .iter()
                        .map(|c| ArrayFormatter::try_new(c.as_ref(), &formatter_options))
                        .collect::<Result<Vec<_>, ArrowError>>()
                    {
                        f
                    } else {
                        return self.render_error(
                            area,
                            buf,
                            "unable to create formatters for arrow record data types".to_string(),
                        );
                    };
                }
            } else {
                current_offset = (current_offset.0, current_offset.1 + 1);
            }
        }

        self.render_grid(area, buf, columns, rows, state);
    }
}
