use std::{cmp::max, cmp::min, sync::Arc};

use anyhow::Result;
use arrow::{
    error::ArrowError,
    util::display::{ArrayFormatter, FormatOptions},
};
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Paragraph, StatefulWidget, Widget, Wrap},
};

#[derive(Clone)]
struct TableRow {
    idx: usize,
    fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TableRecord {
    order: usize,
    record: Arc<arrow::array::RecordBatch>,
}

#[derive(Debug)]
pub struct RecordTableState {
    records: Vec<TableRecord>,
    offset: (usize, usize), // the index in the records vec to start presenting rows
    selected: Option<usize>, // the selected row accouting for the offset
    alternate_selected: bool,
    max_rows_to_display: usize,
    desired_rows_to_buffer: usize,
}

impl Default for RecordTableState {
    fn default() -> Self {
        RecordTableState {
            records: Vec::new(),
            offset: (0, 0),
            selected: Some(0),
            alternate_selected: true,
            max_rows_to_display: 50,
            desired_rows_to_buffer: 100,
        }
    }
}

impl RecordTableState {
    pub fn select(&mut self, val: Option<usize>) {
        self.selected = val;
        if val.is_none() {
            self.offset = (0, 0);
        }
        self.remove_unused_records();
    }
    pub fn selected(&self) -> Option<usize> {
        return self.selected;
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
    pub fn set_alternate_selected(&mut self, val: bool) {
        self.alternate_selected = val;
    }
    pub fn num_records(&self) -> usize {
        self.records.len()
    }
}

#[derive(Debug)]
pub struct RecordTable {
    max_text_chars: u16,
    max_column_width: u16,
    grid_spacing: u16,
    selected_color: Color,
    selected_alternate_color: Color,
    selected_text_color: Color,
    text_color: Color,
    border_color: Color,
}

impl RecordTable {
    fn build_row_layout(
        &self,
        max_column_widths: &Vec<u16>,
        row_heights: &Vec<u16>,
    ) -> (Layout, Layout) {
        let vertical_layout = Layout::default()
            .direction(Direction::Vertical)
            .spacing(self.grid_spacing)
            .constraints(
                row_heights
                    .iter()
                    .map(|item| Constraint::Length(item.clone() as u16)),
            );
        let horizontal_layout = Layout::default()
            .direction(Direction::Horizontal)
            .spacing(self.grid_spacing)
            .constraints(
                max_column_widths
                    .iter()
                    .map(|item| Constraint::Length(item.clone() as u16)),
            );
        (vertical_layout, horizontal_layout)
    }

    fn get_columns_and_rows(
        &self,
        area: Rect,
        buf: &mut Buffer,
        state: &RecordTableState,
    ) -> Option<(Vec<String>, Vec<TableRow>)> {
        let first_rec = if let Some(rec) = state.records.first() {
            rec
        } else {
            self.render_error(area, buf, "table does not have any records".to_string());
            return None;
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
        let mut rows: Vec<TableRow> = Vec::new();
        let mut current_offset = state.offset.clone();

        let mut record_formatters = Vec::new();
        for rec in &state.records {
            let formatter = rec
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
                });
            record_formatters.push(formatter);
        }
        if record_formatters.len() == 0 {
            self.render_error(
                area,
                buf,
                "unable to create formatters for arrow record data types".to_string(),
            );
            return None;
        }

        for idx in 0..state.max_rows_to_display {
            let rec = if let Some(rec) = state.records.get(current_offset.0) {
                rec
            } else {
                break;
            };
            let formatters = record_formatters
                .get(current_offset.0)
                .expect("record formatter not found");

            let mut row = Vec::with_capacity(num_cols);
            for formatter in formatters {
                let value = formatter.value(current_offset.1);
                match value.try_to_string() {
                    Ok(res) => {
                        let res = if res.len() > self.max_text_chars as usize {
                            if self.max_text_chars > 3 {
                                let mut val: String =
                                    res.chars().take(self.max_text_chars as usize - 3).collect();
                                val.push_str("...");
                                val
                            } else {
                                res.chars().take(self.max_text_chars as usize).collect()
                            }
                        } else {
                            res
                        };
                        row.push(res);
                    }
                    Err(_) => row.push("Unable to Display".to_string()),
                }
            }
            rows.push(TableRow { idx, fields: row });

            if current_offset.1 >= rec.record.num_rows() - 1 {
                current_offset = (current_offset.0 + 1, 0);
            } else {
                current_offset = (current_offset.0, current_offset.1 + 1);
            }
        }

        return Some((columns, rows));
    }

    fn render_grid(
        &self,
        area: Rect,
        buf: &mut Buffer,
        columns: Vec<String>,
        rows: Vec<TableRow>,
        state: &mut RecordTableState,
    ) {
        for row in &rows {
            assert_eq!(columns.len(), row.fields.len());
        }

        let mut max_column_widths = Vec::new();
        for idx in 0..columns.len() {
            let column_name_width = columns.get(idx).expect("column value").len();
            let max_column_width_for_rows = rows
                .iter()
                .map(|row| row.fields.get(idx).expect("row value").len())
                .max();

            let desired_column_width =
                max(column_name_width, max_column_width_for_rows.unwrap_or(0));
            let column_width = min(desired_column_width, self.max_column_width as usize - 2);

            max_column_widths.push(column_width as u16);
        }

        let mut row_heights = rows
            .iter()
            .map(|row| {
                row.fields
                    .iter()
                    .enumerate()
                    .map(|(col_idx, row_col)| {
                        let row_len = row_col.len() as u16;
                        let col_width = max_column_widths
                            .get(col_idx)
                            .expect("unable to find max column width");
                        let has_remainder = row_len % col_width;
                        if has_remainder > 0 {
                            1 + row_len / *col_width
                        } else {
                            row_len / *col_width
                        }
                    })
                    .max()
                    .expect("expect row height")
            })
            .collect::<Vec<u16>>();

        // only show enough rows that will fit on the screen
        let mut stop_index: usize = 0;
        let mut total_height: u16 = 0;
        for (idx, row_height) in row_heights.iter().enumerate() {
            if total_height + row_height + 1 > (area.height - 3) {
                break;
            }
            total_height += *row_height + 1;
            stop_index = idx;
        }
        let rows = rows[0..stop_index].to_vec();
        row_heights = row_heights[0..stop_index + 1].to_vec();

        // render the column names
        let [_, table_area] = Layout::new(
            Direction::Horizontal,
            [Constraint::Length(1), Constraint::Fill(1)],
        )
        .areas(area);
        let [_, header_area, _, rows_area] = Layout::new(
            Direction::Vertical,
            [
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Fill(1),
            ],
        )
        .areas(table_area);

        // define the grid needed for the table
        let (vertical_layout, horizontal_layout) =
            self.build_row_layout(&max_column_widths, &row_heights);

        let horizontal_area = horizontal_layout.split(header_area);
        for (col_idx, col_name) in columns.iter().enumerate() {
            let cell_area = horizontal_area[col_idx];
            let style = Style::default().fg(self.text_color);
            let para = Paragraph::new(col_name.clone()).style(style);
            para.render(cell_area, buf);
        }

        // render the rows
        let vertical_areas = vertical_layout.split(rows_area);
        for (row_idx, row) in rows.iter().enumerate() {
            let vertical_area = vertical_areas[row_idx];
            let horizontal_area = horizontal_layout.split(vertical_area);

            for (col_idx, col) in row.fields.iter().enumerate() {
                let cell_area = horizontal_area[col_idx];
                let mut style = Style::default().fg(self.text_color);
                if let Some(selected) = state.selected {
                    if selected == row_idx {
                        style = style.fg(self.selected_text_color);
                        if state.alternate_selected {
                            style = style.bg(self.selected_alternate_color);
                        } else {
                            style = style.bg(self.selected_color);
                        }
                    }
                }
                let para = Paragraph::new(col.clone())
                    .style(style)
                    .wrap(Wrap { trim: false });
                para.render(cell_area, buf);
            }
        }
    }

    fn render_error(&self, area: Rect, buf: &mut Buffer, msg: String) {
        Paragraph::new(msg).render(area, buf);
    }
}

impl Default for RecordTable {
    fn default() -> Self {
        RecordTable {
            max_text_chars: 75,
            max_column_width: 50,
            grid_spacing: 1,
            selected_color: Color::Blue,
            selected_alternate_color: Color::Gray,
            selected_text_color: Color::Black,
            text_color: Color::Cyan,
            border_color: Color::Cyan,
        }
    }
}

impl StatefulWidget for RecordTable {
    type State = RecordTableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        if state.offset.0 >= state.records.len() {
            self.render_error(area, buf, "offset past end of records".to_string());
            return;
        }

        let (columns, rows) = match self.get_columns_and_rows(area, buf, state) {
            Some(val) => val,
            None => {
                return;
            }
        };

        self.render_grid(area, buf, columns, rows, state);
    }
}
