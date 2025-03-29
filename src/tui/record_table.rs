use std::{
    cmp::{max, min},
    collections::HashMap,
    sync::Arc,
};

use anyhow::{anyhow, Result};
use arrow::{
    error::ArrowError,
    util::display::{ArrayFormatter, FormatOptions},
};
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    widgets::{Paragraph, StatefulWidget, Widget, Wrap},
};

#[derive(Debug, Clone)]
struct TableRow {
    idx: usize,
    record_idx: usize,
    row_idx: usize,
    fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TableRecord {
    order: usize,
    record: Arc<arrow::array::RecordBatch>,
}

#[derive(Debug)]
pub struct RecordTableState {
    area: Option<Rect>,
    records: Vec<TableRecord>,
    last_record_idx: Option<usize>,
    offset: (usize, usize), // the index in the records vec to start presenting rows
    selected: Option<usize>, // the selected row accouting for the offset
    alternate_selected: bool,
    max_rows_to_display: usize,
    desired_rows_to_buffer: usize,

    max_text_chars: u16,
    max_column_width: u16,

    // memoize the row text
    // this represents the text that will be displayed
    // to the user and the first row will always be at
    // the "offset" position. You can paginate to the next
    // page which is a stiched view of the record data.
    columns: Vec<String>,
    rows: Vec<TableRow>,
    row_heights: Vec<u16>,
    row_widths: Vec<u16>,
    waiting_for_data: bool,

    err: Option<String>,
}

impl Default for RecordTableState {
    fn default() -> Self {
        RecordTableState {
            area: None,
            records: Vec::new(),
            last_record_idx: None,
            offset: (0, 0),
            selected: Some(0),
            alternate_selected: true,
            max_rows_to_display: 50,
            desired_rows_to_buffer: 100,
            max_text_chars: 75,
            max_column_width: 50,
            columns: Vec::new(),
            rows: Vec::new(),
            row_heights: Vec::new(),
            row_widths: Vec::new(),
            waiting_for_data: true,
            err: None,
        }
    }
}

impl RecordTableState {
    pub fn set_area(&mut self, area: Rect) {
        let updated = if let Some(current_area) = self.area {
            current_area != area
        } else {
            true
        };
        self.area = Some(area);

        if updated {
            self.refresh_view_data();
        }
    }

    pub fn select(&mut self, val: Option<usize>) {
        self.selected = val;
        if val.is_none() {
            self.offset = (0, 0);
        }
    }

    pub fn selected(&self) -> Option<usize> {
        return self.selected;
    }

    fn get_record(&self, idx: usize) -> Option<&TableRecord> {
        self.records
            .binary_search_by_key(&idx, |r| r.order)
            .ok()
            .map(|pos| &self.records[pos])
    }

    pub fn next_page(&mut self) -> bool {
        if self.waiting_for_data {
            return false;
        }

        let max_offset_visible = if let Some(table_row) = self.rows.last() {
            (table_row.record_idx, table_row.row_idx)
        } else {
            return false;
        };
        if self.records.len() <= max_offset_visible.0 {
            return false;
        }

        if let Some(rec) = self.get_record(max_offset_visible.0) {
            if max_offset_visible.1 + 1 >= rec.record.num_rows() {
                self.offset = (max_offset_visible.0 + 1, 0);
            } else {
                self.offset = (max_offset_visible.0, max_offset_visible.1 + 1);
            }
            return true;
        } else {
            return false;
        }
    }

    pub fn previous_page(&mut self) -> bool {
        if self.waiting_for_data {
            return false;
        }

        let min_offset_visible = if let Some(table_row) = self.rows.first() {
            (table_row.record_idx, table_row.row_idx)
        } else {
            return false;
        };
        if self.records.len() <= min_offset_visible.0 {
            return false;
        }

        if let Some(rec) = self.get_record(min_offset_visible.0) {
            if min_offset_visible.1 == 0 && min_offset_visible.0 != 0 {
                let prev_rec = if let Some(prev_rec) = self.get_record(min_offset_visible.0 - 1) {
                    prev_rec
                } else {
                    return false;
                };
                self.offset = (min_offset_visible.0 - 1, prev_rec.record.num_rows() - 1);
            } else if min_offset_visible.1 == 0 && min_offset_visible.0 == 0 {
                return false;
            } else {
            }
            return true;
        } else {
            return false;
        }
    }

    pub fn need_records(&mut self) -> Vec<usize> {
        let mut rows_before_offset: usize = 0;
        let mut rows_after_offset: usize = 0;
        for (rec_idx, rec) in self.records.iter().enumerate() {
            if rec_idx < self.offset.0 {
                rows_before_offset += rec.record.num_rows();
            } else if rec_idx > self.offset.0 {
                rows_after_offset += rec.record.num_rows();
            } else {
                rows_before_offset += self.offset.1;
                rows_after_offset += rec.record.num_rows() - (self.offset.1 + 1);
            }
        }

        let need_previous_record = rows_before_offset >= self.max_rows_to_display;
        let need_next_record = rows_after_offset >= self.max_rows_to_display;

        let mut record_idxs = Vec::new();
        if self.records.len() > 1 {
            if need_previous_record {
                if let Some(rec) = self.records.first() {
                    if rec.order != 0 {
                        record_idxs.push(rec.order - 1);
                    }
                }
            }
            if need_next_record {
                if let Some(rec) = self.records.last() {
                    if self.last_record_idx < Some(rec.order) {
                        record_idxs.push(rec.order + 1);
                    }
                }
            }
        } else if self.records.len() == 1 {
            if need_previous_record || need_next_record {
                if let Some(rec) = self.records.first() {
                    if rec.order != 0 {
                        record_idxs.push(rec.order - 1);
                    }
                }
            }
        } else if self.records.len() == 0 {
            if self.last_record_idx.is_none() {
                record_idxs.push(0);
            }
        }
        if record_idxs.len() > 0 {
            self.waiting_for_data = true;
        }
        return record_idxs;
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
        self.waiting_for_data = false;
        self.refresh_view_data();
    }

    pub fn set_alternate_selected(&mut self, val: bool) {
        self.alternate_selected = val;
    }

    pub fn num_records(&self) -> usize {
        self.records.len()
    }

    fn set_error(&mut self, err: String) {
        self.err = Some(err);
    }

    fn refresh_view_data(&mut self) {
        let area = if let Some(area) = self.area {
            area
        } else {
            return;
        };

        let res = self.set_columns_and_rows(area);
        match res {
            Ok(_) => {
                self.err = None;
            }
            Err(err) => {
                self.set_error(err.to_string());
            }
        }
    }

    fn set_columns_and_rows(&mut self, area: Rect) -> Result<()> {
        if self.waiting_for_data {
            return Ok(());
        }

        let (columns, rows, mut row_heights, max_column_widths) =
            self.get_render_data(self.offset, true)?;

        // only show enough rows that will fit on the screen
        let mut stop_index: usize = 0;
        let mut total_height: u16 = 0;
        for (idx, row_height) in row_heights.iter().enumerate() {
            if total_height + Self::total_row_height(*row_height) > Self::total_area_height(area) {
                break;
            }
            total_height += *row_height + 1;
            stop_index = idx;
        }
        if stop_index + 1 < rows.len() {
            self.waiting_for_data = true;
        }

        let rows = rows[0..stop_index].to_vec();
        row_heights = row_heights[0..stop_index + 1].to_vec();

        self.columns = columns;
        self.rows = rows;
        self.row_heights = row_heights;
        self.row_widths = max_column_widths;

        return Ok(());
    }

    fn get_offset_for_previous_page(&self, lower_offset: (usize, usize)) -> Result<(usize, usize)> {
        let area = if let Some(area) = self.area {
            area
        } else {
            return Err(anyhow!("area not set"));
        };

        let (_, rows, row_heights, _) = self.get_render_data(lower_offset, false)?;

        assert_eq!(rows.len(), row_heights.len());

        let mut upper_offset = lower_offset;
        let mut total_height: u16 = 0;
        for (row, row_height) in rows.iter().zip(row_heights).rev() {
            if total_height + Self::total_row_height(row_height) > Self::total_area_height(area) {
                break;
            }
            total_height += row_height + 1;
            upper_offset = (row.record_idx, row.row_idx);
        }
        Ok(upper_offset)
    }

    fn get_render_data(
        &self,
        offset: (usize, usize),
        forward: bool,
    ) -> Result<(Vec<String>, Vec<TableRow>, Vec<u16>, Vec<u16>)> {
        let first_rec = if let Some(rec) = self.records.first() {
            rec
        } else {
            return Err(anyhow!("table does not have any records"));
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
        let mut current_offset = self.offset.clone();

        let mut record_formatters = HashMap::new();
        for rec in &self.records {
            let formatter = rec
                .record
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &formatter_options))
                .collect::<Result<Vec<_>, ArrowError>>()?;
            record_formatters.insert(rec.order, formatter);
        }
        if record_formatters.len() == 0 {
            return Err(anyhow!(
                "unable to create formatters for arrow record data types"
            ));
        }

        for idx in 0..self.max_rows_to_display {
            let rec = if let Some(rec) = self.get_record(current_offset.0) {
                rec
            } else {
                break;
            };
            let formatters = record_formatters
                .get(&current_offset.0)
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
            rows.push(TableRow {
                idx,
                record_idx: current_offset.0,
                row_idx: current_offset.1,
                fields: row,
            });

            if forward {
                if current_offset.1 >= rec.record.num_rows() - 1 {
                    current_offset = (current_offset.0 + 1, 0);
                } else {
                    current_offset = (current_offset.0, current_offset.1 + 1);
                }
            } else {
                if current_offset.1 <= 0 {
                    let prev_rec = self.get_record(current_offset.1 - 1);
                    if let Some(prev_rec) = prev_rec {
                        current_offset = (current_offset.0 - 1, prev_rec.record.num_rows());
                    } else {
                        break;
                    }
                } else {
                    current_offset = (current_offset.0, current_offset.1 - 1);
                }
            }
        }

        // compute the max column widths and heights
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

        let row_heights = rows
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

        Ok((columns, rows, row_heights, max_column_widths))
    }

    fn total_row_height(row_height: u16) -> u16 {
        row_height + 1
    }

    fn total_area_height(area: Rect) -> u16 {
        area.height - 4
    }
}

#[derive(Debug)]
pub struct RecordTable {
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

    fn render_grid(&self, area: Rect, buf: &mut Buffer, state: &mut RecordTableState) {
        for row in &state.rows {
            assert_eq!(state.columns.len(), row.fields.len());
        }

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
            self.build_row_layout(&state.row_widths, &state.row_heights);

        let horizontal_area = horizontal_layout.split(header_area);
        for (col_idx, col_name) in state.columns.iter().enumerate() {
            let cell_area = horizontal_area[col_idx];
            let style = Style::default().fg(self.text_color);
            let para = Paragraph::new(col_name.clone()).style(style);
            para.render(cell_area, buf);
        }

        // render the rows
        let vertical_areas = vertical_layout.split(rows_area);
        for (row_idx, row) in state.rows.iter().enumerate() {
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
        if let Some(err) = &state.err {
            self.render_error(area, buf, err.clone());
            return;
        }

        self.render_grid(area, buf, state);
    }
}
