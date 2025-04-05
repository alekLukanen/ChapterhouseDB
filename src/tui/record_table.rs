use std::{
    cmp::{max, min},
    fmt::Debug,
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
    offset: (u64, u64, u64),
    fields: Vec<String>,
}

#[derive(Clone)]
pub struct TableRecord {
    record: Arc<arrow::array::RecordBatch>,
    offsets: Vec<(u64, u64, u64)>,
}

impl Debug for TableRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableRecord")
            .field("record.num_rows()", &format!("{}", self.record.num_rows()))
            .field("offsets.len()", &format!("{}", self.offsets.len()))
            .finish()
    }
}

#[derive(Debug)]
pub struct RecordTableState {
    area: Option<Rect>,
    record: Option<TableRecord>,
    offsets: Option<Vec<(u64, u64, u64)>>,
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

    err: Option<String>,
}

impl Default for RecordTableState {
    fn default() -> Self {
        RecordTableState {
            area: None,
            record: None,
            offsets: None,
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
    }

    pub fn selected(&self) -> Option<usize> {
        return self.selected;
    }

    pub fn set_record(
        &mut self,
        record: Arc<arrow::array::RecordBatch>,
        offsets: Vec<(u64, u64, u64)>,
    ) {
        let tab_rec = TableRecord { record, offsets };
        self.record = Some(tab_rec);
        self.refresh_view_data();
    }

    pub fn get_max_visible_offset(&self) -> Option<(u64, u64, u64)> {
        let last_row = if let Some(row) = self.rows.last() {
            row
        } else {
            return None;
        };
        Some(last_row.offset)
    }

    pub fn get_min_visible_offset(&self) -> Option<(u64, u64, u64)> {
        let first_row = if let Some(row) = self.rows.first() {
            row
        } else {
            return None;
        };
        Some(first_row.offset)
    }

    pub fn set_alternate_selected(&mut self, val: bool) {
        self.alternate_selected = val;
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
        let (columns, rows, mut row_heights, max_column_widths) = self.get_render_data()?;

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

        let rows = rows[0..stop_index].to_vec();
        row_heights = row_heights[0..stop_index + 1].to_vec();

        self.columns = columns;
        self.rows = rows;
        self.row_heights = row_heights;
        self.row_widths = max_column_widths;

        return Ok(());
    }

    fn get_render_data(&self) -> Result<(Vec<String>, Vec<TableRow>, Vec<u16>, Vec<u16>)> {
        let tab_rec = if let Some(rec) = &self.record {
            rec
        } else {
            return Err(anyhow!("table does not have a record"));
        };
        let columns: Vec<String> = tab_rec
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

        let record_formatters = tab_rec
            .record
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &formatter_options))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        if record_formatters.len() == 0 {
            return Err(anyhow!(
                "unable to create formatters for arrow record data types"
            ));
        }

        for idx in 0..std::cmp::min(self.max_rows_to_display, tab_rec.record.num_rows()) {
            let mut row = Vec::with_capacity(num_cols);
            for formatter in &record_formatters {
                let value = formatter.value(idx);
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
                offset: tab_rec
                    .offsets
                    .get(idx)
                    .expect("unable to find offset for record row")
                    .clone(),
                fields: row,
            });
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
        if state.record.is_none() {
            self.render_error(area, buf, "no record to display".to_string());
            return;
        }
        if state.rows.len() == 0 {
            self.render_error(
                area,
                buf,
                format!("no rendered row data to display. record={:?}", state.record),
            );
            return;
        }
        if let Some(err) = &state.err {
            self.render_error(area, buf, err.clone());
            return;
        }

        self.render_grid(area, buf, state);
    }
}
