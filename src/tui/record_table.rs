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
    widgets::{List, ListItem, Paragraph, StatefulWidget, Widget, Wrap},
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
    top_offset: Option<(u64, u64, u64)>,
    columns: Vec<String>,
    rows: Vec<TableRow>,
    row_heights: Vec<u16>,
    row_widths: Vec<u16>,

    errs: Vec<String>,
    show_all_errs: bool,
}

impl Default for RecordTableState {
    fn default() -> Self {
        RecordTableState {
            area: None,
            record: None,
            offsets: None,
            selected: None,
            alternate_selected: true,
            max_rows_to_display: 50,
            desired_rows_to_buffer: 100,
            max_text_chars: 75,
            max_column_width: 50,
            top_offset: None,
            columns: Vec::new(),
            rows: Vec::new(),
            row_heights: Vec::new(),
            row_widths: Vec::new(),
            errs: Vec::new(),
            show_all_errs: false,
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
            let top_offset = if let Some(offset) = self.top_offset {
                offset
            } else {
                self.add_error("doesn't have a top offset".to_string());
                return;
            };
            self.add_error(format!("has offset: {:?}", top_offset));
            self.refresh_view_data(top_offset, true);
        }
    }

    pub fn select(&mut self, val: Option<usize>) {
        self.selected = val;
    }

    pub fn selected(&self) -> Option<usize> {
        return self.selected;
    }

    pub fn set_show_all_errs(&mut self, show: bool) {
        self.show_all_errs = show;
    }

    pub fn record_less_than_max_size(&mut self) -> Option<bool> {
        self.add_error(format!("record : {:?}", self.record));
        if let Some(rec) = &self.record {
            Some(rec.record.num_rows() < self.max_rows_to_display)
        } else {
            None
        }
    }

    pub fn set_record(
        &mut self,
        record: Arc<arrow::array::RecordBatch>,
        offsets: Vec<(u64, u64, u64)>,
        first_offset: (u64, u64, u64),
        render_forward: bool,
    ) {
        let tab_rec = TableRecord { record, offsets };
        self.record = Some(tab_rec);
        // this must be the first record so set the top offset
        // so that when the area is added the data will render
        if first_offset == (0, 0, 0) && render_forward {
            self.top_offset = Some(first_offset);
        }
        self.refresh_view_data(first_offset, render_forward);
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

    fn add_error(&mut self, err: String) {
        self.errs.push(err);
    }

    fn refresh_view_data(&mut self, first_offset: (u64, u64, u64), render_forward: bool) {
        let area = if let Some(area) = self.area {
            area
        } else {
            return;
        };

        let res = self.set_columns_and_rows(area, first_offset, render_forward);
        match res {
            Ok(_) => {
                self.errs.clear();
            }
            Err(err) => {
                self.add_error(err.to_string());
            }
        }
    }

    fn set_columns_and_rows(
        &mut self,
        area: Rect,
        first_offset: (u64, u64, u64),
        render_forward: bool,
    ) -> Result<()> {
        let (columns, mut rows, mut row_heights, max_column_widths) = self.get_render_data()?;

        // find the row representing the first offset
        let first_row_idx = if render_forward {
            0
        } else {
            let row = rows
                .iter()
                .enumerate()
                .find(|(_, row)| row.offset == first_offset);
            if let Some((idx, _)) = row {
                idx
            } else {
                0
            }
        };

        if render_forward {
            // only show enough rows that will fit on the screen
            let mut stop_index: usize = first_row_idx;
            let mut total_height: u16 = 0;
            for (idx, row_height) in row_heights
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx >= first_row_idx)
            {
                if total_height + Self::total_row_height(*row_height)
                    > Self::total_area_height(area)
                {
                    break;
                }
                total_height += *row_height + 1;
                stop_index = idx;
            }

            rows = rows[0..stop_index + 1].to_vec();
            row_heights = row_heights[0..stop_index + 1].to_vec();
        } else {
            let mut stop_index_top: usize = 0;
            let mut stop_index_bottom: usize = first_row_idx;
            let mut total_height: u16 = 0;

            // scan backward to top
            for (idx, row_height) in row_heights
                .iter()
                .enumerate()
                .rev()
                .filter(|(idx, _)| *idx <= first_row_idx)
            {
                if total_height + Self::total_row_height(*row_height)
                    > Self::total_area_height(area)
                {
                    break;
                }
                total_height += *row_height + 1;
                stop_index_top = idx;
            }

            // scan forwards to bottom
            for (idx, row_height) in row_heights
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx > first_row_idx)
            {
                if total_height + Self::total_row_height(*row_height)
                    > Self::total_area_height(area)
                {
                    break;
                }
                total_height += *row_height + 1;
                stop_index_bottom = idx;
            }

            rows = rows[stop_index_top..stop_index_bottom + 1].to_vec();
            row_heights = row_heights[stop_index_top..stop_index_bottom + 1].to_vec();
        }

        // persist computation
        self.top_offset = if let Some(row) = rows.first() {
            Some(row.offset)
        } else {
            self.add_error("missing first row for self.top_offset".to_string());
            None
        };
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

        for idx in 0..tab_rec.record.num_rows() {
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

    fn render_errors(&self, area: Rect, buf: &mut Buffer, msgs: Vec<String>) {
        let items: Vec<ListItem> = msgs.into_iter().map(ListItem::new).collect();
        Widget::render(&List::new(items), area, buf);
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
        let mut errs = state.errs.clone();

        if state.record.is_none() {
            errs.push("no record to display".to_string());
        }
        if state.rows.len() == 0 {
            errs.push(format!(
                "no rendered row data to display. record={:?}",
                state.record
            ));
        }
        if (state.errs.len() > 0 && state.show_all_errs) || errs.len() > state.errs.len() {
            self.render_errors(area, buf, state.errs.clone());
            return;
        }

        self.render_grid(area, buf, state);
    }
}
