use std::fs;

use anyhow::Result;

use chapterhouseqe::handlers::query_handler::Status;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    style::{palette::tailwind, Color, Modifier, Style, Stylize},
    symbols::border,
    text::{Line, Span, Text},
    widgets::{
        Block, Cell, Gauge, HighlightSpacing, Paragraph, Row, Table, TableState, Widget, Wrap,
    },
    DefaultTerminal, Frame,
};
use regex::Regex;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The logging level (debug, info, warning, error)
    #[arg(short, long)]
    sql_file: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut terminal = ratatui::init();
    let app_result = QueriesApp::new(args.sql_file).run(&mut terminal);
    ratatui::restore();
    app_result
}

#[derive(Debug)]
struct TableColors {
    buffer_bg: Color,
    header_bg: Color,
    header_fg: Color,
    row_fg: Color,
    selected_row_style_fg: Color,
    selected_column_style_fg: Color,
    selected_cell_style_fg: Color,
    normal_row_color: Color,
    alt_row_color: Color,
    footer_border_color: Color,
}

impl TableColors {
    const fn new(color: &tailwind::Palette) -> Self {
        Self {
            buffer_bg: tailwind::SLATE.c950,
            header_bg: color.c900,
            header_fg: tailwind::SLATE.c200,
            row_fg: tailwind::SLATE.c200,
            selected_row_style_fg: color.c400,
            selected_column_style_fg: color.c400,
            selected_cell_style_fg: color.c600,
            normal_row_color: tailwind::SLATE.c950,
            alt_row_color: tailwind::SLATE.c900,
            footer_border_color: color.c400,
        }
    }
}

#[derive(Debug)]
struct QueryInfo {
    query_txt: String,
    status: Option<Status>,
}

impl QueryInfo {
    fn terminal(&self) -> bool {
        if let Some(status) = &self.status {
            status.terminal()
        } else {
            false
        }
    }
    fn completed(&self) -> bool {
        match self.status {
            Some(Status::Complete) => true,
            _ => false,
        }
    }
    fn errored(&self) -> bool {
        match self.status {
            Some(Status::Error(_)) => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct QueriesApp {
    sql_file: String,
    queries: Option<Vec<QueryInfo>>,
    exit: bool,

    table_state: TableState,
    table_colors: TableColors,
}

impl QueriesApp {
    fn new(sql_file: String) -> QueriesApp {
        QueriesApp {
            sql_file,
            queries: None,
            exit: false,
            table_state: TableState::default().with_selected(0),
            table_colors: TableColors::new(&tailwind::INDIGO),
        }
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        // draw the initial ui
        terminal.draw(|frame| self.draw(frame))?;

        // break up the sql file text in statements
        let sql_data = fs::read_to_string(self.sql_file.clone())?;
        let queries = parse_sql_queries(sql_data)?;
        self.queries = Some(
            queries
                .iter()
                .map(|item| QueryInfo {
                    query_txt: item.clone(),
                    status: None,
                })
                .collect(),
        );

        while !self.exit {
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events()?;
        }

        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame) {
        let title = Line::from(vec![Span::styled(
            " Executing Queries ",
            ratatui::style::Style::default().bold().cyan(),
        )]);
        let instructions = Line::from(vec![" Quit ".into(), "<Q> ".blue().bold()]);
        let block = Block::default()
            .title(title.centered())
            .title_bottom(instructions.centered());

        let view_layout =
            Layout::horizontal([Constraint::Percentage(20), Constraint::Percentage(80)]);
        let [left_area, right_area] = view_layout.areas(block.inner(frame.area()));

        let left_layout = Layout::vertical([Constraint::Length(10), Constraint::Fill(1)]);
        let [left_info_area, left_queries_area] = left_layout.areas(left_area);

        self.render_info_area(left_info_area, frame);
        self.render_queries_area(left_queries_area, frame);
        self.render_table_area(right_area, frame);

        frame.render_widget(block, frame.area())
    }

    fn handle_events(&mut self) -> Result<()> {
        match event::read()? {
            // it's important to check that the event is a key press event as
            // crossterm also emits key release and repeat events on Windows.
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                self.handle_key_event(key_event)
            }
            _ => {}
        };
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            _ => {}
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }

    fn render_info_area(&self, area: Rect, frame: &mut Frame) {
        let info_block = Block::bordered()
            .title(" Execution Info ")
            .border_set(border::ROUNDED)
            .border_style(Style::default().cyan());

        if let Some(queries) = &self.queries {
            let queries_completed = queries.iter().filter(|item| item.completed()).count();
            let queries_errored = queries.iter().filter(|item| item.errored()).count();
            let percent_complete = if queries.len() > 0 {
                (queries_completed as f32 / queries.len() as f32) as u16 + 29
            } else {
                0u16
            };

            let total_queries_items = vec![
                Line::from(vec!["File: ".cyan(), format!("{}", self.sql_file).blue()]),
                Line::from(vec![
                    "Total Queries: ".cyan(),
                    format!("{}", queries.len()).blue(),
                ]),
                Line::from(vec![
                    "Completed: ".cyan(),
                    format!("{}", queries_completed).blue(),
                ]),
                Line::from(vec![
                    "Errored: ".cyan(),
                    format!("{}", queries_errored).blue(),
                ]),
            ];
            let query_lines = total_queries_items.len() as u16;

            let total_queries_txt = Paragraph::new(total_queries_items).wrap(Wrap::default());

            let progress_txt = Paragraph::new(Line::from("Progress:".cyan()));
            let progress_bar = Gauge::default()
                .gauge_style(Style::default().blue())
                .percent(percent_complete);

            let info_block_layout = Layout::vertical([
                Constraint::Length(query_lines),
                Constraint::Length(1),
                Constraint::Fill(1),
            ]);
            let [queries_txt_area, progress_txt_area, progress_bar_area] =
                info_block_layout.areas(info_block.inner(area));

            frame.render_widget(total_queries_txt, queries_txt_area);
            frame.render_widget(progress_txt, progress_txt_area);
            frame.render_widget(progress_bar, progress_bar_area);

            frame.render_widget(info_block, area)
        }
    }

    fn render_table_area(&self, area: Rect, frame: &mut Frame) {
        let table_block = Block::bordered()
            .title(" Query Data ")
            .border_set(border::ROUNDED)
            .border_style(Style::default().cyan());

        frame.render_widget(table_block, area);
    }

    fn render_queries_area(&mut self, area: Rect, frame: &mut Frame) {
        let queries_block = Block::bordered()
            .title(" Queries ")
            .border_set(border::ROUNDED)
            .border_style(Style::default().cyan());

        let header_style = Style::default()
            .fg(self.table_colors.header_fg)
            .bg(self.table_colors.header_bg);
        let selected_row_style = Style::default()
            .add_modifier(Modifier::REVERSED)
            .fg(self.table_colors.selected_row_style_fg);
        let selected_col_style = Style::default().fg(self.table_colors.selected_column_style_fg);
        let selected_cell_style = Style::default()
            .add_modifier(Modifier::REVERSED)
            .fg(self.table_colors.selected_cell_style_fg);

        let header = Row::new(vec![
            Cell::from(Text::from("Query".to_string())),
            Cell::from(Text::from("Status".to_string())),
        ]);

        let queries = if let Some(queries) = &self.queries {
            queries
        } else {
            &vec![]
        };

        let rows = queries.iter().enumerate().map(|(i, data)| {
            let color = match i % 2 {
                0 => self.table_colors.normal_row_color,
                _ => self.table_colors.alt_row_color,
            };

            Row::new(vec![
                Cell::from(Text::from(data.query_txt.to_string())),
                Cell::from(Text::from(format!("Done: {}", data.completed()))),
            ])
            .style(Style::new().fg(self.table_colors.row_fg).bg(color))
            .height(4)
        });

        let bar = " █ ";

        let t = Table::new(
            rows,
            [
                // + 1 is for padding.
                Constraint::Fill(8),
                Constraint::Fill(2),
            ],
        )
        .header(header)
        .highlight_style(selected_row_style)
        .highlight_symbol(Text::from(vec![
            "".into(),
            bar.into(),
            bar.into(),
            "".into(),
        ]))
        .bg(self.table_colors.buffer_bg)
        .highlight_spacing(HighlightSpacing::Always);

        frame.render_stateful_widget(t, area, &mut self.table_state);
        frame.render_widget(queries_block, area);
    }

    pub fn next_row(&mut self) {
        let size = if let Some(queries) = &self.queries {
            queries.len()
        } else {
            0
        };
        let i = match self.table_state.selected() {
            Some(i) => {
                if i >= size - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.table_state.select(Some(i));
    }

    pub fn previous_row(&mut self) {
        let size = if let Some(queries) = &self.queries {
            queries.len()
        } else {
            0
        };

        let i = match self.table_state.selected() {
            Some(i) => {
                if i == 0 {
                    size - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.table_state.select(Some(i));
    }
}

fn parse_sql_queries(val: String) -> Result<Vec<String>> {
    let re = Regex::new(r#"(?s)(?:".*?"|'.*?'|[^'";])*?;"#)?;
    Ok(re
        .find_iter(&val)
        .map(|m| m.as_str().to_string())
        .map(|item| item.trim().to_string())
        .collect())
}
