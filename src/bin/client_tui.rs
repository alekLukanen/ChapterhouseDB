use std::fs;

use anyhow::Result;

use chapterhouseqe::handlers::query_handler::Status;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols::border,
    text::{Line, Span},
    widgets::{Block, Gauge, Padding, Paragraph, Widget, Wrap},
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
}

impl QueriesApp {
    fn new(sql_file: String) -> QueriesApp {
        QueriesApp {
            sql_file,
            queries: None,
            exit: false,
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

    fn draw(&self, frame: &mut Frame) {
        frame.render_widget(self, frame.area());
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

    fn render_info_area(&self, area: Rect, buf: &mut Buffer) {
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

            total_queries_txt.render(queries_txt_area, buf);
            progress_txt.render(progress_txt_area, buf);
            progress_bar.render(progress_bar_area, buf);

            info_block.render(area, buf);
        }
    }

    fn render_table_area(&self, area: Rect, buf: &mut Buffer) {
        let table_block = Block::bordered()
            .title(" Query Data ")
            .border_set(border::ROUNDED)
            .border_style(Style::default().cyan());

        table_block.render(area, buf);
    }

    fn render_queries_area(&self, area: Rect, buf: &mut Buffer) {
        let queries_block = Block::bordered()
            .title(" Queries ")
            .border_set(border::ROUNDED)
            .border_style(Style::default().cyan());
        queries_block.render(area, buf);
    }
}

impl Widget for &QueriesApp {
    fn render(self, area: Rect, buf: &mut Buffer) {
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
        let [left_area, right_area] = view_layout.areas(block.inner(area));

        let left_layout = Layout::vertical([Constraint::Length(10), Constraint::Fill(1)]);
        let [left_info_area, left_queries_area] = left_layout.areas(left_area);

        self.render_info_area(left_info_area, buf);
        self.render_queries_area(left_queries_area, buf);
        self.render_table_area(right_area, buf);

        block.render(area, buf);
    }
}

fn parse_sql_queries(val: String) -> Result<Vec<String>> {
    let re = Regex::new(r#"(?s)(?:".*?"|'.*?'|[^'";])*?;"#)?;
    Ok(re.find_iter(&val).map(|m| m.as_str().to_string()).collect())
}
