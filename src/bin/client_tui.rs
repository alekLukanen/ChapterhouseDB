use std::{
    fs,
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::{anyhow, Context, Result};

use chapterhouseqe::{
    client::{AsyncQueryClient, QueryDataIterator},
    handlers::{message_handler::messages, query_handler::Status},
    tui::{RecordTable, RecordTableState, TableRecord},
};
use chrono::Duration;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    symbols::border,
    text::{Line, Span, Text},
    widgets::{Block, Cell, Gauge, HighlightSpacing, Paragraph, Row, Table, TableState, Wrap},
    DefaultTerminal, Frame,
};
use regex::Regex;

use clap::Parser;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The port used to accept incoming connections
    #[arg(short, long, default_value_t = 7000)]
    port: u32,

    /// Addresses to connect with
    #[arg(short, long)]
    connect_to_address: String,

    /// The logging level (debug, info, warning, error)
    #[arg(short, long)]
    sql_file: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let address = format!("{}:{}", args.connect_to_address, args.port);

    let mut terminal = ratatui::init();

    let client = AsyncQueryClient::new(address);
    let app_result = QueriesApp::new(args.sql_file, client)
        .run(&mut terminal)
        .await;
    ratatui::restore();

    app_result
}

#[derive(Debug)]
enum AppEvent {
    TerminalEvent(event::Event),
    Error(String),
    DataUpdate,
}

#[derive(Debug)]
struct TableColors {
    header_fg: Color,
    row_fg: Color,
    selected_column_style_fg: Color,
    selected_alternate_column_style_fg: Color,
}

impl TableColors {
    const fn new() -> Self {
        Self {
            header_fg: Color::Cyan,
            row_fg: Color::Cyan,
            selected_column_style_fg: Color::Blue,
            selected_alternate_column_style_fg: Color::Gray,
        }
    }
}

#[derive(Debug)]
struct QueryInfo {
    query_txt: String,
    status: Option<Status>,
    control_err: Option<String>,
    query_data_iter: Option<QueryDataIterator>,
    record_table_state: RecordTableState,
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
    fn status_icon(&self) -> &str {
        match self.status {
            Some(Status::Complete) => "✅",
            Some(Status::Error(_)) => "❌",
            Some(Status::Running) => "🔄",
            Some(Status::Queued) => "🕒",
            _ => "🕒",
        }
    }
}

#[derive(Debug)]
pub struct QueriesAppState {
    queries: Option<Vec<QueryInfo>>,
}

impl QueriesAppState {
    fn get_queries_size(&self) -> usize {
        if let Some(queries) = &self.queries {
            queries.len()
        } else {
            0
        }
    }

    fn get_query_sql(&self, idx: usize) -> String {
        if let Some(queries) = &self.queries {
            queries
                .get(idx)
                .expect("query should exist")
                .query_txt
                .clone()
        } else {
            panic!("queries vec isn't set")
        }
    }

    fn update_query_status(&mut self, query_idx: usize, status: Status) -> Result<()> {
        if let Some(queries) = &mut self.queries {
            if let Some(query) = queries.get_mut(query_idx) {
                query.status = Some(status);
                return Ok(());
            } else {
                return Err(anyhow!("query at index {} does not exist", query_idx));
            }
        } else {
            panic!("queries vec isn't set");
        }
    }

    fn update_control_err(&mut self, query_idx: usize, err: String) -> Result<()> {
        if let Some(queries) = &mut self.queries {
            if let Some(query) = queries.get_mut(query_idx) {
                query.control_err = Some(err);
                return Ok(());
            } else {
                return Err(anyhow!("query at index {} does not exist", query_idx));
            }
        } else {
            panic!("queries vec isn't set");
        }
    }

    fn update_query_data_iter(&mut self, query_idx: usize, iter: QueryDataIterator) -> Result<()> {
        if let Some(queries) = &mut self.queries {
            if let Some(query) = queries.get_mut(query_idx) {
                query.query_data_iter = Some(iter);
                return Ok(());
            } else {
                return Err(anyhow!("query at index {} does not exist", query_idx));
            }
        } else {
            panic!("queries vec isn't set");
        }
    }

    fn add_record(
        &mut self,
        query_idx: usize,
        order: usize,
        rec: Arc<arrow::array::RecordBatch>,
    ) -> Result<()> {
        if let Some(queries) = &mut self.queries {
            if let Some(query) = queries.get_mut(query_idx) {
                query.record_table_state.add_record(order, rec);
                return Ok(());
            } else {
                return Err(anyhow!("query at index {} does not exist", query_idx));
            }
        } else {
            panic!("queries vec isn't set");
        }
    }

    async fn execute_queries(
        ct: CancellationToken,
        sender: mpsc::Sender<AppEvent>,
        state: Arc<Mutex<QueriesAppState>>,
        client: Arc<AsyncQueryClient>,
    ) -> Result<()> {
        let queries_len = state
            .lock()
            .map_err(|_| anyhow!("lock failed"))?
            .get_queries_size();

        for idx in 0..queries_len {
            let res = QueriesAppState::execute_query(
                ct.clone(),
                sender.clone(),
                state.clone(),
                client.clone(),
                idx,
            )
            .await;

            let _ = sender.send(AppEvent::DataUpdate).await;
            if let Err(err) = res {
                state
                    .lock()
                    .map_err(|_| anyhow!("lock failed"))?
                    .update_control_err(idx, err.to_string())?;
            }
        }

        Ok(())
    }

    async fn execute_query(
        ct: CancellationToken,
        sender: mpsc::Sender<AppEvent>,
        state: Arc<Mutex<QueriesAppState>>,
        client: Arc<AsyncQueryClient>,
        query_idx: usize,
    ) -> Result<()> {
        let query = state
            .lock()
            .map_err(|_| anyhow!("lock failed"))?
            .get_query_sql(query_idx.clone());

        let run_query_resp = client
            .run_query(ct.clone(), query.to_string())
            .await
            .context("failed initiating a query run")?;

        let query_id = match run_query_resp {
            messages::query::RunQueryResp::Created { query_id } => query_id,
            messages::query::RunQueryResp::NotCreated => {
                return Ok(());
            }
        };

        let query_status = client
            .wait_for_query_to_finish(
                ct.clone(),
                &query_id,
                chrono::Duration::seconds(60),
                chrono::Duration::milliseconds(100),
            )
            .await?;

        match &query_status {
            messages::query::GetQueryStatusResp::Status(status) => {
                state
                    .lock()
                    .map_err(|_| anyhow!("lock failed"))?
                    .update_query_status(query_idx.clone(), status.clone())?;
            }
            messages::query::GetQueryStatusResp::QueryNotFound => {
                state
                    .lock()
                    .map_err(|_| anyhow!("lock failed"))?
                    .update_control_err(query_idx.clone(), format!("{:?}", query_status))?;
            }
        }

        let _ = sender.send(AppEvent::DataUpdate).await;

        // now create the data iterator and fetch the next record
        let mut query_data_iter =
            QueryDataIterator::new(client, query_id, 0, 0, chrono::Duration::seconds(1));

        let rec = query_data_iter.next(ct.clone()).await?;
        match rec {
            Some(rec) => state
                .lock()
                .map_err(|_| anyhow!("lock failed"))?
                .add_record(query_idx.clone(), 0, rec)?,
            None => {}
        }

        state
            .lock()
            .map_err(|_| anyhow!("lock failed"))?
            .update_query_data_iter(query_idx, query_data_iter)?;

        let _ = sender.send(AppEvent::DataUpdate).await;
        Ok(())
    }
}

#[derive(Debug)]
enum SelectedTable {
    QueryTable,
    DataTable,
}

#[derive(Debug)]
pub struct QueriesApp {
    sql_file: String,
    state: Arc<Mutex<QueriesAppState>>,
    exit: bool,

    sender: mpsc::Sender<AppEvent>,
    receiver: mpsc::Receiver<AppEvent>,

    ct: CancellationToken,
    client: Arc<AsyncQueryClient>,

    table_state: TableState,
    table_colors: TableColors,

    selected_table: SelectedTable,
}

impl QueriesApp {
    fn new(sql_file: String, client: AsyncQueryClient) -> QueriesApp {
        let (sender, receiver) = mpsc::channel::<AppEvent>(1000);
        QueriesApp {
            sql_file,
            state: Arc::new(Mutex::new(QueriesAppState { queries: None })),
            exit: false,
            sender,
            receiver,
            ct: CancellationToken::new(),
            client: Arc::new(client),
            table_state: TableState::default().with_selected(0),
            table_colors: TableColors::new(),
            selected_table: SelectedTable::QueryTable,
        }
    }

    pub async fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        // draw the initial ui
        terminal.draw(|frame| self.draw(frame))?;

        // break up the sql file text in statements
        let sql_data = fs::read_to_string(self.sql_file.clone())?;
        let queries = parse_sql_queries(sql_data)?;

        if queries.len() > 0 {
            self.table_state.select(Some(0));
        }

        self.state
            .lock()
            .map_err(|_| anyhow!("lock failed"))?
            .queries = Some(
            queries
                .iter()
                .map(|item| QueryInfo {
                    query_txt: item.clone(),
                    status: None,
                    control_err: None,
                    record_table_state: RecordTableState::default(),
                    query_data_iter: None,
                })
                .collect(),
        );

        // distribute the queries
        let task_ct = self.ct.clone();
        let task_state = self.state.clone();
        let task_client = self.client.clone();
        let task_sender = self.sender.clone();
        tokio::spawn(async move {
            let res = QueriesAppState::execute_queries(
                task_ct,
                task_sender.clone(),
                task_state,
                task_client,
            )
            .await;
            if let Err(err) = res {
                panic!("error: {}", err);
            }
            let _ = task_sender.send(AppEvent::DataUpdate).await;
        });

        // send key events to channel
        let task_ct = self.ct.clone();
        let task_sender = self.sender.clone();
        tokio::spawn(async move {
            if let Err(err) = QueriesApp::handle_key_events(task_ct, task_sender).await {
                panic!("error: {}", err);
            }
        });

        while !self.exit {
            terminal.draw(|frame| self.draw(frame))?;
            self.handle_events().await?;
        }

        Ok(())
    }

    async fn handle_key_events(
        ct: CancellationToken,
        sender: mpsc::Sender<AppEvent>,
    ) -> Result<()> {
        loop {
            if ct.is_cancelled() {
                return Ok(());
            }
            if event::poll(Duration::milliseconds(1000 / 60).to_std()?)? {
                let event = event::read()?;
                sender.send(AppEvent::TerminalEvent(event)).await?;
            }
        }
    }

    fn draw(&mut self, frame: &mut Frame) {
        let title = Line::from(vec![Span::styled(
            " Executing Queries ",
            ratatui::style::Style::default().bold().cyan(),
        )]);
        let instructions = Line::from(vec![
            " Quit ".into(),
            "<Q> ".blue().bold(),
            "| Switch Table ".into(),
            "<Tab> ".blue().bold(),
        ]);
        let block = Block::default()
            .title(title.centered())
            .title_bottom(instructions.centered());

        let view_layout =
            Layout::horizontal([Constraint::Percentage(30), Constraint::Percentage(70)]);
        let [left_area, right_area] = view_layout.areas(block.inner(frame.area()));

        let left_layout = Layout::vertical([Constraint::Length(10), Constraint::Fill(1)]);
        let [left_info_area, left_queries_area] = left_layout.areas(left_area);

        if let Err(err) = self.render_info_area(left_info_area, frame) {
            panic!("error: {}", err);
        }
        if let Err(err) = self.render_queries_area(left_queries_area, frame) {
            panic!("error: {}", err);
        }
        if let Err(err) = self.render_table_area(right_area, frame) {
            panic!("error: {}", err);
        }

        frame.render_widget(block, frame.area())
    }

    async fn handle_events(&mut self) -> Result<()> {
        match self.receiver.recv().await {
            Some(AppEvent::DataUpdate) => {}
            Some(AppEvent::TerminalEvent(event)) => match event {
                Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                    self.handle_key_event(key_event)?;
                }
                _ => {}
            },
            Some(AppEvent::Error(_)) => {}
            None => {}
        }
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) -> Result<()> {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            KeyCode::Up => match self.selected_table {
                SelectedTable::QueryTable => self.previous_row()?,
                SelectedTable::DataTable => self.previous_data_row()?,
            },
            KeyCode::Down => match self.selected_table {
                SelectedTable::QueryTable => self.next_row()?,
                SelectedTable::DataTable => self.next_data_row()?,
            },
            KeyCode::Tab => self.switch_table()?,
            _ => {}
        }
        Ok(())
    }

    fn next_data_row(&mut self) -> Result<()> {
        Ok(())
    }

    fn previous_data_row(&mut self) -> Result<()> {
        Ok(())
    }

    fn switch_table(&mut self) -> Result<()> {
        if let Some(selected_query) = self.table_state.selected() {
            let mut state_guard = self.state.lock().map_err(|_| anyhow!("lock failed"))?;

            if let Some(queries) = &mut state_guard.queries {
                if let Some(query_info) = queries.get_mut(selected_query) {
                    let rec_table_state = &mut query_info.record_table_state;
                    match self.selected_table {
                        SelectedTable::QueryTable => {
                            rec_table_state.set_alternate_selected(false);
                        }
                        SelectedTable::DataTable => {
                            rec_table_state.set_alternate_selected(true);
                        }
                    }
                }
            }
        }

        match self.selected_table {
            SelectedTable::QueryTable => {
                self.selected_table = SelectedTable::DataTable;
            }
            SelectedTable::DataTable => {
                self.selected_table = SelectedTable::QueryTable;
            }
        }

        Ok(())
    }

    fn exit(&mut self) {
        self.exit = true;
        self.ct.cancel();
    }

    fn render_info_area(&self, area: Rect, frame: &mut Frame) -> Result<()> {
        let info_block = Block::bordered()
            .title(" Execution Info ")
            .border_set(border::ROUNDED)
            .border_style(Style::default().cyan());

        if let Some(queries) = &self
            .state
            .lock()
            .map_err(|_| anyhow!("lock failed"))?
            .queries
        {
            let queries_completed = queries.iter().filter(|item| item.completed()).count();
            let queries_errored = queries.iter().filter(|item| item.errored()).count();
            let percent_complete = if queries.len() > 0 {
                100 * (queries_completed as f32 / queries.len() as f32) as u16
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
        Ok(())
    }

    fn render_table_area(&mut self, area: Rect, frame: &mut Frame) -> Result<()> {
        let table_block = Block::bordered()
            .title(Line::from(vec![
                Span::raw(" ✨ "),
                Span::from("Query Data "),
            ]))
            .border_set(border::ROUNDED)
            .border_style(Style::default().fg(Color::Cyan));

        if let Some(selected_query) = self.table_state.selected() {
            let mut state_guard = self.state.lock().map_err(|_| anyhow!("lock failed"))?;

            if let Some(queries) = &mut state_guard.queries {
                if let Some(query_info) = queries.get_mut(selected_query) {
                    let rec_table = RecordTable::default();
                    let rec_table_state = &mut query_info.record_table_state;
                    rec_table_state.set_area(area);
                    frame.render_stateful_widget(
                        rec_table,
                        table_block.inner(area),
                        rec_table_state,
                    );
                }
            }
        }

        frame.render_widget(table_block, area);
        Ok(())
    }

    fn render_queries_area(&mut self, area: Rect, frame: &mut Frame) -> Result<()> {
        let queries_block = Block::bordered()
            .title(" Queries ")
            .border_set(border::ROUNDED)
            .border_style(Style::default().cyan());

        let header_style = Style::default().fg(self.table_colors.header_fg);

        let mut selected_row_style = Style::default().add_modifier(Modifier::REVERSED);
        match self.selected_table {
            SelectedTable::DataTable => {
                selected_row_style =
                    selected_row_style.fg(self.table_colors.selected_alternate_column_style_fg);
            }
            SelectedTable::QueryTable => {
                selected_row_style =
                    selected_row_style.fg(self.table_colors.selected_column_style_fg);
            }
        }

        let header = Row::new(vec![
            Cell::default(),
            Cell::from(Text::from("Idx".to_string())),
            Cell::from(Text::from("Query".to_string())),
        ])
        .style(header_style)
        .height(1);

        let rows = if let Some(queries) = &self
            .state
            .lock()
            .map_err(|_| anyhow!("lock failed"))?
            .queries
        {
            queries
                .iter()
                .enumerate()
                .map(|(i, data)| {
                    Row::new(vec![
                        Cell::from(
                            Span::from(format!("{}", data.status_icon()))
                                .style(Style::default().fg(Color::Green)),
                        ),
                        Cell::from(
                            Text::from(format!("{}, {}", i, data.record_table_state.num_records()))
                                .fg(self.table_colors.row_fg),
                        ),
                        Cell::from(
                            Text::from(data.query_txt.to_string()).fg(self.table_colors.row_fg),
                        ),
                    ])
                    .height(4)
                })
                .collect::<Vec<Row>>()
        } else {
            vec![]
        };

        let bar = "█";

        let table = Table::new(
            rows,
            [
                // + 1 is for padding.
                Constraint::Length(2),
                Constraint::Length(5),
                Constraint::Fill(8),
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
        .highlight_spacing(HighlightSpacing::Always);

        frame.render_stateful_widget(table, queries_block.inner(area), &mut self.table_state);
        frame.render_widget(queries_block, area);

        Ok(())
    }

    pub fn next_row(&mut self) -> Result<()> {
        let size = if let Some(queries) = &self
            .state
            .lock()
            .map_err(|_| anyhow!("lock failed"))?
            .queries
        {
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
        Ok(())
    }

    pub fn previous_row(&mut self) -> Result<()> {
        let size = if let Some(queries) = &self
            .state
            .lock()
            .map_err(|_| anyhow!("lock failed"))?
            .queries
        {
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
        Ok(())
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
