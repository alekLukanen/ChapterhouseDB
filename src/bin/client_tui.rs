use anyhow::Result;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Style, Stylize},
    symbols::border,
    text::{Line, Span},
    widgets::{Block, Widget},
    DefaultTerminal, Frame,
};

fn main() -> Result<()> {
    let mut terminal = ratatui::init();
    let app_result = QueriesApp::default().run(&mut terminal);
    ratatui::restore();
    app_result
}

#[derive(Debug, Default)]
pub struct QueriesApp {
    exit: bool,
}

impl QueriesApp {
    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
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
}

impl Widget for &QueriesApp {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let title = Line::from(vec![Span::styled(
            " Queries ",
            ratatui::style::Style::default().bold(),
        )]);
        let instructions = Line::from(vec![" Quit ".into(), "<Q> ".blue().bold()]);
        let block = Block::bordered()
            .title(title.centered())
            .title_bottom(instructions.centered())
            .border_set(border::THICK)
            .style(Style::default().cyan());
        block.render(area, buf);
    }
}
