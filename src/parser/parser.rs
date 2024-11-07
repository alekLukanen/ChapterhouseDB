use anyhow::Result;

use crate::ast::ast::Statement;
use crate::lexer::lex;

struct Parser {
    tokens: Vec<lex::Token>,
    token_index: usize,
    next_token: Option<lex::Token>,
}

impl Parser {
    pub fn new(query: String) -> Parser {
        let mut parsi = Parser {
            tokens: lex::lex(query),
            token_index: 0,
            next_token: None,
        };
        parsi.read_next_token();
        parsi
    }

    pub fn read_next_token(&mut self) -> bool {
        if self.token_index == 0 && self.next_token == None {
            if self.tokens.len() > 0 {
                self.next_token = Some(self.tokens[0].clone());
                return true;
            } else {
                return false;
            }
        }
        if self.token_index < self.tokens.len() {
            self.token_index += 1;
            self.next_token = Some(self.tokens[self.token_index].clone());
            true
        } else {
            false
        }
    }

    pub fn parse(&mut self) -> Result<Statement> {}
}
