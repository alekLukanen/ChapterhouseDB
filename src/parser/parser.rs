use anyhow::Result;
use thiserror::Error;

use crate::ast::ast::{SelectStatement, Statement, TableExpression};
use crate::lexer::lex;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("empty query string")]
    EmptyQueryString,
    #[error("invalid next token: {0:?}")]
    InvalidToken(lex::Token),
    #[error("no more tokens")]
    NoMoreTokens,
    #[error("unable to claim lock: {0}")]
    UnableToClaimLock(String),
}

struct Parser {
    tokens: Vec<lex::Token>,
    token_index: usize,
    next_token: Option<lex::Token>, // no need for Mutex
}

impl Parser {
    pub fn new(query: String) -> Parser {
        Parser {
            tokens: lex::lex(query),
            token_index: 0,
            next_token: None,
        }
    }

    fn read_next_token(&mut self) -> bool {
        if self.token_index == 0 && self.next_token.is_none() {
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

    fn next_token(&mut self) -> Result<lex::Token, ParseError> {
        if let Some(nt) = self.next_token.clone() {
            Ok(nt)
        } else {
            Err(ParseError::NoMoreTokens)
        }
    }

    pub fn parse(&mut self) -> Result<Statement, ParseError> {
        if !self.read_next_token() {
            return Err(ParseError::EmptyQueryString);
        }

        let next_token = self.next_token()?;
        if next_token == lex::Token::Select {
            self.match_select()
        } else {
            Err(ParseError::InvalidToken(next_token.clone()))
        }
    }

    pub fn match_select(&mut self) -> Result<Statement, ParseError> {
        Ok(Statement::Select(SelectStatement {
            select_expressions: vec![],
            from: TableExpression {},
        }))
    }
}
