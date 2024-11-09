use anyhow::Result;
use thiserror::Error;

use crate::ast::ast::{SelectExpression, SelectStatement, Statement, TableExpression, Term};
use crate::lexer::lex;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("empty query string")]
    EmptyQueryString,
    #[error("invalid next token: {0:?}")]
    InvalidToken(lex::Token),
    #[error("invalid next token, expected {0:?} but received {1:?}")]
    InvalidNextToken(lex::Token, lex::Token),
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

    fn peek_match(&mut self, expected_tokens: Vec<lex::Token>) -> bool {
        if self.token_index + expected_tokens.len() > self.tokens.len() {
            return false;
        }

        expected_tokens.iter().enumerate().all(|(idx, t)| {
            let token = &self.tokens[self.token_index + idx];
            match (t, token) {
                // Ignore `Identifier` variants
                (lex::Token::Identifier(_), lex::Token::Identifier(_)) => true,
                _ => t == token, // Compare other tokens
            }
        })
    }

    fn next_token(&mut self) -> Result<lex::Token, ParseError> {
        if let Some(nt) = self.next_token.clone() {
            Ok(nt)
        } else {
            Err(ParseError::NoMoreTokens)
        }
    }

    fn match_token(&mut self, expected_token: lex::Token) -> Result<(), ParseError> {
        let next_token = self.next_token()?;
        if expected_token == next_token {
            Ok(())
        } else {
            Err(ParseError::InvalidNextToken(expected_token, next_token))
        }
    }

    pub fn parse(&mut self) -> Result<Statement, ParseError> {
        if !self.read_next_token() {
            return Err(ParseError::EmptyQueryString);
        }

        let next_token = self.next_token()?;
        if next_token == lex::Token::Select {
            Ok(self.match_select()?)
        } else {
            Err(ParseError::InvalidToken(next_token.clone()))
        }
    }

    fn match_select(&mut self) -> Result<Statement, ParseError> {
        let select_statement = SelectStatement {
            select_expressions: self.match_select_expressions()?,
            from: self.match_table_expression()?,
        };

        Ok(Statement::Select(select_statement))
    }

    fn match_select_expressions(&mut self) -> Result<Vec<SelectExpression>, ParseError> {
        let mut select_expressions: Vec<SelectExpression> = Vec::new();

        while self.next_token()? != lex::Token::From {
            if self.next_token()? == lex::Token::Star {
                self.match_token(lex::Token::Star)?;
            } else if self.peek_match(vec![
                lex::Token::Identifier("".to_string()),
                lex::Token::Period,
                lex::Token::Star,
            ]) {
            }

            if self.next_token()? != lex::Token::From {
                self.match_token(lex::Token::Comma);
            }
        }

        self.match_token(lex::Token::From)?;

        Ok(select_expressions)
    }

    fn match_table_expression(&mut self) -> Result<TableExpression, ParseError> {}

    fn match_term(&mut self) -> Result<Term, ParseError> {}
}
