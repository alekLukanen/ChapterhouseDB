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
}

impl Parser {
    pub fn new(query: String) -> Parser {
        Parser {
            tokens: lex::lex(query),
            token_index: 0,
        }
    }

    fn read_next_token(&mut self) -> bool {
        self.token_index += 1;
        self.token_index < self.tokens.len()
    }

    fn peek_match_token_types(&mut self, expected_tokens: Vec<lex::Token>) -> bool {
        if self.token_index + expected_tokens.len() > self.tokens.len() {
            return false;
        }

        expected_tokens.iter().enumerate().all(|(idx, t)| {
            let token = &self.tokens[self.token_index + idx];
            lex::Token::token_types_match(t.clone(), token.clone())
        })
    }

    fn next_token(&mut self) -> Result<lex::Token, ParseError> {
        if self.token_index < self.tokens.len() {
            Ok(self.tokens[self.token_index].clone())
        } else {
            Err(ParseError::NoMoreTokens)
        }
    }

    fn match_token(&mut self, expected_token: lex::Token) -> Result<(), ParseError> {
        let next_token = self.next_token()?;
        if expected_token == next_token {
            self.read_next_token();
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
                select_expressions.push(SelectExpression::Star);
                self.match_token(lex::Token::Star)?;
            } else if self.peek_match_token_types(vec![
                lex::Token::Identifier("".to_string()),
                lex::Token::Period,
                lex::Token::Star,
            ]) {
                let id_name = match self.next_token()? {
                    lex::Token::Identifier(name) => name,
                    unexpected_token => return Err(ParseError::InvalidToken(unexpected_token)),
                };

                select_expressions.push(SelectExpression::Family {
                    name: id_name.clone(),
                });
                self.match_token(lex::Token::Identifier(id_name.clone()))?;
                self.match_token(lex::Token::Period)?;
                self.match_token(lex::Token::Star)?;
            } else {
                let term = self.match_term()?;
                if self.next_token()? == lex::Token::As {
                    self.match_token(lex::Token::As)?;

                    let id_name = match self.next_token()? {
                        lex::Token::Identifier(name) => name,
                        unexpected_token => return Err(ParseError::InvalidToken(unexpected_token)),
                    };
                    self.match_token(lex::Token::Identifier(id_name.clone()))?;

                    select_expressions.push(SelectExpression::Term {
                        term,
                        alias: Some(id_name.clone()),
                    });
                } else {
                    select_expressions.push(SelectExpression::Term { term, alias: None })
                }
            }

            if self.next_token()? != lex::Token::From {
                self.match_token(lex::Token::Comma)?;
                if self.next_token()? == lex::Token::From {
                    return Err(ParseError::InvalidToken(lex::Token::From));
                }
            }
        }

        self.match_token(lex::Token::From)?;

        Ok(select_expressions)
    }

    fn match_table_expression(&mut self) -> Result<TableExpression, ParseError> {
        Err(ParseError::NoMoreTokens)
    }

    fn match_term(&mut self) -> Result<Term, ParseError> {
        Err(ParseError::NoMoreTokens)
    }
}
