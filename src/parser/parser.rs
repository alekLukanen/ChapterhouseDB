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
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

#[derive(Debug)]
pub struct Parser {
    tokens: Vec<lex::Token>,
    token_index: usize,
    enable_logging: bool,
}

impl Parser {
    pub fn new(query: String, enable_logging: bool) -> Parser {
        Parser {
            tokens: lex::lex(query),
            token_index: 0,
            enable_logging,
        }
    }

    fn log(&mut self, msg: String) {
        if self.enable_logging {
            println!("parser: {}", msg);
        }
    }

    pub fn log_debug(&mut self) {
        println!("tokens: {:?}", self.tokens);
        println!("token_index: {}", self.token_index);
    }

    fn read_next_token(&mut self) -> bool {
        self.token_index += 1;

        while self.token_index < self.tokens.len()
            && self.tokens[self.token_index] == lex::Token::Space
        {
            self.token_index += 1;
        }
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
        self.log(format!("match_token({:?})", expected_token).to_string());
        let next_token = self.next_token()?;
        if expected_token == next_token {
            self.read_next_token();
            Ok(())
        } else {
            Err(ParseError::InvalidNextToken(expected_token, next_token))
        }
    }

    pub fn parse(&mut self) -> Result<Statement, ParseError> {
        self.log("parse()".to_string());
        if self.tokens.len() == 0 {
            return Err(ParseError::EmptyQueryString);
        }
        if self.next_token()? == lex::Token::Space {
            let has_tokens_remaining = self.read_next_token();
            if !has_tokens_remaining {
                return Err(ParseError::NoMoreTokens);
            }
        }

        let next_token = self.next_token()?;
        if next_token == lex::Token::Select {
            let select_statement = self.match_select()?;
            self.match_token(lex::Token::Semicolon)?;
            Ok(Statement::Select(select_statement))
        } else {
            Err(ParseError::InvalidToken(next_token.clone()))
        }
    }

    fn match_select(&mut self) -> Result<SelectStatement, ParseError> {
        self.log("match_select()".to_string());

        self.match_token(lex::Token::Select)?;

        let select_expressions = self.match_select_expressions()?;
        let from_expression = self.match_table_expression()?;

        let select_statement = SelectStatement {
            select_expressions,
            from: from_expression,
        };

        Ok(select_statement)
    }

    fn match_select_expressions(&mut self) -> Result<Vec<SelectExpression>, ParseError> {
        self.log("match_select_expressions()".to_string());

        let mut select_expressions: Vec<SelectExpression> = Vec::new();

        while self.next_token()? != lex::Token::From {
            if self.next_token()? == lex::Token::Star {
                self.log("match_select_expressions() - match star token".to_string());
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

                    let next_token = self.next_token()?;
                    let id_name = match &next_token {
                        lex::Token::Identifier(name) => name,
                        unexpected_token => {
                            return Err(ParseError::InvalidToken(unexpected_token.clone()))
                        }
                    };

                    self.match_token(next_token.clone())?;

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
        self.log("match_table_expression()".to_string());

        if self.next_token()? == lex::Token::LeftParenthesis {
            self.match_token(lex::Token::LeftParenthesis)?;

            let select_statement = self.match_select()?;
            self.match_token(lex::Token::RightParenthesis)?;

            let mut alias: Option<String> = None;
            if self.next_token()? == lex::Token::As {
                self.match_token(lex::Token::As)?;
                let next_token = self.next_token()?;
                let id_name = match next_token.clone() {
                    lex::Token::Identifier(name) => name,
                    unexpected_token => return Err(ParseError::InvalidToken(unexpected_token)),
                };
                alias = Some(id_name);
                self.match_token(next_token)?;
            }

            Ok(TableExpression::Select {
                select_statement: Box::new(select_statement),
                alias,
            })
        } else if let lex::Token::Identifier(_) = self.next_token()? {
            let (schema, table) = self.match_table_name()?;
            Ok(TableExpression::Table { schema, table })
        } else {
            Err(ParseError::NotImplemented(
                "match_table_expression() - else clause".to_string(),
            ))
        }
    }

    fn match_term(&mut self) -> Result<Term, ParseError> {
        Err(ParseError::NotImplemented("match_term".to_string()))
    }

    fn match_table_name(&mut self) -> Result<(Option<String>, String), ParseError> {
        self.log("match_table_name()".to_string());

        let has_schema_and_table = self.peek_match_token_types(vec![
            lex::Token::Identifier("".to_string()),
            lex::Token::Period,
            lex::Token::Identifier("".to_string()),
        ]);

        let id_name1 = match self.next_token()? {
            lex::Token::Identifier(name) => name,
            ut => return Err(ParseError::InvalidToken(ut)),
        };

        let next_token = self.next_token()?;
        self.match_token(next_token)?;

        if has_schema_and_table {
            self.match_token(lex::Token::Period)?;
            let next_token = self.next_token()?;
            match next_token.clone() {
                lex::Token::Identifier(id_name2) => {
                    self.match_token(next_token)?;
                    Ok((Some(id_name1), id_name2))
                }
                ut => Err(ParseError::InvalidToken(ut)),
            }
        } else {
            Ok((None, id_name1))
        }
    }
}
