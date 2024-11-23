use anyhow::{Context, Result};
use thiserror::Error;

use crate::ast::ast::{
    Column, Function, Numeric, Operand, SelectExpression, SelectStatement, Statement,
    TableExpression, Term, Value,
};
use crate::lexer::lex;
use crate::lexer::lex::Token;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("empty query string")]
    EmptyQueryString,
    #[error("invalid next token: {0:?}")]
    InvalidToken(Token),
    #[error("invalid next token, expected {0:?} but received {1:?}")]
    InvalidNextToken(Token, Token),
    #[error("no more tokens")]
    NoMoreTokens,
    #[error("unable to parse number: {0}")]
    InvalidNumber(String),
    #[error("operand not implemented: {0:?}")]
    OperandTokenNotImplemented(Token),
    #[error("operand compaction issue: {0}")]
    OperandCompactionIssue(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

#[derive(Debug)]
pub struct Parser {
    tokens: Vec<Token>,
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

        while self.token_index < self.tokens.len() && self.tokens[self.token_index] == Token::Space
        {
            self.token_index += 1;
        }
        self.token_index < self.tokens.len()
    }

    fn peek_match_token_types(&mut self, expected_tokens: Vec<Token>) -> bool {
        if self.token_index + expected_tokens.len() > self.tokens.len() {
            return false;
        }

        expected_tokens.iter().enumerate().all(|(idx, t)| {
            let token = &self.tokens[self.token_index + idx];
            lex::Token::token_types_match(t.clone(), token.clone())
        })
    }

    fn next_token(&mut self) -> Result<Token, ParseError> {
        if self.token_index < self.tokens.len() {
            Ok(self.tokens[self.token_index].clone())
        } else {
            Err(ParseError::NoMoreTokens)
        }
    }

    fn match_token(&mut self, expected_token: Token) -> Result<(), ParseError> {
        self.log(format!("match_token({:?})", expected_token).to_string());
        let next_token = self.next_token()?;
        if expected_token == next_token {
            self.read_next_token();
            Ok(())
        } else {
            Err(ParseError::InvalidNextToken(expected_token, next_token))
        }
    }

    pub fn parse(&mut self) -> Result<Statement> {
        self.log("parse()".to_string());
        if self.tokens.len() == 0 {
            return Err(ParseError::EmptyQueryString.into());
        }
        if self.next_token()? == Token::Space {
            let has_tokens_remaining = self.read_next_token();
            if !has_tokens_remaining {
                return Err(ParseError::NoMoreTokens.into());
            }
        }

        let next_token = self.next_token()?;
        if next_token == Token::Select {
            let select_statement = self.match_select().context("failed to match select")?;
            self.match_token(Token::Semicolon)?;
            Ok(Statement::Select(select_statement))
        } else {
            Err(ParseError::InvalidToken(next_token.clone()).into())
        }
    }

    fn match_select(&mut self) -> Result<SelectStatement> {
        self.log("match_select()".to_string());

        self.match_token(Token::Select)?;

        let select_expressions = self
            .match_select_expressions()
            .context("failed to match select expressions")?;
        let from_expression = self
            .match_table_expression()
            .context("failed to match table expression")?;
        let where_expression = self
            .match_where_expression()
            .context("failed to match where expression")?;

        let select_statement = SelectStatement {
            select_expressions,
            from_expression,
            where_expression,
        };

        Ok(select_statement)
    }

    fn match_select_expressions(&mut self) -> Result<Vec<SelectExpression>> {
        self.log("match_select_expressions()".to_string());

        let mut select_expressions: Vec<SelectExpression> = Vec::new();

        while self.next_token()? != Token::From {
            if self.next_token()? == Token::Star {
                select_expressions.push(SelectExpression::Star);
                self.match_token(Token::Star)?;
            } else if self.peek_match_token_types(vec![
                Token::Identifier("".to_string()),
                Token::Period,
                Token::Star,
            ]) {
                let id_name = match self.next_token()? {
                    Token::Identifier(name) => name,
                    unexpected_token => {
                        return Err(ParseError::InvalidToken(unexpected_token).into())
                    }
                };

                select_expressions.push(SelectExpression::Family {
                    name: id_name.clone(),
                });
                self.match_token(Token::Identifier(id_name.clone()))?;
                self.match_token(Token::Period)?;
                self.match_token(Token::Star)?;
            } else {
                let expression = self.match_expression()?;
                if self.next_token()? == Token::As {
                    self.match_token(Token::As)?;

                    let next_token = self.next_token()?;
                    let id_name = match &next_token {
                        Token::Identifier(name) => name,
                        unexpected_token => {
                            return Err(ParseError::InvalidToken(unexpected_token.clone()).into())
                        }
                    };

                    self.match_token(next_token.clone())?;

                    select_expressions.push(SelectExpression::Expression {
                        expression,
                        alias: Some(id_name.clone()),
                    });
                } else {
                    select_expressions.push(SelectExpression::Expression {
                        expression,
                        alias: None,
                    })
                }
            }

            if self.next_token()? != Token::From {
                self.match_token(Token::Comma)?;
                if self.next_token()? == Token::From {
                    return Err(ParseError::InvalidToken(Token::From).into());
                }
            }
        }

        Ok(select_expressions)
    }

    fn match_table_expression(&mut self) -> Result<TableExpression> {
        self.log("match_table_expression()".to_string());

        self.match_token(Token::From)?;

        if self.next_token()? == Token::LeftParenthesis {
            self.match_token(Token::LeftParenthesis)?;

            let select_statement = self.match_select()?;
            self.match_token(Token::RightParenthesis)?;

            let mut alias: Option<String> = None;
            if self.next_token()? == Token::As {
                self.match_token(Token::As)?;
                let next_token = self.next_token()?;
                let id_name = match next_token.clone() {
                    Token::Identifier(name) => name,
                    unexpected_token => {
                        return Err(ParseError::InvalidToken(unexpected_token).into())
                    }
                };
                alias = Some(id_name);
                self.match_token(next_token)?;
            }

            Ok(TableExpression::Select {
                select_statement: Box::new(select_statement),
                alias,
            })
        } else if let Token::Identifier(_) = self.next_token()? {
            let (schema, table) = self
                .match_table_name()
                .context("failed to match table name")?;
            Ok(TableExpression::Table { schema, table })
        } else {
            Err(
                ParseError::NotImplemented("table expression type not implemented".to_string())
                    .into(),
            )
        }
    }

    fn match_table_name(&mut self) -> Result<(Option<String>, String)> {
        self.log("match_table_name()".to_string());

        let has_schema_and_table = self.peek_match_token_types(vec![
            Token::Identifier("".to_string()),
            Token::Period,
            Token::Identifier("".to_string()),
        ]);

        let id_name1 = match self.next_token()? {
            Token::Identifier(name) => name,
            ut => return Err(ParseError::InvalidToken(ut).into()),
        };

        let next_token = self.next_token()?;
        self.match_token(next_token)?;

        if has_schema_and_table {
            self.match_token(Token::Period)?;
            let next_token = self.next_token()?;
            match next_token.clone() {
                Token::Identifier(id_name2) => {
                    self.match_token(next_token)?;
                    Ok((Some(id_name1), id_name2))
                }
                ut => Err(ParseError::InvalidToken(ut).into()),
            }
        } else {
            Ok((None, id_name1))
        }
    }

    fn match_where_expression(&mut self) -> Result<Option<Term>> {
        self.log("match_where_expression()".to_string());

        if self.next_token()? == Token::Where {
            self.match_token(Token::Where)?;
            Ok(Some(self.match_expression()?))
        } else {
            Ok(None)
        }
    }

    // an expression is a logical statement typically including "AND" and "OR"
    fn match_expression(&mut self) -> Result<Term> {
        let mut operands: Vec<Box<Operand>> = Vec::new();
        let mut operators: Vec<Token> = Vec::new();
        let mut last_was_term = false;

        while self.expression_continues()? {
            let next_token = &self.next_token()?;
            println!("next_token: {:?}", next_token);

            if *next_token == Token::LeftParenthesis {
                self.match_token(next_token.clone())?;
                operators.push(next_token.clone());
                last_was_term = false;
                continue;
            }

            // always match and push a term
            if !last_was_term {
                operands.push(Box::new(Operand::Term(self.match_base_term()?)));
                last_was_term = true;
                continue;
            }

            // handle operator or right parenthesis
            if next_token.clone().is_expression_operator() {
                // continue expression

                let number_of_operators = operators
                    .iter()
                    .rev()
                    .take_while(|&token| *token != Token::LeftParenthesis)
                    .count();
                let number_of_operands = number_of_operators + 1;

                if let Some(last_operator) = operators.last() {
                    if Parser::operator_precedence(next_token)
                        <= Parser::operator_precedence(last_operator)
                        && number_of_operands > number_of_operators
                        && operators.len() > 0
                        && operands.len() >= 2
                    {
                        let op1 = operands.remove(operands.len() - 2);
                        let op2 = operands.remove(operands.len() - 1);
                        let last_operator_popped = operators.remove(operators.len() - 1);
                        let compacted_op =
                            self.apply_operator_to_terms(last_operator_popped, op1, op2)?;
                        operands.push(compacted_op);
                    }
                }

                self.match_token(next_token.clone())?;
                operators.push(next_token.clone());
                last_was_term = false;
            } else if *next_token == Token::RightParenthesis {
                // end of sub-expression
                // compact all operands and operators inside of the parentheses
                self.match_token(next_token.clone())?;

                while operands.len()
                    > operators
                        .iter()
                        .rev()
                        .take_while(|&token| *token != Token::LeftParenthesis)
                        .count()
                    && operators.len() > 0
                    && operands.len() >= 2
                {
                    if let Some(last_operand) = operators.last() {
                        if *last_operand == Token::LeftParenthesis {
                            operators.remove(operators.len() - 1);
                            break;
                        }
                    }
                    let op1 = operands.remove(operands.len() - 2);
                    let op2 = operands.remove(operands.len() - 1);
                    let last_operator_popped = operators.remove(operators.len() - 1);
                    let compacted_op =
                        self.apply_operator_to_terms(last_operator_popped, op1, op2)?;
                    operands.push(compacted_op);
                }
                last_was_term = true;
            } else {
                return Err(ParseError::NotImplemented(format!(
                    "expected an expression operator or right parenthesis but found: {:?}",
                    next_token
                ))
                .into());
            }
        }

        while operators.len() > 0 && operands.len() >= 2 {
            if let Some(last_operand) = operators.last() {
                if *last_operand == Token::LeftParenthesis
                    || *last_operand == Token::RightParenthesis
                {
                    return Err(ParseError::OperandCompactionIssue(format!(
                        "unexpected parenthesis on final compaction: {:?}",
                        *last_operand
                    ))
                    .into());
                }
            }
            let op1 = operands.remove(operands.len() - 2);
            let op2 = operands.remove(operands.len() - 1);
            let last_operator_popped = operators.remove(operators.len() - 1);
            let compacted_op = self.apply_operator_to_terms(last_operator_popped, op1, op2)?;
            operands.push(compacted_op);
        }

        if operands.len() != 1 || operators.len() != 0 {
            return Err(ParseError::OperandCompactionIssue(format!(
                "expected to have 1 operand but have {} operands and {} operators",
                operands.len(),
                operators.len()
            ))
            .into());
        }

        let last_operand = operands.remove(0);
        Ok(Term::Operand(last_operand))
    }

    fn apply_operator_to_terms(
        &mut self,
        token: Token,
        left_operand: Box<Operand>,
        right_operand: Box<Operand>,
    ) -> Result<Box<Operand>> {
        self.log(format!("apply_operator_to_terms()"));
        self.log(format!("- token: {:?}", token));
        self.log(format!("- left_operand: {:?}", left_operand));
        self.log(format!("- right_operand: {:?}", right_operand));

        if !token.is_expression_operator() {
            return Err(ParseError::InvalidToken(token.clone()).into());
        }

        let operand = match token {
            Token::Plus => Operand::Addition(left_operand, right_operand),
            Token::Minus => Operand::Subtraction(left_operand, right_operand),
            Token::Star => Operand::Multiplication(left_operand, right_operand),
            Token::ForwardSlash => Operand::Division(left_operand, right_operand),
            Token::Equal => Operand::Equal(left_operand, right_operand),
            Token::NotEqual => Operand::NotEqual(left_operand, right_operand),
            Token::LessThan => Operand::LessThan(left_operand, right_operand),
            Token::LessThanEqual => Operand::LessThanOrEqual(left_operand, right_operand),
            Token::GreaterThan => Operand::GreaterThan(left_operand, right_operand),
            Token::GreaterThanEqual => Operand::GreaterThanOrEqual(left_operand, right_operand),
            Token::Or => Operand::Or(left_operand, right_operand),
            Token::And => Operand::And(left_operand, right_operand),
            _ => return Err(ParseError::OperandTokenNotImplemented(token.clone()).into()),
        };

        Ok(Box::new(operand))
    }

    fn operator_precedence(token: &Token) -> i8 {
        match token {
            Token::Or => 7,
            Token::And => 8,
            Token::Equal => 9,
            Token::NotEqual => 9,
            Token::LessThan => 9,
            Token::LessThanEqual => 9,
            Token::GreaterThan => 9,
            Token::GreaterThanEqual => 9,
            Token::Plus => 10,
            Token::Minus => 10,
            Token::Star => 11,
            Token::ForwardSlash => 11,
            _ => 0,
        }
    }

    fn match_base_term(&mut self) -> Result<Term> {
        let next_token = self.next_token()?;

        if self.peek_match_token_types(vec![
            Token::Identifier("".to_string()),
            Token::LeftParenthesis,
        ]) {
            let id_name = match self.next_token()? {
                Token::Identifier(name) => name,
                ut => return Err(ParseError::InvalidToken(ut).into()),
            };
            let mut expressions: Vec<Term> = Vec::new();

            self.match_token(next_token)?;
            self.match_token(Token::LeftParenthesis)?;

            if self.next_token()? == Token::RightParenthesis {
                return Ok(Term::Function(Function::UserDefined {
                    name: id_name,
                    terms: vec![],
                }));
            }

            // iterate until the end of the function call
            while self.next_token()? != Token::RightParenthesis {
                let expression = self.match_expression()?;
                expressions.push(expression);
                if self.next_token()? == Token::Comma {
                    self.match_token(Token::Comma)?;
                }
            }

            return Ok(Term::Function(Function::UserDefined {
                name: id_name,
                terms: expressions,
            }));
        }

        match self.next_token()? {
            Token::Identifier(_) => {
                let (schema, name) = self.match_table_name()?;
                Ok(Term::Column(Column::Direct {
                    schema,
                    column_name: name,
                }))
            }
            Token::Number(ref value) => {
                self.match_token(Token::Number(value.clone()))?;
                if let Ok(int_val) = value.parse::<i64>() {
                    Ok(Term::Value(Value::Numeric(Numeric::Int(int_val))))
                } else if let Ok(float_val) = value.parse::<f64>() {
                    Ok(Term::Value(Value::Numeric(Numeric::Float(float_val))))
                } else {
                    Err(ParseError::InvalidNumber(value.clone()).into())
                }
            }
            _ => Err(ParseError::NotImplemented("match_term".to_string()).into()),
        }
    }

    fn expression_continues(&mut self) -> Result<bool> {
        Ok(self.next_token()?.is_expression_operator()
            || self.peek_match_token_types(vec![
                Token::Identifier("".to_string()),
                Token::LeftParenthesis,
            ])
            || self.peek_match_token_types(vec![Token::Identifier("".to_string())])
            || self.peek_match_token_types(vec![Token::Number("".to_string())]))
    }
}
