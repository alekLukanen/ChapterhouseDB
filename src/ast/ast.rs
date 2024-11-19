use serde::Serialize;
use std::vec::Vec;

#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStatement),
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub select_expressions: Vec<SelectExpression>,
    pub from_expression: TableExpression,
    pub where_expression: Option<Term>,
}

#[derive(Debug, Clone)]
pub enum SelectExpression {
    Star,
    Family {
        name: String,
    },
    Expression {
        expression: Term,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub enum Term {
    Value(Value),
    // BindParameter -> ?,:1 so data can be inject into the query, kind of like a template
    Function(Function),
    Operand(Box<Operand>),
    Column(Column),
}

#[derive(Debug, Clone, Serialize)]
pub enum Column {
    Direct {
        schema: Option<String>,
        column_name: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub enum Value {
    String(String),
    Numeric(Numeric),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, Serialize)]
pub enum Numeric {
    Float(f64),
    Int(i64),
}

#[derive(Debug, Clone, Serialize)]
pub enum Function {
    UserDefined { name: String, terms: Vec<Term> },
    Sum(Box<Term>),
    Count(CountFunction),
}

#[derive(Debug, Clone, Serialize)]
pub enum CountFunction {
    Star,
    Term(Box<Term>),
}

#[derive(Debug, Clone, Serialize)]
pub enum Operand {
    // term
    Term(Term),
    // string operations
    StringConcatenation(Box<Operand>, Box<Operand>),
    // mathematical operations
    Addition(Box<Operand>, Box<Operand>),
    Subtraction(Box<Operand>, Box<Operand>),
    Multiplication(Box<Operand>, Box<Operand>),
    Division(Box<Operand>, Box<Operand>),
    UnaryMinus(Box<Operand>),
    // logical operations
    And(Box<Operand>, Box<Operand>),
    Or(Box<Operand>, Box<Operand>),
    Not(Box<Operand>),
    IsNull(Box<Operand>),
    IsNotNull(Box<Operand>),
    // comparisons
    Equal(Box<Operand>, Box<Operand>),
    NotEqual(Box<Operand>, Box<Operand>), // != and <>  are the same
    LessThan(Box<Operand>, Box<Operand>),
    GreaterThan(Box<Operand>, Box<Operand>),
    LessThanOrEqual(Box<Operand>, Box<Operand>),
    GreaterThanOrEqual(Box<Operand>, Box<Operand>),
}

#[derive(Debug, Clone)]
pub enum TableExpression {
    Table {
        schema: Option<String>,
        table: String,
    },
    Select {
        select_statement: Box<SelectStatement>,
        alias: Option<String>,
    },
}
