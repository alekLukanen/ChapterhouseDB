use std::vec::Vec;

#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
}

#[derive(Debug)]
pub struct SelectStatement {
    pub select_expressions: Vec<SelectExpression>,
    pub from: TableExpression,
}

#[derive(Debug)]
pub enum SelectExpression {
    Star,
    Family { name: String },
    Term { term: Term, alias: Option<String> },
}

#[derive(Debug)]
pub enum Term {
    Value(Value),
    // BindParameter -> ?,:1 so data can be inject into the query, kind of like a template
    Function(Function),
    Operand(Box<Operand>),
    Column(Column),
}

#[derive(Debug)]
pub enum Column {
    Aliased { alias: String, column_name: String },
    Direct { schema: String, column_name: String },
}

#[derive(Debug)]
pub enum Value {
    String(String),
    Numeric(Numeric),
    Boolean(bool),
    Null,
}

#[derive(Debug)]
pub enum Numeric {
    Float(f64),
    Int(i64),
}

#[derive(Debug)]
pub enum Function {
    UserDefined { name: String, terms: Vec<Term> },
    Sum(Box<Term>),
    Count(CountFunction),
}

#[derive(Debug)]
pub enum CountFunction {
    Star,
    Term(Box<Term>),
}

// Condensed down to one operand node with either a term
// or an operation tree.
// example:
//   5 + 2 * 3
//   Operand:
//
//   Operand:
//     Operation::Multiplication(2, 3)

#[derive(Debug)]
pub enum Operand {
    Term(Term),
    Operation(Box<Operation>),
}

#[derive(Debug)]
pub enum Operation {
    StringConcatenation(Term, Term),
    Addition(Term, Term),
    Subtraction(Term, Term),
    Multiplication(Term, Term),
    Division(Term, Term),
    UnaryMinus(Term),
}

#[derive(Debug)]
pub struct Expression {}

#[derive(Debug)]
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
