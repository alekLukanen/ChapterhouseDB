use std::vec::Vec;

pub enum Statement {
    Select(SelectStatement),
}

pub struct SelectStatement {
    pub select_expressions: Vec<SelectExpression>,
    pub from: TableExpression,
}

pub enum SelectExpression {
    Star,
    Family { name: String },
    Term { term: Term, alias: Option<String> },
}

pub enum Term {
    Value(Value),
    // BindParameter -> ?,:1 so data can be inject into the query, kind of like a template
    Function(Function),
    Operand(Box<Operand>),
    Column(Column),
}

pub enum Column {
    Aliased { alias: String, column_name: String },
    Direct { schema: String, column_name: String },
}

pub enum Value {
    String(String),
    Numeric(Numeric),
    Boolean(bool),
    Null,
}

pub enum Numeric {
    Float(f64),
    Int(i64),
}

pub enum Function {
    UserDefined { name: String, terms: Vec<Term> },
    Sum(Box<Term>),
    Count(CountFunction),
}

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
pub enum Operand {
    Term(Term),
    Operation(Box<Operation>),
}

pub enum Operation {
    StringConcatenation(Term, Term),
    Addition(Term, Term),
    Subtraction(Term, Term),
    Multiplication(Term, Term),
    Division(Term, Term),
    UnaryMinus(Term),
}

pub struct Expression {}

pub struct TableExpression {}
