use anyhow::Result;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PlanError {
    #[error("require exactly 1 statement but received {0}")]
    NumberOfStatementsNotEqualToOne(usize),
}

pub struct QueryPlan {}

pub struct Planner {
    query: String,
    ast: Option<Statement>,
    plan: Option<QueryPlan>,
}

impl Planner {
    pub fn new(query: String) -> Planner {
        Planner {
            query,
            ast: None,
            plan: None,
        }
    }

    pub fn build_ast(&mut self) -> Result<&Planner> {
        let mut ast = Parser::parse_sql(&GenericDialect {}, self.query.as_str())?;
        if ast.len() != 1 {
            Err(PlanError::NumberOfStatementsNotEqualToOne(ast.len()).into())
        } else {
            self.ast = Some(ast.remove(0));
            Ok(self)
        }
    }

    pub fn build_plan(&mut self) -> Result<&Planner> {}
}
