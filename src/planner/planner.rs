use std::borrow::Borrow;
use std::rc::Rc;

use anyhow::Result;
use sqlparser::ast::{
    Expr, FunctionArg, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableAlias,
    TableFactor, TableFunctionArgs, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PlanError {
    #[error("require exactly 1 statement but received {0}")]
    NumberOfStatementsNotEqualToOne(usize),
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

pub enum PlanNode {
    TableFunc {
        alias: Option<String>,
        name: String,
        args: Vec<FunctionArg>,
        next_node: Option<Rc<PlanNode>>,
    },
    Table {
        alias: Option<String>,
        name: String,
        next_node: Option<Rc<PlanNode>>,
    },
    Filter {
        expr: Expr,
        next_node: Option<Rc<PlanNode>>,
    },
    Materialize {
        fields: Vec<SelectItem>,
        next_node: Option<Rc<PlanNode>>,
    },
}

impl PlanNode {
    fn connect_nodes(from: mut Rc<PlanNode>, to: Rc<PlanNode>) {
        match from {
            PlanNode::TableFunc { next_node, .. } => {
                next_node = Some(to.clone());
            }
            PlanNode::Table { next_node, .. } => {
                next_node = Some(to.clone());
            }
            PlanNode::Filter { next_node, .. } => {
                next_node = Some(to.clone());
            }
            PlanNode::Materialize { next_node, .. } => {
                next_node = Some(to.clone());
            }
        }
    }
}

pub enum LogicalPlan {
    Select { root: PlanNode },
}

pub struct Planner {
    query: String,
    ast: Option<Statement>,
    plan: Option<LogicalPlan>,
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

    pub fn build_plan(&mut self) -> Result<&Planner> {
        match self.ast {
            Some(Statement::Query(ref query)) => self.build_select_query_plan(query)?,
            _ => {
                return Err(PlanError::NotImplemented(
                    "sql statement type not implemented".to_string(),
                )
                .into())
            }
        }
        Ok(self)
    }

    fn build_select_query_plan(&self, query: &Box<Query>) -> Result<()> {
        // determine the source of data being queried
        let select: &Box<Select> = match *query.body {
            SetExpr::Select(ref select) => select,
            _ => return Err(PlanError::NotImplemented("non-select query".to_string()).into()),
        };

        let ref mut table_sources = self.build_select_from(&select.from)?;
        let filter = self.build_select_filter(&select.selection)?;
        let materialization = self.build_materialization(&select.projection)?;

        if let Some(filter_node) = filter {
            let rc_filter_node = Rc::new(filter_node);
            for table_source in table_sources {
                PlanNode::connect_nodes(table_source, Rc::clone(&rc_filter_node));
            }

            let rc_materialization_node = Rc::new(materialization);
            PlanNode::connect_nodes(ref mut rc_filter_node, rc_materialization_node);
        } else {
            let rc_materialization_node = Rc::new(materialization);
            for table_source in table_sources {
                PlanNode::connect_nodes(table_source, Rc::clone(&rc_materialization_node));
            }
        }

        Err(PlanError::NotImplemented("end".to_string()).into())
    }

    fn build_materialization(&self, select_items: &Vec<SelectItem>) -> Result<PlanNode> {
        let mut fields: Vec<SelectItem> = Vec::new();
        for select_item in select_items {
            if self.is_valid_select_item(select_item) {
                fields.push(select_item.clone())
            }
        }
        Ok(PlanNode::Materialize {
            fields,
            next_node: None,
        })
    }

    fn is_valid_select_item(&self, select_item: &SelectItem) -> bool {
        match select_item {
            SelectItem::UnnamedExpr(_) => true,
            SelectItem::ExprWithAlias { .. } => true,
            SelectItem::QualifiedWildcard(_, opts) => {
                return opts.opt_ilike.is_none()
                    && opts.opt_except.is_none()
                    && opts.opt_rename.is_none()
                    && opts.opt_exclude.is_none()
                    && opts.opt_replace.is_none()
            }
            SelectItem::Wildcard(opts) => {
                return opts.opt_ilike.is_none()
                    && opts.opt_except.is_none()
                    && opts.opt_rename.is_none()
                    && opts.opt_exclude.is_none()
                    && opts.opt_replace.is_none()
            }
        }
    }

    fn build_select_filter(&self, selection: &Option<Expr>) -> Result<Option<PlanNode>> {
        match selection {
            Some(expr) => Ok(Some(PlanNode::Filter {
                expr: expr.clone(),
                next_node: None,
            })),
            _ => Ok(None),
        }
    }

    fn build_select_from(&self, from: &Vec<TableWithJoins>) -> Result<Vec<PlanNode>> {
        let mut nodes: Vec<PlanNode> = Vec::new();
        for tableWithJoin in from {
            let relation_node = self.build_select_from_relation(&tableWithJoin.relation)?;
            nodes.push(relation_node);
        }
        Ok(nodes)
    }

    fn build_select_from_relation(&self, relation: &TableFactor) -> Result<PlanNode> {
        match relation {
            TableFactor::Table {
                name, alias, args, ..
            } => Ok(self.table_relation_plan_node(name, alias, args)?),
            _ => Err(PlanError::NotImplemented("from relation".to_string()).into()),
        }
    }

    fn table_relation_plan_node(
        &self,
        name: &ObjectName,
        alias: &Option<TableAlias>,
        args: &Option<TableFunctionArgs>,
    ) -> Result<PlanNode> {
        if name.0.len() != 1 {
            return Err(PlanError::NotImplemented(format!(
                "table names with {} components have not been",
                name.0.len()
            ))
            .into());
        }

        let relation_name = if let Some(ident) = name.0.last() {
            ident.value.clone()
        } else {
            return Err(PlanError::NotImplemented("".to_string()).into());
        };

        let alias_name = if let Some(ident) = alias {
            Some(ident.name.value.clone())
        } else {
            None
        };

        if let Some(table_args) = args {
            return Ok(PlanNode::TableFunc {
                alias: alias_name,
                name: relation_name,
                args: table_args.args.clone(),
                next_node: None,
            });
        } else {
            return Ok(PlanNode::Table {
                alias: alias_name,
                name: relation_name,
                next_node: None,
            });
        }
    }
}
