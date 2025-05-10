use std::collections::HashMap;
use std::usize;

use anyhow::Result;
use sqlparser::ast::{
    Expr, FunctionArg, ObjectName, OrderBy, OrderByExpr, Query, Select, SelectItem, SetExpr,
    Statement, TableAlias, TableFactor, TableFunctionArgs, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PlanError {
    #[error("require exactly 1 statement but received {0}")]
    NumberOfStatementsNotEqualToOne(usize),
    #[error("node does not exist: {0}")]
    NodeDoesNotExist(usize),
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum LogicalPlanNodeType {
    TableFunc {
        alias: Option<String>,
        name: String,
        args: Vec<FunctionArg>,
    },
    Table {
        alias: Option<String>,
        name: String,
    },
    Filter {
        expr: Expr,
    },
    OrderBy {
        exprs: Vec<OrderByExpr>,
    },
    Materialize {
        fields: Vec<SelectItem>,
    },
}

impl LogicalPlanNodeType {
    pub fn name(&self) -> String {
        match self {
            Self::TableFunc { .. } => "TableFunc".to_string(),
            Self::Table { .. } => "Table".to_string(),
            Self::Filter { .. } => "Filter".to_string(),
            Self::OrderBy { .. } => "OrderBy".to_string(),
            Self::Materialize { .. } => "Materialize".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPlanNode {
    pub id: usize,
    pub is_root: bool,
    pub node: LogicalPlanNodeType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPlan {
    nodes: Vec<LogicalPlanNode>,
    outbound_edges: HashMap<usize, Vec<usize>>,
    inbound_edges: HashMap<usize, Vec<usize>>,
}

impl LogicalPlan {
    pub fn new() -> LogicalPlan {
        return LogicalPlan {
            nodes: Vec::new(),
            outbound_edges: HashMap::new(),
            inbound_edges: HashMap::new(),
        };
    }

    pub fn get_root_node(&self) -> Option<LogicalPlanNode> {
        for node in &self.nodes {
            if node.is_root {
                return Some(node.clone());
            }
        }
        None
    }

    pub fn get_node(&self, node_idx: usize) -> Option<LogicalPlanNode> {
        if let Some(node) = self.nodes.iter().find(|&item| item.id == node_idx) {
            Some(node.clone())
        } else {
            None
        }
    }

    pub fn get_all_nodes(&self) -> Vec<LogicalPlanNode> {
        self.nodes.clone()
    }

    pub fn get_all_node_ids(&self) -> Vec<usize> {
        let mut ids: Vec<usize> = Vec::new();
        for node in &self.nodes {
            ids.push(node.id);
        }
        ids
    }

    pub fn get_outbound_nodes(&self, node_idx: usize) -> Option<Vec<usize>> {
        match self.outbound_edges.get(&node_idx) {
            Some(nodes) => Some(nodes.clone()),
            _ => None,
        }
    }

    pub fn get_inbound_nodes(&self, node_idx: usize) -> Option<Vec<usize>> {
        match self.inbound_edges.get(&node_idx) {
            Some(nodes) => Some(nodes.clone()),
            _ => None,
        }
    }

    pub fn has_inbound_edges(&self, node_idx: usize) -> Result<bool> {
        match self.inbound_edges.get(&node_idx) {
            Some(nodes) => Ok(nodes.len() > 0),
            _ => Err(PlanError::NodeDoesNotExist(node_idx).into()),
        }
    }

    pub fn add_node(&mut self, node: LogicalPlanNodeType, is_root: bool) -> usize {
        self.nodes.push(LogicalPlanNode {
            node,
            is_root,
            id: self.nodes.len(),
        });
        return self.nodes.len() - 1;
    }

    pub fn connect(&mut self, from_node_idx: usize, to_node_idx: usize) {
        self.add_to_outbound_edges(from_node_idx, to_node_idx);
        self.add_to_inbound_edges(to_node_idx, from_node_idx);
    }

    fn add_to_outbound_edges(&mut self, key_node_idx: usize, value_node_idx: usize) {
        if self.outbound_edges.contains_key(&key_node_idx) {
            if let Some(nodes) = self.outbound_edges.get(&key_node_idx) {
                for val in nodes {
                    if val.clone() == value_node_idx {
                        return;
                    }
                }
            }
            if let Some(nodes) = self.outbound_edges.get_mut(&key_node_idx) {
                nodes.push(value_node_idx);
            }
        } else {
            self.outbound_edges
                .insert(key_node_idx, vec![value_node_idx]);
        }
    }

    fn add_to_inbound_edges(&mut self, key_node_idx: usize, value_node_idx: usize) {
        if self.inbound_edges.contains_key(&key_node_idx) {
            if let Some(nodes) = self.inbound_edges.get(&key_node_idx) {
                for val in nodes {
                    if val.clone() == value_node_idx {
                        return;
                    }
                }
            }
            if let Some(nodes) = self.inbound_edges.get_mut(&key_node_idx) {
                nodes.push(value_node_idx);
            }
        } else {
            self.inbound_edges
                .insert(key_node_idx, vec![value_node_idx]);
        }
    }
}

pub struct LogicalPlanner {
    query: String,
    ast: Option<Statement>,
    plan: Option<LogicalPlan>,
}

impl LogicalPlanner {
    pub fn new(query: String) -> LogicalPlanner {
        LogicalPlanner {
            query,
            ast: None,
            plan: None,
        }
    }

    pub fn build(&mut self) -> Result<LogicalPlan> {
        if let Some(ref plan) = self.plan {
            Ok(plan.clone())
        } else {
            let ast = self.build_ast()?;
            self.ast = Some(ast);

            let plan = self.build_plan()?;
            self.plan = Some(plan.clone());
            Ok(plan)
        }
    }

    fn build_ast(&self) -> Result<Statement> {
        let mut ast = Parser::parse_sql(&GenericDialect {}, self.query.as_str())?;
        if ast.len() != 1 {
            Err(PlanError::NumberOfStatementsNotEqualToOne(ast.len()).into())
        } else {
            Ok(ast.remove(0))
        }
    }

    fn build_plan(&mut self) -> Result<LogicalPlan> {
        let ast = self.ast.clone();
        match ast {
            Some(Statement::Query(ref query)) => Ok(self.build_select_query_plan(query)?),
            _ => {
                return Err(PlanError::NotImplemented(
                    "sql statement type not implemented".to_string(),
                )
                .into())
            }
        }
    }

    fn build_select_query_plan(&mut self, query: &Box<Query>) -> Result<LogicalPlan> {
        // determine the source of data being queried
        let select: &Box<Select> = match *query.body {
            SetExpr::Select(ref select) => select,
            _ => return Err(PlanError::NotImplemented("non-select query".to_string()).into()),
        };

        let ref mut logical_plan = LogicalPlan::new();

        // table source
        let table_source = self.build_select_from(&select.from)?;
        let table_source_node_idx = logical_plan.add_node(table_source, false);

        let mut last_node_idx = table_source_node_idx;

        // filter
        if let Some(filter_node) = self.build_select_filter(&select.selection)? {
            let filter_node_idx = logical_plan.add_node(filter_node, false);
            logical_plan.connect(last_node_idx, filter_node_idx);
            last_node_idx = filter_node_idx;
        }

        // order by
        if let Some(order_by_node) = self.build_order_by(&query.order_by)? {
            let order_by_node_idx = logical_plan.add_node(order_by_node, false);
            logical_plan.connect(last_node_idx, order_by_node_idx);
            last_node_idx = order_by_node_idx;
        }

        // materialize
        let materialize = self.build_materialization(&select.projection)?;
        let materialize_node_idx = logical_plan.add_node(materialize, true);
        logical_plan.connect(last_node_idx, materialize_node_idx);

        Ok(logical_plan.clone())
    }

    fn build_materialization(&self, select_items: &Vec<SelectItem>) -> Result<LogicalPlanNodeType> {
        let mut fields: Vec<SelectItem> = Vec::new();
        for select_item in select_items {
            if self.is_valid_select_item(select_item) {
                fields.push(select_item.clone())
            }
        }
        Ok(LogicalPlanNodeType::Materialize { fields })
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

    fn build_order_by(&self, order_by: &Option<OrderBy>) -> Result<Option<LogicalPlanNodeType>> {
        match order_by {
            Some(order_by) => {
                if order_by.interpolate.is_some() {
                    return Err(PlanError::NotImplemented(
                        "unsupported order by option".to_string(),
                    )
                    .into());
                }
                for ord_expr in &order_by.exprs {
                    if ord_expr.with_fill.is_some() || ord_expr.nulls_first.is_some() {
                        return Err(PlanError::NotImplemented(
                            "unsupported order by option".to_string(),
                        )
                        .into());
                    }
                }

                Ok(Some(LogicalPlanNodeType::OrderBy {
                    exprs: order_by.exprs.clone(),
                }))
            }
            None => Ok(None),
        }
    }

    fn build_select_filter(&self, selection: &Option<Expr>) -> Result<Option<LogicalPlanNodeType>> {
        match selection {
            Some(expr) => Ok(Some(LogicalPlanNodeType::Filter { expr: expr.clone() })),
            _ => Ok(None),
        }
    }

    fn build_select_from(&self, from: &Vec<TableWithJoins>) -> Result<LogicalPlanNodeType> {
        if from.len() != 1 {
            return Err(PlanError::NotImplemented(
                "unsupported from clause; expect exactly one table".to_string(),
            )
            .into());
        }

        if let Some(table) = from.get(0) {
            self.build_select_from_relation(&table.relation)
        } else {
            Err(PlanError::NotImplemented(
                "unsupported from clause; expected exactly one table".to_string(),
            )
            .into())
        }
    }

    fn build_select_from_relation(&self, relation: &TableFactor) -> Result<LogicalPlanNodeType> {
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
    ) -> Result<LogicalPlanNodeType> {
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
            return Ok(LogicalPlanNodeType::TableFunc {
                alias: alias_name,
                name: relation_name,
                args: table_args.args.clone(),
            });
        } else {
            return Ok(LogicalPlanNodeType::Table {
                alias: alias_name,
                name: relation_name,
            });
        }
    }
}
