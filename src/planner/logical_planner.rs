use std::collections::HashMap;
use std::usize;

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
    Materialize {
        fields: Vec<SelectItem>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum StageType {
    TableSource,
    Filter,
    Materialize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Stage {
    pub id: usize,
    pub typ: StageType,
    pub is_root: bool,
}

impl Stage {
    pub fn new(typ: StageType, id: usize, is_root: bool) -> Stage {
        return Stage { id, typ, is_root };
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPlanNode {
    pub id: usize,
    pub node: LogicalPlanNodeType,
    pub stage: Stage,
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
            if node.stage.is_root {
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

    pub fn add_node(&mut self, node: LogicalPlanNodeType, stage: Stage) -> usize {
        self.nodes.push(LogicalPlanNode {
            node,
            stage,
            id: self.nodes.len(),
        });
        return self.nodes.len();
    }

    pub fn connect(&mut self, from_node_idx: usize, to_node_idx: usize) {
        self.add_to_outbound_edges(from_node_idx, to_node_idx);
        self.add_to_inbound_edges(to_node_idx, from_node_idx);
    }

    pub fn connect_stages(&mut self, from_stage: Stage, to_stage: Stage) {
        let mut from_nodes_idxs: Vec<usize> = Vec::new();
        let mut to_nodes_idxs: Vec<usize> = Vec::new();
        for (idx, node) in self.nodes.iter().enumerate() {
            if node.stage == from_stage {
                from_nodes_idxs.push(idx);
            }
            if node.stage == to_stage {
                to_nodes_idxs.push(idx);
            }
        }

        for from_idx in from_nodes_idxs.clone() {
            for to_idx in to_nodes_idxs.clone() {
                self.connect(from_idx.clone(), to_idx.clone());
            }
        }
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
    stage_idx: usize,
}

impl LogicalPlanner {
    pub fn new(query: String) -> LogicalPlanner {
        LogicalPlanner {
            query,
            ast: None,
            plan: None,
            stage_idx: 0,
        }
    }

    fn create_stage_id(&mut self) -> usize {
        let id = self.stage_idx;
        self.stage_idx += 1;
        id
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

        // define static stages used in a select query plan
        let table_sources_stage = Stage::new(StageType::TableSource, self.create_stage_id(), false);
        let filter_stage = Stage::new(StageType::Filter, self.create_stage_id(), false);
        let materialize_stage = Stage::new(StageType::Materialize, self.create_stage_id(), true);

        // get table source(s)
        let table_sources = self.build_select_from(&select.from)?;
        let mut table_source_nodes: Vec<usize> = Vec::new();
        for table_source in table_sources {
            table_source_nodes
                .push(logical_plan.add_node(table_source, table_sources_stage.clone()));
        }

        // filter and materialize
        let filter = self.build_select_filter(&select.selection)?;
        let materialize = self.build_materialization(&select.projection)?;

        if let Some(filter_node) = filter {
            logical_plan.add_node(filter_node, filter_stage.clone());
            logical_plan.add_node(materialize, materialize_stage.clone());
            logical_plan.connect_stages(table_sources_stage.clone(), filter_stage.clone());
            logical_plan.connect_stages(filter_stage.clone(), materialize_stage.clone());
        } else {
            logical_plan.add_node(materialize, materialize_stage.clone());
            logical_plan.connect_stages(table_sources_stage.clone(), materialize_stage.clone());
        }

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

    fn build_select_filter(&self, selection: &Option<Expr>) -> Result<Option<LogicalPlanNodeType>> {
        match selection {
            Some(expr) => Ok(Some(LogicalPlanNodeType::Filter { expr: expr.clone() })),
            _ => Ok(None),
        }
    }

    fn build_select_from(&self, from: &Vec<TableWithJoins>) -> Result<Vec<LogicalPlanNodeType>> {
        let mut nodes: Vec<LogicalPlanNodeType> = Vec::new();
        for table_with_join in from {
            let relation_node = self.build_select_from_relation(&table_with_join.relation)?;
            nodes.push(relation_node);
        }
        Ok(nodes)
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
