use anyhow::Result;
use sqlparser::ast::{Expr, FunctionArg};
use thiserror::Error;

use crate::planner::logical_planner::{LogicalPlan, LogicalPlanNode};

use super::logical_planner;

#[derive(Error, Debug)]
pub enum PhysicalPlanError {
    #[error("unable to find root nood in logical plan")]
    UnableToFindRootNodeInLogicalPlan,
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum DataFormat {
    Parquet,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ResourceType {
    // table source stage
    TableFunc {
        alias: Option<String>,
        func_name: String,
        args: Vec<FunctionArg>,
    },
    Table {
        alias: Option<String>,
        name: String,
    },
    // filter stage
    Filter {
        expr: Expr,
    },
    // materialize stage
    Materialize {
        data_format: DataFormat,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ResourceRequest {
    DataLocation {
        connection: String,
    },
    Operator {
        typ: ResourceType,
        manage_producer_id: String,
        read_data_location_ids: Vec<String>,
        write_data_location_id: String,
    },
    Producer {
        typ: ResourceType,
        read_data_location_ids: Vec<String>,
        write_data_location_id: String,
    },
    // an exchange manage the state of reading from
    // a producer. It allows multiple downstream producers
    // to reach from the same data location. If there aren't
    // anymore consumers the exchange notifies the operator
    // to shutdown the producers, and itself, the exchange
    // exits when the operator exits.
    Exchange {
        typ: ResourceType,
        source_producer_id: String,
        source_operator_id: String,
    },
}

#[derive(Clone, Debug)]
pub struct Resource {
    id: String,
    request: ResourceRequest,
}

#[derive(Clone, Debug)]
pub struct PhysicalPlan {
    resources: Vec<Resource>,
}

impl PhysicalPlan {
    pub fn new() -> PhysicalPlan {
        return PhysicalPlan {
            resources: Vec::new(),
        };
    }
    pub fn add_node(&mut self, resource: Resource) {
        self.resources.push(resource);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPlanner {
    logical_plan: LogicalPlan,
    resource_idx: usize,
}

impl PhysicalPlanner {
    pub fn new(logical_plan: LogicalPlan) -> PhysicalPlanner {
        return PhysicalPlanner {
            logical_plan,
            resource_idx: 0,
        };
    }

    pub fn build(&mut self) -> Result<PhysicalPlan> {
        let node_stack: Vec<LogicalPlanNode> = Vec::new();

        let root_node = if let Some(root_node) = self.logical_plan.find_root_node() {
            root_node
        } else {
            return Err(PhysicalPlanError::UnableToFindRootNodeInLogicalPlan.into());
        };

        Err(PhysicalPlanError::NotImplemented("build".to_string()).into())
    }

    fn new_resource_id(&mut self) -> String {
        let id = self.resource_idx;
        let sid = format!("resource_{}", id);
        self.resource_idx += 1;
        sid
    }
}
