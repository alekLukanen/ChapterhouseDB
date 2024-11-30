use std::collections::HashMap;

use anyhow::Result;
use sqlparser::ast::{Expr, FunctionArg, Value};
use thiserror::Error;

use crate::planner::logical_planner::{LogicalPlan, LogicalPlanNode};

use super::logical_planner::LogicalPlanNodeType;

#[derive(Error, Debug)]
pub enum PhysicalPlanError {
    #[error("unable to find root nood in logical plan")]
    UnableToFindRootNodeInLogicalPlan,
    #[error("unable to build {0} resource for non-{1} logical plan node type")]
    UnableToBuildResourceForLogicalPlanNodeType(&'static str, &'static str),
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum DataFormat {
    Parquet,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OperationType {
    // table source stage
    TableFunc {
        alias: Option<String>,
        func_name: String,
        args: Vec<FunctionArg>,
        max_rows_per_write: usize,
    },
    Table {
        alias: Option<String>,
        name: String,
        max_rows_per_write: usize,
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
pub enum ResourceConfig {
    Manager {
        typ: OperationType,
        manage_producer_id: String,
    },
    Producer {
        typ: OperationType,
        source_exchange_ids: Vec<String>,
    },
    // an exchange manage the state of reading from
    // a producer. It allows multiple downstream producers
    // to reach from the same data location. If there aren't
    // anymore consumers the exchange notifies the operator
    // to shutdown the producers, and itself, the exchange
    // exits when the operator exits.
    Exchange {
        typ: OperationType,
        source_producer_id: String,
        source_operator_id: String,
    },
}

#[derive(Clone, Debug)]
pub struct Resource {
    id: String,
    plan_id: usize,
    resource_config: ResourceConfig,
    // compute requirements
    memory_in_mib: usize,
    cpu_in_tenths: usize, // 10 = 1 cpu 1
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

    pub fn has_nodes_for_plan_id(&mut self, plan_id: usize) -> bool {
        self.resources.iter().any(|item| item.plan_id == plan_id)
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
        let root_node = if let Some(root_node) = self.logical_plan.get_root_node() {
            root_node
        } else {
            return Err(PhysicalPlanError::UnableToFindRootNodeInLogicalPlan.into());
        };

        let mut node_id_stack: Vec<usize> = Vec::new();
        let mut physical_plan = PhysicalPlan::new();

        node_id_stack.push(root_node.id);

        while node_id_stack.len() > 0 {
            let plan_node_id = node_id_stack.remove(node_id_stack.len() - 1);
            if let Some(inbound_nodes) = self.logical_plan.get_inbound_nodes(plan_node_id) {
                for node_id in inbound_nodes {
                    node_id_stack.push(node_id);
                }
            }
        }

        Err(PhysicalPlanError::NotImplemented("build".to_string()).into())
    }

    fn build_resources(&mut self, lpn: LogicalPlanNode) -> Result<Vec<Resource>> {
        match lpn.node {
            LogicalPlanNodeType::Materialize { .. } => self.build_materialize_resources(lpn),
            LogicalPlanNodeType::Filter { .. } => self.build_filter_resources(lpn),
            _ => Err(PhysicalPlanError::NotImplemented(format!(
                "LogicalPlanNodeType isn't implemented to build resources: {:?}",
                lpn.node
            ))
            .into()),
        }
    }

    fn build_filter_resources(&mut self, lpn: LogicalPlanNode) -> Result<Vec<Resource>> {
        let filter_expr = match lpn.node {
            LogicalPlanNodeType::Filter { expr } => expr,
            _ => {
                return Err(
                    PhysicalPlanError::UnableToBuildResourceForLogicalPlanNodeType(
                        "filter", "filter",
                    )
                    .into(),
                );
            }
        };

        let resource_type = OperationType::Filter { expr: filter_expr };
        let mut resources: Vec<Resource> = Vec::new();

        let producer = Resource {
            id: self.new_resource_id(lpn.id),
            plan_id: lpn.id,
            resource_config: ResourceConfig::Producer {
                typ: resource_type.clone(),
                source_exchange_ids: Vec::new(),
            },
            cpu_in_tenths: 10,
            memory_in_mib: 512,
        };
        let operator = Resource {
            id: self.new_resource_id(lpn.id),
            plan_id: lpn.id,
            resource_config: ResourceConfig::Manager {
                typ: resource_type.clone(),
                manage_producer_id: producer.id.clone(),
            },
            cpu_in_tenths: 1,
            memory_in_mib: 128,
        };
        let exchange = Resource {
            id: self.new_resource_id(lpn.id),
            plan_id: lpn.id,
            resource_config: ResourceConfig::Exchange {
                typ: resource_type.clone(),
                source_producer_id: producer.id.clone(),
                source_operator_id: operator.id.clone(),
            },
            cpu_in_tenths: 2,
            memory_in_mib: 128,
        };

        resources.push(producer);
        resources.push(operator);
        resources.push(exchange);

        Ok(resources)
    }

    fn build_materialize_resources(&mut self, lpn: LogicalPlanNode) -> Result<Vec<Resource>> {
        if !matches!(lpn.node, LogicalPlanNodeType::Materialize { .. }) {
            return Err(
                PhysicalPlanError::UnableToBuildResourceForLogicalPlanNodeType(
                    "materialize",
                    "materialize",
                )
                .into(),
            );
        }

        let mut resources: Vec<Resource> = Vec::new();

        let resource_type = OperationType::Materialize {
            data_format: DataFormat::Parquet,
        };

        let producer = Resource {
            id: self.new_resource_id(lpn.id),
            plan_id: lpn.id,
            resource_config: ResourceConfig::Producer {
                typ: resource_type.clone(),
                source_exchange_ids: Vec::new(),
            },
            cpu_in_tenths: 10,
            memory_in_mib: 512,
        };
        let operator = Resource {
            id: self.new_resource_id(lpn.id),
            plan_id: lpn.id,
            resource_config: ResourceConfig::Manager {
                typ: resource_type.clone(),
                manage_producer_id: producer.id.clone(),
            },
            cpu_in_tenths: 1,
            memory_in_mib: 128,
        };
        let exchange = Resource {
            id: self.new_resource_id(lpn.id),
            plan_id: lpn.id,
            resource_config: ResourceConfig::Exchange {
                typ: resource_type.clone(),
                source_producer_id: producer.id.clone(),
                source_operator_id: operator.id.clone(),
            },
            cpu_in_tenths: 2,
            memory_in_mib: 128,
        };

        resources.push(producer);
        resources.push(operator);
        resources.push(exchange);

        Ok(resources)
    }

    fn create_resource_id_map(&mut self, plan_node_ids: Vec<usize>) -> HashMap<usize, String> {
        let mut map: HashMap<usize, String> = HashMap::new();
        for plan_node_id in plan_node_ids {
            map.insert(plan_node_id, self.new_resource_id(plan_node_id));
        }
        map
    }

    fn new_resource_id(&mut self, plan_nod_id: usize) -> String {
        let id = self.resource_idx;
        let sid = format!("resource_p{}_r{}", plan_nod_id, id);
        self.resource_idx += 1;
        sid
    }
}
