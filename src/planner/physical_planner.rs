use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, FunctionArg, SelectItem};
use thiserror::Error;

use crate::planner::logical_planner::{LogicalPlan, LogicalPlanNode};

use super::logical_planner::LogicalPlanNodeType;

#[derive(Error, Debug)]
pub enum PhysicalPlanError {
    #[error("unable to find root nood in logical plan")]
    UnableToFindRootNodeInLogicalPlan,
    #[error("unable to build {0} operator for non-{1} logical plan node type")]
    UnableToBuildOperatorForLogicalPlanNodeType(&'static str, &'static str),
    #[error("max build iterations reached: {0}")]
    MaxBuildIterationsReached(usize),
    #[error("expected all inbound nodes to have operators already")]
    ExpectedAllInboundNodesToHaveOperatorsAlready,
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub enum DataFormat {
    Parquet,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub enum TaskType {
    // table source stage
    TableFunc {
        alias: Option<String>,
        func_name: String,
        args: Vec<FunctionArg>,
        max_rows_per_batch: usize,
    },
    Table {
        alias: Option<String>,
        name: String,
        max_rows_per_batch: usize,
    },
    // filter stage
    Filter {
        expr: Expr,
    },
    // materialize stage
    Materialize {
        data_format: DataFormat,
        fields: Vec<SelectItem>,
    },
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub enum OperatorTask {
    Producer {
        typ: TaskType,
        // push based: will mostly use outbound
        outbound_exchange_id: String,
        inbound_exchange_ids: Vec<String>,
    },
    // an exchange manage the state of reading from
    // a producer. It allows multiple downstream producers
    // to reach from the same data location. If there aren't
    // anymore consumers the exchange notifies the operator
    // to shutdown the producers, and itself, the exchange
    // exits when the operator exits.
    Exchange {
        typ: TaskType,
        // push based: will mostly use outbound
        outbound_producer_ids: Vec<String>,
        inbound_producer_ids: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub struct OperatorCompute {
    pub instances: usize,
    pub memory_in_mib: usize,
    pub cpu_in_thousandths: usize,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub struct Operator {
    pub id: String,
    pub plan_id: usize,
    pub operator_task: OperatorTask,
    // compute requirements
    pub compute: OperatorCompute,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Pipeline {
    id: String,
    operators: Vec<Operator>,
}

impl Pipeline {
    pub fn new(id: String) -> Pipeline {
        return Pipeline {
            id,
            operators: Vec::new(),
        };
    }

    pub fn get_operators(&self) -> Vec<Operator> {
        self.operators.clone()
    }

    pub fn has_operators_for_plan_id(&self, plan_id: usize) -> bool {
        self.operators.iter().any(|item| item.plan_id == plan_id)
    }

    pub fn get_exchange_operators_for_plan_id(&self, plan_id: usize) -> Vec<Operator> {
        let mut operators: Vec<Operator> = Vec::new();
        for op in &self.operators {
            if op.plan_id == plan_id && matches!(op.operator_task, OperatorTask::Exchange { .. }) {
                operators.push(op.clone());
            }
        }
        operators
    }

    pub fn add_operator(&mut self, op: Operator) {
        self.operators.push(op);
    }

    pub fn add_operators(&mut self, ops: Vec<Operator>) {
        for op in ops {
            self.add_operator(op);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPlan {
    pipelines: Vec<Pipeline>,
}

impl PhysicalPlan {
    pub fn new() -> PhysicalPlan {
        return PhysicalPlan {
            pipelines: Vec::new(),
        };
    }

    pub fn add_pipeline(&mut self, pipeline: Pipeline) {
        self.pipelines.push(pipeline);
    }

    pub fn get_pipelines(&self) -> Vec<Pipeline> {
        self.pipelines.clone()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPlanner {
    logical_plan: LogicalPlan,
    pipeline_idx: usize,
    operator_idx: usize,
    max_build_iterations: usize,
}

impl PhysicalPlanner {
    pub fn new(logical_plan: LogicalPlan) -> PhysicalPlanner {
        return PhysicalPlanner {
            logical_plan,
            pipeline_idx: 0,
            operator_idx: 0,
            max_build_iterations: 10,
        };
    }

    pub fn build(&mut self) -> Result<PhysicalPlan> {
        let root_node = if let Some(root_node) = self.logical_plan.get_root_node() {
            root_node
        } else {
            return Err(PhysicalPlanError::UnableToFindRootNodeInLogicalPlan.into());
        };

        let mut node_id_stack: Vec<usize> = Vec::new();
        let ref mut pipeline = Pipeline::new(self.new_pipeline_id());

        node_id_stack.push(root_node.id);

        let mut iters = 0usize;
        while node_id_stack.len() > 0 {
            if iters > self.max_build_iterations {
                return Err(PhysicalPlanError::MaxBuildIterationsReached(
                    self.max_build_iterations,
                )
                .into());
            }
            let plan_node_id = node_id_stack.remove(node_id_stack.len() - 1);

            if pipeline.has_operators_for_plan_id(plan_node_id) {
                continue;
            }

            if let Some(inbound_nodes) = &self.logical_plan.get_inbound_nodes(plan_node_id) {
                let mut inbound_nodes_with_operators = 0usize;
                for inbound_node_id in inbound_nodes {
                    if pipeline.has_operators_for_plan_id(inbound_node_id.clone()) {
                        inbound_nodes_with_operators += 1;
                    }
                }

                if inbound_nodes_with_operators == 0 {
                    node_id_stack.push(plan_node_id);
                    println!("inbound_nodes: {:?}", inbound_nodes);
                    for node_id in inbound_nodes {
                        node_id_stack.push(node_id.clone());
                    }
                    continue;
                } else if inbound_nodes_with_operators != inbound_nodes.len() {
                    return Err(
                        PhysicalPlanError::ExpectedAllInboundNodesToHaveOperatorsAlready.into(),
                    );
                }
            }

            if let Some(plan_node) = self.logical_plan.get_node(plan_node_id) {
                let plan_nodes_physical_operations = self.build_operators(&plan_node)?;
                pipeline.add_operators(plan_nodes_physical_operations);
            }
            iters += 1;
        }

        let mut physical_plan = PhysicalPlan::new();
        physical_plan.add_pipeline(pipeline.clone());
        Ok(physical_plan)
    }

    fn build_operators(&mut self, lpn: &LogicalPlanNode) -> Result<Vec<Operator>> {
        match lpn.node {
            LogicalPlanNodeType::Materialize { .. } => self.build_materialize_operators(lpn),
            LogicalPlanNodeType::Filter { .. } => self.build_filter_operators(lpn),
            LogicalPlanNodeType::TableFunc { .. } => self.build_table_func_operators(lpn),
            _ => Err(PhysicalPlanError::NotImplemented(format!(
                "LogicalPlanNodeType isn't implemented to build resources: {:?}",
                lpn.node
            ))
            .into()),
        }
    }

    pub(crate) fn build_table_func_operators(
        &mut self,
        lpn: &LogicalPlanNode,
    ) -> Result<Vec<Operator>> {
        let task_type = match lpn.node.clone() {
            LogicalPlanNodeType::TableFunc { alias, name, args } => TaskType::TableFunc {
                alias,
                func_name: name,
                args,
                max_rows_per_batch: 10_000, // TODO: - determine how to set this
            },
            _ => {
                return Err(
                    PhysicalPlanError::UnableToBuildOperatorForLogicalPlanNodeType(
                        "filter", "filter",
                    )
                    .into(),
                );
            }
        };

        let mut operators: Vec<Operator> = Vec::new();

        let producer = Operator {
            id: self.new_operator_id(lpn.id, "producer"),
            plan_id: lpn.id,
            operator_task: OperatorTask::Producer {
                typ: task_type.clone(),
                outbound_exchange_id: self.new_operator_id(lpn.id, "exchange"),
                inbound_exchange_ids: Vec::new(),
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 1000,
                memory_in_mib: 512,
            },
        };
        let exchange = Operator {
            id: self.new_operator_id(lpn.id, "exchange"),
            plan_id: lpn.id,
            operator_task: OperatorTask::Exchange {
                typ: task_type.clone(),
                outbound_producer_ids: self.get_outbound_operators(lpn, "producer")?,
                inbound_producer_ids: vec![producer.id.clone()],
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 200,
                memory_in_mib: 128,
            },
        };

        operators.push(producer);
        operators.push(exchange);

        Ok(operators)
    }

    pub(crate) fn build_filter_operators(
        &mut self,
        lpn: &LogicalPlanNode,
    ) -> Result<Vec<Operator>> {
        let filter_expr = match lpn.node.clone() {
            LogicalPlanNodeType::Filter { expr } => expr,
            _ => {
                return Err(
                    PhysicalPlanError::UnableToBuildOperatorForLogicalPlanNodeType(
                        "filter", "filter",
                    )
                    .into(),
                );
            }
        };

        let task_type = TaskType::Filter { expr: filter_expr };
        let mut operators: Vec<Operator> = Vec::new();

        let producer = Operator {
            id: self.new_operator_id(lpn.id, "producer"),
            plan_id: lpn.id,
            operator_task: OperatorTask::Producer {
                typ: task_type.clone(),
                outbound_exchange_id: self.new_operator_id(lpn.id, "exchange"),
                inbound_exchange_ids: self.get_inbound_operators(&lpn, "exchange")?,
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 1000,
                memory_in_mib: 512,
            },
        };
        let exchange = Operator {
            id: self.new_operator_id(lpn.id, "exchange"),
            plan_id: lpn.id,
            operator_task: OperatorTask::Exchange {
                typ: task_type.clone(),
                outbound_producer_ids: self.get_outbound_operators(&lpn, "producer")?,
                inbound_producer_ids: vec![producer.id.clone()],
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 200,
                memory_in_mib: 128,
            },
        };

        operators.push(producer);
        operators.push(exchange);

        Ok(operators)
    }

    pub(crate) fn build_materialize_operators(
        &mut self,
        lpn: &LogicalPlanNode,
    ) -> Result<Vec<Operator>> {
        let fields = match lpn.node.clone() {
            LogicalPlanNodeType::Materialize { fields } => fields,
            _ => {
                return Err(
                    PhysicalPlanError::UnableToBuildOperatorForLogicalPlanNodeType(
                        "materialize",
                        "materialize",
                    )
                    .into(),
                );
            }
        };

        let task_type = TaskType::Materialize {
            data_format: DataFormat::Parquet,
            fields,
        };
        let mut operators: Vec<Operator> = Vec::new();

        let producer = Operator {
            id: self.new_operator_id(lpn.id, "producer"),
            plan_id: lpn.id,
            operator_task: OperatorTask::Producer {
                typ: task_type.clone(),
                outbound_exchange_id: self.new_operator_id(lpn.id, "exchange"),
                inbound_exchange_ids: self.get_inbound_operators(lpn, "exchange")?,
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 1000,
                memory_in_mib: 512,
            },
        };
        let exchange = Operator {
            id: self.new_operator_id(lpn.id, "exchange"),
            plan_id: lpn.id,
            operator_task: OperatorTask::Exchange {
                typ: task_type.clone(),
                outbound_producer_ids: self.get_outbound_operators(lpn, "producer")?,
                inbound_producer_ids: vec![producer.id.clone()],
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 200,
                memory_in_mib: 128,
            },
        };

        operators.push(producer);
        operators.push(exchange);

        Ok(operators)
    }

    fn get_outbound_operators(
        &self,
        lpn: &LogicalPlanNode,
        task_type_name: &str,
    ) -> Result<Vec<String>> {
        let outbound_logical_plan_nodes = self.logical_plan.get_outbound_nodes(lpn.id);
        let mut outbound_exchange_ids: Vec<String> = Vec::new();
        if let Some(outbound_logical_plan_nodes) = outbound_logical_plan_nodes {
            for out_lpn_id in outbound_logical_plan_nodes {
                outbound_exchange_ids.push(self.new_operator_id(out_lpn_id, task_type_name));
            }
        }
        Ok(outbound_exchange_ids)
    }

    fn get_inbound_operators(
        &self,
        lpn: &LogicalPlanNode,
        task_type_name: &str,
    ) -> Result<Vec<String>> {
        let inbound_logical_plan_nodes = self.logical_plan.get_inbound_nodes(lpn.id);
        let mut inbound_exchange_ids: Vec<String> = Vec::new();
        if let Some(inbound_logical_plan_nodes) = inbound_logical_plan_nodes {
            for in_lpn_id in inbound_logical_plan_nodes {
                inbound_exchange_ids.push(self.new_operator_id(in_lpn_id, task_type_name));
            }
        }
        Ok(inbound_exchange_ids)
    }

    fn new_operator_id(&self, plan_node_id: usize, task_type_name: &str) -> String {
        format!("operator_p{}_{}", plan_node_id, task_type_name)
    }

    fn new_pipeline_id(&mut self) -> String {
        let id = self.pipeline_idx;
        let sid = format!("pipeline_{}", id);
        self.pipeline_idx += 1;
        sid
    }
}
