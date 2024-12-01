use anyhow::Result;
use sqlparser::ast::{Expr, FunctionArg, SelectItem};
use thiserror::Error;

use crate::planner::logical_planner::{LogicalPlan, LogicalPlanNode};

use super::logical_planner::LogicalPlanNodeType;

#[derive(Error, Debug)]
pub enum PhysicalPlanError {
    #[error("unable to find root nood in logical plan")]
    UnableToFindRootNodeInLogicalPlan,
    #[error("unable to build {0} operation for non-{1} logical plan node type")]
    UnableToBuildOperationForLogicalPlanNodeType(&'static str, &'static str),
    #[error("missing exchange operators that are required by plan node {0}")]
    MissingExhcnageOperatorsThatAreRequiredByPlanNode(usize),
    #[error("max build iterations reached: {0}")]
    MaxBuildIterationsReached(usize),
    #[error("expected all inbound nodes to have operations already")]
    ExpectedAllInboundNodesToHaveOperationsAlready,
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
pub enum OperationTask {
    Producer {
        typ: TaskType,
        source_exchange_ids: Vec<String>,
    },
    // an exchange manage the state of reading from
    // a producer. It allows multiple downstream producers
    // to reach from the same data location. If there aren't
    // anymore consumers the exchange notifies the operator
    // to shutdown the producers, and itself, the exchange
    // exits when the operator exits.
    Exchange {
        typ: TaskType,
        source_producer_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub struct Operation {
    pub id: String,
    pub plan_id: usize,
    pub operation_task: OperationTask,
    // compute requirements
    pub memory_in_mib: usize,
    pub cpu_in_thousandths: usize, // 10 = 1 cpu 1
}

#[derive(Clone, Debug, PartialEq)]
pub struct Pipeline {
    id: String,
    operations: Vec<Operation>,
}

impl Pipeline {
    pub fn new(id: String) -> Pipeline {
        return Pipeline {
            id,
            operations: Vec::new(),
        };
    }

    pub fn has_operations_for_plan_id(&self, plan_id: usize) -> bool {
        self.operations.iter().any(|item| item.plan_id == plan_id)
    }

    pub fn get_exchange_operations_for_plan_id(&self, plan_id: usize) -> Vec<Operation> {
        let mut operations: Vec<Operation> = Vec::new();
        for op in &self.operations {
            if op.plan_id == plan_id && matches!(op.operation_task, OperationTask::Exchange { .. })
            {
                operations.push(op.clone());
            }
        }
        operations
    }

    pub fn add_operation(&mut self, operation: Operation) {
        self.operations.push(operation);
    }

    pub fn add_operations(&mut self, operations: Vec<Operation>) {
        for op in operations {
            self.add_operation(op);
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
    operation_idx: usize,
    max_build_iterations: usize,
}

impl PhysicalPlanner {
    pub fn new(logical_plan: LogicalPlan) -> PhysicalPlanner {
        return PhysicalPlanner {
            logical_plan,
            pipeline_idx: 0,
            operation_idx: 0,
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
            println!("plan_node_id: {}", plan_node_id);
            println!("pipeline: {:?}", pipeline);
            if pipeline.has_operations_for_plan_id(plan_node_id) {
                continue;
            }

            if let Some(inbound_nodes) = &self.logical_plan.get_inbound_nodes(plan_node_id) {
                let mut inbound_nodes_with_operations = 0usize;
                for inbound_node_id in inbound_nodes {
                    if pipeline.has_operations_for_plan_id(inbound_node_id.clone()) {
                        inbound_nodes_with_operations += 1;
                    }
                }

                if inbound_nodes_with_operations == 0 {
                    node_id_stack.push(plan_node_id);
                    println!("inbound_nodes: {:?}", inbound_nodes);
                    for node_id in inbound_nodes {
                        node_id_stack.push(node_id.clone());
                    }
                    continue;
                } else if inbound_nodes_with_operations != inbound_nodes.len() {
                    return Err(
                        PhysicalPlanError::ExpectedAllInboundNodesToHaveOperationsAlready.into(),
                    );
                }
            }

            if let Some(plan_node) = self.logical_plan.get_node(plan_node_id) {
                let plan_nodes_physical_operations =
                    self.build_operations(&plan_node, &pipeline)?;
                pipeline.add_operations(plan_nodes_physical_operations);
            }
            iters += 1;
        }

        let mut physical_plan = PhysicalPlan::new();
        physical_plan.add_pipeline(pipeline.clone());
        Ok(physical_plan)
    }

    fn build_operations(
        &mut self,
        lpn: &LogicalPlanNode,
        pipeline: &Pipeline,
    ) -> Result<Vec<Operation>> {
        match lpn.node {
            LogicalPlanNodeType::Materialize { .. } => {
                self.build_materialize_operations(lpn, pipeline)
            }
            LogicalPlanNodeType::Filter { .. } => self.build_filter_operations(lpn, pipeline),
            LogicalPlanNodeType::TableFunc { .. } => self.build_table_func_operations(lpn),
            _ => Err(PhysicalPlanError::NotImplemented(format!(
                "LogicalPlanNodeType isn't implemented to build resources: {:?}",
                lpn.node
            ))
            .into()),
        }
    }

    pub(crate) fn build_table_func_operations(
        &mut self,
        lpn: &LogicalPlanNode,
    ) -> Result<Vec<Operation>> {
        let task_type = match lpn.node.clone() {
            LogicalPlanNodeType::TableFunc { alias, name, args } => TaskType::TableFunc {
                alias,
                func_name: name,
                args,
                max_rows_per_batch: 10_000, // TODO: - determine how to set this
            },
            _ => {
                return Err(
                    PhysicalPlanError::UnableToBuildOperationForLogicalPlanNodeType(
                        "filter", "filter",
                    )
                    .into(),
                );
            }
        };

        let mut operations: Vec<Operation> = Vec::new();

        let producer = Operation {
            id: self.new_operation_id(lpn.id),
            plan_id: lpn.id,
            operation_task: OperationTask::Producer {
                typ: task_type.clone(),
                source_exchange_ids: Vec::new(),
            },
            cpu_in_thousandths: 1000,
            memory_in_mib: 512,
        };
        let exchange = Operation {
            id: self.new_operation_id(lpn.id),
            plan_id: lpn.id,
            operation_task: OperationTask::Exchange {
                typ: task_type.clone(),
                source_producer_id: producer.id.clone(),
            },
            cpu_in_thousandths: 200,
            memory_in_mib: 128,
        };

        operations.push(producer);
        operations.push(exchange);

        Ok(operations)
    }

    pub(crate) fn build_filter_operations(
        &mut self,
        lpn: &LogicalPlanNode,
        pipeline: &Pipeline,
    ) -> Result<Vec<Operation>> {
        let filter_expr = match lpn.node.clone() {
            LogicalPlanNodeType::Filter { expr } => expr,
            _ => {
                return Err(
                    PhysicalPlanError::UnableToBuildOperationForLogicalPlanNodeType(
                        "filter", "filter",
                    )
                    .into(),
                );
            }
        };

        let source_exchange_ids = self.get_source_exchange_nodes(&lpn, pipeline)?;

        let task_type = TaskType::Filter { expr: filter_expr };
        let mut operations: Vec<Operation> = Vec::new();

        let producer = Operation {
            id: self.new_operation_id(lpn.id),
            plan_id: lpn.id,
            operation_task: OperationTask::Producer {
                typ: task_type.clone(),
                source_exchange_ids,
            },
            cpu_in_thousandths: 1000,
            memory_in_mib: 512,
        };
        let exchange = Operation {
            id: self.new_operation_id(lpn.id),
            plan_id: lpn.id,
            operation_task: OperationTask::Exchange {
                typ: task_type.clone(),
                source_producer_id: producer.id.clone(),
            },
            cpu_in_thousandths: 200,
            memory_in_mib: 128,
        };

        operations.push(producer);
        operations.push(exchange);

        Ok(operations)
    }

    pub(crate) fn build_materialize_operations(
        &mut self,
        lpn: &LogicalPlanNode,
        pipeline: &Pipeline,
    ) -> Result<Vec<Operation>> {
        let fields = match lpn.node.clone() {
            LogicalPlanNodeType::Materialize { fields } => fields,
            _ => {
                return Err(
                    PhysicalPlanError::UnableToBuildOperationForLogicalPlanNodeType(
                        "materialize",
                        "materialize",
                    )
                    .into(),
                );
            }
        };

        let source_exchange_ids = self.get_source_exchange_nodes(&lpn, pipeline)?;

        let task_type = TaskType::Materialize {
            data_format: DataFormat::Parquet,
            fields,
        };
        let mut operations: Vec<Operation> = Vec::new();

        let producer = Operation {
            id: self.new_operation_id(lpn.id),
            plan_id: lpn.id,
            operation_task: OperationTask::Producer {
                typ: task_type.clone(),
                source_exchange_ids,
            },
            cpu_in_thousandths: 1000,
            memory_in_mib: 512,
        };
        let exchange = Operation {
            id: self.new_operation_id(lpn.id),
            plan_id: lpn.id,
            operation_task: OperationTask::Exchange {
                typ: task_type.clone(),
                source_producer_id: producer.id.clone(),
            },
            cpu_in_thousandths: 200,
            memory_in_mib: 128,
        };

        operations.push(producer);
        operations.push(exchange);

        Ok(operations)
    }

    fn get_source_exchange_nodes(
        &self,
        lpn: &LogicalPlanNode,
        pipeline: &Pipeline,
    ) -> Result<Vec<String>> {
        // get the inbound nodes from the logical plan
        // then find the physical producer operation corresponding
        // to the logical plan node. This operation will be
        // used as the filter source producers.
        let source_logical_plan_nodes = self.logical_plan.get_inbound_nodes(lpn.id);
        let mut source_exchange_ids: Vec<String> = Vec::new();
        if let Some(source_logical_plan_nodes) = source_logical_plan_nodes {
            for slpn_id in source_logical_plan_nodes {
                let source_exchange_operations =
                    pipeline.get_exchange_operations_for_plan_id(slpn_id);
                if source_exchange_operations.len() == 0 {
                    return Err(
                        PhysicalPlanError::MissingExhcnageOperatorsThatAreRequiredByPlanNode(
                            lpn.id,
                        )
                        .into(),
                    );
                }
                for op in source_exchange_operations {
                    source_exchange_ids.push(op.id.clone());
                }
            }
        }
        Ok(source_exchange_ids)
    }

    fn new_operation_id(&mut self, plan_nod_id: usize) -> String {
        let id = self.operation_idx;
        let sid = format!("operation_p{}_op{}", plan_nod_id, id);
        self.operation_idx += 1;
        sid
    }

    fn new_pipeline_id(&mut self) -> String {
        let id = self.pipeline_idx;
        let sid = format!("pipeline_{}", id);
        self.pipeline_idx += 1;
        sid
    }
}
