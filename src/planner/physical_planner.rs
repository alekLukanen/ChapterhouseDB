use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, FunctionArg, OrderByExpr, SelectItem};
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

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub enum DataFormat {
    Parquet,
}

impl DataFormat {
    fn name(&self) -> &str {
        match self {
            Self::Parquet => "Parquet",
        }
    }
}

impl std::fmt::Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataFormat::{}", self.name())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OperatorTask {
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
    Partition {
        partition_method: PartitionMethod,
        partition_range_method: PartitionRangeMethod,
    },
    Sort {
        exprs: Vec<OrderByExpr>,
        num_partitions: usize,
    },
    // materialize stage
    MaterializeFiles {
        data_format: DataFormat,
        fields: Vec<SelectItem>,
    },
}

impl OperatorTask {
    pub fn name(&self) -> &str {
        match self {
            Self::TableFunc { .. } => "TableFunc",
            Self::Table { .. } => "Table",
            Self::Filter { .. } => "Filter",
            Self::Partition { .. } => "Partition",
            Self::Sort { .. } => "Sort",
            Self::MaterializeFiles { .. } => "MaterializeFiles",
        }
    }
}

impl std::fmt::Display for OperatorTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OperatorTask::{}", self.name())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OperatorType {
    Producer {
        task: OperatorTask,
        // push based: will mostly use outbound
        outbound_exchange_id: String,
        inbound_exchange_ids: Vec<String>,
    },
    Exchange {
        // pull based: a producer will request data from the exchange
        outbound_producer_ids: Vec<String>,
        inbound_producer_ids: Vec<String>,
        record_queue_configs: Vec<ExchangeRecordQueueConfig>,
    },
}

impl OperatorType {
    pub fn name(&self) -> &str {
        match self {
            Self::Producer { .. } => "Producer",
            Self::Exchange { .. } => "Exchange",
        }
    }
    pub fn task_name(&self) -> &str {
        match self {
            Self::Producer { task, .. } => task.name(),
            Self::Exchange { .. } => "None",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExchangeRecordQueueConfig {
    pub producer_id: String,
    pub queue_name: String,
    pub input_queue_names: Vec<String>,
    pub sampling_method: ExchangeRecordQueueSamplingMethod,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExchangeRecordQueueSamplingMethod {
    All,
    PercentageWithReserve { sample_rate: f32, min_rows: usize },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PartitionMethod {
    OrderByExprs { exprs: Vec<OrderByExpr> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PartitionRangeMethod {
    SampleDistribution {
        sample_rate: f32,
        min_sampled_rows: usize,
        max_sampled_rows: usize,
        num_partitions: usize,
        exchange_queue_name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperatorCompute {
    pub instances: usize,
    pub memory_in_mib: usize,
    pub cpu_in_thousandths: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Operator {
    pub id: String,
    pub plan_id: usize,
    pub operator_type: OperatorType,
    pub compute: OperatorCompute,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: String,
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

    pub fn get_operators_ref(&self) -> Vec<&Operator> {
        self.operators.iter().collect()
    }

    pub fn get_operators_ref_mut(&mut self) -> Vec<&mut Operator> {
        self.operators.iter_mut().collect()
    }

    pub fn has_operators_for_plan_id(&self, plan_id: usize) -> bool {
        self.operators.iter().any(|item| item.plan_id == plan_id)
    }

    pub fn get_exchange_operators_for_plan_id(&self, plan_id: usize) -> Vec<Operator> {
        let mut operators: Vec<Operator> = Vec::new();
        for op in &self.operators {
            if op.plan_id == plan_id && matches!(op.operator_type, OperatorType::Exchange { .. }) {
                operators.push(op.clone());
            }
        }
        operators
    }

    pub fn find_operator_by_id(&self, op_id: &String) -> Option<&Operator> {
        for op in self.get_operators_ref() {
            if op.id == *op_id {
                return Some(op);
            }
        }
        None
    }

    pub fn find_mut_operator_by_id(&mut self, op_id: &String) -> Option<&mut Operator> {
        for op in self.get_operators_ref_mut() {
            if op.id == *op_id {
                return Some(op);
            }
        }
        None
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

    pub fn get_pipelines_ref<'a>(&'a self) -> &'a Vec<Pipeline> {
        &self.pipelines
    }

    pub fn find_operator(&self, pipeline_id: String, operator_id: String) -> Option<&Operator> {
        for pipeline in self.get_pipelines_ref() {
            if pipeline.id != pipeline_id {
                continue;
            }
            if let Some(op) = pipeline.find_operator_by_id(&operator_id) {
                return Some(op);
            }
        }
        None
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
                iters += 1;
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
                    for node_id in inbound_nodes {
                        node_id_stack.push(node_id.clone());
                    }
                    iters += 1;
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

        // optimize pipeline
        self.add_sampling_configs_to_exchanges(pipeline)?;

        let mut physical_plan = PhysicalPlan::new();
        physical_plan.add_pipeline(pipeline.clone());
        Ok(physical_plan)
    }

    fn add_sampling_configs_to_exchanges(&mut self, pipeline: &mut Pipeline) -> Result<()> {
        let mut exchange_configs = Vec::new();
        for op in pipeline.get_operators_ref() {
            match &op.operator_type {
                OperatorType::Exchange {
                    outbound_producer_ids,
                    ..
                } => {
                    for outbound_op_id in outbound_producer_ids {
                        let outbound_op =
                            if let Some(op) = pipeline.find_operator_by_id(outbound_op_id) {
                                op
                            } else {
                                continue;
                            };

                        let config = match &outbound_op.operator_type {
                            OperatorType::Producer { task, .. } => match task {
                                OperatorTask::Partition {
                                    partition_range_method, ..
                                } => match partition_range_method {
                                    PartitionRangeMethod::SampleDistribution { 
                                        sample_rate, 
                                        min_sampled_rows, 
                                        exchange_queue_name, 
                                        .. 
                                    } => {
                                        ExchangeRecordQueueConfig {
                                            producer_id: outbound_op.id.clone(),
                                            queue_name: exchange_queue_name.clone(),
                                            input_queue_names: vec!["default".to_string()],
                                            sampling_method: ExchangeRecordQueueSamplingMethod::PercentageWithReserve { 
                                                sample_rate: sample_rate.clone(), 
                                                min_rows: min_sampled_rows.clone(),
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    continue;
                                }
                            },
                            _ => {
                                continue;
                            }
                        };
                        
                        exchange_configs.push((op.id.clone(), config));
                    }
                }
                _ => {
                    continue;
                }
            }
        }


        for (op_id, config) in exchange_configs {
            let op = pipeline.find_mut_operator_by_id(&op_id).expect("expected operator to exist");
            match &mut op.operator_type {
                OperatorType::Exchange { record_queue_configs, .. } => {
                    record_queue_configs.push(config);
                }
                _ => {
                    continue;
                }
            }
        }

        Ok(())
    }

    fn build_operators(&mut self, lpn: &LogicalPlanNode) -> Result<Vec<Operator>> {
        match lpn.node {
            LogicalPlanNodeType::Materialize { .. } => self.build_materialize_operators(lpn),
            LogicalPlanNodeType::Filter { .. } => self.build_filter_operators(lpn),
            LogicalPlanNodeType::OrderBy { .. } => self.build_order_by_operators(lpn),
            LogicalPlanNodeType::TableFunc { .. } => self.build_table_func_operators(lpn),
            _ => Err(PhysicalPlanError::NotImplemented(format!(
                "LogicalPlanNodeType isn't implemented to build resources: {:?}",
                lpn.node
            ))
            .into()),
        }
    }

    pub(crate) fn build_order_by_operators(
        &mut self,
        lpn: &LogicalPlanNode,
    ) -> Result<Vec<Operator>> {
        let num_partitions: usize = 3;

        let (part_op_task, sort_op_task) = match &lpn.node.clone() {
            LogicalPlanNodeType::OrderBy { exprs } => (
                OperatorTask::Partition {
                    partition_method: PartitionMethod::OrderByExprs {
                        exprs: exprs.clone(),
                    },
                    partition_range_method: PartitionRangeMethod::SampleDistribution {
                        sample_rate: 0.10,
                        exchange_queue_name: "partition_sample".to_string(), 
                        min_sampled_rows: 10_000,
                        max_sampled_rows: 1_000_000,
                        num_partitions,
                    },
                },
                OperatorTask::Sort {
                    exprs: exprs.clone(),
                    num_partitions,
                },
            ),
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
        let inbound_exchange_ids = self.get_inbound_operators(&lpn, "exchange")?;

        let part_producer_id = self.new_operator_id(lpn.id, "producer");
        let part_exchange_id = self.new_internal_operator_id(lpn.id, 0, "exchange");

        let sort_producer_id = self.new_internal_operator_id(lpn.id, 1, "producer");
        let sort_exchange_id = self.new_operator_id(lpn.id, "exchange");

        // partition producer operator /////////////////////////////////
        let part_producer = Operator {
            id: part_producer_id.clone(),
            plan_id: lpn.id,
            operator_type: OperatorType::Producer {
                task: part_op_task.clone(),
                outbound_exchange_id: part_exchange_id.clone(),
                inbound_exchange_ids: inbound_exchange_ids.clone(),
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 1000,
                memory_in_mib: 500,
            },
        };
        let part_exchange = Operator {
            id: part_exchange_id.clone(),
            plan_id: lpn.id,
            operator_type: OperatorType::Exchange {
                outbound_producer_ids: vec![sort_producer_id.clone()],
                inbound_producer_ids: vec![part_producer.id.clone()],
                record_queue_configs: vec![ExchangeRecordQueueConfig {
                    producer_id: sort_producer_id.clone(),
                    queue_name: "default".to_string(),
                    input_queue_names: Vec::new(),
                    sampling_method: ExchangeRecordQueueSamplingMethod::All,
                }],
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 500,
                memory_in_mib: 500,
            },
        };

        // sort producer operator /////////////////////////////////////
        let sort_producer = Operator {
            id: sort_producer_id.clone(),
            plan_id: lpn.id,
            operator_type: OperatorType::Producer {
                task: sort_op_task.clone(),
                outbound_exchange_id: sort_exchange_id.clone(),
                inbound_exchange_ids: vec![part_exchange.id.clone()],
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 1000,
                memory_in_mib: 500,
            },
        };
        let sort_exchange = Operator {
            id: sort_exchange_id.clone(),
            plan_id: lpn.id,
            operator_type: OperatorType::Exchange {
                outbound_producer_ids: self.get_outbound_operators(lpn, "producer")?,
                inbound_producer_ids: vec![sort_producer.id.clone()],
                record_queue_configs: self
                    .get_outbound_operators(lpn, "producer")?
                    .iter()
                    .map(|op_id| ExchangeRecordQueueConfig {
                        producer_id: op_id.clone(),
                        queue_name: "default".to_string(),
                        input_queue_names: Vec::new(),
                        sampling_method: ExchangeRecordQueueSamplingMethod::All,
                    })
                    .collect(),
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 500,
                memory_in_mib: 500,
            },
        };

        operators.push(part_producer);
        operators.push(part_exchange);
        operators.push(sort_producer);
        operators.push(sort_exchange);

        Ok(operators)
    }

    pub(crate) fn build_table_func_operators(
        &mut self,
        lpn: &LogicalPlanNode,
    ) -> Result<Vec<Operator>> {
        let op_task = match lpn.node.clone() {
            LogicalPlanNodeType::TableFunc { alias, name, args } => OperatorTask::TableFunc {
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
            operator_type: OperatorType::Producer {
                task: op_task.clone(),
                outbound_exchange_id: self.new_operator_id(lpn.id, "exchange"),
                inbound_exchange_ids: Vec::new(),
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 1000,
                memory_in_mib: 500,
            },
        };
        let exchange = Operator {
            id: self.new_operator_id(lpn.id, "exchange"),
            plan_id: lpn.id,
            operator_type: OperatorType::Exchange {
                outbound_producer_ids: self.get_outbound_operators(lpn, "producer")?,
                inbound_producer_ids: vec![producer.id.clone()],
                record_queue_configs: self
                    .get_outbound_operators(lpn, "producer")?
                    .iter()
                    .map(|op_id| ExchangeRecordQueueConfig {
                        producer_id: op_id.clone(),
                        queue_name: "default".to_string(),
                        input_queue_names: Vec::new(),
                        sampling_method: ExchangeRecordQueueSamplingMethod::All,
                    })
                    .collect(),
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 500,
                memory_in_mib: 500,
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

        let op_task = OperatorTask::Filter { expr: filter_expr };
        let mut operators: Vec<Operator> = Vec::new();

        let producer = Operator {
            id: self.new_operator_id(lpn.id, "producer"),
            plan_id: lpn.id,
            operator_type: OperatorType::Producer {
                task: op_task.clone(),
                outbound_exchange_id: self.new_operator_id(lpn.id, "exchange"),
                inbound_exchange_ids: self.get_inbound_operators(&lpn, "exchange")?,
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 1000,
                memory_in_mib: 500,
            },
        };
        let exchange = Operator {
            id: self.new_operator_id(lpn.id, "exchange"),
            plan_id: lpn.id,
            operator_type: OperatorType::Exchange {
                outbound_producer_ids: self.get_outbound_operators(&lpn, "producer")?,
                inbound_producer_ids: vec![producer.id.clone()],
                record_queue_configs: self
                    .get_outbound_operators(lpn, "producer")?
                    .iter()
                    .map(|op_id| ExchangeRecordQueueConfig {
                        producer_id: op_id.clone(),
                        queue_name: "default".to_string(),
                        input_queue_names: Vec::new(),
                        sampling_method: ExchangeRecordQueueSamplingMethod::All,
                    })
                    .collect(),
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 500,
                memory_in_mib: 500,
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

        let op_task = OperatorTask::MaterializeFiles {
            data_format: DataFormat::Parquet,
            fields,
        };
        let mut operators: Vec<Operator> = Vec::new();

        let producer = Operator {
            id: self.new_operator_id(lpn.id, "producer"),
            plan_id: lpn.id,
            operator_type: OperatorType::Producer {
                task: op_task.clone(),
                outbound_exchange_id: self.new_operator_id(lpn.id, "exchange"),
                inbound_exchange_ids: self.get_inbound_operators(lpn, "exchange")?,
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 1000,
                memory_in_mib: 500,
            },
        };
        let exchange = Operator {
            id: self.new_operator_id(lpn.id, "exchange"),
            plan_id: lpn.id,
            operator_type: OperatorType::Exchange {
                outbound_producer_ids: self.get_outbound_operators(lpn, "producer")?,
                inbound_producer_ids: vec![producer.id.clone()],
                record_queue_configs: self
                    .get_outbound_operators(lpn, "producer")?
                    .iter()
                    .map(|op_id| ExchangeRecordQueueConfig {
                        producer_id: op_id.clone(),
                        queue_name: "default".to_string(),
                        input_queue_names: Vec::new(),
                        sampling_method: ExchangeRecordQueueSamplingMethod::All,
                    })
                    .collect(),
            },
            compute: OperatorCompute {
                instances: 1,
                cpu_in_thousandths: 500,
                memory_in_mib: 500,
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

    fn new_internal_operator_id(
        &self,
        plan_node_id: usize,
        internal_id: usize,
        task_type_name: &str,
    ) -> String {
        format!(
            "operator_p{}_in{}_{}",
            plan_node_id, internal_id, task_type_name
        )
    }

    fn new_pipeline_id(&mut self) -> String {
        let id = self.pipeline_idx;
        let sid = format!("pipeline_{}", id);
        self.pipeline_idx += 1;
        sid
    }
}
