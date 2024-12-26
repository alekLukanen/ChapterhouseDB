use std::u128;

use anyhow::Result;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    handlers::operator_handler::TotalOperatorCompute,
    planner::{self, Operator},
};

#[derive(Debug, Clone, Error)]
pub enum QueryHandlerStateError {
    #[error("query not found: {0}")]
    QueryNotFound(u128),
    #[error("operator instance not found: {0}")]
    OperatorInstanceNotFound(u128),
    #[error("operator instance group not found for query_id {0} and instance {1}")]
    OperatorInstanceGroupNotFound(u128, u128),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Queued,
    Running,
    Complete,
    Error(String),
}

impl Status {
    pub fn terminal(&self) -> bool {
        match self {
            Status::Complete => true,
            Status::Error(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperatorInstance {
    pub id: u128,
    pub status: Status,
    pub pipeline_id: String,
    pub operator_id: String,
}

impl OperatorInstance {
    pub fn new(pipeline_id: String, operator_id: String) -> OperatorInstance {
        OperatorInstance {
            id: Uuid::new_v4().as_u128(),
            status: Status::Queued,
            pipeline_id,
            operator_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Query {
    pub id: u128,
    pub query: String,
    pub physical_plan: planner::PhysicalPlan,
    pub status: Status,

    pub operator_instances: Vec<OperatorInstance>,
}

impl Query {
    pub fn new(query: String, physical_plan: planner::PhysicalPlan) -> Query {
        let query = Query {
            id: Uuid::new_v4().as_u128(),
            query,
            physical_plan,
            status: Status::Queued,
            operator_instances: Vec::new(),
        };
        query
    }

    pub fn init(&mut self) -> &Self {
        self.add_operator_instances_from_physical_plan()
    }

    fn add_operator_instances_from_physical_plan(&mut self) -> &Self {
        for pipeline in self.physical_plan.get_pipelines_ref() {
            for op in pipeline.get_operators_ref() {
                self.operator_instances
                    .push(OperatorInstance::new(pipeline.id.clone(), op.id.clone()));
            }
        }
        self
    }
}

#[derive(Debug)]
pub struct QueryHandlerState {
    queries: Vec<Query>,
}

impl QueryHandlerState {
    pub fn new() -> QueryHandlerState {
        QueryHandlerState {
            queries: Vec::new(),
        }
    }

    pub fn add_query(&mut self, query: Query) {
        self.queries.push(query.clone());
    }

    fn find_query(&self, query_id: u128) -> Result<&Query> {
        self.queries
            .iter()
            .find(|item| item.id == query_id)
            .ok_or(QueryHandlerStateError::QueryNotFound(query_id).into())
    }

    fn find_query_mut(&mut self, query_id: u128) -> Result<&mut Query> {
        self.queries
            .iter_mut()
            .find(|item| item.id == query_id)
            .ok_or(QueryHandlerStateError::QueryNotFound(query_id).into())
    }

    fn find_operator_instance<'a>(
        &'a self,
        query: &'a Query,
        op_instance_id: u128,
    ) -> Result<&'a OperatorInstance> {
        query
            .operator_instances
            .iter()
            .find(|item| item.id == op_instance_id.clone())
            .ok_or(QueryHandlerStateError::OperatorInstanceNotFound(op_instance_id).into())
    }

    pub fn get_operator_instance(
        &self,
        query_id: u128,
        op_instance_id: u128,
    ) -> Result<OperatorInstance> {
        let query = self.find_query(query_id)?;
        let op_in = self.find_operator_instance(query, op_instance_id)?;
        Ok(op_in.clone())
    }

    pub fn claim_operator_instances_up_to_compute_available(
        &mut self,
        available_compute: &TotalOperatorCompute,
    ) -> Vec<(u128, &OperatorInstance, &Operator)> {
        let mut compute = available_compute.clone();

        let mut result: Vec<(u128, &OperatorInstance, &Operator)> = Vec::new();
        for query in &mut self.queries {
            if compute.any_depleated() {
                break;
            }
            if query.status.terminal() {
                continue;
            }

            for op_in in &mut query.operator_instances {
                let operator = if let Some(operator) = query
                    .physical_plan
                    .get_operator(op_in.pipeline_id.clone(), op_in.operator_id.clone())
                {
                    operator
                } else {
                    continue;
                };
                let op_in_compute = operator.compute.clone();
                if op_in.status == Status::Running || op_in.status.terminal() {
                    continue;
                } else if compute
                    .clone()
                    .subtract_single_operator_compute(&op_in_compute)
                    .any_depleated()
                {
                    continue;
                }

                query.status = Status::Running;
                op_in.status = Status::Running;
                compute.subtract_single_operator_compute(&op_in_compute);
                result.push((query.id.clone(), op_in, operator));
            }
        }

        result
    }
}
