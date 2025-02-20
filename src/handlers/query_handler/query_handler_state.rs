use std::u128;

use anyhow::Result;
use serde::{Deserialize, Serialize};
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
    #[error("operator not in phytical plan: {0}")]
    OperatorNotInPhysicalPlan(String),
    #[error("expected producer operator type")]
    ExpectedProducerOperatorType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    Queued,
    SendingToWorker,
    Running,
    SentShutdown(chrono::DateTime<chrono::Utc>),
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
    pub fn available(&self) -> bool {
        match self {
            Status::Running => true,
            _ => false,
        }
    }
    pub fn to_string(&self) -> String {
        match self {
            Status::Queued => "Queued".to_string(),
            Status::SendingToWorker => "SendingToWorker".to_string(),
            Status::Running => "Running".to_string(),
            Status::SentShutdown(t) => format!("SentShutdown({})", t),
            Status::Complete => "Complete".to_string(),
            Status::Error(_) => "Error(...)".to_string(),
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

    pub fn find_query(&self, query_id: &u128) -> Result<&Query> {
        self.queries
            .iter()
            .find(|item| item.id == *query_id)
            .ok_or(QueryHandlerStateError::QueryNotFound(query_id.clone()).into())
    }

    pub fn find_query_mut(&mut self, query_id: &u128) -> Result<&mut Query> {
        self.queries
            .iter_mut()
            .find(|item| item.id == *query_id)
            .ok_or(QueryHandlerStateError::QueryNotFound(query_id.clone()).into())
    }

    pub fn find_operator_instance<'a>(
        &'a self,
        query: &'a Query,
        op_instance_id: &u128,
    ) -> Result<&'a OperatorInstance> {
        query
            .operator_instances
            .iter()
            .find(|item| item.id == *op_instance_id)
            .ok_or(QueryHandlerStateError::OperatorInstanceNotFound(op_instance_id.clone()).into())
    }

    pub fn find_operator_instance_mut<'a>(
        &'a mut self,
        query: &'a mut Query,
        op_instance_id: u128,
    ) -> Result<&'a mut OperatorInstance> {
        query
            .operator_instances
            .iter_mut()
            .find(|item| item.id == op_instance_id.clone())
            .ok_or(QueryHandlerStateError::OperatorInstanceNotFound(op_instance_id).into())
    }

    pub fn get_operator_instance(
        &self,
        query_id: &u128,
        op_instance_id: &u128,
    ) -> Result<OperatorInstance> {
        let query = self.find_query(query_id)?;
        let op_in = self.find_operator_instance(query, op_instance_id)?;
        Ok(op_in.clone())
    }

    pub fn get_operator_instances(
        &self,
        query_id: &u128,
        op_id: &String,
    ) -> Result<Vec<OperatorInstance>> {
        let query = self.find_query(query_id)?;
        let op_instances: Vec<OperatorInstance> = query
            .operator_instances
            .iter()
            .filter(|item| item.operator_id == *op_id)
            .map(|item| item.clone())
            .collect();
        Ok(op_instances)
    }

    pub fn update_operator_instance_status(
        &mut self,
        query_id: &u128,
        op_instance_id: &u128,
        status: Status,
    ) -> Result<()> {
        for query in &mut self.queries {
            if query.id != *query_id {
                continue;
            }
            for op_in in &mut query.operator_instances {
                if op_in.id != *op_instance_id {
                    continue;
                }
                op_in.status = status;
                return Ok(());
            }
        }
        Err(QueryHandlerStateError::OperatorInstanceNotFound(op_instance_id.clone()).into())
    }

    pub fn update_query_status(&mut self, query_id: &u128, status: Status) -> Result<()> {
        for query in &mut self.queries {
            if query.id != *query_id {
                continue;
            }
            query.status = status;
            return Ok(());
        }
        Err(QueryHandlerStateError::QueryNotFound(query_id.clone()).into())
    }

    pub fn get_outbound_exchange_id(&self, query_id: &u128, op_in_id: &u128) -> Result<String> {
        let query = self.find_query(query_id)?;
        let op_in = self.find_operator_instance(query, op_in_id)?;
        if let Some(op) = query
            .physical_plan
            .get_operator(op_in.pipeline_id.clone(), op_in.operator_id.clone())
        {
            match &op.operator_type {
                planner::OperatorType::Producer {
                    outbound_exchange_id,
                    ..
                } => Ok(outbound_exchange_id.clone()),
                planner::OperatorType::Exchange { .. } => {
                    Err(QueryHandlerStateError::ExpectedProducerOperatorType.into())
                }
            }
        } else {
            Err(QueryHandlerStateError::OperatorNotInPhysicalPlan(op_in.operator_id.clone()).into())
        }
    }

    pub fn get_inbound_exchange_ids(
        &self,
        query_id: &u128,
        op_in_id: &u128,
    ) -> Result<Vec<String>> {
        let query = self.find_query(query_id)?;
        let op_in = self.find_operator_instance(query, op_in_id)?;
        if let Some(op) = query
            .physical_plan
            .get_operator(op_in.pipeline_id.clone(), op_in.operator_id.clone())
        {
            match &op.operator_type {
                planner::OperatorType::Producer {
                    inbound_exchange_ids,
                    ..
                } => Ok(inbound_exchange_ids.clone()),
                planner::OperatorType::Exchange { .. } => {
                    Err(QueryHandlerStateError::ExpectedProducerOperatorType.into())
                }
            }
        } else {
            Err(QueryHandlerStateError::OperatorNotInPhysicalPlan(op_in.operator_id.clone()).into())
        }
    }

    pub fn operator_instance_is_producer(&self, query_id: &u128, op_in_id: &u128) -> Result<bool> {
        let query = self.find_query(query_id)?;
        let op_in = self.find_operator_instance(query, op_in_id)?;

        let op = query
            .physical_plan
            .get_operator(op_in.pipeline_id.clone(), op_in.operator_id.clone());
        match op {
            Some(op) => match op.operator_type {
                planner::OperatorType::Producer { .. } => Ok(true),
                planner::OperatorType::Exchange { .. } => Ok(false),
            },
            None => Err(QueryHandlerStateError::OperatorNotInPhysicalPlan(
                op_in.operator_id.clone(),
            )
            .into()),
        }
    }

    pub fn all_operator_instances_complete(
        &mut self,
        query_id: &u128,
        op_in_id: &u128,
    ) -> Result<bool> {
        let query = self.find_query(query_id)?;
        let op_in = self.find_operator_instance(query, op_in_id)?;
        let any_not_complete = query
            .operator_instances
            .iter()
            .find(|op_in_item| {
                op_in_item.operator_id == op_in.operator_id && op_in_item.status != Status::Complete
            })
            .is_some();
        Ok(!any_not_complete)
    }

    pub fn get_exchange_ids_without_any_consumers(&self, query_id: &u128) -> Result<Vec<String>> {
        let query = self.find_query(query_id)?;

        let mut exchange_ids: Vec<String> = Vec::new();
        for pipeline in query.physical_plan.get_pipelines_ref() {
            'op_loop: for op in pipeline.get_operators_ref() {
                match &op.operator_type {
                    planner::OperatorType::Exchange {
                        outbound_producer_ids,
                        inbound_producer_ids,
                        ..
                    } => {
                        for op_id in outbound_producer_ids {
                            if self
                                .get_operator_instances(query_id, op_id)?
                                .iter()
                                .find(|op_in| op_in.status != Status::Complete)
                                .is_some()
                            {
                                continue 'op_loop;
                            }
                        }

                        for op_id in inbound_producer_ids {
                            if self
                                .get_operator_instances(query_id, op_id)?
                                .iter()
                                .find(|op_in| op_in.status != Status::Complete)
                                .is_some()
                            {
                                continue 'op_loop;
                            }
                        }

                        exchange_ids.push(op.id.clone());
                    }
                    planner::OperatorType::Producer { .. } => {
                        continue;
                    }
                }
            }
        }

        Ok(exchange_ids)
    }

    pub fn all_query_operator_intances_have_terminal_status(
        &self,
        query_id: &u128,
    ) -> Result<bool> {
        let query = self.find_query(query_id)?;
        for pipeline in query.physical_plan.get_pipelines_ref() {
            for op in pipeline.get_operators_ref() {
                let op_ins = self.get_operator_instances(query_id, &op.id)?;
                if op_ins
                    .iter()
                    .find(|op_in| !op_in.status.terminal())
                    .is_some()
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub fn refresh_query_status(&mut self, query_id: &u128) -> Result<Status> {
        let query = self.find_query(query_id)?;

        if query.status == Status::Queued {
            return Ok(Status::Queued);
        } else if query.status.terminal() {
            return Ok(query.status.clone());
        }

        let mut status = Status::Complete;
        let mut any_still_inprogress = false;
        for pipeline in query.physical_plan.get_pipelines_ref() {
            for op in pipeline.get_operators_ref() {
                let op_ins = self.get_operator_instances(query_id, &op.id)?;
                if op_ins
                    .iter()
                    .find(|op_in| !op_in.status.terminal())
                    .is_some()
                {
                    any_still_inprogress = true;
                    break;
                } else if op_ins
                    .iter()
                    .find(|op_in| matches!(op_in.status, Status::Error(_)))
                    .is_some()
                {
                    status = Status::Error("operator instance error".to_string());
                } else {
                    status = Status::Complete;
                }
            }
        }

        let query = self.find_query_mut(query_id)?;
        if any_still_inprogress {
            query.status = Status::Running;
            Ok(Status::Running)
        } else {
            query.status = status.clone();
            Ok(status)
        }
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

                if query.status == Status::Queued {
                    query.status = Status::Running;
                }
                op_in.status = Status::SendingToWorker;
                compute.subtract_single_operator_compute(&op_in_compute);
                result.push((query.id.clone(), op_in, operator));
            }
        }

        result
    }
}
