use uuid::Uuid;

use crate::planner;

#[derive(Debug, PartialEq, Eq)]
pub enum Status {
    Queued,
    Running,
    Complete,
    Error(String),
}

#[derive(Debug)]
pub struct OperatorInstanceGroup {
    pub operator: planner::Operator,
    pub instances: Vec<OperatorInstance>,
}

impl OperatorInstanceGroup {
    pub fn new(op: planner::Operator) -> OperatorInstanceGroup {
        let instances = op.instances;
        OperatorInstanceGroup {
            operator: op,
            instances: (0..instances)
                .into_iter()
                .map(|_| OperatorInstance::new())
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct OperatorInstance {
    pub id: u128,
    pub status: Status,
}

impl OperatorInstance {
    pub fn new() -> OperatorInstance {
        OperatorInstance {
            id: Uuid::new_v4().as_u128(),
            status: Status::Queued,
        }
    }
}

#[derive(Debug)]
pub struct Query {
    pub id: u128,
    pub query: String,
    pub physical_plan: planner::PhysicalPlan,
    pub status: Status,

    pub operator_instances: Vec<OperatorInstanceGroup>,
}

impl Query {
    pub fn new(query: String, physical_plan: planner::PhysicalPlan) -> Query {
        let mut query = Query {
            id: Uuid::new_v4().as_u128(),
            query,
            physical_plan,
            status: Status::Queued,
            operator_instances: Vec::new(),
        };
        query.add_operator_instances_from_physical_plan();
        query
    }

    pub fn add_operator_instances_from_physical_plan(&mut self) -> &Self {
        for pipeline in self.physical_plan.get_pipelines() {
            for op in pipeline.get_operators() {
                for _ in 0..op.instances {
                    self.operator_instances
                        .push(OperatorInstanceGroup::new(op.clone()))
                }
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
        self.queries.push(query);
    }

    pub fn get_available_operator_instance_ids(&self, query_id: u128) -> Vec<u128> {
        let query = self
            .queries
            .iter()
            .filter(|item| item.id == query_id)
            .next();
        if let Some(query) = query {
            return query
                .operator_instances
                .iter()
                .map(|item| {
                    item.instances
                        .iter()
                        .filter(|inner_item| inner_item.status == Status::Queued)
                        .map(|inner_item| inner_item.id)
                        .collect::<Vec<u128>>()
                })
                .flatten()
                .collect();
        }

        vec![]
    }
}
