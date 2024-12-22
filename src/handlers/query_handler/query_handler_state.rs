use uuid::Uuid;

use crate::planner::PhysicalPlan;

#[derive(Debug)]
pub struct Query {
    pub id: u128,
    pub query: String,
    pub physical_plan: PhysicalPlan,
}

impl Query {
    pub fn new(query: String, physical_plan: PhysicalPlan) -> Query {
        Query {
            id: Uuid::new_v4().as_u128(),
            query,
            physical_plan,
        }
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
}
