#[derive(Debug)]
pub struct Query {
    pub query: String,
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
