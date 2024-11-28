use sqlparser::ast::{Expr, FunctionArg};

use crate::planner::logical_planner::{LogicalPlan, PlanNode};

#[derive(Clone, Debug, PartialEq)]
pub enum PhysicalPlanNode {
    // table source stage
    TableFuncOperator {
        id: usize,
        name: String,
    },
    TableFuncProducer {
        id: usize,
        name: String,
        write_data_location_id: String,
        func_name: String,
        args: Vec<FunctionArg>,
    },
    TableOperator {
        id: usize,
        name: String,
    },
    TableProducer {
        id: usize,
        name: String,
        write_data_location_id: String,
    },
    // filter stage
    FilterOperator {
        id: usize,
        name: String,
        manage_producer_id: usize,
    },
    FilterProducer {
        id: usize,
        name: String,
        read_data_location_ids: Vec<String>,
        write_data_location_id: String,
        expr: Expr,
    },
    // materialize stage
    MaterializeOperator {
        id: usize,
        name: String,
    },
    MaterializeProducer {
        id: usize,
        name: String,
        read_data_location_ids: Vec<String>,
        write_data_location_id: String,
    },
    // common
    // an exchange manage the state of reading from
    // a producer. It allows multiple downstream producers
    // to reach from the same data location. If there aren't
    // anymore consumers the exchange notifies the operator
    // to shutdown the producers, and itself, the exchange
    // exits when the operator exits.
    Exchange {
        id: usize,
        name: String,
        source_producer_id: usize,
        source_operator_id: usize,
    },
}
