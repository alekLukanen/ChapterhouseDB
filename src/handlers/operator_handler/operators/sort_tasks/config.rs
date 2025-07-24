#[derive(Debug)]
pub struct SortConfig {
    pub exprs: Vec<sqlparser::ast::OrderByExpr>,
    pub num_partitions: usize,

    pub outbound_exchange_id: String,
    pub inbound_exchange_ids: Vec<String>,
}
