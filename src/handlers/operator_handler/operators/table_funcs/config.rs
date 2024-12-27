use sqlparser::ast::FunctionArg;

#[derive(Debug)]
pub struct TableFuncConfig {
    pub alias: Option<String>,
    pub func_name: String,
    pub args: Vec<FunctionArg>,
    pub max_rows_per_batch: usize,

    pub outbound_exchange_id: String,
    pub inbound_exchange_ids: Vec<String>,
}
