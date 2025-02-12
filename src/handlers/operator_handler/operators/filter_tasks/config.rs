#[derive(Debug)]
pub struct FilterConfig {
    pub expr: sqlparser::ast::Expr,

    pub outbound_exchange_id: String,
    pub inbound_exchange_ids: Vec<String>,
}
