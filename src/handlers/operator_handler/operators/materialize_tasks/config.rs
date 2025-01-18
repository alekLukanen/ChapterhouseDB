use crate::planner;

#[derive(Debug)]
pub struct MaterializeConfig {
    pub data_format: planner::DataFormat,
    pub fields: Vec<sqlparser::ast::SelectItem>,

    pub outbound_exchange_id: String,
    pub inbound_exchange_ids: Vec<String>,
}
