use crate::planner::{PartitionMethod, PartitionRangeMethod};

#[derive(Debug)]
pub struct PartitionConfig {
    pub partition_method: PartitionMethod,
    pub partition_range_method: PartitionRangeMethod,

    pub outbound_exchange_id: String,
    pub inbound_exchange_ids: Vec<String>,
}
