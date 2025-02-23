use std::sync::Arc;

use super::AsyncQueryClient;

pub struct QueryDataIterator {
    client: Arc<AsyncQueryClient>,
    query_id: u128,
    file_idx: u64,
    file_row_group_idx: u64,
}

impl QueryDataIterator {
    fn new(
        client: Arc<AsyncQueryClient>,
        query_id: u128,
        start_file_idx: u64,
        start_file_row_group_idx: u64,
    ) -> QueryDataIterator {
        QueryDataIterator {
            client,
            query_id,
            file_idx: start_file_idx,
            file_row_group_idx: start_file_row_group_idx,
        }
    }
}
