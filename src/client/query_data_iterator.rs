use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::handlers::message_handler::messages;

use super::AsyncQueryClient;

#[derive(Debug, Error)]
pub enum QueryDataIteratorError {
    #[error("query not found: {0}")]
    QueryNotFound(u128),
    #[error("record row group not found: file_idx={0}, file_row_group_idx={1}")]
    RecordRowGroupNotFound(u64, u64),
    #[error("database error response: {0}")]
    DatabaseErrorResp(String),
}

#[derive(Debug)]
pub struct QueryDataIterator {
    client: Arc<AsyncQueryClient>,
    query_id: u128,
    file_idx: u64,
    file_row_group_idx: u64,
    row_idx: u64,
    limit: u64,
    forward: bool,
    max_wait: chrono::Duration,
}

impl QueryDataIterator {
    pub fn new(
        client: Arc<AsyncQueryClient>,
        query_id: u128,
        start_file_idx: u64,
        start_file_row_group_idx: u64,
        start_row_idx: u64,
        limit: u64,
        forward: bool,
        max_wait: chrono::Duration,
    ) -> QueryDataIterator {
        QueryDataIterator {
            client,
            query_id,
            file_idx: start_file_idx,
            file_row_group_idx: start_file_row_group_idx,
            row_idx: start_row_idx,
            limit,
            forward,
            max_wait,
        }
    }

    pub async fn next(
        &mut self,
        ct: CancellationToken,
    ) -> Result<Option<Arc<arrow::array::RecordBatch>>> {
        let resp = self
            .client
            .get_query_data(
                ct,
                &self.query_id,
                self.file_idx,
                self.file_row_group_idx,
                self.row_idx,
                self.limit,
                self.forward,
                self.max_wait.clone(),
            )
            .await?;
        match resp {
            messages::query::GetQueryDataResp::Record {
                record,
                record_offsets,
            } => Ok(Some(record)),
            messages::query::GetQueryDataResp::ReachedEndOfFiles => Ok(None),
            messages::query::GetQueryDataResp::QueryNotFound => {
                Err(QueryDataIteratorError::QueryNotFound(self.query_id.clone()).into())
            }
            messages::query::GetQueryDataResp::RecordRowGroupNotFound => {
                Err(QueryDataIteratorError::RecordRowGroupNotFound(
                    self.file_idx,
                    self.file_row_group_idx,
                )
                .into())
            }
            messages::query::GetQueryDataResp::Error { err } => {
                Err(QueryDataIteratorError::DatabaseErrorResp(err).into())
            }
        }
    }

    pub fn get_next_offset(&self, rec_offsets: Vec<(u64, u64, u64)>) -> Option<(u64, u64, u64)> {
        let last_offset = if let Some(offset) = rec_offsets.last() {
            offset
        } else {
            return None;
        };

        if self.forward {
            Some((last_offset.0, last_offset.1, last_offset.2 + 1))
        } else {
            if last_offset.0 == 0 && last_offset.1 == 0 && last_offset.2 == 0 {
                None
            } else if last_offset.1 == 0 && last_offset.2 == 0 {
                Some((last_offset.0 - 1, std::u64::MAX, std::u64::MAX))
            } else if last_offset.2 == 0 {
                Some((last_offset.0, last_offset.1 - 1, std::u64::MAX))
            } else {
                Some((last_offset.0, last_offset.1, last_offset.2 - 1))
            }
        }
    }
}
