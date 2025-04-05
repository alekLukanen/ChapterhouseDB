use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::handlers::message_handler::messages;

use super::AsyncQueryClient;

#[derive(Debug, Error)]
pub enum TuiQueryDataIteratorError {
    #[error("query not found: {0}")]
    QueryNotFound(u128),
    #[error("record row group not found: file_idx={0}, file_row_group_idx={1}")]
    RecordRowGroupNotFound(u64, u64),
    #[error("database error response: {0}")]
    DatabaseErrorResp(String),
}

pub struct VisibleWindow {
    pub min_offset: (u64, u64, u64),
    pub max_offset: (u64, u64, u64),
}

#[derive(Debug)]
pub struct TuiQueryDataIterator {
    client: Arc<AsyncQueryClient>,
    query_id: u128,
    start_file_idx: u64,
    start_file_row_group_idx: u64,
    start_row_idx: u64,
    limit: u64,
    max_wait: chrono::Duration,
}

impl TuiQueryDataIterator {
    pub fn new(
        client: Arc<AsyncQueryClient>,
        query_id: u128,
        start_file_idx: u64,
        start_file_row_group_idx: u64,
        start_row_idx: u64,
        limit: u64,
        max_wait: chrono::Duration,
    ) -> TuiQueryDataIterator {
        TuiQueryDataIterator {
            client,
            query_id,
            start_file_idx,
            start_file_row_group_idx,
            start_row_idx,
            limit,
            max_wait,
        }
    }

    pub async fn first(
        &mut self,
        ct: CancellationToken,
    ) -> Result<Option<(Arc<arrow::array::RecordBatch>, Vec<(u64, u64, u64)>)>> {
        self.get_next(
            ct.clone(),
            self.start_file_idx,
            self.start_file_row_group_idx,
            self.start_row_idx,
            true,
        )
        .await
    }

    pub async fn next(
        &mut self,
        ct: CancellationToken,
        visible_window: VisibleWindow,
        forward: bool,
    ) -> Result<Option<(Arc<arrow::array::RecordBatch>, Vec<(u64, u64, u64)>)>> {
        match self.get_next_offset(visible_window, forward) {
            Some(offset) => {
                self.get_next(ct.clone(), offset.0, offset.1, offset.2, forward)
                    .await
            }
            None => Ok(None),
        }
    }

    async fn get_next(
        &self,
        ct: CancellationToken,
        file_idx: u64,
        file_row_group_idx: u64,
        row_idx: u64,
        forward: bool,
    ) -> Result<Option<(Arc<arrow::array::RecordBatch>, Vec<(u64, u64, u64)>)>> {
        let resp = self
            .client
            .get_query_data(
                ct,
                &self.query_id,
                file_idx,
                file_row_group_idx,
                row_idx,
                self.limit,
                forward,
                true,
                self.max_wait.clone(),
            )
            .await?;
        match resp {
            messages::query::GetQueryDataResp::Record {
                record,
                record_offsets,
            } => Ok(Some((record, record_offsets))),
            messages::query::GetQueryDataResp::ReachedEndOfFiles => Ok(None),
            messages::query::GetQueryDataResp::QueryNotFound => {
                Err(TuiQueryDataIteratorError::QueryNotFound(self.query_id.clone()).into())
            }
            messages::query::GetQueryDataResp::RecordRowGroupNotFound => Err(
                TuiQueryDataIteratorError::RecordRowGroupNotFound(file_idx, file_row_group_idx)
                    .into(),
            ),
            messages::query::GetQueryDataResp::Error { err } => {
                Err(TuiQueryDataIteratorError::DatabaseErrorResp(err).into())
            }
        }
    }

    pub fn get_next_offset(
        &self,
        visible_window: VisibleWindow,
        forward: bool,
    ) -> Option<(u64, u64, u64)> {
        if forward {
            let offset = visible_window.max_offset;
            Some((offset.0, offset.1, offset.2 + 1))
        } else {
            let offset = visible_window.min_offset;
            if offset.0 == 0 && offset.1 == 0 && offset.2 == 0 {
                None
            } else if offset.1 == 0 && offset.2 == 0 {
                Some((offset.0 - 1, std::u64::MAX, std::u64::MAX))
            } else if offset.2 == 0 {
                Some((offset.0, offset.1 - 1, std::u64::MAX))
            } else {
                Some((offset.0, offset.1, offset.2 - 1))
            }
        }
    }
}
