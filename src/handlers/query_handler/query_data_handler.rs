use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use futures::StreamExt;
use parquet::arrow::{
    arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    ParquetRecordBatchStreamBuilder,
};
use parquet_opendal::AsyncReader;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::handlers::{
    message_handler::{
        messages::{
            self,
            message::{Message, MessageName},
        },
        MessageRegistry, Pipe,
    },
    message_router_handler::{MessageConsumer, MessageReceiver, MessageRouterState, Subscriber},
    operator_handler::operators::ConnectionRegistry,
};

#[derive(Debug, Error)]
pub enum QueryDataHandlerError {
    #[error("query file does not exist")]
    QueryFileDoesNotExist,
    #[error("row group {0} does not exist in the file {1}")]
    RowGroupDoesNotExistInTheFile(u64, String),
}

pub struct QueryDataHandler {
    operator_id: u128,
    message_router_state: Arc<Mutex<MessageRouterState>>,
    router_pipe: Pipe,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
    conn_reg: Arc<ConnectionRegistry>,
}

impl QueryDataHandler {
    pub async fn new(
        message_router_state: Arc<Mutex<MessageRouterState>>,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
    ) -> QueryDataHandler {
        let operator_id = Uuid::new_v4().as_u128();

        let router_sender = message_router_state.lock().await.sender();
        let (mut pipe, sender) = Pipe::new_with_existing_sender(router_sender, 10);
        pipe.set_sent_from_operation_id(operator_id);

        let handler = QueryDataHandler {
            operator_id,
            message_router_state,
            router_pipe: pipe,
            sender,
            msg_reg,
            conn_reg,
        };

        handler
    }

    pub fn subscriber(&self) -> Box<dyn Subscriber> {
        Box::new(QueryDataHandlerSubscriber {
            operator_id: self.operator_id.clone(),
            sender: self.sender.clone(),
            msg_reg: self.msg_reg.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        debug!(operator_id = self.operator_id, "started query data handler");

        self.message_router_state
            .lock()
            .await
            .add_internal_subscriber(self.subscriber(), self.operator_id);

        let res = self.inner_async_main(ct.clone()).await;

        self.message_router_state
            .lock()
            .await
            .remove_internal_subscriber(&self.operator_id);
        self.router_pipe.close_receiver();

        debug!(
            operator_id = self.operator_id,
            "closed the query data handler"
        );

        res
    }

    async fn inner_async_main(&mut self, ct: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = self.router_pipe.recv() => {
                    let res = self.handle_message(msg).await;
                    if let Err(err) = res {
                        error!("{:?}", err);
                    }
                }
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_message(&mut self, msg: Message) -> Result<()> {
        match msg.msg.msg_name() {
            MessageName::GetQueryData => self
                .handle_get_query_data(&msg)
                .await
                .context("failed handling get query data"),
            _ => {
                info!("unhandled message received: {:?}", msg);
                Ok(())
            }
        }
    }

    async fn handle_get_query_data(&self, msg: &Message) -> Result<()> {
        let get_data_msg: &messages::query::GetQueryData = self.msg_reg.try_cast_msg(msg)?;

        match self
            .get_record_data(
                get_data_msg.query_id,
                get_data_msg.file_idx,
                get_data_msg.file_row_group_idx,
                get_data_msg.row_idx,
                get_data_msg.limit,
                get_data_msg.forward,
                get_data_msg.allow_overflow,
            )
            .await
        {
            Ok(Some((rec, rec_offsets, first_offset))) => {
                let resp = msg.reply(Box::new(messages::query::GetQueryDataResp::Record {
                    record: Arc::new(rec),
                    record_offsets: rec_offsets,
                    first_offset,
                }));
                self.router_pipe.send(resp).await?;
            }
            Ok(None) => {
                let resp = msg.reply(Box::new(
                    messages::query::GetQueryDataResp::RecordRowGroupNotFound,
                ));
                self.router_pipe.send(resp).await?;
            }
            Err(err) => match err.downcast_ref::<QueryDataHandlerError>() {
                Some(cast_err)
                    if matches!(cast_err, QueryDataHandlerError::QueryFileDoesNotExist) =>
                {
                    let resp = msg.reply(Box::new(
                        messages::query::GetQueryDataResp::ReachedEndOfFiles,
                    ));
                    self.router_pipe.send(resp).await?;
                }
                Some(_) | None => {
                    error!("{:?}", err);
                    let resp = msg.reply(Box::new(messages::query::GetQueryDataResp::Error {
                        err: err.to_string(),
                    }));
                    self.router_pipe.send(resp).await?;
                }
            },
        }

        Ok(())
    }

    async fn get_row_group_data(
        &self,
        file_path: &str,
        file_row_group_idx: u64,
    ) -> Result<(arrow::array::RecordBatch, u64)> {
        let storage_conn = self.conn_reg.get_operator("default")?;

        let content_len = if let Ok(meta_data) = storage_conn.stat(file_path).await {
            meta_data.content_length()
        } else {
            return Err(QueryDataHandlerError::QueryFileDoesNotExist.into());
        };

        let reader = storage_conn
            .reader_with(file_path)
            .gap(512 * 1024)
            .chunk(16 * 1024 * 1024)
            .concurrent(4)
            .await?;
        let ref mut parquet_reader_for_meta = AsyncReader::new(reader.clone(), content_len);

        let meta_data =
            ArrowReaderMetadata::load_async(parquet_reader_for_meta, ArrowReaderOptions::new())
                .await?;
        let num_row_groups = meta_data.metadata().num_row_groups() as u64;

        let file_row_group_idx = if file_row_group_idx == std::u64::MAX {
            num_row_groups - 1
        } else {
            file_row_group_idx
        };

        let parquet_reader = AsyncReader::new(reader, content_len);
        let mut stream = ParquetRecordBatchStreamBuilder::new(parquet_reader)
            .await?
            .with_row_groups(vec![file_row_group_idx as usize])
            .build()?;

        match stream.next().await {
            Some(Ok(res)) => Ok((res, num_row_groups)),
            Some(Err(err)) => Err(err.into()),
            None => Err(QueryDataHandlerError::RowGroupDoesNotExistInTheFile(
                file_row_group_idx,
                file_path.to_string(),
            )
            .into()),
        }
    }

    async fn get_record_data(
        &self,
        query_id: u128,
        file_idx: u64,
        mut file_row_group_idx: u64,
        mut row_idx: u64,
        limit: u64,
        mut forward: bool,
        allow_overflow: bool,
    ) -> Result<
        Option<(
            arrow::array::RecordBatch,
            Vec<(u64, u64, u64)>,
            (u64, u64, u64),
        )>,
    > {
        if limit == 0 {
            return Ok(None);
        }

        debug!(
            file_idx = file_idx,
            file_row_group_idx = file_row_group_idx,
            row_idx = row_idx,
            limit = limit,
            forward = forward,
            "get record"
        );

        let query_uuid_id = Uuid::from_u128(query_id.clone());

        let mut forward_recs: Vec<arrow::array::RecordBatch> = Vec::new();
        let mut forward_rec_offsets: Vec<Vec<(u64, u64, u64)>> = Vec::new();
        let mut reverse_recs: Vec<arrow::array::RecordBatch> = Vec::new();
        let mut reverse_rec_offsets: Vec<Vec<(u64, u64, u64)>> = Vec::new();
        let mut total_rows_in_recs: u64 = 0;

        let mut first_offset: Option<(u64, u64, u64)> = None;
        let mut first_file_num_row_groups: Option<u64> = None;
        let mut first_file_num_rows: Option<u64> = None;

        let mut current_file_idx = file_idx;
        let mut current_file_row_group_idx = file_row_group_idx;
        loop {
            let mut file_path = PathBuf::from("/query_results");
            file_path.push(format!("{}", query_uuid_id));
            file_path.push(format!("rec_{}.parquet", current_file_idx));
            let complete_file_path = file_path
                .to_str()
                .expect("expected file path to be non-empty");

            let res = self
                .get_row_group_data(complete_file_path, current_file_row_group_idx)
                .await;

            //tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            match res {
                Ok((rec, num_row_groups)) => {
                    if current_file_row_group_idx == std::u64::MAX {
                        current_file_row_group_idx = num_row_groups - 1;
                    }
                    if file_row_group_idx == std::u64::MAX {
                        file_row_group_idx = num_row_groups - 1;
                    }
                    if row_idx == std::u64::MAX {
                        row_idx = (rec.num_rows() - 1) as u64;
                    }

                    if current_file_idx == file_idx
                        && current_file_row_group_idx == file_row_group_idx
                    {
                        first_file_num_rows = Some(rec.num_rows() as u64);
                        first_file_num_row_groups = Some(num_row_groups);
                    }

                    if current_file_idx == file_idx
                        && current_file_row_group_idx == file_row_group_idx
                        && row_idx >= rec.num_rows() as u64
                    {
                        if forward {
                            if current_file_row_group_idx == num_row_groups - 1 {
                                current_file_idx += 1;
                                current_file_row_group_idx = 0;
                            } else {
                                current_file_row_group_idx += 1;
                            }
                        }
                        continue;
                    }

                    let (start_row_idx, length) = if forward {
                        let start_row_idx = if current_file_idx == file_idx
                            && current_file_row_group_idx == file_row_group_idx
                        {
                            row_idx
                        } else {
                            0u64
                        };

                        (
                            start_row_idx,
                            std::cmp::min(
                                rec.num_rows() as u64 - start_row_idx,
                                limit - total_rows_in_recs,
                            ),
                        )
                    } else {
                        let end_row_idx = if current_file_idx == file_idx
                            && current_file_row_group_idx == file_row_group_idx
                        {
                            if row_idx == std::u64::MAX {
                                (rec.num_rows() - 1) as u64
                            } else {
                                row_idx
                            }
                        } else {
                            (rec.num_rows() - 1) as u64
                        };

                        (
                            end_row_idx - std::cmp::min(end_row_idx, limit - total_rows_in_recs),
                            std::cmp::min(end_row_idx + 1, limit - total_rows_in_recs),
                        )
                    };

                    debug!(
                        file_idx = file_idx,
                        file_row_group_idx = file_row_group_idx,
                        start_row_idx = start_row_idx,
                        length = length,
                        rec_num_rows = rec.num_rows(),
                        "reverse"
                    );

                    // prevent out of bounds access by the requester
                    if start_row_idx >= rec.num_rows() as u64 {
                        break;
                    }

                    let offsets: Vec<(u64, u64, u64)> = (start_row_idx..(start_row_idx + length))
                        .map(|i| (current_file_idx, current_file_row_group_idx, i))
                        .collect();
                    let rec_slice = rec.slice(start_row_idx as usize, length as usize);
                    if first_offset.is_none() {
                        first_offset = if forward {
                            Some(offsets.first().expect("expected first offset").clone())
                        } else {
                            Some(offsets.last().expect("expected first offset").clone())
                        }
                    }

                    total_rows_in_recs += rec_slice.num_rows() as u64;
                    if forward {
                        forward_recs.push(rec_slice);
                        forward_rec_offsets.push(offsets);
                    } else {
                        reverse_recs.push(rec_slice);
                        reverse_rec_offsets.push(offsets);
                    }

                    if total_rows_in_recs >= limit {
                        break;
                    }

                    if forward {
                        if current_file_row_group_idx == num_row_groups - 1 {
                            current_file_idx += 1;
                            current_file_row_group_idx = 0;
                        } else {
                            current_file_row_group_idx += 1;
                        }
                    } else {
                        if current_file_idx == 0
                            && current_file_row_group_idx == 0
                            && start_row_idx == 0
                        {
                            if allow_overflow && total_rows_in_recs < limit {
                                forward = true;
                                if row_idx + 1 < first_file_num_rows.expect("first_file_num_rows") {
                                    row_idx = row_idx + 1;
                                    current_file_row_group_idx = file_row_group_idx;
                                    current_file_idx = file_idx;
                                } else if file_row_group_idx + 1
                                    < first_file_num_row_groups.expect("first_file_num_row_groups")
                                {
                                    current_file_row_group_idx = file_row_group_idx + 1;
                                    current_file_idx = file_idx;
                                } else {
                                    current_file_idx = file_idx + 1;
                                    current_file_row_group_idx = 0;
                                }
                            } else {
                                break;
                            }
                        } else if current_file_idx == 0 && current_file_row_group_idx == 0 {
                            break;
                        } else if current_file_idx > 0 && current_file_row_group_idx == 0 {
                            current_file_idx -= 1;
                            current_file_row_group_idx = std::u64::MAX;
                        } else {
                            current_file_row_group_idx -= 1;
                        }
                    }
                }
                Err(err) => match err.downcast_ref::<QueryDataHandlerError>() {
                    Some(cast_err)
                        if matches!(cast_err, QueryDataHandlerError::QueryFileDoesNotExist) =>
                    {
                        break;
                    }
                    Some(_) | None => {
                        return Err(err);
                    }
                },
            }
        }

        assert_eq!(
            forward_recs.iter().map(|rec| rec.num_rows()).sum::<usize>(),
            forward_rec_offsets
                .iter()
                .map(|item| item.len())
                .sum::<usize>()
        );
        assert_eq!(
            reverse_recs.iter().map(|rec| rec.num_rows()).sum::<usize>(),
            reverse_rec_offsets
                .iter()
                .map(|item| item.len())
                .sum::<usize>()
        );

        let all_recs = reverse_recs.iter().rev().chain(forward_recs.iter());
        let all_rec_offsets = reverse_rec_offsets
            .iter()
            .rev()
            .chain(forward_rec_offsets.iter());

        if let Some(first_rec) = all_recs.clone().next() {
            let final_rec = arrow::compute::concat_batches(first_rec.schema_ref(), all_recs)?;

            let mut final_rec_offsets = Vec::new();
            for offsets in all_rec_offsets {
                final_rec_offsets.extend(offsets);
            }

            let first_offset = first_offset.expect("expected first offset");

            Ok(Some((final_rec, final_rec_offsets, first_offset)))
        } else {
            Ok(None)
        }
    }
}

///////////////////////////////////////////////////
//

#[derive(Debug)]
struct QueryDataHandlerSubscriber {
    operator_id: u128,
    sender: mpsc::Sender<Message>,
    msg_reg: Arc<MessageRegistry>,
}

impl Subscriber for QueryDataHandlerSubscriber {}

impl MessageConsumer for QueryDataHandlerSubscriber {
    fn consumes_message(&self, msg: &Message) -> bool {
        match msg.msg.msg_name() {
            MessageName::GetQueryData => return true,
            _ => (),
        }

        // only accept other messages intended for this operator
        if msg.sent_from_connection_id.is_none()
            && (msg.route_to_connection_id.is_some()
                || msg.route_to_operation_id != Some(self.operator_id))
        {
            return false;
        }

        false
    }
}

impl MessageReceiver for QueryDataHandlerSubscriber {
    fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
}
