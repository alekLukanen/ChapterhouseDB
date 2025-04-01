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

        let query_uuid_id = Uuid::from_u128(get_data_msg.query_id.clone());
        let mut file_path = PathBuf::from("/query_results");
        file_path.push(format!("{}", query_uuid_id));
        file_path.push(format!("rec_{}.parquet", get_data_msg.file_idx));

        let complete_file_path = file_path
            .to_str()
            .expect("expected file path to be non-empty");

        match self
            .get_row_group_data(
                complete_file_path,
                get_data_msg.file_row_group_idx,
                get_data_msg.row_idx,
                get_data_msg.limit,
                get_data_msg.forward,
            )
            .await
        {
            Ok((Some(rec), num_row_groups)) => {
                let next_file_idx = if get_data_msg.file_row_group_idx < num_row_groups - 1 {
                    get_data_msg.file_idx
                } else {
                    get_data_msg.file_idx + 1
                };
                let next_file_row_group_idx =
                    if get_data_msg.file_row_group_idx < num_row_groups - 1 {
                        get_data_msg.file_row_group_idx + 1
                    } else {
                        0
                    };

                let resp = msg.reply(Box::new(messages::query::GetQueryDataResp::Record {
                    record: Arc::new(rec),
                    next_file_idx,
                    next_file_row_group_idx,
                }));
                self.router_pipe.send(resp).await?;
            }
            Ok((None, _)) => {
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
        file_row_group_idx: u64,
        row_idx: u64,
        limit: u64,
        forward: bool,
    ) -> Result<Option<(arrow::array::RecordBatch, Vec<u64, u64, u64)>> {
        let query_uuid_id = Uuid::from_u128(query_id.clone());

        let recs: Vec<arrow::array::RecordBatch> = Vec::new();
        let rec_offsets: Vec<(u64, u64, u64)> = Vec::new();
        let total_rows_in_recs: u64 = 0;

        let current_file_idx = file_idx;
        let current_file_row_group_idx = file_row_group_idx;
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

            match res {
                Ok((rec, num_row_groups)) => {
                    if rec.num_rows() == 0 {
                        continue;
                    }

                    let start_row_idx = if current_file_idx == file_idx
                        && current_file_row_group_idx == file_row_group_idx
                    {
                        row_idx
                    } else if forward {
                        0u64
                    } else {
                        (rec.num_rows() - 1) as u64
                    };
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

        assert_eq!(recs.iter().map(|rec| rec.num_rows()).sum(), rec_offsets.len());

        if let Some(first_rec) = recs.first() {
            let final_rec = arrow::compute::concat_batches(first_rec.schema_ref(), recs)?;
            Ok(Some((final_rec, rec_offsets)))
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
