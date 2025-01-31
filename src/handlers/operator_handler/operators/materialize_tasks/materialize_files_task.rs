use anyhow::{Error, Result};
use std::{path::PathBuf, sync::Arc};
use thiserror::Error;
use tracing::{debug, error};
use uuid::Uuid;

use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::message_handler::{MessageRegistry, Pipe};
use crate::handlers::{
    message_router_handler::MessageConsumer,
    operator_handler::{
        operator_handler_state::OperatorInstanceConfig,
        operators::{
            operator_task_trackers::RestrictedOperatorTaskTracker, record_utils, requests,
            traits::TaskBuilder, ConnectionRegistry,
        },
    },
};

use super::config::MaterializeFilesConfig;

#[derive(Debug, Error)]
pub enum MaterializeFilesTaskError {
    #[error("record path formatting returned None result")]
    RecordPathFormattingReturnedNoneResult,
    #[error("more than one exchange is currently not implement")]
    MoreThanOneExchangeIsCurrentlyNotImplemented,
}

#[derive(Debug)]
struct MaterializeFilesTask {
    operator_instance_config: OperatorInstanceConfig,
    materialize_file_config: MaterializeFilesConfig,

    operator_pipe: Pipe,
    msg_reg: Arc<MessageRegistry>,
    conn_reg: Arc<ConnectionRegistry>,

    exchange_worker_id: Option<u128>,
    exchange_operator_instance_id: Option<u128>,
}

impl MaterializeFilesTask {
    fn new(
        op_in_config: OperatorInstanceConfig,
        materialize_file_config: MaterializeFilesConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
    ) -> MaterializeFilesTask {
        MaterializeFilesTask {
            operator_instance_config: op_in_config,
            materialize_file_config,
            operator_pipe,
            msg_reg,
            conn_reg,
            exchange_worker_id: None,
            exchange_operator_instance_id: None,
        }
    }

    fn consumer(&self) -> Box<dyn MessageConsumer> {
        Box::new(MaterializeFilesConsumer {
            msg_reg: self.msg_reg.clone(),
        })
    }

    async fn async_main(&mut self, ct: tokio_util::sync::CancellationToken) -> Result<()> {
        debug!(
            operator_task = self
                .operator_instance_config
                .operator
                .operator_type
                .task_name(),
            operator_id = self.operator_instance_config.operator.id,
            operator_instance_id = self.operator_instance_config.id,
            "started task",
        );

        // get the default connection
        let storage_conn = self.conn_reg.get_operator("default")?;

        // find the exchange
        let ref mut pipe = self.operator_pipe;
        let req = requests::IdentifyExchangeRequest::request_inbound_exchanges(
            &self.operator_instance_config,
            pipe,
            self.msg_reg.clone(),
        );
        tokio::select! {
            resp = req => {
                match resp {
                    Ok(resp) => {
                        if resp.len() != 1 {
                            return Err(MaterializeFilesTaskError::MoreThanOneExchangeIsCurrentlyNotImplemented.into());
                        }
                        let resp = resp.get(0).unwrap();
                        self.exchange_operator_instance_id = Some(resp.exchange_operator_instance_id);
                        self.exchange_worker_id = Some(resp.exchange_worker_id);
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
            _ = ct.cancelled() => {
                return Ok(());
            }
        }

        assert!(self.exchange_operator_instance_id.is_some());
        assert!(self.exchange_worker_id.is_some());

        let query_uuid_id = Uuid::from_u128(self.operator_instance_config.query_id.clone());

        // loop over all records in the exchange
        let ref mut operator_pipe = self.operator_pipe;

        loop {
            let resp = requests::GetNextRecordRequest::get_next_record_request(
                self.operator_instance_config.operator.id.clone(),
                self.exchange_operator_instance_id.unwrap().clone(),
                self.exchange_worker_id.unwrap().clone(),
                operator_pipe,
                self.msg_reg.clone(),
            )
            .await?;

            match &resp {
                requests::GetNextRecordResponse::Record {
                    record_id,
                    record,
                    table_aliases,
                } => {
                    // TODO: implement heartbeat for record processing
                    // TODO: use thread-pool for record operations
                    // evalute the expressions for each column and materialize the result
                    // to a parquet file
                    let proj_rec = record_utils::project_record(
                        &self.materialize_file_config.fields,
                        record.clone(),
                        table_aliases,
                    )?;

                    // materialize the projected record
                    let mut rec_path_buf = PathBuf::from("/query_results");
                    rec_path_buf.push(format!("{}", query_uuid_id));
                    rec_path_buf.push(format!("rec_{}.parquet", record_id));
                    let rec_path = if let Some(rec_path) = rec_path_buf.to_str() {
                        rec_path
                    } else {
                        return Err(
                            MaterializeFilesTaskError::RecordPathFormattingReturnedNoneResult
                                .into(),
                        );
                    };
                    let writer = storage_conn
                        .writer_with(rec_path)
                        .chunk(16 * 1024 * 1024)
                        .concurrent(4)
                        .await?;

                    let parquet_writer = parquet_opendal::AsyncWriter::new(writer);
                    let mut arrow_parquet_writer = parquet::arrow::AsyncArrowWriter::try_new(
                        parquet_writer,
                        proj_rec.schema(),
                        None,
                    )?;
                    arrow_parquet_writer.write(&proj_rec).await?;
                    arrow_parquet_writer.close().await?;

                    // confirm processing of the record
                    requests::OperatorCompletedRecordProcessingRequest::request(
                        self.operator_instance_config.operator.id.clone(),
                        record_id.clone(),
                        self.exchange_operator_instance_id.unwrap().clone(),
                        self.exchange_worker_id.unwrap().clone(),
                        operator_pipe,
                        self.msg_reg.clone(),
                    )
                    .await?;
                }
                requests::GetNextRecordResponse::NoneLeft => {
                    debug!("complete materialization; read all records from the exchange");
                    tokio::time::sleep(chrono::Duration::seconds(1).to_std()?).await;
                }
            }
        }

        /*
        debug!(
            operator_task = self
                .operator_instance_config
                .operator
                .operator_type
                .task_name(),
            operator_id = self.operator_instance_config.operator.id,
            operator_instance_id = self.operator_instance_config.id,
            "closed task",
        );
        Ok(())
        */
    }
}

//////////////////////////////////////////////////////
// Matterialize Files Producer Builder

#[derive(Debug, Clone)]
pub struct MaterializeFilesTaskBuilder {}

impl MaterializeFilesTaskBuilder {
    pub fn new() -> MaterializeFilesTaskBuilder {
        MaterializeFilesTaskBuilder {}
    }
}

impl TaskBuilder for MaterializeFilesTaskBuilder {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
        tt: &mut RestrictedOperatorTaskTracker,
        ct: tokio_util::sync::CancellationToken,
    ) -> Result<(
        tokio::sync::oneshot::Receiver<Option<Error>>,
        Box<dyn MessageConsumer>,
    )> {
        let mat_files_config = MaterializeFilesConfig::try_from(&op_in_config)?;
        let mut op = MaterializeFilesTask::new(
            op_in_config,
            mat_files_config,
            operator_pipe,
            msg_reg.clone(),
            conn_reg.clone(),
        );

        let consumer = op.consumer();

        let (tx, rx) = tokio::sync::oneshot::channel();
        tt.spawn(async move {
            if let Err(err) = op.async_main(ct).await {
                error!("{:?}", err);
                if let Err(err_send) = tx.send(Some(err)) {
                    error!("{:?}", err_send);
                }
            } else {
                if let Err(err_send) = tx.send(None) {
                    error!("{:?}", err_send);
                }
            }
        })?;

        Ok((rx, consumer))
    }
}

//////////////////////////////////////////////////////
// Message Consumer

#[derive(Debug, Clone)]
pub struct MaterializeFilesConsumer {
    msg_reg: Arc<MessageRegistry>,
}

impl MessageConsumer for MaterializeFilesConsumer {
    fn consumes_message(&self, msg: &Message) -> bool {
        match msg.msg.msg_name() {
            // used to find the exchange
            MessageName::Ping => match self.msg_reg.try_cast_msg::<messages::common::Ping>(msg) {
                Ok(messages::common::Ping::Ping) => false,
                Ok(messages::common::Ping::Pong) => true,
                Err(err) => {
                    error!("{:?}", err);
                    false
                }
            },
            MessageName::QueryHandlerRequests => {
                match self
                    .msg_reg
                    .try_cast_msg::<messages::query::QueryHandlerRequests>(msg)
                {
                    Ok(messages::query::QueryHandlerRequests::ListOperatorInstancesResponse {
                        ..
                    }) => true,
                    Ok(messages::query::QueryHandlerRequests::ListOperatorInstancesRequest {
                        ..
                    }) => false,
                    Err(err) => {
                        error!("{:?}", err);
                        false
                    }
                }
            }
            MessageName::ExchangeRequests => {
                match self
                    .msg_reg
                    .try_cast_msg::<messages::exchange::ExchangeRequests>(msg)
                {
                    Ok(messages::exchange::ExchangeRequests::GetNextRecordResponseRecord {
                        ..
                    }) => true,
                    Ok(messages::exchange::ExchangeRequests::GetNextRecordResponseNoneLeft) => true,
                    Ok(messages::exchange::ExchangeRequests::GetNextRecordResponseNoneAvailable) => true,
                    Ok(messages::exchange::ExchangeRequests::OperatorCompletedRecordProcessingResponse) => true,
                    Err(err) => {
                        error!("{:?}", err);
                        false
                    }
                    _ => false,
                }
            }
            // used ...
            _ => false,
        }
    }
}
