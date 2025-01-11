use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::handlers::message_handler::{
    Message, MessageName, MessageRegistry, Pipe, QueryHandlerRequests, StoreRecordBatch,
};
use crate::handlers::message_router_handler::MessageConsumer;
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;
use crate::handlers::operator_handler::operators::operator_task_trackers::RestrictedOperatorTaskTracker;
use crate::handlers::operator_handler::operators::traits::{
    TableFuncSyntaxValidator, TableFuncTaskBuilder,
};
use crate::handlers::operator_handler::operators::ConnectionRegistry;

use super::config::TableFuncConfig;

#[derive(Debug, Error)]
pub enum ReadFilesError {
    #[error("cancelled")]
    Cancelled,
    #[error("operator type not implemented: {0}")]
    OperatorTypeNotImplemented(String),
    #[error("received the wrong record id: {0}")]
    ReceivedTheWrongRecordId(u64),
    #[error("received the wrong message type")]
    ReceivedTheWrongMessageType,
    #[error("received none message")]
    ReceivedNoneMessage,
    #[error("received no operator instances for the exchange")]
    ReceivedNoOperatorInstancesForTheExchange,
    #[error("received multiple operator instances for the exchange")]
    ReceivedMultipleOperatorInstancesForTheExchange,
}

#[derive(Debug, Error)]
pub enum ReadFilesConfigError {
    #[error("invalid argument")]
    InvalidArgument(usize, &'static str),
    #[error("number of arguments greater than expected: {0}")]
    NumberOfArgumentsGreaterThanExpected(usize),
}

#[derive(Debug, Clone)]
pub struct ReadFilesSyntaxValidator {}

impl ReadFilesSyntaxValidator {
    pub fn new() -> ReadFilesSyntaxValidator {
        ReadFilesSyntaxValidator {}
    }
}

impl TableFuncSyntaxValidator for ReadFilesSyntaxValidator {
    fn valid(&self, config: &TableFuncConfig) -> bool {
        match ReadFilesConfig::parse_config(config) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    fn implements_func_name(&self) -> String {
        "read_files".to_string()
    }
}

#[derive(Debug, Clone)]
pub struct ReadFilesConfig {
    path: String,
    connection: Option<String>,
    max_rows_per_batch: usize,
}

impl ReadFilesConfig {
    fn parse_config(config: &TableFuncConfig) -> Result<ReadFilesConfig> {
        if config.args.len() > 2 {
            return Err(ReadFilesConfigError::NumberOfArgumentsGreaterThanExpected(
                config.args.len(),
            )
            .into());
        }
        let path = match config.args.get(0) {
            Some(sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(val)),
            ))) => val,
            _ => {
                return Err(ReadFilesConfigError::InvalidArgument(0, "pathTemplate").into());
            }
        }
        .clone();
        let connection = match config.args.get(1) {
            Some(sqlparser::ast::FunctionArg::Named {
                name:
                    sqlparser::ast::Ident {
                        value,
                        quote_style: None,
                    },
                arg:
                    sqlparser::ast::FunctionArgExpr::Expr(sqlparser::ast::Expr::Value(
                        sqlparser::ast::Value::SingleQuotedString(connection_name),
                    )),
                ..
            }) if *value == "connection".to_string() => Some(connection_name.clone()),
            None => None,
            _ => {
                return Err(ReadFilesConfigError::InvalidArgument(1, "connection").into());
            }
        };

        Ok(ReadFilesConfig {
            path,
            connection,
            max_rows_per_batch: config.max_rows_per_batch,
        })
    }

    fn parse_path_prefix(&self) -> &str {
        let special_chars = ['*', '?', '[', ']', '{', '}'];
        let prefix_end = self
            .path
            .find(|c| special_chars.contains(&c))
            .unwrap_or_else(|| self.path.len());
        &self.path[..prefix_end].trim_end_matches('/')
    }
}

#[derive(Debug)]
pub struct ReadFilesTask {
    operator_instance_config: OperatorInstanceConfig,
    read_files_config: ReadFilesConfig,

    operator_pipe: Pipe<Message>,
    msg_reg: Arc<MessageRegistry>,
    conn_reg: Arc<ConnectionRegistry>,

    exchange_worker_id: Option<u128>,
    record_id: u64,
}

impl ReadFilesTask {
    pub fn new(
        op_in_config: OperatorInstanceConfig,
        read_files_config: ReadFilesConfig,
        operator_pipe: Pipe<Message>,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
    ) -> ReadFilesTask {
        ReadFilesTask {
            operator_instance_config: op_in_config,
            read_files_config,
            operator_pipe,
            msg_reg,
            conn_reg,
            exchange_worker_id: None,
            record_id: 0,
        }
    }

    pub fn subscriber(&self) -> Box<dyn MessageConsumer> {
        Box::new(ReadFilesConsumer {
            msg_reg: self.msg_reg.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        let conn = match &self.read_files_config.connection {
            Some(conn_name) => self.conn_reg.get_operator(conn_name.as_str())?,
            None => self.conn_reg.get_operator("default")?,
        };

        let mut lister = conn
            .lister_with(self.read_files_config.parse_path_prefix())
            .recursive(true)
            .await?;
        let path_matcher =
            globset::Glob::new(self.read_files_config.path.as_str())?.compile_matcher();

        loop {
            tokio::select! {
                entry = lister.next() => {
                    match entry {
                        Some(Ok(val)) => {
                            let path = val.path();
                            if !path_matcher.is_match(path) {
                                continue;
                            }

                            self.read_records(ct.clone(), path, &conn).await?;
                        },
                        Some(Err(err)) => return Err(err.into()),
                        None => {
                            break;
                        },
                    };
                },
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        info!(
            "closing operator producer for instance {}",
            self.operator_instance_config.id
        );

        Ok(())
    }

    async fn read_records(
        &mut self,
        ct: CancellationToken,
        path: &str,
        conn: &opendal::Operator,
    ) -> Result<()> {
        let reader = conn
            .reader_with(path)
            .gap(512 * 1024)
            .chunk(16 * 1024 * 1024)
            .concurrent(4)
            .await?;
        let content_len = conn.stat(path).await?.content_length();
        let parquet_reader = parquet_opendal::AsyncReader::new(reader, content_len)
            .with_prefetch_footer_size(512 * 1024);
        let mut bldr = parquet::arrow::ParquetRecordBatchStreamBuilder::new(parquet_reader)
            .await?
            .with_batch_size(self.read_files_config.max_rows_per_batch)
            .build()?;

        while let Some(record_res) = bldr.next().await {
            if ct.cancelled().await == () {
                return Err(ReadFilesError::Cancelled.into());
            }
            match record_res {
                Ok(record) => {
                    info!("read record");
                    self.send_record(record)
                        .await
                        .context("unable to send record to the exchange")?;
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }

        Ok(())
    }

    async fn send_record(&mut self, record: arrow::array::RecordBatch) -> Result<()> {
        if self.exchange_worker_id == None {
            // find the worker with the exchange
            let exchange_id = match &self.operator_instance_config.operator.operator_type {
                crate::planner::OperatorType::Producer {
                    outbound_exchange_id,
                    ..
                } => outbound_exchange_id.clone(),
                crate::planner::OperatorType::Exchange { .. } => {
                    return Err(ReadFilesError::OperatorTypeNotImplemented(format!(
                        "{:?}",
                        self.operator_instance_config.operator.operator_type
                    ))
                    .into());
                }
            };
            let ping_msg = Message::new(Box::new(
                QueryHandlerRequests::ListOperatorInstancesRequest {
                    query_id: self.operator_instance_config.query_id,
                    operator_id: exchange_id,
                },
            ));
            self.operator_pipe.send(ping_msg).await?;

            let resp_msg = self.operator_pipe.recv().await;
            match resp_msg {
                Some(resp_msg) => {
                    let resp_msg: &QueryHandlerRequests = self.msg_reg.try_cast_msg(&resp_msg)?;
                    match resp_msg {
                        QueryHandlerRequests::ListOperatorInstancesResponse { op_instance_ids } => {
                            if op_instance_ids.len() == 1 {
                                self.exchange_worker_id =
                                    Some(op_instance_ids.get(0).unwrap().clone());
                            } else if op_instance_ids.len() == 0 {
                                return Err(
                                    ReadFilesError::ReceivedNoOperatorInstancesForTheExchange
                                        .into(),
                                );
                            } else {
                                return Err(
                                    ReadFilesError::ReceivedMultipleOperatorInstancesForTheExchange
                                        .into(),
                                );
                            }
                        }
                        _ => {
                            return Err(ReadFilesError::ReceivedTheWrongMessageType.into());
                        }
                    }
                }
                None => {
                    return Err(ReadFilesError::ReceivedNoneMessage.into());
                }
            }
        }

        assert!(self.exchange_worker_id.is_some());

        let msg_record_id = self.next_record_id();
        let record_msg = Message::new(Box::new(StoreRecordBatch::RequestSendRecord {
            record_id: msg_record_id,
            record,
            table_aliases: Vec::new(),
        }))
        .set_route_to_worker_id(self.exchange_worker_id.unwrap());

        self.operator_pipe.send(record_msg).await?;

        let resp_msg = self.operator_pipe.recv().await;
        if let Some(resp_msg) = resp_msg {
            let resp_msg: &StoreRecordBatch = self.msg_reg.try_cast_msg(&resp_msg)?;
            match resp_msg {
                StoreRecordBatch::ResponseReceivedRecord { record_id } => {
                    if *record_id != msg_record_id {
                        return Err(
                            ReadFilesError::ReceivedTheWrongRecordId(record_id.clone()).into()
                        );
                    }
                }
                _ => {
                    return Err(ReadFilesError::ReceivedTheWrongMessageType.into());
                }
            }
        }

        Ok(())
    }

    fn next_record_id(&mut self) -> u64 {
        let record_id = self.record_id;
        self.record_id += 1;
        record_id
    }
}

//////////////////////////////////////////////////////
// Table Func Producer Builder

#[derive(Debug, Clone)]
pub struct ReadFilesTaskBuilder {}

impl ReadFilesTaskBuilder {
    pub fn new() -> ReadFilesTaskBuilder {
        ReadFilesTaskBuilder {}
    }
}

impl TableFuncTaskBuilder for ReadFilesTaskBuilder {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        table_func_config: TableFuncConfig,
        operator_pipe: Pipe<Message>,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
        tt: &mut RestrictedOperatorTaskTracker,
        ct: CancellationToken,
    ) -> Result<(tokio::sync::oneshot::Receiver<()>, Box<dyn MessageConsumer>)> {
        let read_files_config = ReadFilesConfig::parse_config(&table_func_config)?;
        let mut op = ReadFilesTask::new(
            op_in_config,
            read_files_config,
            operator_pipe,
            msg_reg.clone(),
            conn_reg.clone(),
        );

        let consumer = op.subscriber();

        let (tx, rx) = tokio::sync::oneshot::channel();
        tt.spawn(async move {
            if let Err(err) = op.async_main(ct).await {
                info!("error: {:?}", err);
            }
            if let Err(err) = tx.send(()) {
                info!("error: {:?}", err);
            }
        })?;

        Ok((rx, consumer))
    }
}

//////////////////////////////////////////////////////
// Message Consumer

#[derive(Debug, Clone)]
pub struct ReadFilesConsumer {
    msg_reg: Arc<MessageRegistry>,
}

impl MessageConsumer for ReadFilesConsumer {
    fn consumes_message(&self, msg: &crate::handlers::message_handler::Message) -> bool {
        match msg.msg.msg_name() {
            MessageName::OperatorInstanceAvailable => {
                match self.msg_reg.try_cast_msg::<StoreRecordBatch>(msg) {
                    Ok(StoreRecordBatch::ResponseReceivedRecord { .. }) => true,
                    _ => false,
                }
            }
            _ => false,
        }
    }
}
