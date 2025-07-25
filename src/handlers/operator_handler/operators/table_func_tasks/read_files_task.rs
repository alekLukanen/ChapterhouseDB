use std::sync::Arc;

use anyhow::{Error, Result};
use futures::StreamExt;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::handlers::exchange_handlers;
use crate::handlers::message_handler::messages;
use crate::handlers::message_handler::messages::message::{Message, MessageName};
use crate::handlers::message_handler::{MessageRegistry, Pipe};
use crate::handlers::message_router_handler::{MessageConsumer, MessageRouterState};
use crate::handlers::operator_handler::operator_handler_state::OperatorInstanceConfig;
use crate::handlers::operator_handler::operators::operator_task_trackers::RestrictedOperatorTaskTracker;
use crate::handlers::operator_handler::operators::traits::{TableFuncSyntaxValidator, TaskBuilder};
use crate::handlers::operator_handler::operators::{record_utils, ConnectionRegistry};

use super::config::TableFuncConfig;

#[derive(Debug, Error)]
pub enum ReadFilesError {
    #[error("cancelled")]
    Cancelled,
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

    operator_pipe: Pipe,
    msg_reg: Arc<MessageRegistry>,
    conn_reg: Arc<ConnectionRegistry>,
    msg_router_state: Arc<Mutex<MessageRouterState>>,

    record_id: u64,
}

impl ReadFilesTask {
    pub fn new(
        op_in_config: OperatorInstanceConfig,
        read_files_config: ReadFilesConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> ReadFilesTask {
        ReadFilesTask {
            operator_instance_config: op_in_config,
            read_files_config,
            operator_pipe,
            msg_reg,
            conn_reg,
            msg_router_state,
            record_id: 0,
        }
    }

    pub fn consumer(&self) -> Box<dyn MessageConsumer> {
        Box::new(ReadFilesConsumer {
            msg_reg: self.msg_reg.clone(),
        })
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
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

        let mut rec_handler = exchange_handlers::record_handler::RecordHandler::initiate(
            ct.child_token(),
            &self.operator_instance_config,
            "default".to_string(),
            &mut self.operator_pipe,
            self.msg_reg.clone(),
            self.msg_router_state.clone(),
        )
        .await?;

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

        let ref mut rec_handler_ref = rec_handler;
        loop {
            tokio::select! {
                entry = lister.next() => {
                    match entry {
                        Some(Ok(val)) => {
                            let path = val.path();
                            if !path_matcher.is_match(path) {
                                continue;
                            }

                            self.read_records(ct.clone(), path, &conn, rec_handler_ref).await?;
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

        if let Err(err) = rec_handler.close().await {
            error!("{}", err);
        }

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
    }

    async fn read_records(
        &mut self,
        ct: CancellationToken,
        path: &str,
        conn: &opendal::Operator,
        rec_handler: &mut exchange_handlers::record_handler::RecordHandler,
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
        let mut rec_stream = parquet::arrow::ParquetRecordBatchStreamBuilder::new(parquet_reader)
            .await?
            .with_batch_size(self.read_files_config.max_rows_per_batch)
            .build()?;

        while let Some(record_res) = rec_stream.next().await {
            if ct.is_cancelled() {
                return Err(ReadFilesError::Cancelled.into());
            }
            match record_res {
                Ok(record) => {
                    let record_id = self.next_record_id();
                    let table_aliases = record_utils::get_record_table_aliases(
                        &self.operator_instance_config.operator.operator_type,
                        &record,
                    )?;

                    rec_handler
                        .send_record_to_outbound_exchange(
                            &mut self.operator_pipe,
                            "default".to_string(),
                            record_id,
                            record,
                            table_aliases,
                        )
                        .await?;
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }

        Ok(())
    }

    fn next_record_id(&mut self) -> u64 {
        let rec_id = self.record_id;
        self.record_id += 1;
        rec_id
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

impl TaskBuilder for ReadFilesTaskBuilder {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
        tt: &mut RestrictedOperatorTaskTracker,
        ct: CancellationToken,
    ) -> Result<(
        tokio::sync::oneshot::Receiver<Option<Error>>,
        Box<dyn MessageConsumer>,
    )> {
        let table_func_config = TableFuncConfig::try_from(&op_in_config)?;
        let read_files_config = ReadFilesConfig::parse_config(&table_func_config)?;
        let mut op = ReadFilesTask::new(
            op_in_config,
            read_files_config,
            operator_pipe,
            msg_reg.clone(),
            conn_reg.clone(),
            msg_router_state.clone(),
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
pub struct ReadFilesConsumer {
    msg_reg: Arc<MessageRegistry>,
}

impl MessageConsumer for ReadFilesConsumer {
    fn consumes_message(&self, msg: &Message) -> bool {
        match msg.msg.msg_name() {
            MessageName::Ping => match self.msg_reg.try_cast_msg::<messages::common::Ping>(msg) {
                Ok(messages::common::Ping::Ping) => false,
                Ok(messages::common::Ping::Pong) => true,
                Err(err) => {
                    error!("{:?}", err);
                    false
                }
            },
            MessageName::ExchangeRequests => {
                match self
                    .msg_reg
                    .try_cast_msg::<messages::exchange::ExchangeRequests>(msg)
                {
                    Ok(messages::exchange::ExchangeRequests::SendRecordResponse { .. }) => true,
                    Ok(messages::exchange::ExchangeRequests::SendRecordRequest { .. }) => false,
                    Err(err) => {
                        error!("{:?}", err);
                        false
                    }
                    _ => false,
                }
            }
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
            MessageName::CommonGenericResponse => true,
            _ => false,
        }
    }
}
