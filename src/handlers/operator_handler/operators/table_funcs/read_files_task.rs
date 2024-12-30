use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::handlers::message_handler::{Message, MessageRegistry, Pipe};
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
    fn valid(&self, args: &Vec<sqlparser::ast::FunctionArg>) -> bool {
        match ReadFilesConfig::parse_config(args) {
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
}

impl ReadFilesConfig {
    fn parse_config(args: &Vec<sqlparser::ast::FunctionArg>) -> Result<ReadFilesConfig> {
        if args.len() > 2 {
            return Err(
                ReadFilesConfigError::NumberOfArgumentsGreaterThanExpected(args.len()).into(),
            );
        }
        let path = match args.get(0) {
            Some(sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(val)),
            ))) => val,
            _ => {
                return Err(ReadFilesConfigError::InvalidArgument(0, "pathTemplate").into());
            }
        }
        .clone();
        let connection = match args.get(1) {
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

        Ok(ReadFilesConfig { path, connection })
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

    file_idx: usize,
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
            file_idx: 0,
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
        let mut reader = conn
            .reader_with(path)
            .gap(512 * 1024)
            .chunk(16 * 1024 * 1024)
            .concurrent(8)
            .await?;
        let content_len = conn.stat(path).await?.content_length();
        let parquet_reader = parquet_opendal::AsyncReader::new(reader, content_len)
            .with_prefetch_footer_size(512 * 1024);
        let bldr = parquet::arrow::ParquetRecordBatchStreamBuilder::new(parquet_reader)
            .await?
            .with_batch_size(1024);

        Ok(())
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
    ) -> Result<Box<dyn MessageConsumer>> {
        let read_files_config = ReadFilesConfig::parse_config(&table_func_config.args)?;
        let mut op = ReadFilesTask::new(
            op_in_config,
            read_files_config,
            operator_pipe,
            msg_reg.clone(),
            conn_reg.clone(),
        );

        let consumer = op.subscriber();

        tt.spawn(async move {
            if let Err(err) = op.async_main(ct).await {
                info!("error: {:?}", err);
            }
        })?;

        Ok(consumer)
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
        true
    }
}
