use core::fmt;
use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::handlers::{
    message_handler::{MessageRegistry, Pipe},
    message_router_handler::MessageConsumer,
    operator_handler::operator_handler_state::OperatorInstanceConfig,
};

use super::{
    operator_task_trackers::RestrictedOperatorTaskTracker, table_func_tasks::TableFuncConfig,
    ConnectionRegistry,
};

pub trait TableFuncTaskBuilder: fmt::Debug + Send + Sync {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        table_func_config: TableFuncConfig,
        operator_pipe: Pipe,
        msg_reg: Arc<MessageRegistry>,
        conn_reg: Arc<ConnectionRegistry>,
        tt: &mut RestrictedOperatorTaskTracker,
        ct: CancellationToken,
    ) -> Result<(tokio::sync::oneshot::Receiver<()>, Box<dyn MessageConsumer>)>;
}

pub trait TableFuncSyntaxValidator: fmt::Debug + Send + Sync {
    fn valid(&self, config: &TableFuncConfig) -> bool;
    fn implements_func_name(&self) -> String;
}
