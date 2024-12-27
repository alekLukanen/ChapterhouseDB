use core::fmt;
use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::handlers::{
    message_handler::{Message, MessageRegistry, Pipe},
    message_router_handler::MessageConsumer,
    operator_handler::operator_handler_state::OperatorInstanceConfig,
};

use super::{operator_task_trackers::RestrictedOperatorTaskTracker, table_funcs::TableFuncConfig};

pub trait TableFuncTaskBuilder: fmt::Debug + Send + Sync {
    fn build(
        &self,
        op_in_config: OperatorInstanceConfig,
        table_func_config: TableFuncConfig,
        operator_pipe: Pipe<Message>,
        msg_reg: Arc<MessageRegistry>,
        tt: &mut RestrictedOperatorTaskTracker,
        ct: CancellationToken,
    ) -> Result<Box<dyn MessageConsumer>>;
    fn implements_func_name(&self) -> String;
}
