use anyhow::Result;
use core::fmt;
use tokio_util::sync::CancellationToken;

use crate::handlers::{
    message_handler::{Message, Pipe},
    operator_handler::operator_handler_state::OperatorInstance,
};

pub trait OperatorTask: fmt::Debug {
    async fn init() -> Result<()>;
    async fn msg(msg: Message) -> Result<bool>;
    async fn produce_next() -> Result<()>;
    async fn cleanup() -> Result<()>;
}

pub struct OperatorBuilder {
    operator_instance: OperatorInstance,
    handler_pipe: Pipe<Message>,
    ct: CancellationToken,
}

impl OperatorBuilder {
    pub fn new(
        ct: CancellationToken,
        op_in: OperatorInstance,
        pipe: Pipe<Message>,
    ) -> OperatorBuilder {
        OperatorBuilder {
            operator_instance: op_in,
            handler_pipe: pipe,
            ct,
        }
    }

    pub fn build() -> Result<()> {
        Ok(())
    }
}
