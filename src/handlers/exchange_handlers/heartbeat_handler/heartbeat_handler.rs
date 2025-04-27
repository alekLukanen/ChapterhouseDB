use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::handlers::message_handler::{MessageRegistry, Pipe};

pub struct RecordHeartbeatHandler<'a> {
    operator_id: String,
    exchange_operator_instance_id: u128,
    exchange_worker_id: u128,
    record_id: u64,

    pipe: &'a mut Pipe,
    msg_reg: Arc<MessageRegistry>,
}

impl<'a> RecordHeartbeatHandler<'a> {
    pub fn new(
        operator_id: String,
        exchange_operator_instance_id: u128,
        exchange_worker_id: u128,
        record_id: u64,
        pipe: &'a mut Pipe,
        msg_reg: Arc<MessageRegistry>,
    ) -> RecordHeartbeatHandler<'a> {
        RecordHeartbeatHandler {
            operator_id,
            exchange_operator_instance_id,
            exchange_worker_id,
            record_id,
            pipe,
            msg_reg,
        }
    }

    pub async fn async_main(&mut self, ct: CancellationToken) -> Result<()> {
        debug!(
            operator_id = self.operator_id,
            record_id = self.record_id,
            "started record heartbeat"
        );

        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        debug!(
            operator_id = self.operator_id,
            record_id = self.record_id,
            "completed record heartbeat"
        );

        Ok(())
    }
}
