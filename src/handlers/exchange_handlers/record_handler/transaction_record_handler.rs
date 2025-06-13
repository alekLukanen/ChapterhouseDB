use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::handlers::{
    exchange_handlers::transaction_heartbeat_handler::TransactionHeartbeatHandler,
    message_handler::MessageRegistry, message_router_handler::MessageRouterState,
};

use super::record_handler::RecordHandler;

pub struct TransactionRecordHandler<'a> {
    transaction_id: u64,
    record_handler_inner: &'a mut RecordHandler,

    msg_reg: Arc<MessageRegistry>,
    msg_router_state: Arc<Mutex<MessageRouterState>>,

    tt: tokio_util::task::TaskTracker,
    tracker_ct: CancellationToken,
}

impl<'a> TransactionRecordHandler<'a> {
    pub fn new(
        ct: CancellationToken,
        transaction_id: u64,
        record_handler_inner: &'a mut RecordHandler,
        msg_reg: Arc<MessageRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> TransactionRecordHandler<'a> {
        TransactionRecordHandler {
            transaction_id,
            record_handler_inner,
            msg_reg,
            msg_router_state,
            tt: tokio_util::task::TaskTracker::new(),
            tracker_ct: ct,
        }
    }

    pub async fn run_heartbeat(self) -> Result<Self> {
        let outbound_exchange = self.record_handler_inner.get_outbound_exchange()?;
        let mut handler = TransactionHeartbeatHandler::new(
            self.record_handler_inner.query_id.clone(),
            self.record_handler_inner.operator_id.clone(),
            outbound_exchange.operator_instance_id,
            outbound_exchange.worker_id,
            self.transaction_id.clone(),
            self.msg_reg.clone(),
            self.msg_router_state.clone(),
        )
        .await;

        let ct = self.tracker_ct.child_token();
        self.tt.spawn(async move {
            if let Err(err) = handler.async_main(ct).await {
                error!("{}", err);
            }
        });

        Ok(self)
    }

    pub fn insert_transaction_record(
        &self,
        queue_name: String,
        record_id: u64,
        record: Arc<arrow::array::RecordBatch>,
        table_aliases: Vec<Vec<String>>,
    ) -> Result<()> {
        Ok(())
    }

    fn commit_transaction(&self) -> Result<()> {
        Ok(())
    }
}
