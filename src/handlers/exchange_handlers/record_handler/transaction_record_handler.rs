use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::handlers::{
    exchange_handlers::transaction_heartbeat_handler::TransactionHeartbeatHandler,
    message_handler::{messages, MessageRegistry, Pipe},
    message_router_handler::MessageRouterState,
    operator_handler::operators::requests,
};

use super::{drop_cancellation_token::DropCancellationToken, record_handler::RecordHandler};

#[derive(Debug, Error)]
pub enum TransactionRecordHandlerError {
    #[error("transaction already committed")]
    TransactionAlreadyCommitted,
    #[error("commit transaction response error: {0}")]
    CommitTransactionResponseError(String),
    #[error("timed out waiting for task to close")]
    TimedOutWaitingForTaskToClose,
    #[error("insert transaction record response error: {0}")]
    InsertTransactionRecordResponseError(String),
}

pub struct TransactionRecordHandler {
    transaction_id: u64,
    pub(crate) record_handler_inner: RecordHandler,

    msg_reg: Arc<MessageRegistry>,
    msg_router_state: Arc<Mutex<MessageRouterState>>,

    tt: tokio_util::task::TaskTracker,
    tracker_ct: DropCancellationToken,

    committed: bool,
}

impl TransactionRecordHandler {
    pub fn new(
        ct: CancellationToken,
        transaction_id: u64,
        record_handler_inner: RecordHandler,
        msg_reg: Arc<MessageRegistry>,
        msg_router_state: Arc<Mutex<MessageRouterState>>,
    ) -> TransactionRecordHandler {
        TransactionRecordHandler {
            transaction_id,
            record_handler_inner,
            msg_reg,
            msg_router_state,
            tt: tokio_util::task::TaskTracker::new(),
            tracker_ct: DropCancellationToken::new(ct),
            committed: false,
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

    pub async fn insert_record(
        &self,
        pipe: &mut Pipe,
        queue_name: String,
        deduplication_key: String,
        record: arrow::array::RecordBatch,
        table_aliases: Vec<Vec<String>>,
    ) -> Result<()> {
        let outbound_exchange = self.record_handler_inner.get_outbound_exchange()?;
        let req = requests::exchange::InsertTransactionRecordRequest::insert_record_request(
            self.record_handler_inner.operator_id.clone(),
            outbound_exchange.operator_instance_id,
            outbound_exchange.worker_id,
            self.transaction_id.clone(),
            queue_name,
            deduplication_key,
            record,
            table_aliases,
            pipe,
            self.msg_reg.clone(),
        )
        .await?;
        match req {
            messages::exchange::InsertTransactionRecordResponse::Ok { .. } => Ok(()),
            messages::exchange::InsertTransactionRecordResponse::Err(err) => {
                Err(TransactionRecordHandlerError::InsertTransactionRecordResponseError(err).into())
            }
        }
    }

    pub async fn commit_transaction(self, pipe: &mut Pipe) -> Result<RecordHandler> {
        if self.committed {
            return Err(TransactionRecordHandlerError::TransactionAlreadyCommitted.into());
        }

        // commit the transaction
        let outbound_exchange = self.record_handler_inner.get_outbound_exchange()?;
        let commit_resp = requests::exchange::CommitTransactionRequest::commit_transaction_request(
            self.record_handler_inner.operator_id.clone(),
            outbound_exchange.operator_instance_id,
            outbound_exchange.worker_id,
            self.transaction_id.clone(),
            pipe,
            self.msg_reg.clone(),
        )
        .await?;
        match commit_resp {
            messages::exchange::CommitTransactionResponse::Ok => {}
            messages::exchange::CommitTransactionResponse::Error(err) => {
                return Err(
                    TransactionRecordHandlerError::CommitTransactionResponseError(err).into(),
                );
            }
        }

        // close the heartbeat
        self.tt.close();
        self.tracker_ct.cancel();
        tokio::select! {
            _ = self.tt.wait() => {},
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                return Err(TransactionRecordHandlerError::TimedOutWaitingForTaskToClose.into());
            }
        }

        debug!(
            operator_id = self.record_handler_inner.operator_id,
            transaction_id = self.transaction_id,
            "committed transaction",
        );

        Ok(self.record_handler_inner)
    }
}
