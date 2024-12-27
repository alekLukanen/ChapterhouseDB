use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;
use uuid::Uuid;

use crate::handlers::message_handler::{ConnectionPoolHandler, MessageRegistry};
use crate::handlers::message_router_handler::{MessageRouterHandler, MessageRouterState};
use crate::handlers::operator_handler::operators;
use crate::handlers::operator_handler::{OperatorHandler, TotalOperatorCompute};
use crate::handlers::query_handler::QueryHandler;

pub struct QueryWorkerConfig {
    address: String,
    connect_to_addresses: Vec<String>,
    allowed_compute: TotalOperatorCompute,
}

impl QueryWorkerConfig {
    pub fn new(
        address: String,
        connect_to_addresses: Vec<String>,
        allowed_compute: TotalOperatorCompute,
    ) -> QueryWorkerConfig {
        QueryWorkerConfig {
            address,
            connect_to_addresses,
            allowed_compute,
        }
    }
}

pub struct QueryWorker {
    worker_id: u128,
    config: QueryWorkerConfig,
    cancelation_token: CancellationToken,
}

impl QueryWorker {
    pub fn new(config: QueryWorkerConfig) -> QueryWorker {
        let ct = CancellationToken::new();
        return QueryWorker {
            worker_id: Uuid::new_v4().as_u128(),
            config,
            cancelation_token: ct,
        };
    }

    pub fn start(&mut self) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

        runtime.block_on(self.async_main())
    }

    async fn async_main(&mut self) -> Result<()> {
        info!("worker_id: {}", self.worker_id);

        let tt = TaskTracker::new();

        let msg_reg = Arc::new(MessageRegistry::new());
        let op_reg = Arc::new(operators::build_default_operator_task_registry());

        // Connect Pool and Router ////////////////////////
        let (mut connection_pool_handler, connection_msg_pipe) = ConnectionPoolHandler::new(
            self.worker_id,
            self.config.address.clone(),
            self.config.connect_to_addresses.clone(),
            msg_reg.clone(),
        );

        let (mut message_router, message_router_state) =
            MessageRouterHandler::new(self.worker_id.clone(), connection_msg_pipe, msg_reg.clone());

        // add internal subscribers
        let mut query_handler =
            QueryHandler::new(message_router_state.clone(), msg_reg.clone()).await;

        let mut operator_handler = OperatorHandler::new(
            message_router_state.clone(),
            msg_reg.clone(),
            self.config.allowed_compute.clone(),
        )
        .await;

        let ct = self.cancelation_token.clone();
        tt.spawn(async move {
            if let Err(err) = connection_pool_handler.async_main(ct).await {
                info!("error: {}", err);
            }
        });

        let message_router_ct = self.cancelation_token.clone();
        tt.spawn(async move {
            if let Err(err) = message_router.async_main(message_router_ct).await {
                info!("error: {}", err);
            }
        });

        let query_handler_ct = self.cancelation_token.clone();
        tt.spawn(async move {
            if let Err(err) = query_handler.async_main(query_handler_ct).await {
                info!("error: {}", err);
            }
        });

        let operator_handler_ct = self.cancelation_token.clone();
        tt.spawn(async move {
            if let Err(err) = operator_handler.async_main(operator_handler_ct).await {
                info!("error: {}", err);
            }
        });

        // TaskTracker /////////////////////
        // wait for the cancelation token to be cancelled and all tasks to be cancelled
        tt.close();
        tt.wait().await;

        Ok(())
    }
}
