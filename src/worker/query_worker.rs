use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;
use uuid::Uuid;

use crate::handlers::message_handler::{ConnectionPoolHandler, MessageRegistry};
use crate::handlers::message_router_handler::MessageRouterHandler;

pub struct QueryWorkerConfig {
    address: String,
    connect_to_addresses: Vec<String>,
}

impl QueryWorkerConfig {
    pub fn new(address: String, connect_to_addresses: Vec<String>) -> QueryWorkerConfig {
        QueryWorkerConfig {
            address,
            connect_to_addresses,
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

        let msg_reg = Arc::new(Box::new(MessageRegistry::new()));

        // Connect Pool and Router ////////////////////////
        let (mut connection_pool_handler, connection_msg_pipe) = ConnectionPoolHandler::new(
            self.worker_id,
            self.config.address.clone(),
            self.config.connect_to_addresses.clone(),
            msg_reg.clone(),
        );

        let mut message_router =
            MessageRouterHandler::new(self.worker_id.clone(), connection_msg_pipe);

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

        // TaskTracker /////////////////////
        // wait for the cancelation token to be cancelled and all tasks to be cancelled
        tt.close();
        tt.wait().await;

        Ok(())
    }
}
