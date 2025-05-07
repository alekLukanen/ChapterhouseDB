use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::handlers::message_handler::{ConnectionPoolHandler, MessageRegistry};
use crate::handlers::message_router_handler::MessageRouterHandler;
use crate::handlers::operator_handler::operators::{self, ConnectionRegistry};
use crate::handlers::operator_handler::OperatorHandler;
use crate::handlers::query_handler::{QueryDataHandler, QueryHandler};

pub struct QueryWorker {
    worker_id: u128,
    config: WorkerConfig,
    conn_reg: Arc<ConnectionRegistry>,
    cancelation_token: CancellationToken,
}

impl QueryWorker {
    pub fn new(config: WorkerConfig, conn_reg: Arc<ConnectionRegistry>) -> QueryWorker {
        let ct = CancellationToken::new();
        return QueryWorker {
            worker_id: Uuid::new_v4().as_u128(),
            config,
            conn_reg,
            cancelation_token: ct,
        };
    }

    pub fn start(&mut self) -> Result<()> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

        runtime.block_on(self.async_main())
    }

    async fn async_main(&mut self) -> Result<()> {
        info!("worker_id: {}", self.worker_id);

        let tt = TaskTracker::new();

        let msg_reg = Arc::new(MessageRegistry::new());
        let op_reg = Arc::new(operators::build_default_operator_task_registry()?);
        let conn_reg = self.conn_reg.clone();

        // Connect Pool and Router ////////////////////////
        let (mut connection_pool_handler, connection_msg_pipe) = ConnectionPoolHandler::new(
            self.worker_id,
            format!("0.0.0.0:{}", self.config.port),
            self.config.connect_to_addresses.clone(),
            msg_reg.clone(),
        );

        let (mut message_router, message_router_state) =
            MessageRouterHandler::new(self.worker_id.clone(), connection_msg_pipe, msg_reg.clone());

        // add internal subscribers

        let conn_pool_ct = self.cancelation_token.child_token();
        tt.spawn(async move {
            if let Err(err) = connection_pool_handler.async_main(conn_pool_ct).await {
                error!("{:?}", err);
            }
        });

        let message_router_ct = self.cancelation_token.child_token();
        tt.spawn(async move {
            if let Err(err) = message_router.async_main(message_router_ct).await {
                error!("{:?}", err);
            }
        });

        if self.config.enable_query_handler {
            let mut query_handler = QueryHandler::new(
                self.worker_id.clone(),
                message_router_state.clone(),
                msg_reg.clone(),
            )
            .await;

            let query_handler_ct = self.cancelation_token.child_token();
            tt.spawn(async move {
                if let Err(err) = query_handler.async_main(query_handler_ct).await {
                    error!("{:?}", err);
                }
            });
        }

        if self.config.enable_query_data_handler {
            let mut query_data_handler = QueryDataHandler::new(
                message_router_state.clone(),
                msg_reg.clone(),
                conn_reg.clone(),
            )
            .await;

            let query_data_handler_ct = self.cancelation_token.child_token();
            tt.spawn(async move {
                if let Err(err) = query_data_handler.async_main(query_data_handler_ct).await {
                    error!("{:?}", err);
                }
            });
        }

        if self.config.enable_operator_handler {
            let mut operator_handler = OperatorHandler::new(
                message_router_state.clone(),
                msg_reg.clone(),
                op_reg.clone(),
                conn_reg.clone(),
                self.config.operator_handler_config.compute.clone(),
            )
            .await;

            let operator_handler_ct = self.cancelation_token.child_token();
            tt.spawn(async move {
                if let Err(err) = operator_handler.async_main(operator_handler_ct).await {
                    error!("{:?}", err);
                }
            });
        }

        // TaskTracker /////////////////////
        // wait for the cancelation token to be cancelled and all tasks to be cancelled
        tt.close();
        tt.wait().await;

        Ok(())
    }
}
