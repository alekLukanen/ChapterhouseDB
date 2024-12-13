use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;
use uuid::Uuid;

use crate::handlers::message_handler::MessageHandler;

pub struct QueryWorker {
    worker_id: u128,
    messenger: Arc<MessageHandler>,
    cancelation_token: CancellationToken,
}

impl QueryWorker {
    pub fn new(address: String) -> QueryWorker {
        let ct = CancellationToken::new();
        return QueryWorker {
            worker_id: Uuid::new_v4().as_u128(),
            messenger: Arc::new(MessageHandler::new(address.clone())),
            cancelation_token: ct,
        };
    }

    pub fn start(&self) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

        runtime.block_on(self.async_main())
    }

    async fn async_main(&self) -> Result<()> {
        let tt = TaskTracker::new();

        // Messenger ////////////////////////
        let messenger = Arc::clone(&self.messenger);
        let messenger_ct = self.cancelation_token.clone();
        tt.spawn(async move {
            if let Err(err) = messenger.listen(messenger_ct).await {
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
