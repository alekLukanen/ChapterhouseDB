use crate::messenger::messenger::Messenger;
use anyhow::Result;

pub struct QueryWorker {
    address: String,
}

impl QueryWorker {
    pub fn new(address: String) -> QueryWorker {
        return QueryWorker { address: address };
    }

    pub fn start(&self) -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

        let messenger = Messenger::new(self.address.clone());
        runtime.block_on(messenger.listen())
    }
}
