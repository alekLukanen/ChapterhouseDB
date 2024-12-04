use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::info;

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

        info!("starting the tcp server");

        runtime.block_on(async {
            let listener = TcpListener::bind(&self.address).await?;

            info!("Server listening on {}", self.address);

            loop {
                let (mut socket, _) = listener.accept().await?;
                println!("New connection established");

                // Spawn a new task to handle the connection
                tokio::spawn(async move {
                    let mut buf = vec![0; 1024];
                    match socket.read(&mut buf).await {
                        Ok(n) if n == 0 => return, // Connection closed
                        Ok(n) => {
                            // Echo the data back to the client
                            if let Err(e) = socket.write_all(&buf[0..n]).await {
                                info!("Failed to send response: {}", e);
                            }
                        }
                        Err(e) => info!("Failed to read from socket: {}", e),
                    }
                });
            }
        })
    }
}
