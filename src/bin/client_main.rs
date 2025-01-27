use anyhow::{Context, Result};
use chapterhouseqe::client::AsyncQueryClient;
use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let address = "127.0.0.1:7000";
    let client = AsyncQueryClient::new(address.to_string());

    let query = "select * from read_files('simple/*.parquet');";
    let run_query_resp = client
        .run_query(query.to_string())
        .await
        .context("failed initiating a query run")?;

    info!("run_query_resp: {:?}", run_query_resp);

    Ok(())
}
