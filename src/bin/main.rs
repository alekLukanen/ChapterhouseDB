use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap;
use tracing::error;
use tracing_subscriber;

use chapterhousedb::{
    config::WorkerConfig, handlers::operator_handler::operators, tui::WorkerArgs,
    worker::QueryWorker,
};

use clap::Parser;

fn main() -> Result<()> {
    let args = WorkerArgs::parse();
    args.validate()?;

    let worker_config = WorkerConfig::from_file(args.config_file)?;

    tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(worker_config.log_level()?)
        .init();

    // add all connections
    let mut conn_reg = operators::ConnectionRegistry::new();
    conn_reg.add_worker_connections(&worker_config)?;

    let mut worker = QueryWorker::new(worker_config, Arc::new(conn_reg));
    match worker.start() {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("{}", e);
            Err(anyhow!("worker exited with error"))
        }
    }
}
