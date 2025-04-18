use std::collections::HashMap;

use anyhow::{anyhow, Result};
use clap;
use tracing::error;
use tracing_subscriber;

use chapterhouseqe::{
    config::{ConnectionType, WorkerConfig},
    handlers::operator_handler::{operators, TotalOperatorCompute},
    tui::WorkerArgs,
    worker::{QueryWorker, QueryWorkerConfig},
};

use clap::Parser;

fn main() -> Result<()> {
    let args = WorkerArgs::parse();
    let config = WorkerConfig::from_file(args.config_file)?;

    tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(config.log_level()?)
        .init();

    // add all connections
    let mut conn_reg = operators::ConnectionRegistry::new();

    for connection in config.connections {
        match connection.connection_type {
            ConnectionType::S3 {
                endpoint,
                access_key_id,
                secret_access_key_id,
                bucket,
                region,
                force_path_style,
            } => {
                conn_reg.add_s3_connection(
                    connection.name,
                    endpoint,
                    access_key_id,
                    secret_access_key_id,
                    bucket,
                    force_path_style,
                    region,
                )?;
            }
            ConnectionType::Fs => {
                conn_reg.add_fs_connection(connection.name, connection.path_prefix)?;
            }
        }
    }

    match conn_reg.add_connection(
        "default".to_string(),
        opendal::Scheme::Fs,
        vec![("root".to_string(), "./sample_data".to_string())]
            .into_iter()
            .collect::<HashMap<String, String>>(),
    ) {
        Ok(_) => (),
        Err(err) => {
            error!("{}", err);
            return Err(anyhow!("worker exited with error"));
        }
    }

    let mut worker = QueryWorker::new(QueryWorkerConfig::new(
        format!("0.0.0.0:{}", config.port),
        config.connect_to_addresses,
        TotalOperatorCompute {
            instances: 10,
            memory_in_mib: 1 << 12, // 4096 mebibytes
            cpu_in_thousandths: 4_000,
        },
        conn_reg,
    ));

    match worker.start() {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("{}", e);
            Err(anyhow!("worker exited with error"))
        }
    }
}
