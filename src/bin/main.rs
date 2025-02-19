use std::collections::HashMap;

use clap;
use tracing_subscriber;

use chapterhouseqe::{
    handlers::operator_handler::{operators, TotalOperatorCompute},
    worker::{QueryWorker, QueryWorkerConfig},
};

use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The port used to accept incoming connections
    #[arg(short, long, default_value_t = 7000)]
    port: u32,

    /// Addresses to connect to
    #[arg(short, long)]
    connect_to_addresses: Vec<String>,

    /// The logging level (debug, info, warning, error)
    #[arg(short, long, default_value_t = String::from("info"))]
    log_level: String,
}

fn main() {
    let args = Args::parse();

    let log_level = if args.log_level.to_lowercase() == "info" {
        tracing::Level::INFO
    } else if args.log_level.to_lowercase() == "debug" {
        tracing::Level::DEBUG
    } else if args.log_level.to_lowercase() == "warning" {
        tracing::Level::WARN
    } else if args.log_level.to_lowercase() == "error" {
        tracing::Level::ERROR
    } else {
        panic!("unknown log level")
    };

    tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(log_level)
        .init();

    let mut conn_reg = operators::ConnectionRegistry::new();
    conn_reg.add_connection(
        "default".to_string(),
        opendal::Scheme::Fs,
        vec![("root".to_string(), "./sample_data".to_string())]
            .into_iter()
            .collect::<HashMap<String, String>>(),
    );

    let mut worker = QueryWorker::new(QueryWorkerConfig::new(
        format!("127.0.0.1:{}", args.port),
        args.connect_to_addresses,
        TotalOperatorCompute {
            instances: 10,
            memory_in_mib: 1 << 12, // 4096 mebibytes
            cpu_in_thousandths: 4_000,
        },
        conn_reg,
    ));

    match worker.start() {
        Ok(_) => return,
        Err(e) => println!("error: {}", e),
    }
}
