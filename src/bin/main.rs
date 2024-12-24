use clap;
use tracing_subscriber;

use chapterhouseqe::{
    handlers::operator_handler::TotalOperatorCompute,
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

    // Addresses to connect to
    #[arg(short, long, default_value = "127.0.0.1:7001")]
    connect_to_addresses: Vec<String>,
}

fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let mut worker = QueryWorker::new(QueryWorkerConfig::new(
        format!("127.0.0.1:{}", args.port),
        args.connect_to_addresses,
        TotalOperatorCompute {
            instances: 10,
            memory_in_mib: 1 << 11, // 2048 mebibytes
            cpu_in_thousandths: 2_000,
        },
    ));

    match worker.start() {
        Ok(_) => return,
        Err(e) => println!("error: {}", e),
    }
}
