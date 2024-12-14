use chapterhouseqe::worker::{QueryWorker, QueryWorkerConfig};
use tracing::info;
use tracing_subscriber;

fn main() {
    tracing_subscriber::fmt::init();

    info!("creating the worker");

    let mut worker = QueryWorker::new(QueryWorkerConfig::new(
        "127.0.0.1:7000".to_string(),
        vec!["127.0.0.1:7001".to_string()],
    ));

    match worker.start() {
        Ok(_) => return,
        Err(e) => println!("error: {}", e),
    }
}
