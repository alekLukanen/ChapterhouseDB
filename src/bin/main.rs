use chapterhouseqe::worker::QueryWorker;
use tracing::info;
use tracing_subscriber;

fn main() {
    tracing_subscriber::fmt::init();

    info!("creating the worker");

    let worker = QueryWorker::new("127.0.0.1:7000".to_string());

    match worker.start() {
        Ok(_) => return,
        Err(e) => println!("error: {}", e),
    }
}
