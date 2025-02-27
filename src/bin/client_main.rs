use std::sync::Arc;

use anyhow::{Context, Result};
use chapterhouseqe::{
    client::{AsyncQueryClient, QueryDataIterator},
    handlers::{message_handler::messages, query_handler},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use tracing_subscriber;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The logging level (debug, info, warning, error)
    #[arg(short, long, default_value_t = String::from("info"))]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
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

    let address = "127.0.0.1:7000";
    let client = Arc::new(AsyncQueryClient::new(address.to_string()));
    let ct = CancellationToken::new();

    let query = "
        select 
            id, 
            id + 10.0 as id_plus_10, 
            (value2 + 10) / 100 as value2 
        from read_files('simple/*.parquet')
            where id > 25 + 0.0;";
    let run_query_resp = client
        .run_query(ct.clone(), query.to_string())
        .await
        .context("failed initiating a query run")?;

    debug!("run_query_resp: {:?}", run_query_resp);

    let query_id = match run_query_resp {
        messages::query::RunQueryResp::Created { query_id } => query_id,
        messages::query::RunQueryResp::NotCreated => {
            error!("query failed to be created");
            return Ok(());
        }
    };

    debug!("waiting for query to finish...");
    let query_status = client
        .wait_for_query_to_finish(
            ct.clone(),
            &query_id,
            chrono::Duration::seconds(60),
            chrono::Duration::milliseconds(100),
        )
        .await?;

    debug!("get query status resp: {:?}", query_status);

    match query_status {
        messages::query::GetQueryStatusResp::Status(status) => match status {
            query_handler::Status::Complete => {}
            query_handler::Status::Error(err) => {
                error!("{:?}", err);
                return Ok(());
            }
            status => {
                error!("unexpected status: {:?}", status);
                return Ok(());
            }
        },
        messages::query::GetQueryStatusResp::QueryNotFound => {
            error!("query was not found");
            return Ok(());
        }
    }

    debug!("get the query data");
    let mut query_data_iter =
        QueryDataIterator::new(client, query_id, 0, 0, chrono::Duration::seconds(1));
    loop {
        let rec = query_data_iter.next(ct.clone()).await?;
        match rec {
            Some(rec) => {
                let recs = vec![rec.as_ref().clone()];
                let rec_txt = arrow::util::pretty::pretty_format_batches(&recs)?;
                info!("record data\n{}", rec_txt);
            }
            None => {
                break;
            }
        }
    }

    info!("query has completed!");

    Ok(())
}
