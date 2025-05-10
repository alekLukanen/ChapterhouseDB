use anyhow::{anyhow, Result};
use aws_config::meta::region::RegionProviderChain;

use chapterhousedb::config::{ConnectionType, WorkerConfig};
use clap::Parser;
use rand::Rng;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber;

use chapterhousedb::handlers::operator_handler::operators::ConnectionRegistry;
use chapterhousedb::tui::CreateSampleDataArgs;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = CreateSampleDataArgs::parse();
    args.validate()?;

    let worker_config = WorkerConfig::from_file(args.config_file)?;

    let connection = if let Some(conn) = worker_config
        .connections
        .iter()
        .find(|conn| conn.name == args.connection_name)
    {
        conn
    } else {
        return Err(anyhow!("connection not found"));
    };

    // get the connection specfic base path
    let base_path = match &connection.connection_type {
        ConnectionType::S3 { .. } => std::path::PathBuf::from(args.path_prefix.clone()),
        ConnectionType::Fs { .. } => std::path::PathBuf::from(args.path_prefix.clone()),
    };

    // setup initial resources
    match &connection.connection_type {
        ConnectionType::S3 {
            endpoint,
            access_key_id,
            secret_access_key_id,
            bucket,
            region,
            force_path_style,
            ..
        } => {
            let region_provider =
                RegionProviderChain::first_try(aws_sdk_s3::config::Region::new(region.clone()));
            let region = region_provider.region().await.unwrap();

            let shared_config = aws_config::defaults(aws_config::BehaviorVersion::v2025_01_17())
                .region(region_provider)
                .load()
                .await;

            let config = aws_sdk_s3::config::Builder::from(&shared_config)
                .region(region)
                .endpoint_url(endpoint.clone())
                .force_path_style(force_path_style.clone())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    access_key_id.clone(),
                    secret_access_key_id.clone(),
                    None,
                    None,
                    "static",
                ))
                .build();

            let client = aws_sdk_s3::Client::from_conf(config);

            let exists = match client.head_bucket().bucket(bucket).send().await {
                Ok(_) => true,
                Err(e) => {
                    if let aws_sdk_s3::operation::head_bucket::HeadBucketError::NotFound(_) =
                        e.into_service_error()
                    {
                        false
                    } else {
                        return Err(anyhow!("unable to cast S3 error"));
                    }
                }
            };

            if !exists {
                client.create_bucket().bucket(bucket).send().await?;
                info!("Created bucket '{}'", bucket);
            } else {
                info!("Bucket '{}' already exists", bucket);
            }
        }
        ConnectionType::Fs { .. } => {}
    }

    // create the opendal connection
    let mut conn_reg = ConnectionRegistry::new();
    conn_reg.add_worker_connections(&worker_config)?;

    // get the operator
    let operator = conn_reg.get_operator(args.connection_name.as_str())?;

    create_simple_data(&operator, base_path.clone()).await?;
    create_simple_wide_string_data(&operator, base_path.clone()).await?;

    create_large_simple_data(&operator, base_path.clone()).await?;
    create_huge_simple_data(&operator, base_path.clone()).await?;

    Ok(())
}

async fn create_large_simple_data(
    operator: &opendal::Operator,
    mut base_path: std::path::PathBuf,
) -> Result<()> {
    base_path.push("large_simple");

    simple_data(operator, &base_path, 10_000, 8, 1000).await?;

    Ok(())
}

async fn create_huge_simple_data(
    operator: &opendal::Operator,
    mut base_path: std::path::PathBuf,
) -> Result<()> {
    base_path.push("huge_simple");

    simple_data(operator, &base_path, 1_000_000, 8, 10_000).await?;

    Ok(())
}

async fn create_simple_data(
    operator: &opendal::Operator,
    mut base_path: std::path::PathBuf,
) -> Result<()> {
    base_path.push("simple");

    simple_data(operator, &base_path, 100, 8, 33).await?;

    Ok(())
}

async fn create_simple_wide_string_data(
    operator: &opendal::Operator,
    mut base_path: std::path::PathBuf,
) -> Result<()> {
    base_path.push("simple_wide_string");

    simple_data(operator, &base_path, 100, 100, 33).await?;

    Ok(())
}

async fn simple_data(
    operator: &opendal::Operator,
    base_path: &std::path::PathBuf,
    size: usize,
    string_size: usize,
    rows_per_file: usize,
) -> Result<()> {
    // Create the schema
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("value1", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("value2", arrow::datatypes::DataType::Float32, false),
    ]));

    // Generate data
    let rng = rand::thread_rng();
    let id_values: Vec<i32> = (0..size as i32).collect();
    let value1_values: Vec<String> = (0..size)
        .map(|_| {
            let chars: String = rng
                .clone()
                .sample_iter(&rand::distributions::Uniform::new_inclusive('a', 'z'))
                .take(string_size)
                .collect();
            chars
        })
        .collect();
    let value2_values: Vec<f32> = (0..size)
        .map(|_| {
            rng.clone()
                .sample(rand::distributions::Uniform::new(0.0, 100.0))
        })
        .collect();

    // Create the Arrow arrays
    let id_array =
        Arc::new(arrow::array::Int32Array::from(id_values)) as Arc<dyn arrow::array::Array>;
    let value1_array =
        Arc::new(arrow::array::StringArray::from(value1_values)) as Arc<dyn arrow::array::Array>;
    let value2_array =
        Arc::new(arrow::array::Float32Array::from(value2_values)) as Arc<dyn arrow::array::Array>;

    // Create the RecordBatch
    let batch = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![id_array.clone(), value1_array.clone(), value2_array.clone()],
    )?;

    for i in 0..size.div_ceil(rows_per_file) {
        let start = i * rows_per_file;
        let end = std::cmp::min((i + 1) * rows_per_file, size);

        let sliced_batch = batch.slice(start, end - start);

        let mut file_name = base_path.clone();
        file_name.push(format!("part_{}.parquet", i + 1));

        let writer = operator
            .writer_with(clean_path_for_dataset(&file_name))
            .chunk(16 * 1024 * 1024)
            .concurrent(4)
            .await?;

        let parquet_writer = parquet_opendal::AsyncWriter::new(writer);
        let mut arrow_parquet_writer =
            parquet::arrow::AsyncArrowWriter::try_new(parquet_writer, sliced_batch.schema(), None)?;
        arrow_parquet_writer.write(&sliced_batch).await?;
        arrow_parquet_writer.close().await?;

        info!("Wrote {} rows to {:?}", sliced_batch.num_rows(), file_name);
    }

    Ok(())
}

fn clean_path_for_dataset(path: &std::path::PathBuf) -> &str {
    let str_path = path.to_str().expect("expected path to_str() to work");
    str_path.strip_prefix("./").unwrap_or(str_path)
}
