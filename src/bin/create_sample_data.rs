use anyhow::{anyhow, Result};
use aws_config::meta::region::RegionProviderChain;
use chapterhouseqe::handlers::operator_handler::operators::ConnectionRegistry;
use rand::Rng;
use std::sync::Arc;
use std::{collections::HashMap, fs::File};
use tracing::info;
use tracing_subscriber;

use clap::{Parser, Subcommand};
use path_clean::PathClean;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// A prefix or directory to store all data
    path_prefix: String,
    #[command(subcommand)]
    command: Option<Command>,
}

impl Args {
    fn validate(&self) -> Result<()> {
        if self.path_prefix.len() == 0 {
            return Err(anyhow!("path_prefix must be a value"));
        }
        return Ok(());
    }
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    S3 {
        /// The endpoint that S3 is located at
        endpoint: String,
        /// The access key
        access_key_id: String,
        /// The secret key
        secret_access_key_id: String,
        /// The bucket to create data in
        bucket: String,
        /// region
        region: String,
    },
    Fs,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    args.validate()?;

    // get the connection specfic base path
    let base_path = match &args.command {
        Some(Command::S3 { .. }) => std::path::PathBuf::from(args.path_prefix.clone()),
        Some(Command::Fs) => std::path::PathBuf::from(args.path_prefix.clone()).clean(),
        None => {
            info!("You must set a command");
            return Ok(());
        }
    };

    // setup initial resources
    match &args.command {
        Some(Command::S3 {
            endpoint,
            access_key_id,
            secret_access_key_id,
            bucket,
            region,
        }) => {
            let region_provider =
                RegionProviderChain::first_try(aws_sdk_s3::config::Region::new(region.clone()));
            let region = region_provider.region().await.unwrap();

            let config = aws_sdk_s3::config::Builder::new()
                .region(region)
                .endpoint_url(endpoint.clone())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    access_key_id.clone(),
                    secret_access_key_id.clone(),
                    None,
                    None,
                    "static",
                ))
                .build();

            let client = aws_sdk_s3::Client::from_conf(config);

            // Try to create the bucket (safe even if it already exists in MinIO)
            client.create_bucket().bucket(bucket.clone()).send().await?;
        }
        Some(Command::Fs) => {
            create_dir(&base_path)?;
        }
        None => {
            info!("You must provide a valid command");
            return Ok(());
        }
    }

    // create the opendal connection
    let mut conn_reg = ConnectionRegistry::new();
    match &args.command {
        Some(Command::S3 {
            endpoint,
            access_key_id,
            secret_access_key_id,
            bucket,
            region,
        }) => conn_reg.add_connection(
            "default".to_string(),
            opendal::Scheme::S3,
            vec![
                ("bucket".to_string(), bucket.clone()),
                ("endpoint".to_string(), endpoint.clone()),
                ("access_key_id".to_string(), access_key_id.clone()),
                (
                    "secret_access_key".to_string(),
                    secret_access_key_id.clone(),
                ),
                ("enable_virtual_host_style".to_string(), "false".to_string()),
                // Optional:
                ("region".to_string(), region.clone()), // MinIO doesn’t enforce this
            ]
            .into_iter()
            .collect::<HashMap<String, String>>(),
        ),
        Some(Command::Fs) => conn_reg.add_connection(
            "default".to_string(),
            opendal::Scheme::Fs,
            vec![("root".to_string(), args.path_prefix)]
                .into_iter()
                .collect::<HashMap<String, String>>(),
        ),
        None => {
            info!("You must provide a command");
            return Ok(());
        }
    }

    // get the operator
    let operator = conn_reg.get_operator("default")?;

    create_simple_data(&operator, base_path.clone()).await?;
    create_simple_wide_string_data(&operator, base_path.clone()).await?;

    create_large_simple_data(&operator, base_path.clone()).await?;
    create_huge_simple_data(&operator, base_path.clone()).await?;

    Ok(())
}

fn create_dir(base_path: &std::path::PathBuf) -> Result<()> {
    if !std::path::Path::new(&base_path).exists() {
        std::fs::create_dir_all(&base_path)?;
        info!("Directory created: {:?}", base_path);
    } else {
        info!("Directory already exists: {:?}", base_path);
    }
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
    create_dir(&base_path)?;

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
            .writer_with(file_name.to_str().expect("expected file_name"))
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
