use anyhow::Result;
use rand::Rng;
use std::fs::File;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut base_path = std::path::PathBuf::new();
    base_path.push("./sample_data");

    create_simple_data(base_path.clone())?;

    Ok(())
}

fn create_simple_data(mut base_path: std::path::PathBuf) -> Result<()> {
    base_path.push("simple");

    if !std::path::Path::new(&base_path).exists() {
        std::fs::create_dir_all(&base_path)?;
        info!("Directory created: {:?}", base_path);
    } else {
        info!("Directory already exists: {:?}", base_path);
    }

    // Create the schema
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("value1", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("value2", arrow::datatypes::DataType::Float32, false),
    ]));

    // Generate data
    let rng = rand::thread_rng();
    let id_values: Vec<i32> = (0..100).collect();
    let value1_values: Vec<String> = (0..100)
        .map(|_| {
            let chars: String = rng
                .clone()
                .sample_iter(&rand::distributions::Uniform::new_inclusive('a', 'z'))
                .take(8)
                .collect();
            chars
        })
        .collect();
    let value2_values: Vec<f32> = (0..100)
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

    // Split the RecordBatch into 3 parts and write each to a Parquet file
    let rows_per_file = 33; // Approximately divide 100 rows into 3 files
    for i in 0..3 {
        let start = i * rows_per_file;
        let end = if i == 2 { 100 } else { (i + 1) * rows_per_file };

        let sliced_batch = batch.slice(start, end - start);

        let mut file_name = base_path.clone();
        file_name.push(format!("part_{}.parquet", i + 1));
        let file = File::create(&file_name)?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None)?;

        writer.write(&sliced_batch)?;
        writer.close()?;

        info!("Wrote {} rows to {:?}", sliced_batch.num_rows(), file_name);
    }

    Ok(())
}
