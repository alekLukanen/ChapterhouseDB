use anyhow::Result;
use opendal::{services, Operator};
use rand::Rng;
use serde_json;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::fs::File;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber;

use chapterhouseqe::client::QueryClient;

use std::any::Any;
use tempdir::TempDir;

trait MyTrait: Any {
    fn as_any(&self) -> &dyn Any;
}

struct MyStruct {
    val: u32,
}

impl MyTrait for MyStruct {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // sql_parser_examples()

    // client_examples()?;

    /*
    let start = std::time::Instant::now();
    downcast_example();
    let duration = start.elapsed();
    println!("Time taken: {:?}", duration);
    */

    // opendal_testing().await?;

    // temp_dir()?;

    create_sample_data()?;

    Ok(())
}

fn create_sample_data() -> Result<()> {
    // Create the schema
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("value1", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("value2", arrow::datatypes::DataType::Float64, false),
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
    let value2_values: Vec<f64> = (0..100)
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
        Arc::new(arrow::array::Float64Array::from(value2_values)) as Arc<dyn arrow::array::Array>;

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

        let file_name = format!("./sample_data/part_{}.parquet", i + 1);
        let file = File::create(&file_name)?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None)?;

        writer.write(&sliced_batch)?;
        writer.close()?;

        println!("Wrote {} rows to {}", sliced_batch.num_rows(), file_name);
    }

    Ok(())
}

fn temp_dir() -> Result<()> {
    let path2 = TempDir::new("read_files")?;
    info!("path2: {:?}", path2);

    Ok(())
}

async fn opendal_testing() -> Result<()> {
    let mut builder = services::Fs::default().root("./");
    let op: Operator = Operator::new(builder)?.finish();

    let files = op.list("./").await?;
    for item in files {
        info!("item: {}", item.path());
        let stats = op.stat(item.path()).await?;
        info!("- metadata: {:?}", stats);
    }

    Ok(())
}

fn downcast_example() {
    let mut sum = 0u32;
    for _ in 0..1_000_000 {
        let obj: Box<dyn MyTrait> = Box::new(MyStruct { val: 123 });
        if let Some(sample) = obj.as_any().downcast_ref::<MyStruct>() {
            sum += sample.val;
        }
    }
}

fn client_examples() -> Result<()> {
    let mut client = QueryClient::new("127.0.0.1:7000".to_string())?;

    client.send_ping_message(3)?;

    Ok(())
}

fn sql_parser_examples() {
    let dialect = GenericDialect {};

    let query0 = "
        select * from bike 
        where id = 42 and value > 90.0 and name = '🥵';";
    let ast0 = match Parser::parse_sql(&dialect, query0) {
        Ok(res) => res,
        Err(err) => {
            println!("error: {}", err);
            return;
        }
    };
    println!("query0: {}", query0);
    println!("ast0: {:?}", ast0);

    let query1 = "select key, value0, value1 from read_some_files('abc', 123, kwarg='hello', kwarg1={'abcd': 1234}) data
                            where key = 12+2*52*(5+4*3)
                            order by key desc
                            limit 100
                            offset 10;";
    let mut ast1 = match Parser::parse_sql(&dialect, query1) {
        Ok(res) => res,
        Err(err) => {
            println!("error: {}", err);
            return;
        }
    };
    println!("query1: {}", query1);
    println!("ast1: {:?}", ast1);

    let query_ast1 = ast1.remove(0);
    let json1 = serde_json::to_string_pretty(&query_ast1).unwrap();
    println!("json1: {}", json1);

    let query2 =
        "COPY (select * from table1) TO 'data.csv' WITH (FORMAT 'CSV', NULL 'null_value');";
    let ast2 = match Parser::parse_sql(&dialect, query2) {
        Ok(res) => res,
        Err(err) => {
            println!("error: {}", err);
            return;
        }
    };
    println!("query2: {}", query2);
    println!("ast2: {:?}", ast2);

    let query3 = "
        CREATE TABLE my_table(
            row_id INT PRIMARY KEY,
            file_data_format STRING,
            file_data_here BINARY
        )
        LOCATION 'path/to/file/{col:row_id}.{col:file_data_format}.{prop:compression}'
        TBLPROPERTIES (
            'table-type'='row-to-file',
            'compression'='GZIP',
            'connection'='big_s3',
            'max-concurrent-puts'=10,
            'format-column'='file_data_format',
            'data-column'='file_data_here'
        );
    ";
    let ast3 = match Parser::parse_sql(&dialect, query3) {
        Ok(res) => res,
        Err(err) => {
            println!("error: {}", err);
            return;
        }
    };
    println!("query3: {}", query3);
    println!("ast3: {:?}", ast3);
}
