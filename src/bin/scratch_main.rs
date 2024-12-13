use anyhow::Result;
use serde_json;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tracing::info;
use tracing_subscriber;

use chapterhouseqe::client::query_client::QueryClient;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // sql_parser_examples()

    client_examples()?;

    Ok(())
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
