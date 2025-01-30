# ChapterhouseQE
A parallel SQL based query engine for analytics queries on Parquet files. 

## Running the Base System

Start first worker
```
cargo run --bin main -- -p=7000 -c=127.0.0.1:7001
```

Start second worker
```
cargo run --bin main -- -p=7001 -c=127.0.0.1:7000
```

The workers will each form a TCP connection with one the other worker and 
begin transmitting messages back and forth in preparation for processing 
queries.

## Architecture

The system is built upon a set of distributed actors that communicate through
messages. Each worker can communicate with all other workers connected to it
and any worker can accept and manage queries. Queries create operators, a type of actor
capable of performing the tasks necessary to compute a query result. For example, the query:
```
select * from read_files('simple/*.parquet')
  where value2 > 10.0;
```

will produce these operators

```
[read files operator] -> [exchange operator] -> [filter operator] -> [exchange operator] -> [materialize operator] -> [exchange operator]
```

Each of the operators in this query can also have individual instances of themselves so that
its task can be computed in parallel. These operators perform some operation
on an Apache Arrow record batch. The read files operator reads records from the parquet
files and pushes them to the exchange operator. Then the filter operator pulls the next
available record from that exchange operator and produces a record containing only
the data matching the "where" expression. And so on until the DAG of operators has completed. By 
structuring the operators in this way it makes it relatively easy to create new operators
as each operator either pulls data from an exchange or an external source, and pushes
data to an exchanges.

## Rust Notes

To enable backtraces you can run commands with the following environment variable like
the following
```
RUST_BACKTRACE=1 cargo run 
```

## External Links

1. SQL grammar for H2 embedded database system: http://www.h2database.com/html/commands.html#select
2. SQL grammar for phoenix an SQL layer over the HBase database system: https://forcedotcom.github.io/phoenix/
3. Parsing example: http://craftinginterpreters.com/parsing-expressions.html
4. Rayon and Tokio methods for blocking code: https://ryhl.io/blog/async-what-is-blocking/
5. Globset crate: https://docs.rs/globset/latest/globset/
6. Arrow IPC format: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc

