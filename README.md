# 📖 ChapterhouseDB
A distributed SQL query engine built in Rust, designed for extensibility, 
and developer-first workflows. While still in early development, its 
long-term vision is to provide a seamless environment where SQL and Rust work 
together to power data pipelines and backend systems. You will be able to write 
Rust to connect to APIs, transform complex or nested data, and define custom operators. 
And use SQL to orchestrate processing, move and clean data, and manage 
storage—all in a unified, scalable engine.

> [!NOTE]
> This project has been renamed to ChapterhouseDB from ChapterhouseQE.

![Query TUI](./imgs/query_tui_example.gif)


## Running the Docker Container

1. Build the image
```
DOCKER_BUILDKIT=1 docker compose build chdb-debug-node
```

2. Start a container
```
DOCKER_BUILDKIT=1 docker compose up chdb-debug-node
```

At this point the system will be ready to accept requests. The image
is built with a small set of example datasets that can be queried.


## Running the TUI

You can run the TUI with a set of example queries using this command

```
cargo run --bin client_tui -- --sql-file="sample_queries/simple.sql" --connect-to-address="127.0.0.1"
```

The TUI will send the queries to the worker and allow you to visualize
the result data of the queries in a table.


## 🛢️ Supported SQL

- [X] Types
  - [X] Numeric types
  - [X] String type
  - [ ] Time types
  - [ ] Decimal types
- [x] Expressions
  - [X] Basic mathematical operations
  - [X] AND, OR
  - [ ] XOR
  - [ ] String concatenation ||
  - [ ] LIKE, ILIKE
- [X] Select statement
  - [X] Projection
  - [X] Where
  - [ ] Order by
  - [ ] Group by
  - [ ] Having
  - [ ] Inner join
  - [ ] Left join
  - [ ] Right join
  - [ ] Full join
  - [ ] With 
  - [X] Read from files (Ex `read_files('simple/*.parquet')`)
    - [X] Parquet
    - [ ] CSV
    - [ ] JSON
  - [ ] Read from table
- [ ] Functions
  - [ ] User-defined functions
  - [ ] Standard SQL functions


## 🛠 Architecture

The system is built upon a set of distributed actors that communicate through
messages. Each worker can communicate with all other workers connected to it
and any worker can accept and manage queries. Queries create operators, a type of actor
capable of performing the tasks necessary to compute a query result. For example, the query:
```sql
select * from read_files('simple/*.parquet')
  where value2 > 10.0;
```

will produce these operators
```
[read files] -> [exchange] -> [filter] -> [exchange] -> [materialize] -> [exchange]
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


## Future Functionality

- [ ] Support common SQL operations such as those listed in the "Supported SQL" section.
- [ ] UDFs and custom deployable operators that act as data sources
- [ ] Create a Kubernetes integration which allows the system to scale based on demand. The nodes will need
to communicate their cluster IP through S3.





