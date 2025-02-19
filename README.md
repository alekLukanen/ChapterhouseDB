# 📖 ChapterhouseQE
A simple SQL query engine capable of distributing work across a set of
workers. The intent of this project is to allow me to test out various 
query optimization techniques.

## 🚀 Running the Base System

1. Create the sample data by running the following command

  ```bash
  cargo run --bin create_sample_data
  ```

2. Now start the workers to form a cluster. Start first worker

  ```bash
  cargo run --bin main -- -p=7000 -c=127.0.0.1:7001
  ```

  Start second worker

  ```bash
  cargo run --bin main -- -p=7001 -c=127.0.0.1:7000
  ```

  The workers will each form a TCP connection with the other worker and 
  begin transmitting messages back and forth in preparation for processing 
  queries.

3. To run a simple query you can run the client example

  ```bash
  cargo run --bin client_main
  ```

  This client will connect to port 7000 and initiate a query. The result
  will show up in the `sample_data/query_results/` directory.

## 🛢️ Supported SQL

- [:white_check_mark:] Types
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

## 🛠 Architecture

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


