# ChapterhouseQE
An SQL based query engine for analytics workloads.

## Running the Base System

You and use the following commands to test out starting the system.

Start first worker
```
cargo run --bin main -- -p=7000 -c=127.0.0.1:7001
```

Start second worker
```
cargo run --bin main -- -p=7001 -c=127.0.0.1:7000
```

## Query Stages

### Parsing

In this stage the system will parse the SQL by first tokenizing the SQL text.
Then it will construct a parse tree of the SQL query to validate that the 
SQL is syntactically correct. This parse tree will also be used to generate the
query plan later on. After the parse tree has been created the system will
validate that the use of certain variables is correct given the schema information
present, if any. It's likely that no schema will be available since the query
engine is intended to be run on arbitrary files in object storage. Certain 
uses can be validated such as values defined within the query itself.

### Code Generation

Given the parse tree from the parsing stage the system will generate a set of instructions
defining the core processes needed to execute a query. This code will be interpreted 
by a virtual machine designed to execute it. For example, if you have a query like the following

### Execution

The code is sent to a virtual machine which can interpret the code. 
The machine that accepts this code is known as the query coordinator and
is responsible for managing the processing of the query across a set of worker
machines. The worker machines are capable of executing top level steps and reporting
back to the coordinator when done. Both of these machines connect to a central object
storage to share data. It might also be necessary to include a key-value database
to ensure that transactional operations are performant.

# Rust Notes

To enable backtraces you can run commands with the following environment variable like
the following
```
RUST_BACKTRACE=1 cargo run 
```

# External Links

1. SQL grammar for H2 embedded database system: http://www.h2database.com/html/commands.html#select
2. SQL grammar for phoenix an SQL layer over the HBase database system: https://forcedotcom.github.io/phoenix/
3. Parsing example: http://craftinginterpreters.com/parsing-expressions.html

