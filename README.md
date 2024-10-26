# ChapterhouseQE
An SQL based query engine for analytics workloads.

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

```SQL
select sum(table1.B) from read_files('/table1/*.parquet', connection='etl_s3') table1
  where table1.A = 'abc';
```

It would produce code something like the following

```yaml
steps:
  - step: 0
    type: "func"
    call: "read_files"
    args: ["/table1/*.parquet"]
    kwargs:
      connection: "etl_s3"
    outputFields: ["A", "B"]
    setVar: "t0"
  - step: 1
    type: "proc"
    ex: "scan"
    where:
      - step: 0
        type: "proc"
        ex: "eq"
        left:
          type: "field"
          table: "t0"
          field: "A"
        right:
          type: "string_literal"
          val: "abc"
        setVar: "s1.t0"
    resultVar: "s1.t0"
    outputFields: ["B"]
    setVar: "t1"
  - step: 2
    type: "proc"
    ex: "sum"
    fromVar: "t1"
    setVar: "t2"
  - step: 3
    type: "proc"
    ex: "materialize"
    outputFields:
      - var: "t2"
    format: "avro"
```

This is a rough idea of what the code could look like, but it will probably look
much different in the actual implementation. The basic idea is that you define
the process of extracting and filtering the data by sharing data between
steps through variables. Some steps can have their own steps if they
need to perform a complex action like comparisons for filtering.

### Execution

The code is sent to a virtual machine which can interpret the code. 
The machine that accepts this code is know is the query coordinator and
is responsible for managing the processing of the query across a set of worker
machines. The worker machines are capable of executing top level steps and reporting
back to the coordinator when done. Both of these machines connect to a central object
storage to share data. It might also be necessary to include a key-value database
to ensure that transactional operations are performant.
