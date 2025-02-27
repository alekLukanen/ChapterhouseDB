
## Next work

- [X] Implement computation of the where filter and projection expressions. For each Arrow record
you will apply the expression to it. The result will either be a projection or a filter 
of rows. Only implement numeric and string operations. The Arrow module should have all of 
the functionality necessary to perform basic math and string operations.
  * Compute module: https://arrow.apache.org/rust/arrow/compute/index.html
  * Compute kernels module: https://arrow.apache.org/rust/arrow/compute/kernels/index.html
  * Datum trait for single values: https://arrow.apache.org/rust/arrow/array/trait.Datum.html
  * Scalar type which implements Datum trait: https://arrow.apache.org/rust/arrow/array/struct.Scalar.html
  * Support these `BinaryOperator`'s: Plus, Minus, Multiply, Divide, Modulo, Gt, Lt, GtEq, LtEq,
  Eq, NotEq, And, Or. For strings support only the Eq operator.
- [X] When the exchange operator shuts down it should send out a status change message like
the producer operator does.
- [X] Make the message serialization and de-serialization async.
- [X] The client should be able to request records from the query result iteratively. The client
should be able to request one record at a time, where each record to one row group from a 
parquet file.
  * You can use a tuple `(file, row group, row index)` as the cursor. The row group is the 
  row group number in the file and the row index is the row index within the row group.
  Files should be sequential starting from `0`.
- [ ] Implement a basic terminal UI using Ratatui so that you can easily write queries, 
monitor query status including the status of individual operators and get view the query
results in a table. The editor should be on the left, system monitor on the right and the 
table at the bottom or in different view that is linked to by a result table underneath the 
system monitor. You should also implement basic saving of the queries into an sql file in the
current directory named `_chdb_editor_queries.sql` by default.
  * Docs: https://ratatui.rs/ 
- [ ] Materializing files should be be able to compact the records into a larger parquet file
with a max number of row groups and rows in each group. The task should be
able to take many records from the exchange until it has enough to fit into a row group,
then write that to the row group and continue on until the max number of row groups
have been reached or there is no more data left. 
  * Think about adding record slicing to the exchange, description in TODO item below.
- [ ] Test out on large dataset stored in S3 or Minio.
- [ ] Implement message shedding so that a slow consumer doesn't block the message router from
reading or sending messages. Since each consumer has a cap on the number of messages that
can be queued it's possible that a slow consumer's queue could get filled up and block the
router from sending the next message until there is room. This could cause a deadlock.
- [ ] Support string operations like concatenation, and basic comparisons from 
the following package: https://arrow.apache.org/rust/arrow/compute/kernels/comparison/index.html
- [ ] Implement memory management for the exchange operator. It should be able to keep its
memory usage under an threshold by writing records to disk which won't or haven't been used
in awhile. For example, it should keep records that will be used by producers in memory if
possible but write records that won't be used for awhile to disk. When a record from the front
of the record stack is no longer needed you can remove it's reference and load into a record
that will soon be used. Write the records to Arrow files not Parquet files and write these
records to the "default" connection (file systems, S3, etc...) under folder 
`/exchange/<query_id>/<exchange_instance_id>/<record_id>.arrow`.
- [ ] Refactor the query handler state so that the find and get method use an option for
things that they can't find instead of returning an error. For example, if a query is not
found return None instead of a query not found error.
- [ ] Update the Connection struct in `connection.rs` so that it handles errors more cleanly.
It should run the shutdown after the main loop before returning.

## TODO

- [ ] Handle the special case where a producer operator starts and completes before the 
exchange starts. This can happen when the producer doesn't produce any results. The producer
should wait for the exchange to boot up before trying to produce data. Make this generic
by putting the logic in the `producer_operator.rs` file instead of in each of the individual
tasks. The materialize files task already has this to some extent but not fully.
- [ ] When assigning operator to workers store the assignment in the state object and only 
send out producer operator assignments when a worker has claimed all dependencies for the operator.
So an exchange doesn't have any dependencies so those can be assigned immediately, but producers
should only assigned after all of its inbound and outbound exchanges have been assigned and
are running on a worker.
- [ ] The exchange operator should re-queue records that haven't taken too long to process
since their last heartbeat. Might also need to handle the case were only one producer operator
is left and all others have completed. This last producer operator might be slow. Will probably
need to integrate with the query handler so that it can initiate another producer operator
instance.
- [ ] (not likely to be needed) The materialization task should send the file locations to 
the exchange. This will be through a means other than Arrow records. The exchange will 
accept these record locations and load load the files into memory as requested by the next producer.
- [ ] In the exchange add slicing across records so that more or fewer rows can be requested.
This will require that the record number be re-indexed though. That might make it
difficult to keep record ordering. I think that the exchange could start keeping
track of slices of these records and those slices would become a new logical record
with their own record id. Each time a producer requests the next set of tuples the
exchange would create a new slice with a new record id, sequentially counting up 
from zero. This would ensure that the ordering of data is maintained. And since the
exchange operates on Arrow records I can slice to whatever granularity I need.
- [ ] Add SIMD instructions for compute kernels: https://arrow.apache.org/rust/arrow/compute/kernels/cmp/index.html
- [ ] Use a custom `Record` type for all record functionality. This will
allow for common operations where the record batch and table aliases are
needed. It prevents extra code needed to pass both around.
- [ ] Implement some kind of memory management struct which constrains how much memory
each utility function is allowed to use when computing values. When calling functions
which operate on the `Record` type the manager should be passed in and the function
should reserve space before creating new arrays or records. These arrays and records
should also store a weak reference in the manager so that the manager can keep track of
what is still active. The `Record` type should implement the `Drop` trait so that
when the record is dropped it removes itself from the manager.
- [ ] Update the query status polling loop so that the client can send a request and then 
receive a generic ok response. Then at this point the query handler will push an update to
the client when the query completes.

## 📄 Rust Notes

To enable backtraces you can run commands with the following environment variable like
the following
```
RUST_BACKTRACE=1 cargo run 
```

## 🔗 External Links

1. SQL grammar for H2 embedded database system: http://www.h2database.com/html/commands.html#select
2. SQL grammar for phoenix an SQL layer over the HBase database system: https://forcedotcom.github.io/phoenix/
3. Parsing example: http://craftinginterpreters.com/parsing-expressions.html
4. Rayon and Tokio methods for blocking code: https://ryhl.io/blog/async-what-is-blocking/
5. Globset crate: https://docs.rs/globset/latest/globset/
6. Arrow IPC format: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc

