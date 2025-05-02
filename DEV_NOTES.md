
## How to Create gif

```
ffmpeg -i vid_for_gif.webm -vf "fps=10,scale=1600:-1:flags=lanczos,palettegen" -y palette.png
ffmpeg -i vid_for_gif.webm -i palette.png -filter_complex "fps=10,scale=1600:-1:flags=lanczos[x];[x][1:v]paletteuse" -y output.gif
gifsicle -O3 --colors 128 output.gif > optimized.gif
```


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
- [X] Implement a basic terminal UI using Ratatui so that you can easily write queries, 
monitor query status and view the result data. The user should be able to point the program
to a file of SQL queries and have it run those queries.
  * Docs: https://ratatui.rs/ 
- [X] Add an support for using Minio as an object storage. The workers should be able to connect
to the storage and read and write data to it. The script to create sample data should also be
able to write data to Minio just like it can write to the file system.
- [ ] Implement the heartbeat for records received from an exchange. When requesting a record
the operator will construct a handler which pings the exchange with a new heartbeat every 
(heartbeat limit)/2 seconds. This will allow the record to be re-queued if the worker fails to 
process it. Then in the exchange make sure that the record re-queuing process is implemented
and runs every few seconds.
- [ ] Improve the message passing construct. 
  - [ ] Each operator should be given an "execution context" which stores all communication 
    related dependencies. 
  - [ ] Add the operator id to the message
  - [ ] The message router handler should be operator and operator instance aware. When registering an 
    internal subscriber you will provide the execution context which will allow you to subscribe
    to messages. The router will then know about subscribers based on their operator and operator
    instance ids.
  - [ ] The message router handler should be able to communicate with other workers and identify
    where an operator instance exists. If the operator instance is provided on a message but a worker 
    id isn't provided then the handler will first send a message to each worker asking if it has the
    operator instance. The worker with the operator instance will send back a true or false message if 
    it has the operator instance. The router will then send the message to the worker that first responded
    back with yes. And the location of that operator instance will be cached for later use. The next time
    a message is requested to be sent to that operator instance the router will route the message to that
    worker. That workers router will either route it to the operator instance or if that operator instance
    doesn't exist it will send back an "unroutable message" response message with the message id. The 
    router will then perform the operator instance search again before sending the message to the worker
    with the operator instance.
  - [ ] Update the message passing request workflow such that the operator can perform a send operation
    which the router receives and handles. The send will contain the message the operator wants to 
    pass. By structuring the message passing in a request pattern I can add support for routing exceptions
    such as not being able to route the message the operator instance. The execution context should 
    have a method called `send` which returns a `SendResponse` enum which will either be an Ok or Err. The
    `send` method should also handle waiting for a message based on how long the caller expects the response
    to take.
- [ ] Harden the operator instance liveness tracking.
  - [ ] Implement a method for killing a query. It should be able to send out messages to all of the 
  operator instances telling them to stop running.
  - [ ] Implement operator instance heartbeat. The heartbeat will ping the query handler every few seconds
  to tell it that it is alive. The query handler will then need to react to cases where the heartbeat 
  hasn't been seen for a period of time. The query handler should response back with a terminate response
  if the query has completed for any reason.
  - [ ] Implement operator instance restarts such that when an operator instance misses a heartbeat
  it will be restarted.
  - [ ] If an exchange's heartbeat hasn't been seen for a period of time then kill the whole query.
- [ ] Improve query scheduling
  - [ ] Make sure that a query doesn't start until there is enough room in the cluster for it to start.
- [ ] Materializing files should be be able to compact the records into a larger parquet file
with a max number of row groups and rows in each group. The task should be
able to take many records from the exchange until it has enough to fit into a row group,
then write that to the row group and continue on until the max number of row groups
have been reached or there is no more data left. 
  * Think about adding record slicing to the exchange, description in TODO item below.
- [ ] Only read columns used in the query when reading parquet files.
- [ ] Look into bug where queries don't show data in the TUI when running multiple queries at once. 
  The file `simple-error-case.sql` has an example of this.
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

