
## Next work

- [ ] Implement computation of the where filter and projection expressions. For each Arrow record
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
- [ ] The client should be able to request records from the query result iteratively. The client
should be able to request one record at a time, where each record to one row group from a 
parquet file.
- [ ] Materializing files should be be able to compact the records into a larger parquet file
with a max number of row groups and rows in each group. The task should be
able to take many records from the exchange until it has enough to fit into a row group,
then write that to the row group and continue on until the max number of row groups
have been reached or there is no more data left.
- [ ] Test out on large dataset stored in S3 or Minio.
- [ ] Implement message shedding so that a slow consumer doesn't block the message router from
reading or sending messages. Since each consumer has a cap on the number of messages that
can be queued it's possible that a slow consumer's queue could get filled up and block the
router from sending the next message until there is room. This could cause a deadlock.

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
- [ ] Create a re-partition producer operator which takes the records from an exchange and
re-partitions them into equal size records. This is helpful for reducing IO operations
when requesting and sending records. This will result in some issue with record numbering.
Currently records number from 0 to infinity. But now that probably won't be possible if this
operator is run in parallel. I want to preserve ordering though. An alternative approach
could be to implement a feature on the exchange which allows operators to request multiple
sequential records (record 0, 1, 2, etc...) which do not exceed a certain size. So if
records 0 and 1 are less then X rows I would get both. I would get a message for record
0 first which tells the request that it will receive another message for record 1. Then
when I am finished with both records I send back a single confirmation message that I processed
both records. And when I push the result to the next exchange I use the lowest record
number as the record new record number.
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
