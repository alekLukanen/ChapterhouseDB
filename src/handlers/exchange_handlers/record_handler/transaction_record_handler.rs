use super::record_handler::RecordHandler;

pub struct TransactionRecordHandler<'a> {
    transaction_idx: u64,
    record_handler_inner: &'a mut RecordHandler,
}

impl<'a> TransactionRecordHandler<'a> {
    pub fn new(
        transaction_idx: u64,
        record_handler_inner: &'a mut RecordHandler,
    ) -> TransactionRecordHandler<'a> {
        TransactionRecordHandler {
            transaction_idx,
            record_handler_inner,
        }
    }
}
