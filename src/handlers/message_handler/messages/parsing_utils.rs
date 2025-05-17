use std::io::Cursor;

use anyhow::Result;
use bytes::{Buf, BytesMut};
use std::io::Read;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseRecordError {
    #[error("read exact failed")]
    ReadExactFailed,
    #[error("received multiple record batches")]
    ReceivedMultipleRecordBatches,
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

pub fn parse_record(buf: &mut Cursor<&[u8]>) -> Result<arrow::array::RecordBatch> {
    let mut record_data = BytesMut::with_capacity(buf.remaining());
    record_data.resize(buf.remaining(), 0);
    match buf.read_exact(&mut record_data) {
        Err(_) => return Err(ParseRecordError::ReadExactFailed.into()),
        _ => (),
    }

    let mut record_data_cursor = std::io::Cursor::new(&record_data[..]);
    let record_reader = arrow::ipc::reader::StreamReader::try_new(&mut record_data_cursor, None)?;
    let mut result_record: Option<arrow::array::RecordBatch> = None;
    for record in record_reader {
        match record {
            Ok(record) => {
                if result_record.is_some() {
                    return Err(ParseRecordError::ReceivedMultipleRecordBatches.into());
                }
                result_record = Some(record);
            }
            Err(err) => return Err(err.into()),
        }
    }

    if let Some(record) = result_record {
        Ok(record)
    } else {
        Err(ParseRecordError::NotImplemented("no record".to_string()).into())
    }
}
