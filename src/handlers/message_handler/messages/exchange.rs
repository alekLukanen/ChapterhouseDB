use std::{any::Any, sync::Arc};

use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read};
use thiserror::Error;

use super::message::{
    GenericMessage, Message, MessageName, MessageParser, SendableMessage, SerializedMessage,
};

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecordHeartbeat {
    Ping { operator_id: String, record_id: u64 },
}

impl GenericMessage for RecordHeartbeat {
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: RecordHeartbeat = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
    fn msg_name() -> MessageName {
        MessageName::ExchangeRecordHeartbeat
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorStatusChange {
    Complete { operator_id: String },
}

impl GenericMessage for OperatorStatusChange {
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: OperatorStatusChange = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
    fn msg_name() -> MessageName {
        MessageName::ExchangeOperatorStatusChange
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Error)]
pub enum ExchangeRequestsError {
    #[error("read exact failed")]
    ReadExactFailed,
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("received multiple record batches")]
    ReceivedMultipleRecordBatches,
}

#[derive(Debug, Clone, Serialize)]
pub enum ExchangeRequests {
    GetNextRecordRequest {
        operator_id: String,
    },
    GetNextRecordResponseRecord {
        record_id: u64,
        #[serde(skip_serializing)]
        record: Arc<arrow::array::RecordBatch>,
        table_aliases: Vec<Vec<String>>, // [["tableName", "tableAlias"], ...]
    },
    GetNextRecordResponseNoneLeft,
    GetNextRecordResponseNoneAvailable,
    SendRecordRequest {
        record_id: u64,
        #[serde(skip_serializing)]
        record: Arc<arrow::array::RecordBatch>,
        table_aliases: Vec<Vec<String>>, // [["tableName", "tableAlias"], ...]
    },
    SendRecordResponse {
        record_id: u64,
    },
    OperatorCompletedRecordProcessingRequest {
        operator_id: String,
        record_id: u64,
    },
    OperatorCompletedRecordProcessingResponse,
}

impl ExchangeRequests {
    fn msg_id(&self) -> u8 {
        match self {
            Self::GetNextRecordRequest { .. } => 0,
            Self::GetNextRecordResponseRecord { .. } => 1,
            Self::GetNextRecordResponseNoneLeft => 2,
            Self::GetNextRecordResponseNoneAvailable => 3,
            Self::SendRecordRequest { .. } => 4,
            Self::SendRecordResponse { .. } => 5,
            Self::OperatorCompletedRecordProcessingRequest { .. } => 6,
            Self::OperatorCompletedRecordProcessingResponse => 7,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ExchangeRequestsGetNextRecordResponseRecord {
    record_id: u64,
    table_aliases: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ExchangeRequestsGetNextRecordRequest {
    operator_id: String,
}

#[derive(Debug, Deserialize)]
struct ExchangeRequestsSendRecordRequest {
    record_id: u64,
    table_aliases: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ExchangeRequestsSendRecordResponse {
    record_id: u64,
}

#[derive(Debug, Deserialize)]
struct ExchangeRequestsOperatorCompletedRecordProcessingRequest {
    operator_id: String,
    record_id: u64,
}

impl SendableMessage for ExchangeRequests {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Self::GetNextRecordRequest { .. } => {
                let meta_data = serde_json::to_vec(self)?;
                let mut buf = BytesMut::with_capacity(1 + 8 + meta_data.len());
                buf.put_u8(self.msg_id());
                buf.put_u64(meta_data.len() as u64);
                buf.put(&meta_data[..]);
                return Ok(buf.to_vec());
            }
            Self::GetNextRecordResponseRecord { record, .. } => {
                let mut data_buf = Vec::new();
                {
                    let mut record_writer =
                        arrow::ipc::writer::StreamWriter::try_new(&mut data_buf, &record.schema())?;
                    record_writer.write(record)?;
                    record_writer.finish()?
                }

                let meta_data = serde_json::to_vec(self)?;

                let mut buf = BytesMut::with_capacity(1 + 8 + meta_data.len() + data_buf.len());
                buf.put_u8(self.msg_id());
                buf.put_u64(meta_data.len() as u64);
                buf.put(&meta_data[..]);
                buf.put(&data_buf[..]);

                return Ok(buf.to_vec());
            }
            Self::GetNextRecordResponseNoneLeft => {
                let meta_data = serde_json::to_vec(self)?;
                let mut buf = BytesMut::with_capacity(1 + 8 + meta_data.len());
                buf.put_u8(self.msg_id());
                buf.put_u64(meta_data.len() as u64);
                buf.put(&meta_data[..]);
                return Ok(buf.to_vec());
            }
            Self::GetNextRecordResponseNoneAvailable => {
                let meta_data = serde_json::to_vec(self)?;
                let mut buf = BytesMut::with_capacity(1 + 8 + meta_data.len());
                buf.put_u8(self.msg_id());
                buf.put_u64(meta_data.len() as u64);
                buf.put(&meta_data[..]);
                return Ok(buf.to_vec());
            }
            Self::SendRecordRequest { record, .. } => {
                let mut data_buf = Vec::new();
                {
                    let mut record_writer =
                        arrow::ipc::writer::StreamWriter::try_new(&mut data_buf, &record.schema())?;
                    record_writer.write(record)?;
                    record_writer.finish()?
                }

                let meta_data = serde_json::to_vec(self)?;

                let mut buf = BytesMut::with_capacity(1 + 8 + meta_data.len() + data_buf.len());
                buf.put_u8(self.msg_id());
                buf.put_u64(meta_data.len() as u64);
                buf.put(&meta_data[..]);
                buf.put(&data_buf[..]);

                return Ok(buf.to_vec());
            }
            Self::SendRecordResponse { .. } => {
                let meta_data = serde_json::to_vec(self)?;
                let mut buf = BytesMut::with_capacity(1 + 8 + meta_data.len());
                buf.put_u8(self.msg_id());
                buf.put_u64(meta_data.len() as u64);
                buf.put(&meta_data[..]);
                return Ok(buf.to_vec());
            }
            Self::OperatorCompletedRecordProcessingRequest { .. } => {
                let meta_data = serde_json::to_vec(self)?;
                let mut buf = BytesMut::with_capacity(1 + 8 + meta_data.len());
                buf.put_u8(self.msg_id());
                buf.put_u64(meta_data.len() as u64);
                buf.put(&meta_data[..]);
                return Ok(buf.to_vec());
            }
            Self::OperatorCompletedRecordProcessingResponse { .. } => {
                let meta_data = serde_json::to_vec(self)?;
                let mut buf = BytesMut::with_capacity(1 + 8 + meta_data.len());
                buf.put_u8(self.msg_id());
                buf.put_u64(meta_data.len() as u64);
                buf.put(&meta_data[..]);
                return Ok(buf.to_vec());
            }
        }
    }
    fn msg_name(&self) -> MessageName {
        MessageName::ExchangeRequests
    }
    fn clone_box(&self) -> Box<dyn SendableMessage> {
        Box::new(self.clone())
    }
    fn as_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct ExchangeRequestsParser {}

impl ExchangeRequestsParser {
    pub fn new() -> ExchangeRequestsParser {
        ExchangeRequestsParser {}
    }

    fn parse_record(&self, buf: &mut Cursor<&[u8]>) -> Result<arrow::array::RecordBatch> {
        let mut record_data = BytesMut::with_capacity(buf.remaining());
        record_data.resize(buf.remaining(), 0);
        match buf.read_exact(&mut record_data) {
            Err(_) => return Err(ExchangeRequestsError::ReadExactFailed.into()),
            _ => (),
        }

        let mut record_data_cursor = std::io::Cursor::new(&record_data[..]);
        let record_reader =
            arrow::ipc::reader::StreamReader::try_new(&mut record_data_cursor, None)?;
        let mut result_record: Option<arrow::array::RecordBatch> = None;
        for record in record_reader {
            match record {
                Ok(record) => {
                    if result_record.is_some() {
                        return Err(ExchangeRequestsError::ReceivedMultipleRecordBatches.into());
                    }
                    result_record = Some(record);
                }
                Err(err) => return Err(err.into()),
            }
        }

        if let Some(record) = result_record {
            Ok(record)
        } else {
            Err(ExchangeRequestsError::NotImplemented("no record".to_string()).into())
        }
    }
}

impl MessageParser for ExchangeRequestsParser {
    fn to_msg(&self, ser_msg: SerializedMessage) -> Result<Message> {
        let mut buf = Cursor::new(&ser_msg.msg_data[..]);
        buf.set_position(0);

        let msg_id = buf.get_u8();
        let meta_data_len = buf.get_u64();

        if msg_id == 0 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(ExchangeRequestsError::ReadExactFailed.into()),
                _ => (),
            }

            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: ExchangeRequestsGetNextRecordRequest =
                serde_json::from_value(meta["GetNextRecordRequest"].clone())?;

            let msg = ExchangeRequests::GetNextRecordRequest {
                operator_id: meta.operator_id,
            };

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 1 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(ExchangeRequestsError::ReadExactFailed.into()),
                _ => (),
            }

            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: ExchangeRequestsGetNextRecordResponseRecord =
                serde_json::from_value(meta["GetNextRecordResponseRecord"].clone())?;

            let record = self.parse_record(&mut buf)?;
            let msg = ExchangeRequests::GetNextRecordResponseRecord {
                record_id: meta.record_id,
                record: Arc::new(record),
                table_aliases: meta.table_aliases,
            };

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 2 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(ExchangeRequestsError::ReadExactFailed.into()),
                _ => (),
            }

            let msg = ExchangeRequests::GetNextRecordResponseNoneLeft;

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 3 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(ExchangeRequestsError::ReadExactFailed.into()),
                _ => (),
            }

            let msg = ExchangeRequests::GetNextRecordResponseNoneAvailable;

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 4 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(ExchangeRequestsError::ReadExactFailed.into()),
                _ => (),
            }

            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: ExchangeRequestsSendRecordRequest =
                serde_json::from_value(meta["SendRecordRequest"].clone())?;

            let record = self.parse_record(&mut buf)?;
            let msg = ExchangeRequests::SendRecordRequest {
                record_id: meta.record_id,
                record: Arc::new(record),
                table_aliases: meta.table_aliases,
            };

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 5 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(ExchangeRequestsError::ReadExactFailed.into()),
                _ => (),
            }

            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: ExchangeRequestsSendRecordResponse =
                serde_json::from_value(meta["SendRecordResponse"].clone())?;

            let msg = ExchangeRequests::SendRecordResponse {
                record_id: meta.record_id,
            };

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 6 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(ExchangeRequestsError::ReadExactFailed.into()),
                _ => (),
            }

            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: ExchangeRequestsOperatorCompletedRecordProcessingRequest =
                serde_json::from_value(meta["OperatorCompletedRecordProcessingRequest"].clone())?;

            let msg = ExchangeRequests::OperatorCompletedRecordProcessingRequest {
                operator_id: meta.operator_id,
                record_id: meta.record_id,
            };

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 7 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(ExchangeRequestsError::ReadExactFailed.into()),
                _ => (),
            }

            let msg = ExchangeRequests::OperatorCompletedRecordProcessingResponse;

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else {
            return Err(
                ExchangeRequestsError::NotImplemented(format!("msg id: {}", msg_id)).into(),
            );
        }
    }
    fn msg_name(&self) -> MessageName {
        MessageName::ExchangeRequests
    }
}
