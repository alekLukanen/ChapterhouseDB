use std::{any::Any, sync::Arc};

use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read};
use thiserror::Error;

use super::message::{
    GenericMessage, Message, MessageName, MessageParser, SendableMessage, SerializedMessage,
};
use super::parsing_utils;

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionHeartbeat {
    Ping { transaction_id: u64 },
}

impl GenericMessage for TransactionHeartbeat {
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: TransactionHeartbeat = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
    fn msg_name() -> MessageName {
        MessageName::ExchangeTransactionHeartbeat
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitTransaction {
    pub transaction_id: u64,
}

impl GenericMessage for CommitTransaction {
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: CommitTransaction = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
    fn msg_name() -> MessageName {
        MessageName::ExchangeCommitTransaction
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommitTransactionResponse {
    Ok,
    Error(String),
}

impl GenericMessage for CommitTransactionResponse {
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: CommitTransactionResponse = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
    fn msg_name() -> MessageName {
        MessageName::ExchangeCommitTransactionResponse
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize)]
pub struct InsertTransactionRecord {
    pub transaction_id: u64,
    pub queue_name: String,
    pub deduplication_key: String,
    #[serde(skip_serializing)]
    pub record: Arc<arrow::array::RecordBatch>,
    pub table_aliases: Vec<Vec<String>>, // [["tableName", "tableAlias"], ...]
}

#[derive(Debug, Clone, Deserialize)]
pub struct InsertTransactionRecordMetaData {
    transaction_id: u64,
    queue_name: String,
    deduplication_key: String,
    table_aliases: Vec<Vec<String>>, // [["tableName", "tableAlias"], ...]
}

#[derive(Debug, Error)]
pub enum InsertTransactionRecordError {
    #[error("read exact failed")]
    ReadExactFailed,
}

impl SendableMessage for InsertTransactionRecord {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut data_buf = Vec::new();
        {
            let mut record_writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut data_buf, &self.record.schema())?;
            record_writer.write(&self.record)?;
            record_writer.finish()?
        }

        let meta_data = serde_json::to_vec(self)?;

        let mut buf = BytesMut::with_capacity(1 + 8 + meta_data.len() + data_buf.len());
        buf.put_u64(meta_data.len() as u64);
        buf.put(&meta_data[..]);
        buf.put(&data_buf[..]);

        return Ok(buf.to_vec());
    }
    fn msg_name(&self) -> MessageName {
        MessageName::ExchangeInsertTransactionRecord
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
pub struct InsertTransactionRecordParser {}

impl InsertTransactionRecordParser {
    pub fn new() -> InsertTransactionRecordParser {
        InsertTransactionRecordParser {}
    }
}

impl MessageParser for InsertTransactionRecordParser {
    fn to_msg(&self, ser_msg: SerializedMessage) -> Result<Message> {
        let mut buf = Cursor::new(&ser_msg.msg_data[..]);
        buf.set_position(0);

        let meta_data_len = buf.get_u64();

        let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
        meta_data.resize(meta_data_len as usize, 0);
        match buf.read_exact(&mut meta_data) {
            Err(_) => return Err(InsertTransactionRecordError::ReadExactFailed.into()),
            _ => (),
        }

        let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
        let meta: InsertTransactionRecordMetaData = serde_json::from_value(meta.clone())?;

        let record = parsing_utils::parse_record(&mut buf)?;
        let msg = InsertTransactionRecord {
            transaction_id: meta.transaction_id,
            queue_name: meta.queue_name,
            deduplication_key: meta.deduplication_key,
            record: Arc::new(record),
            table_aliases: meta.table_aliases,
        };

        Ok(Message::build_from_serialized_message(
            ser_msg,
            Box::new(msg),
        ))
    }
    fn msg_name(&self) -> MessageName {
        MessageName::ExchangeInsertTransactionRecord
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InsertTransactionRecordResponse {
    Ok { deduplication_key: String },
    Err(String),
}

impl GenericMessage for InsertTransactionRecordResponse {
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: InsertTransactionRecordResponse = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
    fn msg_name() -> MessageName {
        MessageName::ExchangeInsertTransactionRecordResponse
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTransaction {
    pub key: String,
}

impl GenericMessage for CreateTransaction {
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: CreateTransaction = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
    fn msg_name() -> MessageName {
        MessageName::ExchangeCreateTransaction
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CreateTransactionResponse {
    Ok { transaction_id: u64 },
    Err(String),
}

impl GenericMessage for CreateTransactionResponse {
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: CreateTransactionResponse = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
    fn msg_name() -> MessageName {
        MessageName::ExchangeCreateTransactionResponse
    }
}

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
}

#[derive(Debug, Clone, Serialize)]
pub enum ExchangeRequests {
    GetNextRecordRequest {
        operator_id: String,
        queue_name: String,
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
        queue_name: String,
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
        queue_name: String,
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
    queue_name: String,
}

#[derive(Debug, Deserialize)]
struct ExchangeRequestsSendRecordRequest {
    queue_name: String,
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
    queue_name: String,
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
}

impl MessageParser for ExchangeRequestsParser {
    fn to_msg(&self, ser_msg: SerializedMessage) -> Result<Message> {
        let mut buf = Cursor::new(&ser_msg.msg_data[..]);
        buf.set_position(0);

        let msg_id = buf.get_u8();
        let meta_data_len = buf.get_u64();

        let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
        meta_data.resize(meta_data_len as usize, 0);
        match buf.read_exact(&mut meta_data) {
            Err(_) => return Err(ExchangeRequestsError::ReadExactFailed.into()),
            _ => (),
        }

        if msg_id == 0 {
            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: ExchangeRequestsGetNextRecordRequest =
                serde_json::from_value(meta["GetNextRecordRequest"].clone())?;

            let msg = ExchangeRequests::GetNextRecordRequest {
                operator_id: meta.operator_id,
                queue_name: meta.queue_name,
            };

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 1 {
            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: ExchangeRequestsGetNextRecordResponseRecord =
                serde_json::from_value(meta["GetNextRecordResponseRecord"].clone())?;

            let record = parsing_utils::parse_record(&mut buf)?;
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
            let msg = ExchangeRequests::GetNextRecordResponseNoneLeft;

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 3 {
            let msg = ExchangeRequests::GetNextRecordResponseNoneAvailable;

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 4 {
            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: ExchangeRequestsSendRecordRequest =
                serde_json::from_value(meta["SendRecordRequest"].clone())?;

            let record = parsing_utils::parse_record(&mut buf)?;
            let msg = ExchangeRequests::SendRecordRequest {
                queue_name: meta.queue_name,
                record_id: meta.record_id,
                record: Arc::new(record),
                table_aliases: meta.table_aliases,
            };

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 5 {
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
            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: ExchangeRequestsOperatorCompletedRecordProcessingRequest =
                serde_json::from_value(meta["OperatorCompletedRecordProcessingRequest"].clone())?;

            let msg = ExchangeRequests::OperatorCompletedRecordProcessingRequest {
                operator_id: meta.operator_id,
                record_id: meta.record_id,
                queue_name: meta.queue_name,
            };

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 7 {
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
