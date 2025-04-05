use std::io::Read;
use std::{any::Any, io::Cursor, sync::Arc};

use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    handlers::{operator_handler::TotalOperatorCompute, query_handler},
    planner,
};

use super::message::{
    GenericMessage, Message, MessageName, MessageParser, SendableMessage, SerializedMessage,
};

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize)]
pub enum GetQueryDataResp {
    QueryNotFound,
    RecordRowGroupNotFound,
    Record {
        #[serde(skip_serializing)]
        record: Arc<arrow::array::RecordBatch>,
        record_offsets: Vec<(u64, u64, u64)>,
    },
    Error {
        err: String,
    },
    ReachedEndOfFiles,
}

#[derive(Debug, Deserialize)]
pub struct GetQueryDataRespError {
    err: String,
}

#[derive(Debug, Deserialize)]
pub struct GetQueryDataRespRecord {
    record_offsets: Vec<(u64, u64, u64)>,
}

impl GetQueryDataResp {
    fn msg_id(&self) -> u8 {
        match self {
            Self::QueryNotFound => 0,
            Self::RecordRowGroupNotFound => 1,
            Self::Record { .. } => 2,
            Self::Error { .. } => 3,
            Self::ReachedEndOfFiles => 4,
        }
    }
}

impl SendableMessage for GetQueryDataResp {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Self::QueryNotFound
            | Self::RecordRowGroupNotFound
            | Self::Error { .. }
            | Self::ReachedEndOfFiles => {
                let meta_data = serde_json::to_vec(self)?;
                let mut buf = BytesMut::with_capacity(1 + 8 + meta_data.len());
                buf.put_u8(self.msg_id());
                buf.put_u64(meta_data.len() as u64);
                buf.put(&meta_data[..]);
                return Ok(buf.to_vec());
            }
            Self::Record { record, .. } => {
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
        }
    }
    fn msg_name(&self) -> MessageName {
        MessageName::GetQueryDataResp
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

#[derive(Debug, Error)]
pub enum GetQueryDataRespParserError {
    #[error("read exact failed")]
    ReadExactFailed,
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("received multiple record batches")]
    ReceivedMultipleRecordBatches,
}

#[derive(Debug, Clone)]
pub struct GetQueryDataRespParser {}

impl GetQueryDataRespParser {
    pub fn new() -> GetQueryDataRespParser {
        GetQueryDataRespParser {}
    }

    fn parse_record(&self, buf: &mut Cursor<&[u8]>) -> Result<arrow::array::RecordBatch> {
        let mut record_data = BytesMut::with_capacity(buf.remaining());
        record_data.resize(buf.remaining(), 0);
        match buf.read_exact(&mut record_data) {
            Err(_) => return Err(GetQueryDataRespParserError::ReadExactFailed.into()),
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
                        return Err(
                            GetQueryDataRespParserError::ReceivedMultipleRecordBatches.into()
                        );
                    }
                    result_record = Some(record);
                }
                Err(err) => return Err(err.into()),
            }
        }

        if let Some(record) = result_record {
            Ok(record)
        } else {
            Err(GetQueryDataRespParserError::NotImplemented("no record".to_string()).into())
        }
    }
}

impl MessageParser for GetQueryDataRespParser {
    fn to_msg(&self, ser_msg: SerializedMessage) -> Result<Message> {
        let mut buf = Cursor::new(&ser_msg.msg_data[..]);
        buf.set_position(0);

        let msg_id = buf.get_u8();
        let meta_data_len = buf.get_u64();

        if msg_id == 0 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(GetQueryDataRespParserError::ReadExactFailed.into()),
                _ => (),
            }

            let msg = GetQueryDataResp::QueryNotFound;

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 1 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(GetQueryDataRespParserError::ReadExactFailed.into()),
                _ => (),
            }

            let msg = GetQueryDataResp::RecordRowGroupNotFound;

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 2 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(GetQueryDataRespParserError::ReadExactFailed.into()),
                _ => (),
            }

            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: GetQueryDataRespRecord = serde_json::from_value(meta["Record"].clone())?;

            let record = self.parse_record(&mut buf)?;
            let msg = GetQueryDataResp::Record {
                record: Arc::new(record),
                record_offsets: meta.record_offsets,
            };

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 3 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(GetQueryDataRespParserError::ReadExactFailed.into()),
                _ => (),
            }

            let meta: serde_json::Value = serde_json::from_slice(&meta_data[..])?;
            let meta: GetQueryDataRespError = serde_json::from_value(meta["Error"].clone())?;

            let msg = GetQueryDataResp::Error { err: meta.err };

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else if msg_id == 4 {
            let mut meta_data = BytesMut::with_capacity(meta_data_len as usize);
            meta_data.resize(meta_data_len as usize, 0);
            match buf.read_exact(&mut meta_data) {
                Err(_) => return Err(GetQueryDataRespParserError::ReadExactFailed.into()),
                _ => (),
            }

            let msg = GetQueryDataResp::ReachedEndOfFiles;

            Ok(Message::build_from_serialized_message(
                ser_msg,
                Box::new(msg),
            ))
        } else {
            return Err(
                GetQueryDataRespParserError::NotImplemented(format!("msg id: {}", msg_id)).into(),
            );
        }
    }

    fn msg_name(&self) -> MessageName {
        MessageName::GetQueryDataResp
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetQueryData {
    pub query_id: u128,
    pub file_idx: u64,
    pub file_row_group_idx: u64,
    pub row_idx: u64,
    pub limit: u64,
    pub forward: bool,
    pub allow_overflow: bool,
}

impl GenericMessage for GetQueryData {
    fn msg_name() -> MessageName {
        MessageName::GetQueryData
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: GetQueryData = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetQueryStatus {
    pub query_id: u128,
}

impl GenericMessage for GetQueryStatus {
    fn msg_name() -> MessageName {
        MessageName::GetQueryStatus
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: GetQueryStatus = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GetQueryStatusResp {
    QueryNotFound,
    Status(query_handler::Status),
}

impl GenericMessage for GetQueryStatusResp {
    fn msg_name() -> MessageName {
        MessageName::GetQueryStatusResp
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: GetQueryStatusResp = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorInstanceStatusChange {
    Complete {
        query_id: u128,
        operator_instance_id: u128,
    },
    Error {
        query_id: u128,
        operator_instance_id: u128,
        error: String,
    },
}

impl GenericMessage for OperatorInstanceStatusChange {
    fn msg_name() -> MessageName {
        MessageName::QueryOperatorInstanceStatusChange
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: OperatorInstanceStatusChange = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunQuery {
    pub query: String,
}

impl RunQuery {
    pub fn new(query: String) -> RunQuery {
        RunQuery { query }
    }
}

impl GenericMessage for RunQuery {
    fn msg_name() -> MessageName {
        MessageName::RunQuery
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: RunQuery = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RunQueryResp {
    Created { query_id: u128 },
    NotCreated,
}

impl RunQueryResp {
    pub fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: RunQueryResp = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

impl GenericMessage for RunQueryResp {
    fn msg_name() -> MessageName {
        MessageName::RunQueryResp
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: RunQueryResp = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorInstanceAvailable {
    Notification,
    NotificationResponse {
        can_accept_up_to: TotalOperatorCompute,
    },
}

impl GenericMessage for OperatorInstanceAvailable {
    fn msg_name() -> MessageName {
        MessageName::OperatorInstanceAvailable
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: OperatorInstanceAvailable = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorInstanceAssignment {
    Assign {
        query_id: u128,
        op_instance_id: u128,
        pipeline_id: String,
        operator: planner::Operator,
    },
    AssignAcceptedResponse {
        query_id: u128,
        op_instance_id: u128,
        pipeline_id: String,
    },
    AssignRejectedResponse {
        query_id: u128,
        op_instance_id: u128,
        pipeline_id: String,
        error: String,
    },
}

impl OperatorInstanceAssignment {
    pub fn get_query_id(&self) -> u128 {
        match self {
            Self::Assign { query_id, .. } => query_id.clone(),
            Self::AssignAcceptedResponse { query_id, .. } => query_id.clone(),
            Self::AssignRejectedResponse { query_id, .. } => query_id.clone(),
        }
    }
    pub fn get_op_instance_id(&self) -> u128 {
        match self {
            Self::Assign { op_instance_id, .. } => op_instance_id.clone(),
            Self::AssignAcceptedResponse { op_instance_id, .. } => op_instance_id.clone(),
            Self::AssignRejectedResponse { op_instance_id, .. } => op_instance_id.clone(),
        }
    }
    pub fn get_pipeline_id(&self) -> String {
        match self {
            Self::Assign { pipeline_id, .. } => pipeline_id.clone(),
            Self::AssignAcceptedResponse { pipeline_id, .. } => pipeline_id.clone(),
            Self::AssignRejectedResponse { pipeline_id, .. } => pipeline_id.clone(),
        }
    }
}

impl GenericMessage for OperatorInstanceAssignment {
    fn msg_name() -> MessageName {
        MessageName::OperatorInstanceAssignment
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: OperatorInstanceAssignment = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}

////////////////////////////////////////////////////////////
//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryHandlerRequests {
    ListOperatorInstancesRequest { query_id: u128, operator_id: String },
    ListOperatorInstancesResponse { op_instance_ids: Vec<u128> },
}

impl GenericMessage for QueryHandlerRequests {
    fn msg_name() -> MessageName {
        MessageName::QueryHandlerRequests
    }
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>> {
        let msg: QueryHandlerRequests = serde_json::from_slice(data)?;
        Ok(Box::new(msg))
    }
}
