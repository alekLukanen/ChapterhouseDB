use core::fmt;
use std::any::Any;

use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read};
use thiserror::Error;
use uuid::Uuid;

const HEADER_VERSION: u16 = 0;

#[derive(Debug, Error)]
pub enum SerializedMessageError {
    #[error("incomplete")]
    Incomplete,
    #[error("buffer read to end failed")]
    BufferReadToEndFailed,
    #[error("unable to cast message type {0} to base type")]
    UnableToCastMessageToType(String),
}

pub trait SendableMessage: fmt::Debug + Send + Sync + Any {
    fn to_bytes(&self) -> Result<Vec<u8>>;
    fn msg_name(&self) -> MessageName;
    fn as_any(self: Box<Self>) -> Box<dyn Any>;
    fn as_any_ref(&self) -> &dyn Any;
    fn clone_box(&self) -> Box<dyn SendableMessage>;
}

pub trait MessageParser: fmt::Debug + Send + Sync {
    fn to_msg(&self, ser_msg: SerializedMessage) -> Result<Message>;
    fn msg_name(&self) -> MessageName;
}

pub trait GenericMessage:
    Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + fmt::Debug + 'static
{
    fn msg_name() -> MessageName;
    fn build_msg(data: &Vec<u8>) -> Result<Box<dyn SendableMessage>>;
}

impl<T> SendableMessage for T
where
    T: GenericMessage,
{
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    fn msg_name(&self) -> MessageName {
        T::msg_name()
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

#[derive(Debug)]
pub struct GenericMessageParser<T> {
    _marker: std::marker::PhantomData<T>,
}

impl<T> GenericMessageParser<T> {
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> MessageParser for GenericMessageParser<T>
where
    T: GenericMessage,
{
    fn to_msg(&self, ser_msg: SerializedMessage) -> Result<Message> {
        let msg = T::build_msg(&ser_msg.msg_data)?;
        Ok(Message::build_from_serialized_message(ser_msg, msg))
    }

    fn msg_name(&self) -> MessageName {
        T::msg_name()
    }
}

#[derive(Debug, PartialEq)]
pub struct SerializedMessage {
    // lengths are in bytes
    header_len: u32,
    header_version: u16,
    data_len: u64,
    msg_name_id: u16,
    msg_id: u128,
    request_id: u128,

    // determines which of the "senf_from" values
    // have been set
    // 1 - worker_id set (bit 0)
    // 2 - pipeline_id set (bit 1)
    // 4 - operation_id set (bit 2)
    // 8 - connection_id set (bit 3)
    sent_from_flags: u8,
    sent_from_worker_id: u128,
    sent_from_query_id: u128,
    sent_from_operation_id: u128,
    sent_from_connection_id: u128,

    // determines which of the routing values
    // have been set
    // 1 - worker_id set (bit 0)
    // 2 - operation_id set (bit 1)
    // 4 - connection_id set (bit 2)
    routing_flags: u8,
    route_to_worker_id: u128,
    route_to_operation_id: u128,
    route_to_connection_id: u128,

    // the actual user-space message
    pub msg_data: Vec<u8>,
}

impl SerializedMessage {
    pub fn new(msg: &Message) -> Result<SerializedMessage> {
        let msg_data = msg.msg.to_bytes()?;

        let data_len: u64 = msg_data.len() as u64;
        let msg_name_id = msg.msg_name_id;
        let msg_id = msg.msg_id;
        let request_id = msg.request_id;
        let mut routing_flags: u8 = 0;
        let mut sent_from_flags: u8 = 0;

        let mut route_to_worker_id: u128 = 0;
        if let Some(_id) = msg.route_to_worker_id {
            route_to_worker_id = _id;
            routing_flags = routing_flags | 1;
        }

        let mut route_to_operation_id: u128 = 0;
        if let Some(_id) = msg.route_to_operation_id {
            route_to_operation_id = _id;
            routing_flags = routing_flags | (1 << 1);
        }

        let mut route_to_connection_id: u128 = 0;
        if let Some(_id) = msg.route_to_connection_id {
            route_to_connection_id = _id;
            routing_flags = routing_flags | (1 << 2);
        }

        let mut sent_from_worker_id: u128 = 0;
        if let Some(_id) = msg.sent_from_worker_id {
            sent_from_worker_id = _id;
            sent_from_flags = sent_from_flags | 1;
        }

        let mut sent_from_query_id: u128 = 0;
        if let Some(_id) = msg.sent_from_query_id {
            sent_from_query_id = _id;
            sent_from_flags = sent_from_flags | (1 << 1);
        }

        let mut sent_from_operation_id: u128 = 0;
        if let Some(_id) = msg.sent_from_operation_id {
            sent_from_operation_id = _id;
            sent_from_flags = sent_from_flags | (1 << 2);
        }

        let mut sent_from_connection_id: u128 = 0;
        if let Some(_id) = msg.sent_from_connection_id {
            sent_from_connection_id = _id;
            sent_from_flags = sent_from_flags | (1 << 3);
        }

        let ser_msg = SerializedMessage {
            header_len: Self::header_len(),
            data_len,
            header_version: HEADER_VERSION,
            msg_name_id,
            msg_id,
            request_id,
            sent_from_flags,
            sent_from_worker_id,
            sent_from_query_id,
            sent_from_operation_id,
            sent_from_connection_id,
            routing_flags,
            route_to_worker_id,
            route_to_operation_id,
            route_to_connection_id,
            msg_data,
        };
        Ok(ser_msg)
    }

    pub fn header_len() -> u32 {
        8 + 2 + 2 + 16 + 16 + 1 + 16 + 16 + 16 + 16 + 1 + 16 + 16 + 16
    }

    pub fn parse_registered_msg_id(data: &mut BytesMut) -> Result<u16> {
        let mut buf = Cursor::new(&data[..]);
        if data.len() < 4 + 8 + 2 + 2 {
            return Err(SerializedMessageError::Incomplete.into());
        }

        buf.set_position(4 + 8 + 2);

        let reg_msg_id = buf.get_u16();
        Ok(reg_msg_id)
    }

    pub fn parse(data: &mut BytesMut) -> Result<SerializedMessage, SerializedMessageError> {
        match Self::check(data) {
            Ok(_) => {
                let mut buf = Cursor::new(&data[..]);
                buf.set_position(0);

                let header_len = buf.get_u32();
                let data_len = buf.get_u64();
                let header_version = buf.get_u16();
                let msg_name_id = buf.get_u16();
                let msg_id = buf.get_u128();
                let request_id = buf.get_u128();

                let sent_from_flags = buf.get_u8();
                let sent_from_worker_id = buf.get_u128();
                let sent_from_query_id = buf.get_u128();
                let sent_from_operation_id = buf.get_u128();
                let sent_from_connection_id = buf.get_u128();

                let routing_flags = buf.get_u8();
                let route_to_worker_id = buf.get_u128();
                let route_to_operation_id = buf.get_u128();
                let route_to_connection_id = buf.get_u128();

                let mut msg_data = BytesMut::with_capacity(data_len as usize);
                msg_data.resize(data_len as usize, 0);
                match buf.read_exact(&mut msg_data) {
                    Err(_) => return Err(SerializedMessageError::BufferReadToEndFailed),
                    _ => (),
                }

                let ser_msg = SerializedMessage {
                    header_len,
                    data_len,
                    header_version,
                    msg_name_id,
                    msg_id,
                    request_id,
                    sent_from_flags,
                    sent_from_worker_id,
                    sent_from_query_id,
                    sent_from_operation_id,
                    sent_from_connection_id,
                    routing_flags,
                    route_to_worker_id,
                    route_to_operation_id,
                    route_to_connection_id,
                    msg_data: msg_data.to_vec(),
                };

                // claim the data from the buffer
                data.advance(4 + header_len as usize + data_len as usize);

                Ok(ser_msg)
            }
            Err(err) => Err(err),
        }
    }

    pub fn check(data: &BytesMut) -> Result<(), SerializedMessageError> {
        let mut buf = Cursor::new(&data[..]);
        if data.len() < 4 + 8 {
            return Err(SerializedMessageError::Incomplete.into());
        }

        buf.set_position(0);

        let header_len = buf.get_u32();
        let data_len = buf.get_u64();

        if (data.len() as u64) < (header_len as u64 + data_len) {
            Err(SerializedMessageError::Incomplete.into())
        } else {
            Ok(())
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf =
            BytesMut::with_capacity(4 + Self::header_len() as usize + self.data_len as usize);

        buf.put_u32(self.header_len);
        buf.put_u64(self.data_len);
        buf.put_u16(self.header_version);
        buf.put_u16(self.msg_name_id);
        buf.put_u128(self.msg_id);
        buf.put_u128(self.request_id);
        buf.put_u8(self.sent_from_flags);
        buf.put_u128(self.sent_from_worker_id);
        buf.put_u128(self.sent_from_query_id);
        buf.put_u128(self.sent_from_operation_id);
        buf.put_u128(self.sent_from_connection_id);
        buf.put_u8(self.routing_flags);
        buf.put_u128(self.route_to_worker_id);
        buf.put_u128(self.route_to_operation_id);
        buf.put_u128(self.route_to_connection_id);
        buf.put(&self.msg_data[..]);

        buf.to_vec()
    }
}

#[derive(Debug)]
pub struct Message {
    pub msg_name_id: u16,
    pub msg_id: u128,
    pub request_id: u128,
    pub msg: Box<dyn SendableMessage>,

    // sent from
    pub sent_from_worker_id: Option<u128>,
    pub sent_from_query_id: Option<u128>,
    pub sent_from_operation_id: Option<u128>,
    pub sent_from_connection_id: Option<u128>,

    // routing
    pub route_to_worker_id: Option<u128>,
    pub route_to_operation_id: Option<u128>,
    pub route_to_connection_id: Option<u128>,

    // source
    pub inbound_stream_id: Option<u128>,
    pub outbound_stream_id: Option<u128>,
    pub created_on_this_worker: bool,
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Message {
            msg_name_id: self.msg_name_id,
            msg_id: self.msg_id,
            request_id: self.request_id,
            msg: self.msg.clone_box(),
            sent_from_worker_id: self.sent_from_worker_id,
            sent_from_query_id: self.sent_from_query_id,
            sent_from_operation_id: self.sent_from_operation_id,
            sent_from_connection_id: self.sent_from_connection_id,
            route_to_worker_id: self.route_to_worker_id,
            route_to_operation_id: self.route_to_operation_id,
            route_to_connection_id: self.route_to_connection_id,
            inbound_stream_id: self.inbound_stream_id,
            outbound_stream_id: self.outbound_stream_id,
            created_on_this_worker: self.created_on_this_worker,
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg.msg_name())
    }
}

impl Message {
    pub fn new(msg: Box<dyn SendableMessage>) -> Message {
        Message {
            msg_name_id: msg.msg_name().as_u16(),
            msg_id: Uuid::new_v4().as_u128(),
            request_id: Uuid::new_v4().as_u128(),
            msg,
            sent_from_worker_id: None,
            sent_from_query_id: None,
            sent_from_operation_id: None,
            sent_from_connection_id: None,
            route_to_worker_id: None,
            route_to_operation_id: None,
            route_to_connection_id: None,
            inbound_stream_id: None,
            outbound_stream_id: None,
            created_on_this_worker: true,
        }
    }

    pub fn reply(&self, sendable: Box<dyn SendableMessage>) -> Message {
        let mut msg = Message::new(sendable);
        msg.request_id = self.request_id.clone();
        msg.route_to_worker_id = self.sent_from_worker_id.clone();
        msg.route_to_operation_id = self.sent_from_operation_id.clone();
        msg.route_to_connection_id = self.sent_from_connection_id.clone();
        msg.inbound_stream_id = self.inbound_stream_id.clone();
        msg.outbound_stream_id = self.outbound_stream_id.clone();
        msg
    }

    pub fn take_msg(self) -> Box<dyn SendableMessage> {
        self.msg
    }

    pub fn set_request_id(mut self, _id: u128) -> Message {
        self.request_id = _id;
        self
    }

    pub fn set_inbound_stream_id(mut self, _id: u128) -> Message {
        self.inbound_stream_id = Some(_id);
        self
    }

    pub fn set_outbound_stream(mut self, _id: u128) -> Message {
        self.outbound_stream_id = Some(_id);
        self
    }

    pub fn set_sent_from_worker_id(mut self, _id: u128) -> Message {
        self.sent_from_worker_id = Some(_id);
        self
    }

    pub fn set_sent_from_query_id(mut self, _id: u128) -> Message {
        self.sent_from_query_id = Some(_id);
        self
    }

    pub fn set_sent_from_operation_id(mut self, _id: u128) -> Message {
        self.sent_from_operation_id = Some(_id);
        self
    }

    pub fn set_sent_from_connection_id(&mut self, _id: u128) -> &mut Message {
        self.sent_from_connection_id = Some(_id);
        self
    }

    pub fn set_route_to_worker_id(mut self, _id: u128) -> Message {
        self.route_to_worker_id = Some(_id);
        self
    }

    pub fn set_route_to_operation_id(mut self, _id: u128) -> Message {
        self.route_to_operation_id = Some(_id);
        self
    }

    pub fn set_route_to_connection_id(mut self, _id: u128) -> Message {
        self.route_to_connection_id = Some(_id);
        self
    }

    pub fn build_from_serialized_message(
        ser_msg: SerializedMessage,
        msg: Box<dyn SendableMessage>,
    ) -> Message {
        // sent from values
        let sent_from_worker_id = if ser_msg.sent_from_flags & 1 == 1 {
            Some(ser_msg.sent_from_worker_id)
        } else {
            None
        };

        let sent_from_query_id = if ser_msg.sent_from_flags & 2 == 2 {
            Some(ser_msg.sent_from_query_id)
        } else {
            None
        };

        let sent_from_operation_id = if ser_msg.sent_from_flags & 4 == 4 {
            Some(ser_msg.sent_from_operation_id)
        } else {
            None
        };

        let sent_from_connection_id = if ser_msg.sent_from_flags & 8 == 8 {
            Some(ser_msg.sent_from_connection_id)
        } else {
            None
        };

        // route to values
        let route_to_worker_id = if ser_msg.routing_flags & 1 == 1 {
            Some(ser_msg.route_to_worker_id)
        } else {
            None
        };

        let route_to_operation_id = if ser_msg.routing_flags & 2 == 2 {
            Some(ser_msg.route_to_operation_id)
        } else {
            None
        };

        let route_to_connection_id = if ser_msg.routing_flags & 4 == 4 {
            Some(ser_msg.route_to_connection_id)
        } else {
            None
        };

        let msg = Message {
            msg_name_id: ser_msg.msg_name_id,
            msg_id: ser_msg.msg_id,
            request_id: ser_msg.request_id,
            msg,
            sent_from_worker_id,
            sent_from_query_id,
            sent_from_operation_id,
            sent_from_connection_id,
            route_to_worker_id,
            route_to_operation_id,
            route_to_connection_id,
            inbound_stream_id: None,
            outbound_stream_id: None,
            created_on_this_worker: false,
        };

        msg
    }

    pub fn to_serialized_msg(&self) -> Result<SerializedMessage> {
        Ok(SerializedMessage::new(&self)?)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.to_serialized_msg()?.to_bytes())
    }
}

///////////////////////////////////
// messages ///////////////////////
///////////////////////////////////

#[derive(Debug, Clone, PartialEq)]
pub enum MessageName {
    Ping,
    Identify,
    RunQuery,
    RunQueryResp,
    OperatorInstanceAvailable,
    OperatorInstanceAssignment,
    QueryHandlerRequests,
    ExchangeRequests,
    OperatorOperatorInstanceStatusChange,
    CommonGenericResponse,
    QueryOperatorInstanceStatusChange,
    ExchangeOperatorStatusChange,
    OperatorShutdown,
    GetQueryStatus,
    GetQueryStatusResp,
    GetQueryData,
    GetQueryDataResp,
    ExchangeRecordHeartbeat,
}

impl MessageName {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ping => "Ping",
            Self::Identify => "Identify",
            Self::RunQuery => "RunQuery",
            Self::RunQueryResp => "RunQueryResp",
            Self::OperatorInstanceAvailable => "OperatorInstanceAvailable",
            Self::OperatorInstanceAssignment => "OperatorInstanceAssignment",
            Self::QueryHandlerRequests => "QueryHandlerRequests",
            Self::ExchangeRequests => "ExchangeRequests",
            Self::OperatorOperatorInstanceStatusChange => "OperatorOperatorInstanceStatusChange",
            Self::CommonGenericResponse => "CommonGenericResponse",
            Self::QueryOperatorInstanceStatusChange => "QueryOperatorInstanceStatusChange",
            Self::ExchangeOperatorStatusChange => "ExchangeOperatorStatusChange",
            Self::OperatorShutdown => "OperatorShutdown",
            Self::GetQueryStatus => "GetQueryStatus",
            Self::GetQueryStatusResp => "GetQueryStatusResp",
            Self::GetQueryData => "GetQueryData",
            Self::GetQueryDataResp => "GetQueryDataResp",
            Self::ExchangeRecordHeartbeat => "ExchangeRecordHeartbeat",
        }
    }
    pub fn as_u16(&self) -> u16 {
        match self {
            Self::Ping => 0,
            Self::Identify => 1,
            Self::RunQuery => 2,
            Self::RunQueryResp => 3,
            Self::OperatorInstanceAvailable => 4,
            Self::OperatorInstanceAssignment => 5,
            Self::QueryHandlerRequests => 6,
            Self::ExchangeRequests => 7,
            Self::OperatorOperatorInstanceStatusChange => 8,
            Self::CommonGenericResponse => 9,
            Self::QueryOperatorInstanceStatusChange => 10,
            Self::ExchangeOperatorStatusChange => 11,
            Self::OperatorShutdown => 12,
            Self::GetQueryStatus => 13,
            Self::GetQueryStatusResp => 14,
            Self::GetQueryData => 15,
            Self::GetQueryDataResp => 16,
            Self::ExchangeRecordHeartbeat => 17,
        }
    }
}

impl fmt::Display for MessageName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
