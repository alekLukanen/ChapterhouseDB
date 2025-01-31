use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{handlers::operator_handler::TotalOperatorCompute, planner};

use super::message::{GenericMessage, MessageName, SendableMessage};

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
