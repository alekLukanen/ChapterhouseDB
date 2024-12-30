use std::collections::HashMap;

use anyhow::Result;
use opendal::layers::LoggingLayer;
use opendal::layers::RetryLayer;
use opendal::services;
use opendal::Builder;
use opendal::Operator;
use opendal::Scheme;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnectionRegistryError {
    #[error("connection name not found: {0}")]
    ConnectionNameNotFound(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

#[derive(Debug)]
pub struct Connection {
    name: String,
    scheme: Scheme,
    config: HashMap<String, String>,
}

#[derive(Debug)]
pub struct ConnectionRegistry {
    connections: Vec<Connection>,
}

impl ConnectionRegistry {
    pub fn new() -> ConnectionRegistry {
        ConnectionRegistry {
            connections: Vec::new(),
        }
    }

    pub fn add_connection(
        &mut self,
        name: String,
        scheme: Scheme,
        config: HashMap<String, String>,
    ) {
        self.connections.push(Connection {
            name,
            scheme,
            config,
        });
    }

    pub fn find_connection(&self, name: &str) -> Option<&Connection> {
        self.connections.iter().find(|item| item.name == name)
    }

    pub fn get_operator(&self, name: &str) -> Result<Operator> {
        let conn = if let Some(conn) = self.find_connection(name) {
            conn
        } else {
            return Err(ConnectionRegistryError::ConnectionNameNotFound(name.to_string()).into());
        };

        match conn.scheme {
            Scheme::S3 => Ok(init_service::<services::S3>(conn.config.clone())?),
            val => Err(ConnectionRegistryError::NotImplemented(val.to_string()).into()),
        }
    }
}

fn init_service<B: Builder>(cfg: HashMap<String, String>) -> Result<Operator> {
    let op = Operator::from_iter::<B>(cfg)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new())
        .finish();

    Ok(op)
}
