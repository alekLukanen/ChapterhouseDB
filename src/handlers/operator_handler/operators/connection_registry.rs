use std::collections::HashMap;

use anyhow::Result;
use opendal::layers::LoggingLayer;
use opendal::layers::RetryLayer;
use opendal::services;
use opendal::Builder;
use opendal::Operator;
use opendal::Scheme;
use path_clean::PathClean;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnectionRegistryError {
    #[error("connection name not found: {0}")]
    ConnectionNameNotFound(String),
    #[error("connection with name '{0}' already exists")]
    ConnectionNameAlreadyExists(String),
    #[error("failed to clean fs root dir: {0}")]
    FailedToCleanFsRootDir(String),
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
    ) -> Result<()> {
        if self
            .connections
            .iter()
            .find(|conn| conn.name == name)
            .is_some()
        {
            return Err(ConnectionRegistryError::ConnectionNameAlreadyExists(name).into());
        }

        self.connections.push(Connection {
            name,
            scheme,
            config,
        });
        Ok(())
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
            Scheme::Fs => Ok(init_service::<services::Fs>(conn.config.clone())?),
            val => Err(ConnectionRegistryError::NotImplemented(format!(
                "opendal schema type {} ",
                val.to_string()
            ))
            .into()),
        }
    }

    // common connection types //////////////////////////////

    pub fn add_fs_connection(&mut self, name: String, root: String) -> Result<()> {
        let cleaned_root =
            if let Some(cleaned_root) = std::path::PathBuf::from(root.clone()).clean().to_str() {
                cleaned_root.to_string()
            } else {
                return Err(ConnectionRegistryError::FailedToCleanFsRootDir(root.clone()).into());
            };

        self.add_connection(
            name,
            opendal::Scheme::Fs,
            vec![("root".to_string(), cleaned_root)]
                .into_iter()
                .collect::<HashMap<String, String>>(),
        )?;
        Ok(())
    }

    pub fn add_s3_connection(
        &mut self,
        name: String,
        endpoint: String,
        access_key_id: String,
        secret_access_key_id: String,
        bucket: String,
        force_path_style: String,
        region: String,
    ) -> Result<()> {
        self.add_connection(
            name,
            opendal::Scheme::S3,
            vec![
                ("endpoint".to_string(), endpoint.clone()),
                ("access_key_id".to_string(), access_key_id.clone()),
                (
                    "secret_access_key".to_string(),
                    secret_access_key_id.clone(),
                ),
                ("bucket".to_string(), bucket.clone()),
                (
                    "enable_virtual_host_style".to_string(),
                    (force_path_style.to_lowercase() != "true".to_string()).to_string(),
                ),
                // Optional:
                ("region".to_string(), region.clone()),
            ]
            .into_iter()
            .collect::<HashMap<String, String>>(),
        )?;
        Ok(())
    }
}

fn init_service<B: Builder>(cfg: HashMap<String, String>) -> Result<Operator> {
    let op = Operator::from_iter::<B>(cfg)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new())
        .finish();

    Ok(op)
}
