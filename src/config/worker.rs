use anyhow::Result;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkerConfigError {
    #[error("validation failed: {0}")]
    ValidationFailed(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Log level
    pub log_level: String,
    /// What port to host this service on
    pub port: u32,
    /// Connect to these other workers
    pub connect_to_addresses: Vec<String>,
    /// Connections
    pub connections: Vec<ConnectionConfig>,
}

impl WorkerConfig {
    pub fn from_file(file_path: String) -> Result<Self> {
        let file = std::fs::File::open(file_path)?;
        let reader = std::io::BufReader::new(file);
        let config: Self = serde_json::from_reader(reader)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        self.log_level()?;

        if self.port == 0 {
            return Err(
                WorkerConfigError::ValidationFailed(format!("port - '{}'", self.port)).into(),
            );
        }

        if self
            .connect_to_addresses
            .iter()
            .find(|item| item.len() == 0)
            .is_some()
        {
            return Err(
                WorkerConfigError::ValidationFailed(format!("connect_to_addresses")).into(),
            );
        }

        let first_err = self
            .connections
            .iter()
            .map(|item| item.validate())
            .find(|item| item.is_err());
        match first_err {
            Some(res) => res,
            None => Ok(()),
        }
    }

    pub fn log_level(&self) -> Result<tracing::Level> {
        let log_level = if self.log_level.to_lowercase() == "info" {
            tracing::Level::INFO
        } else if self.log_level.to_lowercase() == "debug" {
            tracing::Level::DEBUG
        } else if self.log_level.to_lowercase() == "warning" {
            tracing::Level::WARN
        } else if self.log_level.to_lowercase() == "error" {
            tracing::Level::ERROR
        } else {
            return Err(WorkerConfigError::ValidationFailed(format!(
                "log_level - '{}'",
                self.log_level
            ))
            .into());
        };
        Ok(log_level)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub name: String,
    pub connection_type: ConnectionType,
}

impl ConnectionConfig {
    pub fn validate(&self) -> Result<()> {
        if self.name.len() == 0 || !self.name.is_ascii() {
            return Err(
                WorkerConfigError::ValidationFailed(format!("name - '{}'", self.name)).into(),
            );
        }

        self.connection_type.validate()?;

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ConnectionType {
    S3 {
        /// The endpoint that S3 is located at
        endpoint: String,
        /// The access key
        access_key_id: String,
        /// The secret key
        secret_access_key_id: String,
        /// The bucket to create data in
        bucket: String,
        /// Region
        region: String,
        /// Should the path style be used
        force_path_style: bool,
    },
    Fs {
        /// The root directory this connection has access to
        root: String,
    },
}

impl ConnectionType {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::S3 {
                endpoint,
                bucket,
                force_path_style,
                ..
            } => {
                if endpoint.len() == 0 {
                    return Err(WorkerConfigError::ValidationFailed(format!(
                        "endpoint - '{}'",
                        endpoint
                    ))
                    .into());
                }
                if bucket.len() == 0 {
                    return Err(WorkerConfigError::ValidationFailed(format!(
                        "bucket - '{}'",
                        bucket
                    ))
                    .into());
                }
            }
            Self::Fs { root } => {
                if root.len() == 0 {
                    return Err(
                        WorkerConfigError::ValidationFailed(format!("root - '{}'", root,)).into(),
                    );
                }
            }
        }

        Ok(())
    }
}
