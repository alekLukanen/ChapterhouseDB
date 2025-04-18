use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};

#[derive(Subcommand, Debug, Clone)]
pub enum ConnectionCommand {
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
        force_path_style: String,
    },
    Fs,
}

// sample data args //////////////////////////////////

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CreateSampleDataArgs {
    #[command(subcommand)]
    pub command: Option<ConnectionCommand>,
    /// A prefix or directory to store all data
    #[arg(short, long)]
    pub path_prefix: String,
}

impl CreateSampleDataArgs {
    pub fn validate(&self) -> Result<()> {
        if self.path_prefix.len() == 0 {
            return Err(anyhow!("path_prefix must be a value"));
        }
        return Ok(());
    }
}

// worker args //////////////////////////////////////

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct WorkerArgs {
    /// Path to a .json configuration file
    #[arg(short, long)]
    pub config_file: String,
}
