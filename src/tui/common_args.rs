use anyhow::{anyhow, Result};
use clap::Parser;

// sample data args //////////////////////////////////

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CreateSampleDataArgs {
    /// The connection to create the sample data in
    #[arg(short, long)]
    pub connection_name: String,
    /// A prefix or directory to store all data
    #[arg(short, long)]
    pub path_prefix: String,
    /// Path to a .json configuration file
    #[arg(short, long)]
    pub config_file: String,
}

impl CreateSampleDataArgs {
    pub fn validate(&self) -> Result<()> {
        if self.path_prefix.len() == 0 {
            return Err(anyhow!("path_prefix must be a value"));
        }
        if self.config_file.len() == 0 {
            return Err(anyhow!("config_file must be a value"));
        }
        Ok(())
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

impl WorkerArgs {
    pub fn validate(&self) -> Result<()> {
        if self.config_file.len() == 0 {
            return Err(anyhow!("config_file must be a value"));
        }
        Ok(())
    }
}
