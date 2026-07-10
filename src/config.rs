use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub data: DataConfig,
    pub wal: WalConfig,
    pub flush: FlushConfig,
    #[serde(default)]
    pub s3: Option<S3Config>,
    pub iotedgedb: IotedgedbConfig,
    pub agent: AgentConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_port() -> u16 { 8080 }

#[derive(Debug, Clone, Deserialize)]
pub struct DataConfig {
    #[serde(default = "default_data_dir")]
    pub dir: PathBuf,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/iedb-agent")
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalConfig {
    #[serde(default = "default_wal_flush_interval")]
    pub flush_interval_secs: u64,
    #[serde(default = "default_max_write_buffer_ops")]
    pub max_write_buffer_ops: usize,
}

fn default_wal_flush_interval() -> u64 { 1 }
fn default_max_write_buffer_ops() -> usize { 100_000 }

#[derive(Debug, Clone, Deserialize)]
pub struct FlushConfig {
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval: String,  // e.g. "10m"
    #[serde(default = "default_backend")]
    pub backend: String,            // "http" or "s3"
    #[serde(default = "default_memory_limit")]
    pub memory_limit: String,       // e.g. "512MB"
}

fn default_snapshot_interval() -> String { "10m".into() }
fn default_backend() -> String { "http".into() }
fn default_memory_limit() -> String { "512MB".into() }

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IotedgedbConfig {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    pub id: String,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn snapshot_interval_secs(&self) -> i64 {
        parse_duration(&self.flush.snapshot_interval)
    }

    pub fn memory_limit_bytes(&self) -> usize {
        parse_bytes(&self.flush.memory_limit)
    }
}

fn parse_duration(s: &str) -> i64 {
    let s = s.trim();
    if s.ends_with('m') {
        s[..s.len()-1].parse::<i64>().unwrap_or(10) * 60
    } else if s.ends_with('s') {
        s[..s.len()-1].parse::<i64>().unwrap_or(600)
    } else {
        600
    }
}

fn parse_bytes(s: &str) -> usize {
    let s = s.trim().to_uppercase();
    if s.ends_with("MB") {
        s[..s.len()-2].parse::<usize>().unwrap_or(512) * 1024 * 1024
    } else if s.ends_with("GB") {
        s[..s.len()-2].parse::<usize>().unwrap_or(1) * 1024 * 1024 * 1024
    } else {
        512 * 1024 * 1024
    }
}
