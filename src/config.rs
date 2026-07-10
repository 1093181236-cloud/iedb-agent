use serde::Deserialize;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp_toml(name: &str, content: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("iedb_cfg_{}", name));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    #[test]
    fn test_parse_duration_values() {
        assert_eq!(parse_duration("10m"), 600);
        assert_eq!(parse_duration("60s"), 60);
        assert_eq!(parse_duration("5m"), 300);
        assert_eq!(parse_duration("90s"), 90);
    }

    #[test]
    fn test_parse_duration_unknown_returns_default() {
        assert_eq!(parse_duration("abc"), 600);
        assert_eq!(parse_duration(""), 600);
    }

    #[test]
    fn test_parse_bytes_values() {
        assert_eq!(parse_bytes("512MB"), 536_870_912);
        assert_eq!(parse_bytes("1GB"), 1_073_741_824);
        assert_eq!(parse_bytes("256MB"), 268_435_456);
        assert_eq!(parse_bytes("2GB"), 2_147_483_648);
    }

    #[test]
    fn test_parse_bytes_unknown_returns_default() {
        assert_eq!(parse_bytes("abc"), 536_870_912);
        assert_eq!(parse_bytes(""), 536_870_912);
    }

    #[test]
    fn test_default_values_when_fields_missing() {
        let config: Config = toml::from_str(
            r#"
            [server]
            [data]
            [wal]
            [flush]
            [iotedgedb]
            url = "http://localhost"
            [agent]
            id = "test-agent"
            "#,
        )
        .unwrap();

        assert_eq!(config.flush.snapshot_interval, "10m");
        assert_eq!(config.flush.memory_limit, "512MB");
        assert_eq!(config.flush.backend, "http");
        assert_eq!(config.wal.max_write_buffer_ops, 100_000);
        assert_eq!(config.wal.flush_interval_secs, 1);
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.data.dir, std::path::PathBuf::from("/var/lib/iedb-agent"));
    }

    #[test]
    fn test_full_config_from_toml_file() {
        let name = format!("full_cfg_{}", std::process::id());
        let path = write_temp_toml(
            &name,
            r#"
            [server]
            port = 9090

            [data]
            dir = "/tmp/test-data"

            [wal]
            flush_interval_secs = 5
            max_write_buffer_ops = 50000

            [flush]
            snapshot_interval = "2m"
            backend = "s3"
            memory_limit = "256MB"

            [s3]
            bucket = "my-bucket"
            region = "us-east-1"
            endpoint = "https://s3.example.com"
            access_key = "AKID"
            secret_key = "secret"

            [iotedgedb]
            url = "http://localhost:8086"

            [agent]
            id = "agent-01"
            "#,
        );

        let config = Config::from_file(path.to_str().unwrap()).unwrap();
        assert_eq!(config.server.port, 9090);
        assert_eq!(config.data.dir, std::path::PathBuf::from("/tmp/test-data"));
        assert_eq!(config.wal.flush_interval_secs, 5);
        assert_eq!(config.wal.max_write_buffer_ops, 50000);
        assert_eq!(config.flush.snapshot_interval, "2m");
        assert_eq!(config.flush.backend, "s3");
        assert_eq!(config.flush.memory_limit, "256MB");
        assert_eq!(config.iotedgedb.url, "http://localhost:8086");
        assert_eq!(config.agent.id, "agent-01");
        // Verify derived helpers
        assert_eq!(config.snapshot_interval_secs(), 120);
        assert_eq!(config.memory_limit_bytes(), 268_435_456);

        // Cleanup
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        // Verify default values for fields we didn't set
        assert_eq!(config.wal.flush_interval_secs, 5); // explicitly set
    }
}
