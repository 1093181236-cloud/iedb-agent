mod buffer;
mod config;
mod wal;

use config::Config;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = Config::from_file("iedb-agent.toml")?;
    tracing::info!(agent_id = %config.agent.id, "Starting iedb-agent");

    tracing::info!(port = config.server.port, "Server listening");
    // Will be wired up in later tasks

    Ok(())
}
