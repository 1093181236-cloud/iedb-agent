use crate::config::Config;
use reqwest::Client;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing;

#[derive(Debug, Serialize)]
struct RegisterRequest {
    id: String,
    url: String,
}

#[derive(Debug, Serialize)]
struct HeartbeatRequest {
    id: String,
    tables_changed: Vec<TableChange>,
}

#[derive(Debug, Serialize)]
pub struct TableChange {
    pub db: String,
    pub table: String,
    pub min_time: i64,
    pub max_time: i64,
    pub row_count: usize,
}

pub struct AgentClient {
    pub config: Arc<Config>,
    pub client: Client,
    pub agent_url: String,  // this agent's own URL for query routing
}

impl AgentClient {
    /// Register this agent with iotedgedb.
    pub async fn register(&self) -> Result<(), String> {
        let body = RegisterRequest {
            id: self.config.agent.id.clone(),
            url: self.agent_url.clone(),
        };

        let url = format!("{}/api/v1/agents/register", self.config.iotedgedb.url);
        let resp = self.client.post(&url).json(&body).send().await
            .map_err(|e| format!("register: {}", e))?;

        if resp.status().is_success() {
            tracing::info!("Agent registered with iotedgedb");
            Ok(())
        } else {
            Err(format!("register failed: {}", resp.status()))
        }
    }

    /// Send heartbeat with only changed tables since last heartbeat.
    pub async fn heartbeat(
        &self,
        tables_changed: Vec<TableChange>,
    ) -> Result<(), String> {
        let body = HeartbeatRequest {
            id: self.config.agent.id.clone(),
            tables_changed,
        };

        let url = format!("{}/api/v1/agents/heartbeat", self.config.iotedgedb.url);
        let resp = self.client.post(&url).json(&body).send().await
            .map_err(|e| format!("heartbeat: {}", e))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(format!("heartbeat failed: {}", resp.status()))
        }
    }
}

/// Background heartbeat loop. Computes which tables changed since last heartbeat.
pub async fn heartbeat_loop(
    client: Arc<AgentClient>,
    buffer: Arc<Mutex<crate::buffer::Buffer>>,
) {
    let interval = Duration::from_secs(10);
    let mut last_state: std::collections::HashMap<String, (i64, i64, usize)> =
        std::collections::HashMap::new();

    loop {
        tokio::time::sleep(interval).await;

        let buf = buffer.lock().await;
        let mut changed = Vec::new();

        for (db_name, tables) in &buf.databases {
            for (table_name, table) in tables {
                let key = format!("{}.{}", db_name, table_name);
                let min_time = table.chunks.iter().map(|c| c.time_min).min().unwrap_or(0);
                let max_time = table.chunks.iter().map(|c| c.time_max).max().unwrap_or(0);
                let row_count: usize = table.chunks.iter().map(|c| c.rows.len()).sum();

                let prev = last_state.get(&key);
                let is_changed = prev.map_or(true, |(p_min, p_max, p_cnt)| {
                    *p_min != min_time || *p_max != max_time || *p_cnt != row_count
                });

                if is_changed {
                    changed.push(TableChange {
                        db: db_name.clone(),
                        table: table_name.clone(),
                        min_time,
                        max_time,
                        row_count,
                    });
                    last_state.insert(key, (min_time, max_time, row_count));
                }
            }
        }

        // Also report tables that were present before but are now gone (row_count=0)
        let current_keys: std::collections::HashSet<String> = buf.databases.iter()
            .flat_map(|(db, tables)| tables.keys().map(move |t| format!("{}.{}", db, t)))
            .collect();
        for (key, _) in last_state.clone() {
            if !current_keys.contains(&key) {
                let parts: Vec<&str> = key.splitn(2, '.').collect();
                if parts.len() == 2 {
                    changed.push(TableChange {
                        db: parts[0].to_string(),
                        table: parts[1].to_string(),
                        min_time: 0,
                        max_time: 0,
                        row_count: 0, // signals table is gone
                    });
                    last_state.remove(&key);
                }
            }
        }

        if let Err(e) = client.heartbeat(changed).await {
            tracing::warn!(error = %e, "Heartbeat failed");
        }
    }
}
