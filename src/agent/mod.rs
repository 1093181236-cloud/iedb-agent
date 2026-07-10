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

/// Compute which tables changed since the last heartbeat.
///
/// Scans the buffer, compares against `last_state`, and returns a list of
/// `TableChange` entries for tables that are new, modified, or removed.
/// `last_state` is updated in place to reflect the new state.
pub fn compute_table_changes(
    buffer: &crate::buffer::Buffer,
    last_state: &mut std::collections::HashMap<String, (i64, i64, usize)>,
) -> Vec<TableChange> {
    let mut changed = Vec::new();

    for (db_name, tables) in &buffer.databases {
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
    let current_keys: std::collections::HashSet<String> = buffer
        .databases
        .iter()
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

    changed
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
        let changed = compute_table_changes(&buf, &mut last_state);

        if let Err(e) = client.heartbeat(changed).await {
            tracing::warn!(error = %e, "Heartbeat failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::Buffer;
    use crate::buffer::chunk::{Chunk, Row};
    use std::collections::HashMap;

    fn make_buffer_with_one_table(db: &str, table: &str, rows: Vec<Row>, time_min: i64, time_max: i64) -> Buffer {
        let mut buffer = Buffer::new();
        let t = buffer.get_or_create_table(db, table);
        let mut chunk = Chunk::new(1000);
        chunk.time_min = time_min;
        chunk.time_max = time_max;
        chunk.rows = rows;
        t.chunks.push(chunk);
        buffer
    }

    fn empty_row() -> Row {
        Row {
            time: 0,
            tag_values: vec![],
            field_values: vec![],
        }
    }

    #[test]
    fn test_first_heartbeat_all_tables_changed() {
        let buffer = make_buffer_with_one_table("mydb", "cpu", vec![empty_row(), empty_row()], 100, 200);
        let mut last_state: HashMap<String, (i64, i64, usize)> = HashMap::new();

        let changes = compute_table_changes(&buffer, &mut last_state);

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].db, "mydb");
        assert_eq!(changes[0].table, "cpu");
        assert_eq!(changes[0].min_time, 100);
        assert_eq!(changes[0].max_time, 200);
        assert_eq!(changes[0].row_count, 2);
        // last_state is updated
        assert_eq!(last_state.get("mydb.cpu"), Some(&(100, 200, 2)));
    }

    #[test]
    fn test_second_heartbeat_no_changes_with_same_state() {
        let buffer = make_buffer_with_one_table("mydb", "cpu", vec![empty_row(), empty_row()], 100, 200);
        let mut last_state: HashMap<String, (i64, i64, usize)> = HashMap::new();

        // First heartbeat: should report changes
        let changes1 = compute_table_changes(&buffer, &mut last_state);
        assert_eq!(changes1.len(), 1);

        // Second heartbeat with same buffer: no changes
        let changes2 = compute_table_changes(&buffer, &mut last_state);
        assert!(changes2.is_empty(), "expected no changes on second heartbeat, got {:?}", changes2);
    }

    #[test]
    fn test_new_table_reported() {
        let mut buffer = make_buffer_with_one_table("mydb", "cpu", vec![empty_row()], 100, 200);
        let mut last_state: HashMap<String, (i64, i64, usize)> = HashMap::new();

        // First heartbeat records cpu table
        let _ = compute_table_changes(&buffer, &mut last_state);

        // Add a second table to the buffer
        let mem_table = buffer.get_or_create_table("mydb", "mem");
        let mut chunk = Chunk::new(2000);
        chunk.time_min = 300;
        chunk.time_max = 400;
        chunk.rows.push(empty_row());
        mem_table.chunks.push(chunk);

        // Second heartbeat: only the new table should be reported
        let changes = compute_table_changes(&buffer, &mut last_state);
        assert_eq!(changes.len(), 1, "only new table should be reported");
        assert_eq!(changes[0].db, "mydb");
        assert_eq!(changes[0].table, "mem");
        assert_eq!(changes[0].row_count, 1);
    }

    #[test]
    fn test_removed_table_reported_with_zero_row_count() {
        let buffer_with_table = make_buffer_with_one_table("mydb", "cpu", vec![empty_row()], 100, 200);
        let mut last_state: HashMap<String, (i64, i64, usize)> = HashMap::new();

        // First heartbeat: records mydb.cpu
        let changes = compute_table_changes(&buffer_with_table, &mut last_state);
        assert_eq!(changes.len(), 1);
        assert!(last_state.contains_key("mydb.cpu"));

        // Second heartbeat: use an empty buffer (table was removed)
        let empty_buffer = Buffer::new();
        let changes = compute_table_changes(&empty_buffer, &mut last_state);

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].db, "mydb");
        assert_eq!(changes[0].table, "cpu");
        assert_eq!(changes[0].min_time, 0);
        assert_eq!(changes[0].max_time, 0);
        assert_eq!(changes[0].row_count, 0);
        // last_state should no longer have the removed table
        assert!(!last_state.contains_key("mydb.cpu"));
    }

    #[test]
    fn test_multiple_tables_in_multiple_databases() {
        let mut buffer = Buffer::new();
        // Add tables in two databases
        let t1 = buffer.get_or_create_table("db1", "table_a");
        let mut c1 = Chunk::new(1000);
        c1.time_min = 10; c1.time_max = 50;
        c1.rows.push(empty_row());
        t1.chunks.push(c1);

        let t2 = buffer.get_or_create_table("db1", "table_b");
        let mut c2 = Chunk::new(2000);
        c2.time_min = 20; c2.time_max = 60;
        c2.rows.push(empty_row()); c2.rows.push(empty_row());
        t2.chunks.push(c2);

        let t3 = buffer.get_or_create_table("db2", "table_c");
        let mut c3 = Chunk::new(3000);
        c3.time_min = 30; c3.time_max = 70;
        c3.rows.push(empty_row()); c3.rows.push(empty_row()); c3.rows.push(empty_row());
        t3.chunks.push(c3);

        let mut last_state: HashMap<String, (i64, i64, usize)> = HashMap::new();

        // First heartbeat: all 3 tables changed
        let changes = compute_table_changes(&buffer, &mut last_state);
        assert_eq!(changes.len(), 3);

        let keys: Vec<String> = changes.iter().map(|c| format!("{}.{}", c.db, c.table)).collect();
        assert!(keys.contains(&"db1.table_a".to_string()));
        assert!(keys.contains(&"db1.table_b".to_string()));
        assert!(keys.contains(&"db2.table_c".to_string()));

        // Second heartbeat: no changes
        let changes2 = compute_table_changes(&buffer, &mut last_state);
        assert!(changes2.is_empty());
    }

    #[test]
    fn test_table_reported_when_row_count_changes() {
        let mut buffer = make_buffer_with_one_table("mydb", "cpu", vec![empty_row()], 100, 200);
        let mut last_state: HashMap<String, (i64, i64, usize)> = HashMap::new();

        // First heartbeat
        let _ = compute_table_changes(&buffer, &mut last_state);

        // Add more rows to the same table
        let table = buffer.get_table_mut("mydb", "cpu").unwrap();
        table.chunks[0].rows.push(empty_row());
        table.chunks[0].rows.push(empty_row());

        // Second heartbeat: should detect the row count change
        let changes = compute_table_changes(&buffer, &mut last_state);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].table, "cpu");
        assert_eq!(changes[0].row_count, 3);
    }
}
