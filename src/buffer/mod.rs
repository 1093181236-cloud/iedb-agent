pub mod chunk;
pub mod query;

use chunk::Table;
use std::collections::HashMap;

/// Stores all tables across all databases.
#[derive(Debug)]
pub struct Buffer {
    /// db_name => table_name => Table
    pub databases: HashMap<String, HashMap<String, Table>>,
}

impl Buffer {
    pub fn new() -> Self {
        Buffer {
            databases: HashMap::new(),
        }
    }

    pub fn get_or_create_table(&mut self, db: &str, table: &str) -> &mut Table {
        self.databases
            .entry(db.to_string())
            .or_default()
            .entry(table.to_string())
            .or_insert_with(|| Table::new(table.to_string()))
    }

    pub fn get_table(&self, db: &str, table: &str) -> Option<&Table> {
        self.databases.get(db).and_then(|tables| tables.get(table))
    }

    pub fn get_table_mut(&mut self, db: &str, table: &str) -> Option<&mut Table> {
        self.databases.get_mut(db).and_then(|tables| tables.get_mut(table))
    }

    /// Total estimated memory usage across all tables and chunks.
    pub fn total_estimated_size(&self) -> usize {
        self.databases
            .values()
            .flat_map(|tables| tables.values())
            .map(|t| t.estimated_size())
            .sum()
    }
}
