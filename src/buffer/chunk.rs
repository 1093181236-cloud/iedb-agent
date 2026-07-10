use std::collections::HashMap;

/// A field value in a time-series row.
#[derive(Debug, Clone)]
pub enum FieldValue {
    I64(i64),
    F64(f64),
    U64(u64),
    Bool(bool),
    String(String),
}

/// The type of a field column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    I64,
    F64,
    U64,
    Bool,
    String,
}

impl FieldValue {
    pub fn field_type(&self) -> FieldType {
        match self {
            FieldValue::I64(_) => FieldType::I64,
            FieldValue::F64(_) => FieldType::F64,
            FieldValue::U64(_) => FieldType::U64,
            FieldValue::Bool(_) => FieldType::Bool,
            FieldValue::String(_) => FieldType::String,
        }
    }
}

/// A field definition in the table schema.
#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name: String,
    pub value_type: FieldType,
}

/// Table-level schema shared across all rows and chunks.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub tag_keys: Vec<String>,
    pub field_defs: Vec<FieldDef>,
}

impl TableSchema {
    pub fn new() -> Self {
        TableSchema {
            tag_keys: Vec::new(),
            field_defs: Vec::new(),
        }
    }

    /// Return the index of a field, adding it if new (schema evolution).
    pub fn ensure_field(&mut self, name: &str, value_type: FieldType) -> usize {
        if let Some(pos) = self.field_defs.iter().position(|f| f.name == name) {
            return pos;
        }
        self.field_defs.push(FieldDef {
            name: name.to_string(),
            value_type,
        });
        self.field_defs.len() - 1
    }

    /// Return the index of a tag key, adding it if new.
    pub fn ensure_tag_key(&mut self, key: &str) -> usize {
        if let Some(pos) = self.tag_keys.iter().position(|k| k == key) {
            return pos;
        }
        self.tag_keys.push(key.to_string());
        self.tag_keys.len() - 1
    }
}

/// A row stores only values; keys come from TableSchema.
#[derive(Debug, Clone)]
pub struct Row {
    pub time: i64,
    pub tag_values: Vec<String>,
    pub field_values: Vec<Option<FieldValue>>,
}

/// A time-partitioned chunk of rows.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub chunk_time: i64,
    pub rows: Vec<Row>,
    pub tag_index: HashMap<String, HashMap<String, Vec<usize>>>,
    pub time_min: i64,
    pub time_max: i64,
    pub avg_row_bytes: usize,
    pub min_wal_seq: u64,
    pub max_wal_seq: u64,
}

impl Chunk {
    pub fn new(chunk_time: i64) -> Self {
        Chunk {
            chunk_time,
            rows: Vec::new(),
            tag_index: HashMap::new(),
            time_min: i64::MAX,
            time_max: i64::MIN,
            avg_row_bytes: 0,
            min_wal_seq: u64::MAX,
            max_wal_seq: 0,
        }
    }

    pub fn estimated_size(&self) -> usize {
        self.rows.len() * self.avg_row_bytes.max(64)
    }

    /// Insert a row into this chunk.
    pub fn insert(&mut self, row: Row, wal_seq: u64) {
        let _row_idx = self.rows.len();

        // Update time bounds
        if row.time < self.time_min { self.time_min = row.time; }
        if row.time > self.time_max { self.time_max = row.time; }

        // Update WAL seq bounds
        if wal_seq < self.min_wal_seq { self.min_wal_seq = wal_seq; }
        if wal_seq > self.max_wal_seq { self.max_wal_seq = wal_seq; }

        // Update tag index
        // We know tag keys from the caller, not stored per-row.
        // The caller (Table) updates the index after adjusting schema.

        self.rows.push(row);
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// A table holds a schema and time-ordered chunks (usually 1-2).
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub schema: TableSchema,
    pub chunks: Vec<Chunk>,
}

impl Table {
    pub fn new(name: String) -> Self {
        Table {
            name,
            schema: TableSchema::new(),
            chunks: Vec::new(),
        }
    }

    /// Find or create the chunk for the given chunk_time.
    pub fn get_or_create_chunk(&mut self, chunk_time: i64) -> &mut Chunk {
        match self.chunks.binary_search_by(|c| c.chunk_time.cmp(&chunk_time)) {
            Ok(idx) => &mut self.chunks[idx],
            Err(idx) => {
                self.chunks.insert(idx, Chunk::new(chunk_time));
                &mut self.chunks[idx]
            }
        }
    }

    /// Total estimated memory size of all chunks.
    pub fn estimated_size(&self) -> usize {
        self.chunks.iter().map(|c| c.estimated_size()).sum()
    }

    /// Build tag_index entries for a row's tag values given the schema.
    pub fn build_tag_index(&mut self, chunk: &mut Chunk, row_idx: usize, tag_values: &[String]) {
        let tag_keys = &self.schema.tag_keys;
        for (key_idx, key) in tag_keys.iter().enumerate() {
            if let Some(value) = tag_values.get(key_idx) {
                chunk.tag_index
                    .entry(key.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .push(row_idx);
            }
        }
    }
}
