use super::WalContents;
use bitcode;

const FILE_TYPE_IDENTIFIER: &[u8; 8] = b"iedb.a01";

impl WalContents {
    /// Serialize to the WAL file format: identifier + CRC32 + bitcode bytes.
    pub fn serialize_to_file(&self) -> Vec<u8> {
        let payload = bitcode::serialize(self).expect("WAL serialize");
        let crc = crc32fast::hash(&payload);
        let mut out = Vec::with_capacity(8 + 4 + payload.len());
        out.extend_from_slice(FILE_TYPE_IDENTIFIER);
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Deserialize from WAL file bytes.
    pub fn deserialize_from_file(data: &[u8]) -> Result<Self, String> {
        if data.len() < 12 {
            return Err("WAL file too short".into());
        }
        if &data[..8] != FILE_TYPE_IDENTIFIER {
            return Err("invalid WAL file identifier".into());
        }
        let stored_crc = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let payload = &data[12..];
        let actual_crc = crc32fast::hash(payload);
        if stored_crc != actual_crc {
            return Err(format!(
                "WAL CRC mismatch: stored={:08x} actual={:08x}",
                stored_crc, actual_crc
            ));
        }
        bitcode::deserialize(payload).map_err(|e| format!("WAL deserialize: {}", e))
    }
}
