use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;
use tracing;

/// Upload Parquet bytes to iotededb via HTTP.
pub async fn upload_parquet(
    client: &Client,
    iotededb_url: &str,
    db: &str,
    table: &str,
    data: &[u8],
) -> Result<(), UploadError> {
    let url = format!(
        "{}/api/v1/ingest/parquet?db={}&measurement={}",
        iotededb_url.trim_end_matches('/'),
        urlencoding(db),
        urlencoding(table)
    );

    let resp = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .body(data.to_vec())
        .send()
        .await
        .map_err(|e| UploadError::Http(e.to_string()))?;

    if resp.status().is_success() {
        tracing::info!(db = db, table = table, bytes = data.len(), "Parquet uploaded");
        Ok(())
    } else {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(UploadError::ServerError { status, body })
    }
}

/// Save Parquet bytes to local staging on upload failure.
pub fn staging_save(
    staging_dir: &Path,
    db: &str,
    table: &str,
    data: &[u8],
) -> Result<PathBuf, std::io::Error> {
    let dir = staging_dir.join(db).join(table);
    fs::create_dir_all(&dir)?;

    let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S_%f");
    let path = dir.join(format!("{}.parquet", ts));
    fs::write(&path, data)?;
    tracing::info!(path = %path.display(), bytes = data.len(), "Parquet saved to staging");
    Ok(path)
}

fn urlencoding(s: &str) -> String {
    s.replace(' ', "%20")
}

#[derive(Debug)]
pub enum UploadError {
    Http(String),
    ServerError { status: u16, body: String },
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UploadError::Http(e) => write!(f, "HTTP error: {}", e),
            UploadError::ServerError { status, body } => {
                write!(f, "server error {}: {}", status, body)
            }
        }
    }
}
