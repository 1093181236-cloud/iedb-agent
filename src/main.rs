use iedb_agent::{agent, buffer, config, flush, http, wal};
use config::Config;
use flush::scheduler::SnapshotScheduler;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, Method};
use hyper_util::rt::TokioIo;
use reqwest::Client;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = Arc::new(Config::from_file("iedb-agent.toml")?);
    let data_dir = config.data.dir.clone();
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(data_dir.join("wal"))?;
    std::fs::create_dir_all(data_dir.join("meta"))?;
    std::fs::create_dir_all(data_dir.join("staging"))?;

    tracing::info!(agent_id = %config.agent.id, "Starting iedb-agent");

    // Initialize buffer
    let buffer = Arc::new(Mutex::new(buffer::Buffer::new()));

    // Initialize WAL
    let wal_manager = Arc::new(Mutex::new(
        wal::wal_core::WalManager::new(&data_dir, &config.wal).await?
    ));

    // Replay WAL
    wal_manager.lock().await.replay(&buffer).await?;

    // HTTP client
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // Register agent
    let agent_addr = format!("http://localhost:{}", config.server.port);
    let agent_client = Arc::new(agent::AgentClient {
        config: config.clone(),
        client: client.clone(),
        agent_url: agent_addr,
    });

    if let Err(e) = agent_client.register().await {
        tracing::warn!(error = %e, "Agent registration failed, will retry via heartbeat");
    }

    // Start heartbeat loop
    let hb_buffer = buffer.clone();
    let hb_client = agent_client.clone();
    tokio::spawn(async move {
        agent::heartbeat_loop(hb_client, hb_buffer).await;
    });

    // Start WAL flush background task
    let wal_flush = wal_manager.clone();
    let wal_flush_buffer = buffer.clone();
    let wal_flush_interval = config.wal.flush_interval_secs;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(
            std::time::Duration::from_secs(wal_flush_interval)
        );
        loop {
            interval.tick().await;
            match wal_flush.lock().await.flush().await {
                Ok(ops) => {
                    for op in ops {
                        if let wal::WalOp::Write(batch) = op {
                            let mut buf = wal_flush_buffer.lock().await;
                            wal::wal_core::apply_write_batch(&mut buf, &batch, 0);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "WAL flush failed");
                }
            }
        }
    });

    // Start snapshot scheduler
    let snapshot_scheduler = SnapshotScheduler::new(
        buffer.clone(),
        wal_manager.clone(),
        config.clone(),
        client.clone(),
    );
    tokio::spawn(async move {
        snapshot_scheduler.run().await;
    });

    // Start staging retry background task
    let retry_client = client.clone();
    let retry_staging = data_dir.join("staging");
    let retry_config = config.clone();
    tokio::spawn(async move {
        retry_staging_files(retry_client, retry_staging, retry_config).await;
    });

    // HTTP server
    let write_handler = Arc::new(http::write::WriteHandler {
        buffer: buffer.clone(),
        wal: wal_manager.clone(),
        config: config.clone(),
    });
    let query_handler = Arc::new(http::query::QueryHandler {
        buffer: buffer.clone(),
    });

    let addr: SocketAddr = format!("0.0.0.0:{}", config.server.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "Server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let write_handler = write_handler.clone();
        let query_handler = query_handler.clone();

        tokio::spawn(async move {
            let svc = service_fn(move |req: Request<Incoming>| {
                let write = write_handler.clone();
                let query = query_handler.clone();
                async move {
                    match (req.method(), req.uri().path()) {
                        (&Method::POST, "/write") => write.handle(req).await,
                        (&Method::GET, "/query") => query.handle(req).await,
                        (&Method::GET, "/health") => {
                            Ok::<_, hyper::Error>(
                                Response::builder()
                                    .status(200)
                                    .body("ok".into())
                                    .unwrap()
                            )
                        }
                        _ => Ok(Response::builder().status(404).body("not found".into()).unwrap()),
                    }
                }
            });

            if let Err(e) = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(io, svc)
                .await
            {
                tracing::error!(error = %e, "Connection error");
            }
        });
    }
}

async fn retry_staging_files(
    client: Client,
    staging_dir: std::path::PathBuf,
    config: Arc<Config>,
) {
    let interval = std::time::Duration::from_secs(30);
    loop {
        tokio::time::sleep(interval).await;

        if let Ok(entries) = std::fs::read_dir(&staging_dir) {
            for entry in entries.flatten() {
                let db_dir = entry.path();
                if !db_dir.is_dir() { continue; }
                let db = db_dir.file_name().unwrap().to_string_lossy().to_string();

                if let Ok(table_dirs) = std::fs::read_dir(&db_dir) {
                    for t_entry in table_dirs.flatten() {
                        let table_dir = t_entry.path();
                        if !table_dir.is_dir() { continue; }
                        let table = table_dir.file_name().unwrap().to_string_lossy().to_string();

                        if let Ok(files) = std::fs::read_dir(&table_dir) {
                            for f_entry in files.flatten() {
                                let path = f_entry.path();
                                if path.extension().map_or(false, |e| e == "parquet") {
                                    match std::fs::read(&path) {
                                        Ok(data) => {
                                            let url = format!(
                                                "{}/api/v1/ingest/parquet?db={}&measurement={}",
                                                config.iotedgedb.url,
                                                urlencoding(&db),
                                                urlencoding(&table),
                                            );
                                            match client.post(&url)
                                                .header("Content-Type", "application/octet-stream")
                                                .body(data)
                                                .send()
                                                .await
                                            {
                                                Ok(resp) if resp.status().is_success() => {
                                                    let _ = std::fs::remove_file(&path);
                                                    tracing::info!(path = %path.display(), "Staging file uploaded and removed");
                                                }
                                                Ok(resp) => {
                                                    tracing::warn!(status = %resp.status(), "Staging retry failed");
                                                }
                                                Err(e) => {
                                                    tracing::warn!(error = %e, "Staging retry HTTP error");
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(path = %path.display(), error = %e, "Cannot read staging file");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn urlencoding(s: &str) -> String {
    s.replace(' ', "%20")
}
