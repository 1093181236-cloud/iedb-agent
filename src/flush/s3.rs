use crate::config::S3Config;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use reqwest::Client;
use std::time::SystemTime;

/// Upload Parquet bytes to S3 using SigV4 signing.
pub async fn upload_to_s3(
    client: &Client,
    config: &S3Config,
    key: &str,
    data: &[u8],
) -> Result<(), String> {
    let host = format!("{}.s3.{}.amazonaws.com", config.bucket, config.region);
    let uri = format!("https://{}/{}", host, key);

    // Set up credentials and identity for signing
    let creds = Credentials::new(
        &config.access_key,
        &config.secret_key,
        None,
        None,
        "iedb-agent",
    );
    let identity: Identity = Identity::new(creds, None);

    // Build signing params
    let settings = SigningSettings::default();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(&config.region)
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .map_err(|e| format!("Build signing params: {e}"))?
        .into();

    // Create signable request
    let signable = SignableRequest::new(
        "PUT",
        &uri,
        std::iter::once(("host", host.as_str()))
            .chain(std::iter::once(("content-type", "application/octet-stream"))),
        SignableBody::Bytes(data),
    )
    .map_err(|e| format!("Build signable request: {e}"))?;

    // Sign
    let (signing_instructions, _signature) = sign(signable, &signing_params)
        .map_err(|e| format!("Sign: {e}"))?
        .into_parts();

    // Build the actual HTTP request and apply signing instructions
    let mut http_req = http::Request::builder()
        .method("PUT")
        .uri(&uri)
        .header("host", &host)
        .header("content-type", "application/octet-stream")
        .body(data.to_vec())
        .map_err(|e| format!("Build request: {e}"))?;

    signing_instructions.apply_to_request_http1x(&mut http_req);

    // Execute the signed request
    let reqwest_req = reqwest::Request::try_from(http_req)
        .map_err(|e| format!("Convert to reqwest: {e}"))?;

    let resp = client
        .execute(reqwest_req)
        .await
        .map_err(|e| format!("S3 PUT: {e}"))?;

    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!(
            "S3 error {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        ))
    }
}

/// Build S3 object key from db/table/timestamp.
pub fn s3_key(agent_id: &str, db: &str, table: &str, ts_nanos: i64) -> String {
    let dt = chrono::DateTime::from_timestamp_nanos(ts_nanos);
    let year = dt.format("%Y");
    let month = dt.format("%m");
    let day = dt.format("%d");
    let hour = dt.format("%H");
    let ts_str = dt.format("%Y%m%d_%H%M%S");
    let nanos = ts_nanos % 1_000_000_000;

    format!(
        "{}/{}/{}/{}/{}/{}/{}_{}_{:09}.parquet",
        db, table, year, month, day, hour, agent_id, ts_str, nanos
    )
}
