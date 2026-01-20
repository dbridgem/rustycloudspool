use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_config::Region;
use base64::Engine;
use chrono::Utc;
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{Config as RedisConfig, Pool as RedisPool, Runtime as RedisRuntime};
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::{Client as HttpClient, ClientBuilder, StatusCode};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;
const MAX_CACHE_BYTES: usize = 1_048_576;

async fn cache_in_redis(
    conn: &mut deadpool_redis::Connection,
    key: &str,
    data: &[u8],
    ttl: u64,
) -> Result<(), String> {
    if ttl == 0 {
        return Ok(());
    }

    if data.len() <= MAX_CACHE_BYTES {
        conn.set_ex(key, data, ttl)
            .await
            .map_err(|e| format!("Failed to cache in Redis: {}", e))
    } else {
        println!(
            "Skipping Redis cache for large object ({} bytes)",
            data.len()
        );
        Ok(())
    }
}

fn encode_path_segments(path: &str) -> String {
    path.split('/')
        .map(|s| utf8_percent_encode(s, NON_ALPHANUMERIC).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn infer_content_type(key: &str) -> Option<&'static str> {
    let extension = key.rsplit('.').next()?;
    match extension.to_lowercase().as_str() {
        // Text
        "txt" => Some("text/plain"),
        "html" | "htm" => Some("text/html"),
        "css" => Some("text/css"),
        "js" => Some("application/javascript"),
        "json" => Some("application/json"),
        "xml" => Some("application/xml"),
        "csv" => Some("text/csv"),
        // Images
        "jpg" | "jpeg" => Some("image/jpeg"),
        "png" => Some("image/png"),
        "gif" => Some("image/gif"),
        "svg" => Some("image/svg+xml"),
        "webp" => Some("image/webp"),
        "ico" => Some("image/x-icon"),
        // Documents
        "pdf" => Some("application/pdf"),
        "doc" => Some("application/msword"),
        "docx" => Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        "xls" => Some("application/vnd.ms-excel"),
        "xlsx" => Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        "ppt" => Some("application/vnd.ms-powerpoint"),
        "pptx" => Some("application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        // Archives
        "zip" => Some("application/zip"),
        "gz" | "gzip" => Some("application/gzip"),
        "tar" => Some("application/x-tar"),
        "7z" => Some("application/x-7z-compressed"),
        "rar" => Some("application/vnd.rar"),
        // Media
        "mp3" => Some("audio/mpeg"),
        "mp4" => Some("video/mp4"),
        "mpeg" => Some("video/mpeg"),
        "avi" => Some("video/x-msvideo"),
        "wav" => Some("audio/wav"),
        "webm" => Some("video/webm"),
        // Other
        "wasm" => Some("application/wasm"),
        "bin" => Some("application/octet-stream"),
        _ => None,
    }
}

#[async_trait]
pub trait BaseClientHandler: Send + Sync {
    async fn download_file(&self, key: &str) -> Result<Vec<u8>, String>;
    async fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<(), String>;
    async fn list_files(&self, prefix: &str) -> Result<Vec<String>, String>;
}

pub struct S3ClientHandler {
    bucket: String,
    client: aws_sdk_s3::Client,
    redis_pool: Option<RedisPool>,
    ttl: u64,
}

pub struct AzureBlobClientHandler {
    bucket: String,
    account_name: String,
    account_key: Vec<u8>,
    client: HttpClient,
    redis_pool: Option<RedisPool>,
    ttl: u64,
}

impl S3ClientHandler {
    pub async fn new(bucket: String, region: String, redis_url: String, ttl: &u64) -> Self {
        let behavior_version = BehaviorVersion::latest();
        let region_provider = RegionProviderChain::first_try(Some(Region::new(region.clone())));
        let config = aws_config::defaults(behavior_version)
            .region(region_provider)
            .load()
            .await;
        let client = aws_sdk_s3::Client::new(&config);

        let redis_pool = if !redis_url.is_empty() && *ttl > 0 {
            let mut redis_cfg = RedisConfig::from_url(redis_url);
            // tune pool size for your workload (start with 50-200 depending on hardware)
            redis_cfg.pool.get_or_insert(Default::default()).max_size = 100;
            Some(redis_cfg.create_pool(Some(RedisRuntime::Tokio1)).unwrap())
        } else {
            None
        };

        S3ClientHandler {
            bucket,
            client,
            redis_pool,
            ttl: *ttl,
        }
    }
}

impl AzureBlobClientHandler {
    pub async fn new(
        bucket: String,
        connection_string: String,
        redis_url: String,
        ttl: &u64,
    ) -> Self {
        // Parse connection string manually
        let parts: HashMap<String, String> = connection_string
            .split(';')
            .filter_map(|s| {
                let mut split = s.splitn(2, '=');
                match (split.next(), split.next()) {
                    (Some(k), Some(v)) => Some((k.to_string(), v.to_string())),
                    _ => None,
                }
            })
            .collect();

        let account_name = parts
            .get("AccountName")
            .expect("Connection string missing AccountName")
            .clone();
        let account_key_b64 = parts
            .get("AccountKey")
            .expect("Connection string missing AccountKey");
        let account_key = base64::engine::general_purpose::STANDARD
            .decode(account_key_b64)
            .expect("Invalid base64 AccountKey");

        let client = ClientBuilder::new()
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(32)
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .build()
            .expect("failed to build reqwest client");

        let redis_pool = if !redis_url.is_empty() && *ttl > 0 {
            let mut redis_cfg = RedisConfig::from_url(redis_url);
            redis_cfg.pool.get_or_insert(Default::default()).max_size = 100;
            Some(redis_cfg.create_pool(Some(RedisRuntime::Tokio1)).unwrap())
        } else {
            None
        };

        AzureBlobClientHandler {
            bucket,
            account_name,
            account_key,
            client,
            redis_pool,
            ttl: *ttl,
        }
    }

    fn sign_request(
        &self,
        method: &str,
        content_length: &str,
        headers: &str,  // canonicalized headers
        resource: &str, // canonicalized resource
    ) -> String {
        // StringToSign = VERB + "\n" +
        // Content-Encoding + "\n" +
        // Content-Language + "\n" +
        // Content-Length + "\n" +
        // Content-MD5 + "\n" +
        // Content-Type + "\n" +
        // Date + "\n" +
        // If-Modified-Since + "\n" +
        // If-Match + "\n" +
        // If-None-Match + "\n" +
        // If-Unmodified-Since + "\n" +
        // Range + "\n" +
        // CanonicalizedHeaders +
        // CanonicalizedResource;

        // Simplified for our usage (most standard headers empty)
        let string_to_sign = format!(
            "{}\n\n\n{}\n\n\n\n\n\n\n\n\n{}\n{}",
            method, content_length, headers, resource
        );

        let mut mac = HmacSha256::new_from_slice(&self.account_key).unwrap();
        mac.update(string_to_sign.as_bytes());
        let result = mac.finalize();
        let signature = base64::engine::general_purpose::STANDARD.encode(result.into_bytes());
        format!("SharedKey {}:{}", self.account_name, signature)
    }
}

#[async_trait]
impl BaseClientHandler for S3ClientHandler {
    async fn download_file(&self, key: &str) -> Result<Vec<u8>, String> {
        // Try to get from Redis cache first if pool exists
        if let Some(pool) = &self.redis_pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| format!("Failed to get Redis connection: {}", e))?;
            let cached: Option<Vec<u8>> =
                deadpool_redis::redis::AsyncCommands::get(&mut *conn, key)
                    .await
                    .map_err(|e| format!("Failed to read from Redis: {}", e))?;
            if let Some(data) = cached {
                return Ok(data);
            }
        }

        // If not cached, fetch from S3
        let max_retries = 3u32;
        let base = std::time::Duration::from_millis(100);
        let mut attempt: u32 = 0;
        let obj = loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(o) => break o,
                Err(e) => {
                    // Check if file doesn't exist
                    if e.as_service_error()
                        .map(|se| se.is_no_such_key())
                        .unwrap_or(false)
                    {
                        return Ok(Vec::new());
                    }

                    attempt += 1;
                    if attempt > max_retries {
                        return Err(format!("Failed to upload S3 object after retries: {}", e));
                    }
                    let multiplier = 1u64 << (attempt - 1);
                    let mut delay_ms = base.as_millis() as u64;
                    delay_ms = delay_ms.saturating_mul(multiplier);
                    if delay_ms > 5000 {
                        delay_ms = 5000;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
            }
        };

        let data = obj
            .body
            .collect()
            .await
            .map(|data| data.into_bytes().to_vec())
            .map_err(|e| format!("Failed to read S3 object body: {}", e))?;

        if let Some(pool) = &self.redis_pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| format!("Failed to get Redis connection: {}", e))?;
            cache_in_redis(&mut conn, key, &data, self.ttl).await?;
        }

        Ok(data.to_vec())
    }

    async fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<(), String> {
        if let Some(pool) = &self.redis_pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| format!("Failed to get Redis connection: {}", e))?;
            cache_in_redis(&mut conn, key, &data, self.ttl).await?;
        }

        // Infer content type from file extension
        let content_type = infer_content_type(key);

        // retry with exponential backoff (max 3 attempts, 100ms-5s delay)
        let max_retries = 3u32;
        let base = std::time::Duration::from_millis(100);
        let mut attempt: u32 = 0;
        let data_clone = data.clone();
        loop {
            let mut put_request = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(aws_sdk_s3::primitives::ByteStream::from(data_clone.clone()));

            if let Some(ct) = content_type {
                put_request = put_request.content_type(ct);
            }

            match put_request.send().await {
                Ok(_) => break,
                Err(e) => {
                    attempt += 1;
                    if attempt > max_retries {
                        return Err(format!("Failed to upload S3 object after retries: {}", e));
                    }
                    let multiplier = 1u64 << (attempt - 1);
                    let mut delay_ms = base.as_millis() as u64;
                    delay_ms = delay_ms.saturating_mul(multiplier);
                    if delay_ms > 5000 {
                        delay_ms = 5000;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }

        Ok(())
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>, String> {
        let mut files = Vec::new();

        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .into_paginator()
            .send();

        while let Some(page_result) = pages.next().await {
            let page = page_result.map_err(|e| format!("Failed to list S3 objects: {:?}", e))?;

            if let Some(contents) = page.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        files.push(key);
                    }
                }
            }
        }

        Ok(files)
    }
}

#[async_trait]
impl BaseClientHandler for AzureBlobClientHandler {
    async fn download_file(&self, key: &str) -> Result<Vec<u8>, String> {
        // Try to get from Redis cache first if pool exists
        if let Some(pool) = &self.redis_pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| format!("Failed to get Redis connection: {}", e))?;
            let cached: Option<Vec<u8>> =
                deadpool_redis::redis::AsyncCommands::get(&mut *conn, key)
                    .await
                    .map_err(|e| format!("Failed to read from Redis: {}", e))?;
            if let Some(data) = cached {
                return Ok(data);
            }
        }

        let now = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let version = "2020-04-08";
        let resource_path = format!(
            "/{}/{}",
            encode_path_segments(&self.bucket),
            encode_path_segments(key)
        );
        let url = format!(
            "https://{}.blob.core.windows.net{}",
            self.account_name, resource_path
        );

        let canonicalized_headers = format!("x-ms-date:{}\nx-ms-version:{}", now, version);
        let canonicalized_resource = format!(
            "/{}/{}",
            self.account_name,
            encode_path_segments(&format!("{}/{}", self.bucket, key))
        );

        let auth = self.sign_request(
            "GET",
            "", // Content-Length
            &canonicalized_headers,
            &canonicalized_resource,
        );

        let resp = self
            .client
            .get(&url)
            .header("x-ms-date", &now)
            .header("x-ms-version", version)
            .header("Authorization", auth)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        if !resp.status().is_success() {
            return Err(format!("Azure download failed: {}", resp.status()));
        }

        let data = resp.bytes().await.map_err(|e| e.to_string())?.to_vec();

        if let Some(pool) = &self.redis_pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| format!("Failed to get Redis connection: {}", e))?;
            cache_in_redis(&mut conn, key, &data, self.ttl).await?;
        }

        Ok(data.to_vec())
    }

    async fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<(), String> {
        if let Some(pool) = &self.redis_pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| format!("Failed to get Redis connection: {}", e))?;
            cache_in_redis(&mut conn, key, &data, self.ttl).await?;
        }

        // Infer content type from file extension
        let content_type = infer_content_type(key);

        let now = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let version = "2020-04-08";
        let content_len = data.len().to_string();
        let resource_path = format!(
            "/{}/{}",
            encode_path_segments(&self.bucket),
            encode_path_segments(key)
        );
        let url = format!(
            "https://{}.blob.core.windows.net{}",
            self.account_name, resource_path
        );

        let canonicalized_headers = format!(
            "x-ms-blob-type:BlockBlob\nx-ms-date:{}\nx-ms-version:{}",
            now, version
        );
        let canonicalized_resource = format!(
            "/{}/{}",
            self.account_name,
            encode_path_segments(&format!("{}/{}", self.bucket, key))
        );

        let auth = self.sign_request(
            "PUT",
            &content_len,
            &canonicalized_headers,
            &canonicalized_resource,
        );

        let mut request = self
            .client
            .put(&url)
            .header("x-ms-date", &now)
            .header("x-ms-version", version)
            .header("x-ms-blob-type", "BlockBlob")
            .header("Authorization", auth);

        if let Some(ct) = content_type {
            request = request.header("Content-Type", ct);
        }

        let resp = request
            .body(reqwest::Body::from(data))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("Azure upload failed: {} - {}", text, url));
        }
        Ok(())
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>, String> {
        let now = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let version = "2020-04-08";
        let resource_path = format!("/{}", encode_path_segments(&self.bucket));

        // Build query params dynamically to handle optional prefix
        let mut query_params = vec![("comp", "list"), ("restype", "container")];
        if !prefix.is_empty() {
            query_params.push(("prefix", prefix));
        }
        // Sort params for both URL and CanonicalizedResource
        query_params.sort_by(|a, b| a.0.cmp(b.0));

        // Construct URL
        let mut url = format!(
            "https://{}.blob.core.windows.net{}?",
            self.account_name, resource_path
        );
        for (i, (k, v)) in query_params.iter().enumerate() {
            if i > 0 {
                url.push('&');
            }
            url.push_str(&format!("{}={}", k, v));
        }

        let canonicalized_headers = format!("x-ms-date:{}\nx-ms-version:{}", now, version);
        let mut canonicalized_resource = format!(
            "/{}/{}",
            self.account_name,
            encode_path_segments(&self.bucket)
        );
        for (k, v) in &query_params {
            canonicalized_resource.push_str(&format!("\n{}:{}", k, v));
        }

        let auth = self.sign_request("GET", "", &canonicalized_headers, &canonicalized_resource);

        let resp = self
            .client
            .get(&url)
            .header("x-ms-date", &now)
            .header("x-ms-version", version)
            .header("Authorization", auth)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Azure list failed: {} - {}", status, body));
        }

        let xml_resp = resp.text().await.map_err(|e| e.to_string())?;

        // Simple XML parsing using quick-xml
        use quick_xml::events::Event;
        use quick_xml::reader::Reader;

        let mut reader = Reader::from_str(&xml_resp);
        let mut buf = Vec::new();
        let mut files = Vec::new();
        let mut in_name = false;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) if e.name().as_ref() == b"Name" => in_name = true,
                Ok(Event::Text(e)) if in_name => {
                    // Directly convert bytes to string.
                    // Note: This assumes filenames don't contain XML entities like &amp;
                    let text = String::from_utf8_lossy(&e).into_owned();
                    files.push(text);
                    in_name = false;
                }
                Ok(Event::End(ref e)) if e.name().as_ref() == b"Name" => in_name = false,
                Ok(Event::Eof) => break,
                Err(e) => {
                    return Err(format!("Error parsing XML response: {:?}", e,));
                }
                _ => {}
            }
            buf.clear();
        }

        Ok(files)
    }
}
