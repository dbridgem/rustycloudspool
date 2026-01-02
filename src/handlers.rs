use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_config::Region;
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{Config as RedisConfig, Pool as RedisPool, Runtime as RedisRuntime};

const MAX_CACHE_BYTES: usize = 1_048_576;

#[async_trait]
pub trait BaseClientHandler: Send + Sync {
    async fn download_file(&self, key: &str) -> Result<Vec<u8>, String>;
    async fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<(), String>;
    async fn list_files(&self, prefix: &str) -> Result<Vec<String>, String>;
}

pub struct S3ClientHandler {
    bucket: String,
    region: String,
    client: aws_sdk_s3::Client,
    redis_pool: RedisPool,
    ttl: u64,
}

impl S3ClientHandler {
    pub async fn new(bucket: String, region: String, ttl: &u64) -> Self {
        let behavior_version = BehaviorVersion::latest();
        let region_provider = RegionProviderChain::first_try(Some(Region::new(region.clone())));
        let config = aws_config::defaults(behavior_version)
            .region(region_provider)
            .load()
            .await;
        let client = aws_sdk_s3::Client::new(&config);

        let mut redis_cfg = RedisConfig::from_url("redis://127.0.0.1:6379");
        // tune pool size for your workload (start with 50-200 depending on hardware)
        redis_cfg.pool.get_or_insert(Default::default()).max_size = 100;
        let redis_pool = redis_cfg.create_pool(Some(RedisRuntime::Tokio1)).unwrap();

        S3ClientHandler {
            bucket,
            region,
            client,
            redis_pool,
            ttl: *ttl,
        }
    }

    async fn cache_in_redis(
        &self,
        conn: &mut deadpool_redis::Connection,
        key: &str,
        data: &[u8],
    ) -> Result<(), String> {
        if data.len() <= MAX_CACHE_BYTES {
            conn.set_ex(key, data, self.ttl)
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
}

#[async_trait]
impl BaseClientHandler for S3ClientHandler {
    async fn download_file(&self, key: &str) -> Result<Vec<u8>, String> {
        // Try to get from Redis cache first
        let mut conn = self
            .redis_pool
            .get()
            .await
            .map_err(|e| format!("Failed to get Redis connection: {}", e))?;
        let cached: Option<Vec<u8>> = deadpool_redis::redis::AsyncCommands::get(&mut *conn, key)
            .await
            .map_err(|e| format!("Failed to read from Redis: {}", e))?;
        if let Some(data) = cached {
            return Ok(data);
        }

        // If not cached, fetch from S3
        let max_retries = 3u32;
        let base = std::time::Duration::from_millis(100);
        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
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
                        return Ok(b"file not found".to_vec());
                    }

                    attempt += 1;
                    last_err = Some(format!("{}", e));
                    if attempt > max_retries {
                        return Err(format!(
                            "Failed to download S3 object after retries: {}",
                            last_err.unwrap()
                        ));
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

        self.cache_in_redis(&mut conn, key, &data).await?;

        Ok(data)
    }

    async fn upload_file(&self, key: &str, data: Vec<u8>) -> Result<(), String> {
        let mut conn = self
            .redis_pool
            .get()
            .await
            .map_err(|e| format!("Failed to get Redis connection: {}", e))?;

        self.cache_in_redis(&mut conn, key, &data).await?;

        // retry with exponential backoff (max 3 attempts, 100ms-5s delay)
        let max_retries = 3u32;
        let base = std::time::Duration::from_millis(100);
        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        let data_clone = data.clone();
        loop {
            match self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(aws_sdk_s3::primitives::ByteStream::from(data_clone.clone()))
                .send()
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    attempt += 1;
                    last_err = Some(format!("{}", e));
                    if attempt > max_retries {
                        return Err(format!(
                            "Failed to upload S3 object after retries: {}",
                            last_err.unwrap()
                        ));
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
