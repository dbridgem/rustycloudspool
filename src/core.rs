use crate::handlers::{AzureBlobClientHandler, BaseClientHandler, S3ClientHandler};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create Tokio runtime"))
}

pub struct RustyCloudSpoolCore {
    pub provider: String,
    pub bucket: String,
    pub redis_url: String,
    pub ttl: u64,
    pub client_handler: Arc<dyn BaseClientHandler>,
}

impl RustyCloudSpoolCore {
    pub fn new(
        provider: String,
        bucket: String,
        azure_connection_string: String,
        region: String,
        redis_url: String,
        ttl: u64,
    ) -> Self {
        let client_handler: Arc<dyn BaseClientHandler> = match provider.as_str() {
            "aws" => {
                let runtime = get_runtime();
                let handler = runtime.block_on(async {
                    S3ClientHandler::new(bucket.clone(), region.clone(), &ttl).await
                });
                Arc::new(handler)
            }
            "gcp" => {
                panic!("GCP not implemented yet");
            }
            "azure" => {
                let runtime = get_runtime();
                let handler: AzureBlobClientHandler = runtime.block_on(async {
                    AzureBlobClientHandler::new(
                        bucket.clone(),
                        azure_connection_string.clone(),
                        &ttl,
                    )
                    .await
                });
                Arc::new(handler)
            }
            _ => {
                panic!("Unsupported cloud provider");
            }
        };

        RustyCloudSpoolCore {
            provider,
            bucket,
            redis_url,
            ttl,
            client_handler,
        }
    }

    pub fn download_files(&self, keys: Vec<String>) -> Result<HashMap<String, Vec<u8>>, String> {
        let runtime = get_runtime();
        let handler = Arc::clone(&self.client_handler);
        runtime.block_on(async {
            use futures::future::join_all;
            let tasks = keys.into_iter().map(|key| {
                let h = Arc::clone(&handler);
                async move { h.download_file(&key).await.map(|data| (key, data)) }
            });
            let results = join_all(tasks).await;
            let mut map = HashMap::new();
            for res in results {
                match res {
                    Ok((k, data)) => {
                        map.insert(k, data);
                    }
                    Err(e) => return Err(e),
                }
            }
            // Remove any empty files (not found)
            map.retain(|_, v| !v.is_empty());
            Ok(map)
        })
    }

    pub fn upload_files(&self, files: HashMap<String, Vec<u8>>) -> Result<(), String> {
        let runtime = get_runtime();
        let handler = Arc::clone(&self.client_handler);
        runtime.block_on(async {
            let mut futs = FuturesUnordered::new();
            for (key, data) in files {
                let h = Arc::clone(&handler);
                futs.push(async move { h.upload_file(&key, data).await });
            }
            while let Some(res) = futs.next().await {
                res?;
            }
            Ok(())
        })
    }

    pub fn list_files(&self, prefix: String) -> Result<Vec<String>, String> {
        let runtime = get_runtime();
        runtime.block_on(async { self.client_handler.list_files(&prefix).await })
    }
}
