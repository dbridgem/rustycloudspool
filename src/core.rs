use crate::handlers::{BaseClientHandler, S3ClientHandler};
use std::collections::HashMap;
use std::sync::OnceLock;
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
    pub client_handler: Box<dyn BaseClientHandler>,
}

impl RustyCloudSpoolCore {
    pub fn new(
        provider: String,
        region: String,
        bucket: String,
        redis_url: String,
        ttl: u64,
    ) -> Self {
        let client_handler: Box<dyn BaseClientHandler> = match provider.as_str() {
            "aws" => {
                let runtime = get_runtime();
                let handler = runtime.block_on(async {
                    S3ClientHandler::new(bucket.clone(), region.clone(), &ttl).await
                });
                Box::new(handler)
            }
            "gcp" => {
                panic!("GCP not implemented yet");
            }
            "azure" => {
                panic!("Azure not implemented yet");
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
        runtime.block_on(async {
            use futures::future::join_all;
            let tasks = keys.iter().map(|key| {
                let k = key.clone();
                async move {
                    match self.client_handler.download_file(&k).await {
                        Ok(data) => Ok((k, data)),
                        Err(e) => Err(e),
                    }
                }
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
            Ok(map)
        })
    }

    pub fn upload_files(&self, files: HashMap<String, Vec<u8>>) -> Result<(), String> {
        let runtime = get_runtime();
        runtime.block_on(async {
            for (key, data) in files {
                self.client_handler
                    .upload_file(&key, data)
                    .await
                    .map_err(|e| e)?;
            }
            Ok(())
        })
    }

    pub fn list_files(&self, prefix: String) -> Result<Vec<String>, String> {
        let runtime = get_runtime();
        runtime.block_on(async { self.client_handler.list_files(&prefix).await })
    }
}
