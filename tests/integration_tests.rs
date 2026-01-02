use aws_config::default_provider::region;
use dotenv::dotenv;
use rustycloudspool::core::RustyCloudSpoolCore;

fn test_setup() -> RustyCloudSpoolCore {
    dotenv().ok();
    let bucket = std::env::var("TEST_BUCKET")
        .ok()
        .expect("TEST_BUCKET must be set");
    let region = std::env::var("TEST_REGION").unwrap_or("us-east-2".to_string());
    let redis_url = std::env::var("TEST_REDIS_URL").unwrap_or("redis://localhost:6379".to_string());
    let spool = RustyCloudSpoolCore::new(
        "aws".to_string(),
        region.clone(),
        bucket.clone(),
        redis_url.clone(),
        10,
    );
    return spool;
}

#[test]
fn test_list_files() {
    dotenv().ok();
    let spool: RustyCloudSpoolCore = test_setup();

    let prefix: String = "".to_string();
    let files: Vec<String> = spool.list_files(prefix).unwrap();
    assert!(files.len() > 0);

    let prefix = "demo/".to_string();
    let files: Vec<String> = spool.list_files(prefix).unwrap();
    println!("Files with 'demo/' prefix: {:?}", files);
    assert!(files.len() == 2);
}

#[test]
fn test_download_files() {
    dotenv().ok();
    let bucket = std::env::var("TEST_BUCKET")
        .ok()
        .expect("TEST_BUCKET must be set");
    let region = std::env::var("TEST_REGION").unwrap_or("us-east-2".to_string());

    let spool = RustyCloudSpoolCore::new(
        "aws".to_string(),
        region.clone(),
        bucket.clone(),
        "".to_string(),
        1,
    );

    let keys: Vec<String> = vec!["demo/main.rs".to_string()];
    let files = spool.download_files(keys).unwrap();
    assert_eq!(files.len(), 1);
    assert!(files.get("demo/main.rs").unwrap().len() > 0);
}

#[test]
fn test_upload_files() {
    dotenv().ok();
    let bucket = std::env::var("TEST_BUCKET")
        .ok()
        .expect("TEST_BUCKET must be set");
    let region = std::env::var("TEST_REGION").unwrap_or("us-east-2".to_string());

    let spool = RustyCloudSpoolCore::new(
        "aws".to_string(),
        region.clone(),
        bucket.clone(),
        "".to_string(),
        60,
    );
    let mut files: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    files.insert(
        "demo/test_upload.txt".to_string(),
        b"This is a test upload file.".to_vec(),
    );
    spool.upload_files(files).unwrap();
}

#[test]
fn test_downloading_cached_file() {
    let bucket = std::env::var("TEST_BUCKET")
        .ok()
        .expect("TEST_BUCKET must be set");
    let region = std::env::var("TEST_REGION").unwrap_or("us-east-2".to_string());

    let spool = RustyCloudSpoolCore::new(
        "aws".to_string(),
        region.clone(),
        bucket.clone(),
        "redis://localhost:6379".to_string(),
        10,
    );
    let keys: Vec<String> = vec!["demo/main.rs".to_string()];
    // First download to cache it
    let files = spool.download_files(keys.clone()).unwrap();
    assert_eq!(files.len(), 1);
    assert!(files.get("demo/main.rs").unwrap().len() > 0);

    // Download again to hit the cache
    let files_cached = spool.download_files(keys).unwrap();
    assert_eq!(files_cached.len(), 1);
    assert!(files_cached.get("demo/main.rs").unwrap().len() > 0);
}
