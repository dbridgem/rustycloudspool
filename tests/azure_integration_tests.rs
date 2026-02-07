use dotenv::dotenv;
use rustycloudspool::core::RustyCloudSpoolCore;

fn test_setup() -> RustyCloudSpoolCore {
    dotenv().ok();
    let bucket = std::env::var("AZURE_TEST_BUCKET").expect("AZURE_TEST_BUCKET must be set");
    let region = std::env::var("AZURE_TEST_REGION").unwrap_or("us-east-2".to_string());
    let redis_url = std::env::var("TEST_REDIS_URL").unwrap_or("redis://localhost:6379".to_string());
    let connection_string = std::env::var("AZURE_TEST_CONNECTION_STRING")
        .expect("AZURE_TEST_CONNECTION_STRING must be set");

    RustyCloudSpoolCore::new(
        "azure".to_string(),
        bucket.clone(),
        connection_string.clone(),
        region.clone(),
        redis_url.clone(),
        10,
    )
}

#[test]
fn test_list_files() {
    let spool: RustyCloudSpoolCore = test_setup();

    let prefix: String = "".to_string();
    let files: Vec<String> = spool.list_files(prefix).unwrap();
    assert!(!files.is_empty());

    let prefix = "2_file_test/".to_string();
    let files: Vec<String> = spool.list_files(prefix).unwrap();
    println!("Files with '2_file_test/' prefix: {:?}", files);
    assert_eq!(files.len(), 2);
}

#[test]
fn test_download_files() {
    let spool: RustyCloudSpoolCore = test_setup();

    let keys: Vec<String> = vec!["2_file_test/1 copy.txt".to_string()];
    let files = spool.download_files(keys).unwrap();
    assert_eq!(files.len(), 1);
    assert!(files.get("2_file_test/1 copy.txt").unwrap().is_empty());
}

#[test]
fn test_upload_files() {
    let spool: RustyCloudSpoolCore = test_setup();
    let mut files: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    files.insert(
        "upload_test/test_upload.txt".to_string(),
        b"This is a test upload file.".to_vec(),
    );
    spool.upload_files(files).unwrap();
}

#[test]
fn test_downloading_cached_file() {
    let spool: RustyCloudSpoolCore = test_setup();
    let keys: Vec<String> = vec!["2_file_test/1 copy.txt".to_string()];
    // First download to cache it
    let files = spool.download_files(keys.clone()).unwrap();
    assert_eq!(files.len(), 1);
    assert!(!files.get("2_file_test/1 copy.txt").unwrap().is_empty());
    // Download again to hit the cache
    let files_cached = spool.download_files(keys).unwrap();
    assert_eq!(files_cached.len(), 1);
    assert!(!files_cached
        .get("2_file_test/1 copy.txt")
        .unwrap()
        .is_empty());
}

#[test]
fn test_downloading_nonexistent_file() {
    let spool: RustyCloudSpoolCore = test_setup();
    let keys: Vec<String> = vec!["nonexistent_file.txt".to_string()];
    let result = spool.download_files(keys);
    //We should not get an error, but should get an empty map
    assert!(result.is_ok());
    let files = result.unwrap();
    assert!(!files.is_empty());
}
