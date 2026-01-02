# RustyCloudSpool

RustyCloudSpool is a high-performance cloud storage library written in Rust, with a Python extension powered by [PyO3](https://pyo3.rs/). It provides fast interactions with AWS S3, and is designed for extensibility to support Azure and GCP in the future.

## Features

- **Fast S3 file uploads, downloads, and listing**
- **Redis-based caching for improved performance**
- **Python bindings via PyO3**
- **Extensible architecture for future cloud providers**

## Installation

### Python (via pip)

```bash
pip install rustycloudspool
```

## Usage

### Python

```python
import rustycloudspool

spool = rustycloudspool.RustyCloudSpool(
    provider="aws",
    region="bucket-region",
    bucket="your-bucket",
    redis_url="redis://localhost:6379",
    ttl=60
)

files = spool.list_files("your/prefix/")
spool.upload_files({"your/file.txt": b"Hello, world!"})
downloaded = spool.download_files(["your/file.txt"])
```

## Integration Tests

Integration tests require AWS and a Redis instance.  
**Do not commit secrets.**  
Set environment variables or use a `.env` file:

```
TEST_BUCKET=your-bucket
TEST_REGION=us-east-2
TEST_REDIS_URL=redis://localhost:6379
```

## Contributing

Contributions are welcome!  
Open an issue or pull request for bug fixes, features, or cloud provider support.

## License

MIT