mod handlers;

pub mod core;
pub use core::RustyCloudSpoolCore;

#[cfg(feature = "python-api")]
mod python_api {
    use crate::core::get_runtime;
    use crate::core::RustyCloudSpoolCore;
    use pyo3::prelude::*;
    use std::collections::HashMap;

    #[pyclass]
    pub struct RustyCloudSpool {
        inner: RustyCloudSpoolCore,
    }

    #[pymethods]
    impl RustyCloudSpool {
        #[new]
        #[pyo3(signature = (
            provider, 
            bucket, 
            azure_connection_string="", 
            region="us-east-1",
            redis_url = "", 
            ttl=0
        ))]
        pub fn new(
            provider: String,
            bucket: String,
            azure_connection_string: String,
            region: String,
            redis_url: String,
            ttl: u64,
        ) -> Self {
            RustyCloudSpool {
                inner: RustyCloudSpoolCore::new(
                    provider, 
                    bucket, 
                    azure_connection_string, 
                    region, 
                    redis_url, 
                    ttl),
            }
        }

        pub fn download_files(&self, keys: Vec<String>) -> PyResult<HashMap<String, Vec<u8>>> {
            self.inner
                .download_files(keys)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
        }

        pub fn upload_files(&self, files: HashMap<String, Vec<u8>>) -> PyResult<()> {
            self.inner
                .upload_files(files)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
        }

        pub fn list_files(&self, prefix: String) -> PyResult<Vec<String>> {
            self.inner
                .list_files(prefix)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
        }
    }

    #[pymodule]
    fn rustycloudspool(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<RustyCloudSpool>()?;
        let _ = get_runtime();
        Ok(())
    }
}
