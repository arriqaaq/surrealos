//! Test infrastructure for cloud/object-store E2E tests.
//!
//! Provides helpers for creating LocalFileSystem-backed and S3-backed
//! CloudStore instances for integration testing.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cloud_store::CloudStore;
use crate::paths::StorePaths;

/// Creates a LocalFileSystem-backed CloudStore for E2E testing.
/// Uses the given `base_path` as root — no external services needed.
/// `local_sst_dir` controls whether local SST caching is tested.
pub(crate) fn create_local_table_store(
	base_path: &Path,
	local_sst_dir: Option<PathBuf>,
) -> (Arc<dyn object_store::ObjectStore>, Arc<CloudStore>) {
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(
		object_store::local::LocalFileSystem::new_with_prefix(base_path)
			.expect("failed to create LocalFileSystem"),
	);
	let path_resolver = StorePaths::new("");
	let ts = Arc::new(CloudStore::new(Arc::clone(&store), path_resolver, local_sst_dir));
	(store, ts)
}

// =============================================================================
// S3 support (only compiled with cloud-tests feature)
// =============================================================================

/// Configuration for S3-compatible cloud tests (e.g., MinIO).
#[cfg(feature = "cloud-tests")]
pub(crate) struct CloudTestConfig {
	pub bucket: String,
	pub region: String,
	pub endpoint: Option<String>,
	pub access_key: Option<String>,
	pub secret_key: Option<String>,
}

#[cfg(feature = "cloud-tests")]
impl CloudTestConfig {
	/// Read configuration from environment variables.
	/// Returns `None` if `SURREALKV_TEST_S3_BUCKET` is not set.
	pub fn from_env() -> Option<Self> {
		let bucket = std::env::var("SURREALKV_TEST_S3_BUCKET").ok()?;
		Some(Self {
			bucket,
			region: std::env::var("SURREALKV_TEST_S3_REGION")
				.unwrap_or_else(|_| "us-east-1".into()),
			endpoint: std::env::var("SURREALKV_TEST_S3_ENDPOINT").ok(),
			access_key: std::env::var("SURREALKV_TEST_S3_ACCESS_KEY").ok(),
			secret_key: std::env::var("SURREALKV_TEST_S3_SECRET_KEY").ok(),
		})
	}
}

/// Creates an S3-backed CloudStore for testing against MinIO or real S3.
/// Each test gets a unique prefix to avoid collisions.
/// Returns `None` if env vars are not configured.
#[cfg(feature = "cloud-tests")]
pub(crate) fn create_s3_table_store(
	prefix: &str,
	local_sst_dir: Option<PathBuf>,
) -> Option<(Arc<dyn object_store::ObjectStore>, Arc<CloudStore>)> {
	let config = CloudTestConfig::from_env()?;

	let mut builder = object_store::aws::AmazonS3Builder::new()
		.with_bucket_name(&config.bucket)
		.with_region(&config.region)
		.with_allow_http(true);

	if let Some(ref endpoint) = config.endpoint {
		builder = builder.with_endpoint(endpoint);
	}
	if let Some(ref key) = config.access_key {
		builder = builder.with_access_key_id(key);
	}
	if let Some(ref secret) = config.secret_key {
		builder = builder.with_secret_access_key(secret);
	}

	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(builder.build().expect("failed to build S3 store"));
	let path_resolver = StorePaths::new(prefix);
	let ts = Arc::new(CloudStore::new(Arc::clone(&store), path_resolver, local_sst_dir));
	Some((store, ts))
}

/// Cleanup helper: delete all objects under a prefix in the object store.
#[cfg(feature = "cloud-tests")]
pub(crate) async fn cleanup_prefix(
	store: &Arc<dyn object_store::ObjectStore>,
	prefix: &str,
) -> crate::Result<()> {
	use object_store::ObjectStore;
	let prefix_path = object_store::path::Path::from(prefix);
	let list_result = store.list_with_delimiter(Some(&prefix_path)).await?;
	for obj in list_result.objects {
		let _ = store.delete(&obj.location).await;
	}
	Ok(())
}

/// Macro to skip S3 tests when env vars are not configured.
#[cfg(feature = "cloud-tests")]
macro_rules! require_s3_config {
	() => {
		match $crate::test::cloud_test_helpers::CloudTestConfig::from_env() {
			Some(c) => c,
			None => {
				eprintln!("Skipping: SURREALKV_TEST_S3_BUCKET not set");
				return;
			}
		}
	};
}

#[cfg(feature = "cloud-tests")]
pub(crate) use require_s3_config;
