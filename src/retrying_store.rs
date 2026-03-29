use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
	Attribute,
	AttributeValue,
	GetOptions,
	GetResult,
	ListResult,
	MultipartUpload,
	ObjectMeta,
	ObjectStore,
	PutMode,
	PutMultipartOptions,
	PutOptions,
	PutPayload,
	PutResult,
};

/// Configuration for retry behavior on object store operations.
#[derive(Clone, Debug)]
pub struct RetryConfig {
	/// Minimum delay between retries in milliseconds. Default: 100
	pub min_delay_ms: u64,
	/// Maximum delay between retries in milliseconds. Default: 1000
	pub max_delay_ms: u64,
	/// Multiplicative factor for exponential backoff. Default: 2.0
	pub factor: f32,
	/// Whether to add random jitter to delays. Default: true
	pub jitter: bool,
}

impl Default for RetryConfig {
	fn default() -> Self {
		Self {
			min_delay_ms: 100,
			max_delay_ms: 1_000,
			factor: 2.0,
			jitter: true,
		}
	}
}

/// The attribute key used to store a ULID for conditional put verification.
const PUT_ID_ATTR_KEY: &str = "surrealoputid";

/// A wrapper around an `ObjectStore` that adds retry logic with exponential backoff.
///
/// Non-retryable errors (`AlreadyExists`, `NotFound`, `NotImplemented`, `NotSupported`,
/// `Precondition`) are returned immediately. All other errors (transient network errors,
/// timeouts, server errors) are retried with exponential backoff and no max retry limit.
///
/// For conditional puts (`PutMode::Create`), this wrapper implements ULID-based
/// verification to handle the case where a write succeeds on the server but the client
/// times out before receiving the response.
pub(crate) struct RetryingObjectStore {
	inner: Arc<dyn ObjectStore>,
	config: RetryConfig,
}

impl RetryingObjectStore {
	pub(crate) fn new(inner: Arc<dyn ObjectStore>, config: RetryConfig) -> Self {
		Self {
			inner,
			config,
		}
	}

	fn backoff_builder(&self) -> ExponentialBuilder {
		let builder = ExponentialBuilder::new()
			.with_min_delay(Duration::from_millis(self.config.min_delay_ms))
			.with_max_delay(Duration::from_millis(self.config.max_delay_ms))
			.with_factor(self.config.factor)
			.without_max_times();
		if self.config.jitter {
			builder.with_jitter()
		} else {
			builder
		}
	}
}

/// Returns true if an object_store error should be retried.
///
/// Non-retryable: `AlreadyExists`, `NotFound`, `NotImplemented`, `NotSupported`, `Precondition`
/// Retryable: everything else (Generic, network, timeout, 429, 5xx, etc.)
fn is_retryable(err: &object_store::Error) -> bool {
	!matches!(
		err,
		object_store::Error::AlreadyExists { .. }
			| object_store::Error::NotFound { .. }
			| object_store::Error::NotImplemented
			| object_store::Error::NotSupported { .. }
			| object_store::Error::Precondition { .. }
	)
}

impl fmt::Display for RetryingObjectStore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "RetryingObjectStore({})", self.inner)
	}
}

impl fmt::Debug for RetryingObjectStore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RetryingObjectStore")
			.field("inner", &self.inner)
			.field("config", &self.config)
			.finish()
	}
}

#[async_trait]
impl ObjectStore for RetryingObjectStore {
	async fn put_opts(
		&self,
		location: &Path,
		payload: PutPayload,
		opts: PutOptions,
	) -> object_store::Result<PutResult> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let location = location.clone();

		// For conditional puts (PutMode::Create), implement ULID verification:
		// 1. Generate a ULID and store it in the object's attributes
		// 2. On AlreadyExists error, HEAD the object and check if our ULID matches
		// 3. If match: our write succeeded (timeout-after-write), return Ok
		// 4. If no match: genuinely exists from another writer, return AlreadyExists
		if matches!(opts.mode, PutMode::Create) {
			let put_id = ulid::Ulid::new().to_string();
			let mut attrs = opts.attributes.clone();
			attrs.insert(
				Attribute::Metadata(PUT_ID_ATTR_KEY.into()),
				AttributeValue::from(put_id.clone()),
			);
			let opts = PutOptions {
				attributes: attrs,
				..opts
			};

			let payload_bytes: Bytes = payload.into();

			let result = (|| {
				let inner = Arc::clone(&inner);
				let location = location.clone();
				let opts = opts.clone();
				let payload = PutPayload::from(payload_bytes.clone());
				async move { inner.put_opts(&location, payload, opts).await }
			})
			.retry(backoff)
			.when(|e| is_retryable(e))
			.notify(|err, dur| {
				log::warn!(
					"Retrying put_opts (Create) to {}: {:?} (backoff {:?})",
					location,
					err,
					dur
				);
			})
			.await;

			match result {
				Ok(r) => Ok(r),
				Err(object_store::Error::AlreadyExists {
					path,
					source,
				}) => {
					// Verify if our write actually succeeded by checking the ULID
					match inner
						.get_opts(
							&location,
							GetOptions {
								head: true,
								..Default::default()
							},
						)
						.await
					{
						Ok(get_result) => {
							let stored_id = get_result
								.attributes
								.get(&Attribute::Metadata(PUT_ID_ATTR_KEY.into()));
							if stored_id.map(|v| v.as_ref()) == Some(put_id.as_str()) {
								// Our write succeeded, the AlreadyExists was from a
								// timeout-after-write retry
								Ok(PutResult {
									e_tag: get_result.meta.e_tag,
									version: get_result.meta.version,
								})
							} else {
								// Genuinely written by another writer
								Err(object_store::Error::AlreadyExists {
									path,
									source,
								})
							}
						}
						Err(_) => {
							// HEAD failed (e.g., attributes not supported) --
							// fall back to returning AlreadyExists
							Err(object_store::Error::AlreadyExists {
								path,
								source,
							})
						}
					}
				}
				Err(e) => Err(e),
			}
		} else {
			// Non-conditional put: simple retry
			let payload_bytes: Bytes = payload.into();
			(|| {
				let inner = Arc::clone(&inner);
				let location = location.clone();
				let opts = opts.clone();
				let payload = PutPayload::from(payload_bytes.clone());
				async move { inner.put_opts(&location, payload, opts).await }
			})
			.retry(backoff)
			.when(|e| is_retryable(e))
			.notify(|err, dur| {
				log::warn!("Retrying put_opts to {}: {:?} (backoff {:?})", location, err, dur);
			})
			.await
		}
	}

	async fn put_multipart_opts(
		&self,
		location: &Path,
		opts: PutMultipartOptions,
	) -> object_store::Result<Box<dyn MultipartUpload>> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let location = location.clone();

		(|| {
			let inner = Arc::clone(&inner);
			let location = location.clone();
			let opts = opts.clone();
			async move { inner.put_multipart_opts(&location, opts).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!(
				"Retrying put_multipart_opts to {}: {:?} (backoff {:?})",
				location,
				err,
				dur
			);
		})
		.await
	}

	async fn get_opts(
		&self,
		location: &Path,
		options: GetOptions,
	) -> object_store::Result<GetResult> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let location = location.clone();

		(|| {
			let inner = Arc::clone(&inner);
			let location = location.clone();
			let options = options.clone();
			async move { inner.get_opts(&location, options).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!("Retrying get_opts for {}: {:?} (backoff {:?})", location, err, dur);
		})
		.await
	}

	async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let location = location.clone();

		(|| {
			let inner = Arc::clone(&inner);
			let location = location.clone();
			let range = range.clone();
			async move { inner.get_range(&location, range).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!("Retrying get_range for {}: {:?} (backoff {:?})", location, err, dur);
		})
		.await
	}

	async fn get_ranges(
		&self,
		location: &Path,
		ranges: &[Range<u64>],
	) -> object_store::Result<Vec<Bytes>> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let location = location.clone();
		let ranges = ranges.to_vec();

		(|| {
			let inner = Arc::clone(&inner);
			let location = location.clone();
			let ranges = ranges.clone();
			async move { inner.get_ranges(&location, &ranges).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!("Retrying get_ranges for {}: {:?} (backoff {:?})", location, err, dur);
		})
		.await
	}

	async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let location = location.clone();

		(|| {
			let inner = Arc::clone(&inner);
			let location = location.clone();
			async move { inner.head(&location).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!("Retrying head for {}: {:?} (backoff {:?})", location, err, dur);
		})
		.await
	}

	async fn delete(&self, location: &Path) -> object_store::Result<()> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let location = location.clone();

		(|| {
			let inner = Arc::clone(&inner);
			let location = location.clone();
			async move { inner.delete(&location).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!("Retrying delete for {}: {:?} (backoff {:?})", location, err, dur);
		})
		.await
	}

	fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
		// list() returns a stream -- we delegate directly since retrying individual
		// stream items is not straightforward and the caller typically retries the
		// entire list operation on error.
		self.inner.list(prefix)
	}

	async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let prefix = prefix.cloned();

		(|| {
			let inner = Arc::clone(&inner);
			let prefix = prefix.clone();
			async move { inner.list_with_delimiter(prefix.as_ref()).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!("Retrying list_with_delimiter: {:?} (backoff {:?})", err, dur);
		})
		.await
	}

	async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let from = from.clone();
		let to = to.clone();

		(|| {
			let inner = Arc::clone(&inner);
			let from = from.clone();
			let to = to.clone();
			async move { inner.copy(&from, &to).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!("Retrying copy: {:?} (backoff {:?})", err, dur);
		})
		.await
	}

	async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let from = from.clone();
		let to = to.clone();

		(|| {
			let inner = Arc::clone(&inner);
			let from = from.clone();
			let to = to.clone();
			async move { inner.rename(&from, &to).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!("Retrying rename: {:?} (backoff {:?})", err, dur);
		})
		.await
	}

	async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let from = from.clone();
		let to = to.clone();

		(|| {
			let inner = Arc::clone(&inner);
			let from = from.clone();
			let to = to.clone();
			async move { inner.copy_if_not_exists(&from, &to).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!("Retrying copy_if_not_exists: {:?} (backoff {:?})", err, dur);
		})
		.await
	}

	async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
		let inner = Arc::clone(&self.inner);
		let backoff = self.backoff_builder();
		let from = from.clone();
		let to = to.clone();

		(|| {
			let inner = Arc::clone(&inner);
			let from = from.clone();
			let to = to.clone();
			async move { inner.rename_if_not_exists(&from, &to).await }
		})
		.retry(backoff)
		.when(|e| is_retryable(e))
		.notify(|err, dur| {
			log::warn!("Retrying rename_if_not_exists: {:?} (backoff {:?})", err, dur);
		})
		.await
	}
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::Arc;

	use bytes::Bytes;
	use futures::stream::BoxStream;
	use object_store::path::Path;
	use object_store::{
		GetOptions,
		GetResult,
		ListResult,
		MultipartUpload,
		ObjectMeta,
		ObjectStore,
		PutMultipartOptions,
		PutOptions,
		PutPayload,
		PutResult,
	};

	use super::*;

	/// A mock ObjectStore that fails N times with a retryable error, then delegates to an
	/// inner store.
	#[derive(Debug)]
	struct FailingObjectStore {
		inner: Arc<dyn ObjectStore>,
		/// Number of failures remaining for each operation call.
		remaining_failures: AtomicUsize,
		/// The error to return on failure (retryable by default).
		error_fn: fn() -> object_store::Error,
	}

	impl FailingObjectStore {
		fn new(inner: Arc<dyn ObjectStore>, fail_count: usize) -> Self {
			Self {
				inner,
				remaining_failures: AtomicUsize::new(fail_count),
				error_fn: || object_store::Error::Generic {
					store: "test",
					source: "transient error".into(),
				},
			}
		}

		fn with_error(
			inner: Arc<dyn ObjectStore>,
			fail_count: usize,
			error_fn: fn() -> object_store::Error,
		) -> Self {
			Self {
				inner,
				remaining_failures: AtomicUsize::new(fail_count),
				error_fn,
			}
		}

		fn maybe_fail(&self) -> Result<(), object_store::Error> {
			let remaining = self.remaining_failures.load(Ordering::SeqCst);
			if remaining > 0 {
				self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
				Err((self.error_fn)())
			} else {
				Ok(())
			}
		}
	}

	impl fmt::Display for FailingObjectStore {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			write!(f, "FailingObjectStore({})", self.inner)
		}
	}

	#[async_trait]
	impl ObjectStore for FailingObjectStore {
		async fn put_opts(
			&self,
			location: &Path,
			payload: PutPayload,
			opts: PutOptions,
		) -> object_store::Result<PutResult> {
			self.maybe_fail()?;
			self.inner.put_opts(location, payload, opts).await
		}

		async fn put_multipart_opts(
			&self,
			location: &Path,
			opts: PutMultipartOptions,
		) -> object_store::Result<Box<dyn MultipartUpload>> {
			self.maybe_fail()?;
			self.inner.put_multipart_opts(location, opts).await
		}

		async fn get_opts(
			&self,
			location: &Path,
			options: GetOptions,
		) -> object_store::Result<GetResult> {
			self.maybe_fail()?;
			self.inner.get_opts(location, options).await
		}

		async fn delete(&self, location: &Path) -> object_store::Result<()> {
			self.maybe_fail()?;
			self.inner.delete(location).await
		}

		fn list(
			&self,
			prefix: Option<&Path>,
		) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
			self.inner.list(prefix)
		}

		async fn list_with_delimiter(
			&self,
			prefix: Option<&Path>,
		) -> object_store::Result<ListResult> {
			self.maybe_fail()?;
			self.inner.list_with_delimiter(prefix).await
		}

		async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
			self.maybe_fail()?;
			self.inner.copy_if_not_exists(from, to).await
		}

		async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
			self.maybe_fail()?;
			self.inner.copy(from, to).await
		}
	}

	fn test_retry_config() -> RetryConfig {
		RetryConfig {
			min_delay_ms: 1,
			max_delay_ms: 5,
			factor: 2.0,
			jitter: false,
		}
	}

	#[tokio::test]
	async fn test_retries_transient_error_on_put() {
		let mem = Arc::new(object_store::memory::InMemory::new());
		let failing = Arc::new(FailingObjectStore::new(Arc::clone(&mem) as _, 3));
		let store = RetryingObjectStore::new(failing, test_retry_config());

		let path = Path::from("test/file.txt");
		let payload = PutPayload::from(Bytes::from("hello"));
		store
			.put_opts(&path, payload, PutOptions::default())
			.await
			.expect("should succeed after retries");

		// Verify the data was written
		let result = mem.get(&path).await.unwrap();
		assert_eq!(result.bytes().await.unwrap(), Bytes::from("hello"));
	}

	#[tokio::test]
	async fn test_retries_transient_error_on_get() {
		let mem = Arc::new(object_store::memory::InMemory::new());
		let path = Path::from("test/file.txt");
		mem.put(&path, PutPayload::from(Bytes::from("data"))).await.unwrap();

		let failing = Arc::new(FailingObjectStore::new(Arc::clone(&mem) as _, 2));
		let store = RetryingObjectStore::new(failing, test_retry_config());

		let result = store.get(&path).await.unwrap();
		assert_eq!(result.bytes().await.unwrap(), Bytes::from("data"));
	}

	#[tokio::test]
	async fn test_retries_transient_error_on_delete() {
		let mem = Arc::new(object_store::memory::InMemory::new());
		let path = Path::from("test/file.txt");
		mem.put(&path, PutPayload::from(Bytes::from("data"))).await.unwrap();

		let failing = Arc::new(FailingObjectStore::new(Arc::clone(&mem) as _, 2));
		let store = RetryingObjectStore::new(failing, test_retry_config());

		store.delete(&path).await.expect("should succeed after retries");

		// Verify deletion
		let err = mem.head(&path).await.unwrap_err();
		assert!(matches!(err, object_store::Error::NotFound { .. }));
	}

	#[tokio::test]
	async fn test_retries_transient_error_on_list_with_delimiter() {
		let mem = Arc::new(object_store::memory::InMemory::new());
		let path = Path::from("prefix/file.txt");
		mem.put(&path, PutPayload::from(Bytes::from("data"))).await.unwrap();

		let failing = Arc::new(FailingObjectStore::new(Arc::clone(&mem) as _, 2));
		let store = RetryingObjectStore::new(failing, test_retry_config());

		let prefix = Path::from("prefix");
		let result = store.list_with_delimiter(Some(&prefix)).await.unwrap();
		assert_eq!(result.objects.len(), 1);
	}

	#[tokio::test]
	async fn test_not_found_not_retried() {
		let mem = Arc::new(object_store::memory::InMemory::new());
		// FailingObjectStore with NotFound error -- should not be retried
		let failing = Arc::new(FailingObjectStore::with_error(
			Arc::clone(&mem) as _,
			100, // Would retry 100 times if retryable
			|| object_store::Error::NotFound {
				path: "test".to_string(),
				source: "not found".into(),
			},
		));
		let store = RetryingObjectStore::new(failing, test_retry_config());

		let path = Path::from("nonexistent");
		let result = store.get(&path).await;
		assert!(result.is_err());
		assert!(matches!(result.unwrap_err(), object_store::Error::NotFound { .. }));
	}

	#[tokio::test]
	async fn test_already_exists_not_retried_on_put() {
		let mem = Arc::new(object_store::memory::InMemory::new());
		let failing = Arc::new(FailingObjectStore::with_error(Arc::clone(&mem) as _, 100, || {
			object_store::Error::AlreadyExists {
				path: "test".to_string(),
				source: "already exists".into(),
			}
		}));
		let store = RetryingObjectStore::new(failing, test_retry_config());

		let path = Path::from("test");
		let opts = PutOptions::from(PutMode::Create);
		let result = store.put_opts(&path, PutPayload::from(Bytes::from("data")), opts).await;
		assert!(result.is_err());
		assert!(matches!(result.unwrap_err(), object_store::Error::AlreadyExists { .. }));
	}

	#[tokio::test]
	async fn test_retries_transient_error_on_head() {
		let mem = Arc::new(object_store::memory::InMemory::new());
		let path = Path::from("test/file.txt");
		mem.put(&path, PutPayload::from(Bytes::from("data"))).await.unwrap();

		let failing = Arc::new(FailingObjectStore::new(Arc::clone(&mem) as _, 2));
		let store = RetryingObjectStore::new(failing, test_retry_config());

		let meta = store.head(&path).await.unwrap();
		assert_eq!(meta.size, 4);
	}

	#[tokio::test]
	async fn test_retries_transient_error_on_get_range() {
		let mem = Arc::new(object_store::memory::InMemory::new());
		let path = Path::from("test/file.txt");
		mem.put(&path, PutPayload::from(Bytes::from("hello world"))).await.unwrap();

		let failing = Arc::new(FailingObjectStore::new(Arc::clone(&mem) as _, 2));
		let store = RetryingObjectStore::new(failing, test_retry_config());

		let data = store.get_range(&path, 0..5).await.unwrap();
		assert_eq!(data, Bytes::from("hello"));
	}

	#[tokio::test]
	async fn test_precondition_not_retried() {
		let mem = Arc::new(object_store::memory::InMemory::new());
		let failing = Arc::new(FailingObjectStore::with_error(Arc::clone(&mem) as _, 100, || {
			object_store::Error::Precondition {
				path: "test".to_string(),
				source: "precondition failed".into(),
			}
		}));
		let store = RetryingObjectStore::new(failing, test_retry_config());

		let path = Path::from("test");
		let result = store.get(&path).await;
		assert!(result.is_err());
		assert!(matches!(result.unwrap_err(), object_store::Error::Precondition { .. }));
	}
}
