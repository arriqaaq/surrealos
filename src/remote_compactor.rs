use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::cloud_store::CloudStore;
use crate::compaction::compactor::{CompactionOptions, Compactor};
use crate::compaction::leveled::Strategy;
use crate::compaction::{CompactionChoice, CompactionStrategy};
use crate::epoch::EpochManifest;
use crate::error::{BackgroundErrorHandler, Result};
use crate::manifest::{load_from_bytes, ManifestUploader};
use crate::memtable::ImmutableMemtables;
use crate::paths::StorePaths;
use crate::snapshot::SnapshotTracker;
use crate::Options;

/// Configuration for the remote compactor process.
#[derive(Clone, Debug)]
pub struct CompactorConfig {
	/// How often to poll the manifest for new work. Default: 1 second.
	pub poll_interval: Duration,
	/// Local directory for temporary compaction I/O (manifest files, SST writes).
	pub local_work_dir: PathBuf,
}

impl Default for CompactorConfig {
	fn default() -> Self {
		Self {
			poll_interval: Duration::from_secs(1),
			local_work_dir: std::env::temp_dir().join("surrealos-compactor"),
		}
	}
}

/// Controls whether compaction runs in-process or is handled by an external process.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum CompactionMode {
	/// Default: the writer process runs both memtable flushes and level compaction.
	#[default]
	InProcess,
	/// The writer process only flushes memtables to L0.
	/// An external `RemoteCompactor` handles L1+ compaction via the object store.
	Remote,
}

/// A standalone compactor that runs compaction independently via object store.
///
/// The remote compactor is completely decoupled from the writer process:
/// - It polls the manifest from the object store to discover new L0 SSTs.
/// - It claims a compactor epoch via CAS for fencing.
/// - It runs the leveled compaction strategy and merges SSTs.
/// - It updates the manifest via CAS with retry on conflict.
///
/// The writer has zero knowledge that the compactor exists.
pub struct RemoteCompactor {
	object_store: Arc<dyn object_store::ObjectStore>,
	root: String,
	config: CompactorConfig,
	opts: Arc<Options>,
}

impl RemoteCompactor {
	/// Create a new remote compactor.
	///
	/// # Arguments
	/// * `object_store` - The object store containing SSTs and manifests.
	/// * `root` - Root path prefix in the object store.
	/// * `config` - Compactor configuration (poll interval, work directory).
	/// * `opts` - LSM options (must match the writer's configuration for block size, compression,
	///   level count, etc.).
	pub fn new(
		object_store: Arc<dyn object_store::ObjectStore>,
		root: impl Into<String>,
		config: CompactorConfig,
		opts: Options,
	) -> Self {
		Self {
			object_store,
			root: root.into(),
			config,
			opts: Arc::new(opts),
		}
	}

	/// Run the compaction loop until a shutdown signal is received.
	///
	/// This method:
	/// 1. Creates a local CloudStore for object store I/O
	/// 2. Claims the compactor epoch via CAS fencing
	/// 3. Polls the manifest, checks fencing, runs compaction strategy
	/// 4. On manifest conflict: refreshes and retries
	///
	/// # Arguments
	/// * `shutdown` - A watch channel receiver. When the value changes, the loop exits.
	pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
		// Ensure local work directory exists
		std::fs::create_dir_all(&self.config.local_work_dir)?;
		let manifest_dir = self.config.local_work_dir.join("manifest");
		std::fs::create_dir_all(&manifest_dir)?;

		// Wrap the object store with retry logic
		let object_store: Arc<dyn object_store::ObjectStore> =
			Arc::new(crate::retrying_store::RetryingObjectStore::new(
				Arc::clone(&self.object_store),
				self.opts.retry_config.clone(),
			));

		// Create local CloudStore for object store I/O
		let local_sst_dir = self.config.local_work_dir.join("sst");
		std::fs::create_dir_all(&local_sst_dir)?;
		let table_store = Arc::new(CloudStore::new(
			Arc::clone(&object_store),
			StorePaths::new_with_branch(self.root.as_str(), self.opts.branch_name.clone()),
			Some(local_sst_dir),
		));

		// Read latest manifest from object store
		let (_, manifest_bytes) = table_store.read_latest_manifest().await?.ok_or_else(|| {
			crate::error::Error::Other("No manifest found in object store".into())
		})?;

		let mut manifest = load_from_bytes(
			&manifest_bytes,
			manifest_dir.clone(),
			Arc::clone(&self.opts),
			Arc::clone(&table_store),
		)
		.await?;

		// Claim compactor epoch via CAS
		let mut fencing = EpochManifest::new_unfenced();
		fencing
			.claim_compactor_epoch(
				&mut manifest,
				&table_store,
				&self.opts,
				self.opts.manifest_update_timeout,
			)
			.await?;

		let local_compactor_epoch = fencing.compactor_epoch();

		log::info!(
			"Remote compactor started: epoch={}, poll_interval={:?}",
			local_compactor_epoch,
			self.config.poll_interval,
		);

		// Compaction loop
		loop {
			// Wait for poll interval or shutdown signal
			tokio::select! {
				result = shutdown.changed() => {
					if result.is_ok() {
						log::info!("Remote compactor received shutdown signal");
					}
					break;
				}
				_ = tokio::time::sleep(self.config.poll_interval) => {}
			}

			// Refresh manifest from object store
			let (_, bytes) = match table_store.read_latest_manifest().await? {
				Some(m) => m,
				None => {
					log::warn!("No manifest found during poll, skipping cycle");
					continue;
				}
			};

			let manifest = match load_from_bytes(
				&bytes,
				manifest_dir.clone(),
				Arc::clone(&self.opts),
				Arc::clone(&table_store),
			)
			.await
			{
				Ok(m) => m,
				Err(e) => {
					log::error!("Failed to load manifest: {e}");
					continue;
				}
			};

			// Check if we've been fenced by a newer compactor
			if manifest.compactor_epoch > local_compactor_epoch {
				log::error!(
					"Compactor fenced: local_epoch={}, manifest_epoch={}",
					local_compactor_epoch,
					manifest.compactor_epoch,
				);
				return Err(crate::error::Error::Fenced);
			}

			// Run leveled compaction strategy
			let strategy: Arc<dyn CompactionStrategy> =
				Arc::new(Strategy::from_options(Arc::clone(&self.opts)));
			let manifest_lock = Arc::new(RwLock::new(manifest));

			let choice = {
				let levels = manifest_lock.write().map_err(|e| {
					crate::error::Error::Other(format!("Failed to acquire manifest lock: {e}"))
				})?;
				strategy.pick_levels(&levels)?
			};

			// Execute compaction if needed
			match choice {
				CompactionChoice::Merge(_) => {
					log::info!("Remote compactor: compaction needed, executing merge");

					// Create a manifest uploader for the compactor
					let (manifest_tx, mut manifest_rx) =
						tokio::sync::watch::channel::<Option<(u64, bytes::Bytes)>>(None);
					let manifest_uploader = Arc::new(ManifestUploader::new(manifest_tx));

					// Spawn background manifest upload task
					let ts = Arc::clone(&table_store);
					let upload_handle = tokio::spawn(async move {
						loop {
							if manifest_rx.changed().await.is_err() {
								break;
							}
							let entry = manifest_rx.borrow_and_update().clone();
							if let Some((id, data)) = entry {
								match ts.write_manifest_if_absent(id, data).await {
									Ok(()) => {}
									Err(crate::error::Error::ManifestVersionExists) => {
										log::info!(
											"Manifest {} already exists (expected during concurrent updates)",
											id
										);
									}
									Err(e) => {
										log::warn!("Failed to upload manifest {}: {e}", id);
									}
								}
							}
						}
					});

					let comp_opts = CompactionOptions {
						lopts: Arc::clone(&self.opts),
						level_manifest: Arc::clone(&manifest_lock),
						immutable_memtables: Arc::new(RwLock::new(ImmutableMemtables::default())),
						error_handler: Arc::new(BackgroundErrorHandler::new()),
						snapshot_tracker: SnapshotTracker::new(),
						table_store: Arc::clone(&table_store),
						manifest_uploader,
						local_compactor_epoch,
						branching_enabled: self.opts.branch_name.is_some(),
						db_stats: Arc::new(crate::metrics::DbStats::new()),
					};

					let compactor = Compactor::new(comp_opts, Arc::clone(&strategy));
					match compactor.compact().await {
						Ok(()) => {
							log::info!("Remote compactor: compaction completed successfully");
						}
						Err(crate::error::Error::Fenced) => {
							log::error!("Remote compactor fenced during compaction");
							upload_handle.abort();
							return Err(crate::error::Error::Fenced);
						}
						Err(crate::error::Error::ManifestVersionExists) => {
							// CAS conflict — another writer updated the manifest.
							// Next loop iteration will refresh and retry.
							log::info!(
								"Remote compactor: manifest conflict, will retry next cycle"
							);
						}
						Err(e) => {
							log::error!("Remote compactor: compaction error: {e}");
							// Continue loop — transient errors may resolve
						}
					}

					// Drop the upload handle (manifest uploader sender will be dropped
					// when comp_opts goes out of scope, which stops the upload task)
					upload_handle.abort();
				}
				CompactionChoice::Skip => {
					log::debug!("Remote compactor: no compaction needed");
				}
			}
		}

		log::info!("Remote compactor shut down cleanly");
		Ok(())
	}
}
