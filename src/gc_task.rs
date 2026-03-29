use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use object_store::ObjectStore;

use crate::cloud_store::CloudStore;
use crate::levels::LevelManifest;
use crate::sstable::SstId;

/// Configuration for background garbage collection.
#[derive(Clone, Debug)]
pub struct GcConfig {
	/// Whether background GC is enabled.
	/// Default: true
	pub enabled: bool,
	/// Interval between GC cycles in seconds.
	/// Default: 600 (10 minutes)
	pub interval_secs: u64,
	/// Number of most recent manifests to keep.
	/// Default: 10
	pub manifest_keep_count: usize,
	/// Minimum age in seconds (based on ULID timestamp) before an SST
	/// can be purged. Prevents deleting SSTs still being uploaded.
	/// Default: 3600 (1 hour)
	pub sst_min_age_secs: u64,
}

impl Default for GcConfig {
	fn default() -> Self {
		Self {
			enabled: true,
			interval_secs: 600,
			manifest_keep_count: 10,
			sst_min_age_secs: 3600,
		}
	}
}

/// Background GC task that periodically purges old manifests and orphaned SSTs.
pub(crate) struct GcTask {
	stop_flag: Arc<AtomicBool>,
	handle: Option<tokio::task::JoinHandle<()>>,
}

impl GcTask {
	/// Spawn the background GC task.
	///
	/// When branching is enabled, uses `purge_orphaned_ssts()` which scans
	/// all branch manifests. Otherwise, uses `purge_orphaned_ssts_simple()`
	/// which derives the live set from the in-memory `LevelManifest`.
	pub(crate) fn spawn(
		config: GcConfig,
		table_store: Arc<CloudStore>,
		level_manifest: Arc<RwLock<LevelManifest>>,
		object_store: Arc<dyn ObjectStore>,
		object_store_root: String,
		branching_enabled: bool,
	) -> Self {
		let stop_flag = Arc::new(AtomicBool::new(false));
		let stop = Arc::clone(&stop_flag);

		let handle = tokio::spawn(async move {
			let mut interval = tokio::time::interval(Duration::from_secs(config.interval_secs));

			loop {
				interval.tick().await;

				if stop.load(Ordering::Relaxed) {
					break;
				}

				// 1. Purge old manifests
				match crate::gc::purge_old_manifests(&table_store, config.manifest_keep_count).await
				{
					Ok(n) if n > 0 => {
						log::info!("GC: purged {} old manifests", n);
					}
					Ok(_) => {}
					Err(e) => {
						log::warn!("GC: manifest purge failed: {e}");
					}
				}

				// 2. Purge orphaned SSTs
				if branching_enabled {
					// Branch-aware GC: scan all branch manifests
					let root_path: object_store::path::Path = object_store_root.as_str().into();
					match crate::branch::load_registry(object_store.as_ref(), &root_path).await {
						Ok(registry) => {
							if let Err(e) = crate::gc::purge_orphaned_ssts(
								&object_store,
								&object_store_root,
								&registry,
							)
							.await
							{
								log::warn!("GC: branch-aware SST purge failed: {e}");
							}
						}
						Err(e) => {
							log::warn!("GC: failed to load branch registry for purge: {e}");
						}
					}
				} else {
					// Simple GC: collect live set under lock, then release
					// before any async work.
					let live_ssts: HashSet<SstId> = match level_manifest.read() {
						Ok(guard) => guard.get_all_tables().keys().copied().collect(),
						Err(e) => {
							log::warn!("GC: failed to read level manifest: {e}");
							continue;
						}
					};
					if let Err(e) =
						crate::gc::purge_orphaned_ssts_simple(&table_store, &live_ssts).await
					{
						log::warn!("GC: simple SST purge failed: {e}");
					}
				}
			}
		});

		Self {
			stop_flag,
			handle: Some(handle),
		}
	}

	/// Signal the GC task to stop and wait for it to finish.
	pub(crate) async fn stop(&mut self) {
		self.stop_flag.store(true, Ordering::Relaxed);
		if let Some(handle) = self.handle.take() {
			handle.abort();
			let _ = handle.await;
		}
	}
}
