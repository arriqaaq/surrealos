use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;

use crate::branch::{BranchRegistry, BranchStatus};
use crate::cloud_store::CloudStore;
use crate::error::Result;
use crate::levels::LevelManifest;
use crate::manifest::extract_sst_ids_from_manifest;
use crate::paths::StorePaths;
use crate::sstable::SstId;

/// Remove local SST files not referenced by the current manifest.
/// Called on startup to clean up after crashes or leader transitions.
/// Also removes leftover `.sst.tmp` files from interrupted atomic writes.
pub(crate) fn cleanup_local_orphans(sst_dir: &Path, manifest: &LevelManifest) -> usize {
	let referenced: HashSet<String> =
		manifest.get_all_tables().keys().map(|id| format!("{id}.sst")).collect();

	let entries = match std::fs::read_dir(sst_dir) {
		Ok(entries) => entries,
		Err(_) => return 0,
	};

	let mut deleted = 0;
	for entry in entries.flatten() {
		let name = entry.file_name();
		let name_str = name.to_string_lossy();

		// Clean up .tmp files from interrupted atomic writes
		if name_str.ends_with(".sst.tmp") {
			let _ = std::fs::remove_file(entry.path());
			deleted += 1;
			continue;
		}

		// Remove orphaned .sst files not referenced by manifest
		if name_str.ends_with(".sst") && !referenced.contains(name_str.as_ref()) {
			let _ = std::fs::remove_file(entry.path());
			deleted += 1;
		}
	}

	if deleted > 0 {
		log::info!("Cleaned up {} local orphaned SST files", deleted);
	}
	deleted
}

/// Collect SST IDs referenced by ALL branches.
///
/// Must be called before deleting any SST from object store.
/// For "active" branches, reads the branch's own manifest.
/// For "creating" branches, reads the parent's manifest at source_manifest_id
/// (branch manifest may not exist yet, but parent SSTs must be protected).
///
/// If any manifest read fails, the entire operation aborts (no deletions).
pub(crate) async fn collect_live_ssts(
	object_store: &Arc<dyn ObjectStore>,
	root: &str,
	registry: &BranchRegistry,
) -> Result<HashSet<SstId>> {
	let root_path: object_store::path::Path = root.into();
	let mut live = HashSet::new();

	// Also include the root (non-branched) manifest if it exists
	let root_resolver = StorePaths::new(root);
	let root_store = CloudStore::new(Arc::clone(object_store), root_resolver, None);
	if let Some((_id, manifest_bytes)) = root_store.read_latest_manifest().await? {
		let sst_ids = extract_sst_ids_from_manifest(&manifest_bytes)?;
		live.extend(sst_ids);
	}

	for (name, info) in registry.branches() {
		match info.status {
			BranchStatus::Active => {
				let resolver = StorePaths::new_with_branch(root_path.clone(), Some(name.clone()));
				let store = CloudStore::new(Arc::clone(object_store), resolver, None);
				if let Some((_id, manifest_bytes)) = store.read_latest_manifest().await? {
					let sst_ids = extract_sst_ids_from_manifest(&manifest_bytes)?;
					live.extend(sst_ids);
				}
			}
			BranchStatus::Creating => {
				// Branch manifest may not exist yet.
				// Read the PARENT's manifest at source_manifest_id to protect
				// the SSTs that will be inherited by this branch.
				let parent_name = info.parent_branch.clone();
				let parent_resolver = StorePaths::new_with_branch(root_path.clone(), parent_name);
				let parent_store = CloudStore::new(Arc::clone(object_store), parent_resolver, None);
				if let Ok(bytes) = parent_store.read_manifest(info.source_manifest_id).await {
					let sst_ids = extract_sst_ids_from_manifest(&bytes)?;
					live.extend(sst_ids);
				}
			}
		}
	}

	Ok(live)
}

/// Minimum age (from ULID timestamp) before an SST can be purged.
/// Prevents deleting SSTs that are still being uploaded during compaction
/// (between upload and manifest write).
const MIN_PURGE_AGE: Duration = Duration::from_secs(3600); // 1 hour

/// Extract creation timestamp from a ULID-based SstId.
fn sst_age(id: &SstId) -> Duration {
	let ts_ms = id.timestamp_ms();
	let now_ms = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis() as u64;
	Duration::from_millis(now_ms.saturating_sub(ts_ms))
}

/// Delete manifests older than the latest `keep_count`.
///
/// Lists all manifests (sorted ascending by ID), keeps the most recent
/// `keep_count`, and deletes the rest. Returns the number of manifests deleted.
pub(crate) async fn purge_old_manifests(store: &CloudStore, keep_count: usize) -> Result<usize> {
	let ids = store.list_manifest_ids().await?; // Already sorted ascending
	if ids.len() <= keep_count {
		return Ok(0);
	}
	let to_delete = &ids[..ids.len() - keep_count];
	let mut deleted = 0;
	for &id in to_delete {
		if let Err(e) = store.delete_manifest(id).await {
			log::warn!("Failed to delete manifest {}: {}", id, e);
		} else {
			deleted += 1;
		}
	}
	if deleted > 0 {
		log::info!("Purged {} old manifest files from object store", deleted);
	}
	Ok(deleted)
}

/// For non-branching mode: purge SSTs not in current manifest.
///
/// Collects the live SST set from the manifest (synchronously), then lists
/// all SSTs in the object store and deletes orphaned ones older than
/// `MIN_PURGE_AGE` (based on ULID timestamp).
///
/// The `live_ssts` parameter should be obtained from
/// `LevelManifest::get_all_tables().keys()` under a read lock before calling
/// this function, so the lock is not held across await points.
pub(crate) async fn purge_orphaned_ssts_simple(
	store: &CloudStore,
	live_ssts: &HashSet<SstId>,
) -> Result<usize> {
	let all_ssts = store.list_sst_ids().await?;
	let mut deleted = 0;
	for sst_id in all_ssts {
		if !live_ssts.contains(&sst_id) && sst_age(&sst_id) > MIN_PURGE_AGE {
			if let Err(e) = store.delete_sst(&sst_id).await {
				log::warn!("Failed to purge orphaned SST {}: {}", sst_id, e);
			} else {
				deleted += 1;
			}
		}
	}
	if deleted > 0 {
		log::info!("Purged {} orphaned SST files from object store (simple mode)", deleted);
	}
	Ok(deleted)
}

/// Delete SSTs from object store that are not referenced by any branch.
///
/// Only deletes SSTs older than MIN_PURGE_AGE to avoid racing with
/// in-progress compaction uploads.
pub(crate) async fn purge_orphaned_ssts(
	object_store: &Arc<dyn ObjectStore>,
	root: &str,
	registry: &BranchRegistry,
) -> Result<usize> {
	let live = collect_live_ssts(object_store, root, registry).await?;

	let root_resolver = StorePaths::new(root);
	let root_store = CloudStore::new(Arc::clone(object_store), root_resolver, None);
	let all_ssts = root_store.list_sst_ids().await?;

	let mut deleted = 0;
	for sst_id in all_ssts {
		if !live.contains(&sst_id) && sst_age(&sst_id) > MIN_PURGE_AGE {
			if let Err(e) = root_store.delete_sst(&sst_id).await {
				log::warn!("Failed to purge orphaned SST {}: {}", sst_id, e);
			} else {
				deleted += 1;
			}
		}
	}

	if deleted > 0 {
		log::info!("Purged {} orphaned SST files from object store", deleted);
	}
	Ok(deleted)
}
