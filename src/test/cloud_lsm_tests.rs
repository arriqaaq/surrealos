//! Full-stack LSM E2E tests over real backends (LocalFileSystem and S3).
//!
//! WAL on local tempdir, SSTs + manifests on object store.
//!
//! Tier 1: LocalFileSystem — always runs in `cargo test`.
//! Tier 2: S3 via MinIO — runs with `cargo test --features cloud-tests -- --ignored`.

use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::compaction::leveled::Strategy;
use crate::test::{collect_transaction_all, collect_transaction_reverse};
use crate::{LSMIterator, TreeBuilder};

// =============================================================================
// Helper: create a Tree with object store backend
// =============================================================================

/// Create a tree with WAL on local disk, SSTs/manifests on given object store.
/// `local_sst_cache=false` forces all reads through the object store.
async fn create_cloud_tree<F>(
	wal_path: PathBuf,
	store: Arc<dyn object_store::ObjectStore>,
	configure: F,
) -> crate::Tree
where
	F: FnOnce(TreeBuilder) -> TreeBuilder,
{
	let mut opts = crate::Options::new();
	opts.path = wal_path;
	opts.object_store = store;
	opts.local_sst_cache = false; // Force reads through object store
	opts.max_memtable_size = 64 * 1024; // 64KB for fast flushes

	let builder = TreeBuilder::with_options(opts);
	configure(builder).build().await.unwrap()
}

// =============================================================================
// Tier 1: LocalFileSystem LSM tests
// =============================================================================

#[test(tokio::test)]
async fn local_fs_lsm_basic_write_read() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), store, |b| b).await;

	tree.set(b"hello", b"world").await.unwrap();
	let val = tree.get(b"hello").await.unwrap().unwrap();
	assert_eq!(val, b"world");

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_flush_to_disk() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), Arc::clone(&store), |b| b).await;

	// Write enough to have data to flush
	for i in 0..100 {
		let key = format!("key_{i:05}");
		let value = format!("val_{i:05}");
		tree.set(key.as_bytes(), value.as_bytes()).await.unwrap();
	}
	tree.flush().await.unwrap();

	// Verify SSTs exist in object store
	let sst_ids = tree.core.inner.table_store.list_sst_ids().await.unwrap();
	assert!(!sst_ids.is_empty(), "Expected SSTs in object store after flush");

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_recovery() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path().join("wal");
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&wal_dir).unwrap();
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	// Phase 1: Write, flush, close
	{
		let tree = create_cloud_tree(wal_dir.clone(), Arc::clone(&store), |b| b).await;
		tree.set(b"key1", b"val1").await.unwrap();
		tree.set(b"key2", b"val2").await.unwrap();
		tree.flush().await.unwrap();
		tree.close().await.unwrap();
	}

	// Phase 2: Reopen — recovery reads manifest + SSTs from object store
	{
		let tree = create_cloud_tree(wal_dir.clone(), Arc::clone(&store), |b| b).await;
		let v1 = tree.get(b"key1").await.unwrap().unwrap();
		assert_eq!(v1, b"val1");
		let v2 = tree.get(b"key2").await.unwrap().unwrap();
		assert_eq!(v2, b"val2");
		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn local_fs_lsm_compaction() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), Arc::clone(&store), |b| b).await;

	// Write enough data in multiple batches to generate multiple L0 SSTs
	for batch_num in 0..10u32 {
		for i in 0..50u32 {
			let key = format!("key_{i:05}");
			let value = format!("val_{batch_num}_{i:05}");
			tree.set(key.as_bytes(), value.as_bytes()).await.unwrap();
		}
		tree.flush().await.unwrap();
	}

	// Verify latest values survive compaction
	for i in 0..50u32 {
		let key = format!("key_{i:05}");
		let expected = format!("val_9_{i:05}");
		let val = tree.get(key.as_bytes()).await.unwrap().unwrap();
		assert_eq!(val, expected.as_bytes());
	}

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_large_values() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), store, |b| {
		b.with_max_memtable_size(2 * 1024 * 1024) // 2MB memtable for large values
	})
	.await;

	// Write a 1MB value
	let big_value = vec![0xBBu8; 1024 * 1024];
	tree.set(b"big_key", &big_value).await.unwrap();
	tree.flush().await.unwrap();

	let val = tree.get(b"big_key").await.unwrap().unwrap();
	assert_eq!(val.len(), big_value.len());
	assert_eq!(val[0], 0xBB);
	assert_eq!(val[val.len() - 1], 0xBB);

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_many_keys() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), store, |b| b).await;

	let num_keys = 10_000u32;
	for i in 0..num_keys {
		let key = format!("key_{i:08}");
		let value = format!("val_{i:08}");
		tree.set(key.as_bytes(), value.as_bytes()).await.unwrap();
	}
	tree.flush().await.unwrap();

	// Spot-check a few keys
	for i in [0, 100, 5000, 9999] {
		let key = format!("key_{i:08}");
		let expected = format!("val_{i:08}");
		let val = tree.get(key.as_bytes()).await.unwrap().unwrap();
		assert_eq!(val, expected.as_bytes());
	}

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_delete_and_compact() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), Arc::clone(&store), |b| b).await;

	// Write keys
	for i in 0..20u32 {
		tree.set(format!("key_{i:05}").as_bytes(), b"value").await.unwrap();
	}
	tree.flush().await.unwrap();

	// Delete half the keys
	for i in 0..10u32 {
		tree.delete(format!("key_{i:05}").as_bytes()).await.unwrap();
	}
	tree.flush().await.unwrap();

	// Deleted keys should return None
	for i in 0..10u32 {
		assert!(
			tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().is_none(),
			"key_{i:05} should be deleted"
		);
	}
	// Remaining keys should still exist
	for i in 10..20u32 {
		assert!(
			tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().is_some(),
			"key_{i:05} should exist"
		);
	}

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_local_sst_cache() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	// Use local_sst_cache=true (default) — reads should serve from local cache
	let wal_dir = temp_dir.path().join("wal");
	let mut opts = crate::Options::new();
	opts.path = wal_dir.clone();
	opts.object_store = Arc::clone(&store);
	opts.local_sst_cache = true; // Enable local SST caching
	opts.max_memtable_size = 64 * 1024;

	let tree = TreeBuilder::with_options(opts).build().await.unwrap();

	for i in 0..50u32 {
		tree.set(format!("key_{i:05}").as_bytes(), format!("val_{i:05}").as_bytes()).await.unwrap();
	}
	tree.flush().await.unwrap();

	// SSTs should exist both in object store and locally
	let sst_ids = tree.core.inner.table_store.list_sst_ids().await.unwrap();
	assert!(!sst_ids.is_empty());

	// Verify data readable (served from local cache)
	let val = tree.get(b"key_00025").await.unwrap().unwrap();
	assert_eq!(val, b"val_00025");

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_recovery_after_multiple_flushes() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path().join("wal");
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&wal_dir).unwrap();
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	// Phase 1: Multiple writes with flushes
	{
		let tree = create_cloud_tree(wal_dir.clone(), Arc::clone(&store), |b| b).await;

		tree.set(b"foo", b"v1").await.unwrap();
		tree.flush().await.unwrap();

		tree.set(b"foo", b"v2").await.unwrap();
		tree.set(b"bar", b"v1").await.unwrap();
		tree.flush().await.unwrap();

		// Write without flush — stays in WAL
		tree.set(b"baz", b"v1").await.unwrap();
		tree.close().await.unwrap();
	}

	// Phase 2: Reopen and verify all data
	{
		let tree = create_cloud_tree(wal_dir.clone(), Arc::clone(&store), |b| b).await;

		// Latest value for foo
		assert_eq!(tree.get(b"foo").await.unwrap().unwrap(), b"v2");
		// bar from second flush
		assert_eq!(tree.get(b"bar").await.unwrap().unwrap(), b"v1");
		// baz from WAL recovery
		assert_eq!(tree.get(b"baz").await.unwrap().unwrap(), b"v1");

		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn local_fs_lsm_snapshot_survives_flush() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), Arc::clone(&store), |b| b).await;

	// Write initial values and flush to cloud SSTs
	for i in 0..10u32 {
		tree.set(format!("key_{i:05}").as_bytes(), b"v1").await.unwrap();
	}
	tree.flush().await.unwrap();

	// Take snapshot before overwriting
	let snapshot = tree.new_snapshot();

	// Overwrite with new values and flush
	for i in 0..10u32 {
		tree.set(format!("key_{i:05}").as_bytes(), b"v2").await.unwrap();
	}
	tree.flush().await.unwrap();

	// Snapshot reads old values from cloud SSTs
	let val = snapshot.get(b"key_00005").await.unwrap().unwrap();
	assert_eq!(val.0, b"v1");

	// Latest reads return new values
	let val = tree.get(b"key_00005").await.unwrap().unwrap();
	assert_eq!(val, b"v2");

	// Range scan on snapshot returns all v1 values
	let results = collect_transaction_all(
		&mut snapshot.range(Some(b"key_00000"), Some(b"key_99999")).unwrap(),
	)
	.await
	.unwrap();
	assert_eq!(results.len(), 10);
	for (_, v) in &results {
		assert_eq!(v, b"v1");
	}

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_versioned_history_after_flush() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), Arc::clone(&store), |b| {
		b.with_versioning(true, 0)
	})
	.await;

	// Write version 1 at timestamp 100
	tree.set_at(b"key_a", b"value_v1", 100).await.unwrap();
	tree.flush().await.unwrap();

	// Write version 2 at timestamp 200
	tree.set_at(b"key_a", b"value_v2", 200).await.unwrap();
	tree.flush().await.unwrap();

	let snap = tree.new_snapshot();

	// history_iter shows both versions (newest first)
	let mut iter = snap.history_iter(Some(b"key_a"), Some(b"key_b"), false, None, None).unwrap();
	iter.seek_first().await.unwrap();
	let mut results = Vec::new();
	while iter.valid() {
		let key_ref = iter.key();
		let value = iter.value().unwrap();
		results.push((key_ref.user_key().to_vec(), value, key_ref.timestamp()));
		iter.next().await.unwrap();
	}
	assert_eq!(results.len(), 2);
	assert_eq!(results[0].1, b"value_v2"); // newest first
	assert_eq!(results[0].2, 200);
	assert_eq!(results[1].1, b"value_v1");
	assert_eq!(results[1].2, 100);

	// get_at returns correct version by timestamp
	let val = snap.get_at(b"key_a", 100).await.unwrap().unwrap();
	assert_eq!(val, b"value_v1");
	let val = snap.get_at(b"key_a", 200).await.unwrap().unwrap();
	assert_eq!(val, b"value_v2");
	// Timestamp between versions returns earlier version
	let val = snap.get_at(b"key_a", 150).await.unwrap().unwrap();
	assert_eq!(val, b"value_v1");

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_range_scan_across_ssts() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), Arc::clone(&store), |b| b).await;

	// Create 3 SSTs with non-overlapping key ranges
	for i in 0..100u32 {
		tree.set(format!("key_{i:03}").as_bytes(), format!("batch_0_{i:03}").as_bytes())
			.await
			.unwrap();
	}
	tree.flush().await.unwrap();

	for i in 100..200u32 {
		tree.set(format!("key_{i:03}").as_bytes(), format!("batch_1_{i:03}").as_bytes())
			.await
			.unwrap();
	}
	tree.flush().await.unwrap();

	for i in 200..300u32 {
		tree.set(format!("key_{i:03}").as_bytes(), format!("batch_2_{i:03}").as_bytes())
			.await
			.unwrap();
	}
	tree.flush().await.unwrap();

	let snap = tree.new_snapshot();

	// Forward range scan crossing SST boundaries
	let results =
		collect_transaction_all(&mut snap.range(Some(b"key_050"), Some(b"key_250")).unwrap())
			.await
			.unwrap();
	assert_eq!(results.len(), 200);
	// Verify ordered
	for i in 1..results.len() {
		assert!(results[i].0 > results[i - 1].0);
	}

	// Reverse scan
	let results_rev =
		collect_transaction_reverse(&mut snap.range(Some(b"key_050"), Some(b"key_250")).unwrap())
			.await
			.unwrap();
	assert_eq!(results_rev.len(), 200);
	// Verify reverse ordered
	for i in 1..results_rev.len() {
		assert!(results_rev[i].0 < results_rev[i - 1].0);
	}

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_compaction_removes_old_ssts() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), Arc::clone(&store), |b| b).await;

	// Create multiple L0 SSTs
	for batch in 0..5u32 {
		for i in 0..50u32 {
			tree.set(format!("key_{i:05}").as_bytes(), format!("val_{batch}_{i:05}").as_bytes())
				.await
				.unwrap();
		}
		tree.flush().await.unwrap();
	}

	let sst_count_before = tree.core.inner.table_store.list_sst_ids().await.unwrap().len();
	assert!(sst_count_before >= 5);

	// Force compaction — merges L0 into L1, deletes old SSTs from object store
	tree.compact(Arc::new(Strategy::default())).await.unwrap();

	let sst_count_after = tree.core.inner.table_store.list_sst_ids().await.unwrap().len();
	assert!(
		sst_count_after < sst_count_before,
		"Compaction should reduce SST count: before={sst_count_before}, after={sst_count_after}"
	);

	// All data still readable
	for i in 0..50u32 {
		let val = tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().unwrap();
		assert_eq!(val, format!("val_4_{i:05}").as_bytes()); // last batch wins
	}

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_two_trees_one_store() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	// Tree A with prefix "tenant_a/"
	let mut opts_a = crate::Options::new();
	opts_a.path = temp_dir.path().join("wal_a");
	opts_a.object_store = Arc::clone(&store);
	opts_a.object_store_root = "tenant_a/".to_string();
	opts_a.local_sst_cache = false;
	opts_a.max_memtable_size = 64 * 1024;
	let tree_a = TreeBuilder::with_options(opts_a).build().await.unwrap();

	// Tree B with prefix "tenant_b/"
	let mut opts_b = crate::Options::new();
	opts_b.path = temp_dir.path().join("wal_b");
	opts_b.object_store = Arc::clone(&store);
	opts_b.object_store_root = "tenant_b/".to_string();
	opts_b.local_sst_cache = false;
	opts_b.max_memtable_size = 64 * 1024;
	let tree_b = TreeBuilder::with_options(opts_b).build().await.unwrap();

	tree_a.set(b"shared_key", b"value_a").await.unwrap();
	tree_b.set(b"shared_key", b"value_b").await.unwrap();
	tree_a.flush().await.unwrap();
	tree_b.flush().await.unwrap();

	// Each tree sees its own value
	assert_eq!(tree_a.get(b"shared_key").await.unwrap().unwrap(), b"value_a");
	assert_eq!(tree_b.get(b"shared_key").await.unwrap().unwrap(), b"value_b");

	// SST IDs from each tree's table_store should be non-empty
	let ids_a = tree_a.core.inner.table_store.list_sst_ids().await.unwrap();
	let ids_b = tree_b.core.inner.table_store.list_sst_ids().await.unwrap();
	assert!(!ids_a.is_empty());
	assert!(!ids_b.is_empty());

	tree_a.close().await.unwrap();
	tree_b.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_recovery_after_compaction() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path().join("wal");
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&wal_dir).unwrap();
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	// Phase 1: Write, compact, close
	{
		let tree = create_cloud_tree(wal_dir.clone(), Arc::clone(&store), |b| b).await;
		for batch in 0..5u32 {
			for i in 0..50u32 {
				tree.set(
					format!("key_{i:05}").as_bytes(),
					format!("val_{batch}_{i:05}").as_bytes(),
				)
				.await
				.unwrap();
			}
			tree.flush().await.unwrap();
		}
		tree.compact(Arc::new(Strategy::default())).await.unwrap();
		assert_eq!(tree.get(b"key_00025").await.unwrap().unwrap(), b"val_4_00025");
		tree.close().await.unwrap();
	}

	// Phase 2: Reopen — reads post-compaction manifest + SSTs from object store
	{
		let tree = create_cloud_tree(wal_dir.clone(), Arc::clone(&store), |b| b).await;
		for i in 0..50u32 {
			let expected = format!("val_4_{i:05}");
			let val = tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().unwrap();
			assert_eq!(val, expected.as_bytes());
		}
		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn local_fs_lsm_snapshot_survives_compaction() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), Arc::clone(&store), |b| b).await;

	// Write initial values and flush
	for i in 0..50u32 {
		tree.set(format!("key_{i:05}").as_bytes(), b"old_value").await.unwrap();
	}
	tree.flush().await.unwrap();

	// Take snapshot
	let snapshot = tree.new_snapshot();

	// Overwrite with new values
	for i in 0..50u32 {
		tree.set(format!("key_{i:05}").as_bytes(), b"new_value").await.unwrap();
	}
	tree.flush().await.unwrap();

	// Compact — replaces SSTs
	tree.compact(Arc::new(Strategy::default())).await.unwrap();

	// Snapshot still sees old values
	let val = snapshot.get(b"key_00025").await.unwrap().unwrap();
	assert_eq!(val.0, b"old_value");

	// Tree sees new values
	assert_eq!(tree.get(b"key_00025").await.unwrap().unwrap(), b"new_value");

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_mixed_batch_operations() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), Arc::clone(&store), |b| b).await;

	// Write initial keys
	for i in 0..20u32 {
		tree.set(format!("key_{i:05}").as_bytes(), b"initial").await.unwrap();
	}
	tree.flush().await.unwrap();

	// Single batch: delete first 10, set 10 new keys
	let mut batch = tree.new_batch();
	for i in 0..10u32 {
		batch.delete(format!("key_{i:05}").as_bytes()).unwrap();
	}
	for i in 20..30u32 {
		batch.set(format!("key_{i:05}").as_bytes(), b"new_value").unwrap();
	}
	tree.apply(batch, false).await.unwrap();
	tree.flush().await.unwrap();

	// Verify deleted keys return None
	for i in 0..10u32 {
		assert!(tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().is_none());
	}
	// Remaining keys should still exist
	for i in 10..30u32 {
		assert!(tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().is_some());
	}

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_large_batch_many_rotations() {
	let temp_dir = TempDir::new().unwrap();
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	let tree = create_cloud_tree(temp_dir.path().join("wal"), Arc::clone(&store), |b| {
		b.with_max_memtable_size(32 * 1024)
	})
	.await;

	for i in 0..1000u32 {
		let value = vec![(i % 256) as u8; 1024]; // 1KB values
		tree.set(format!("key_{i:05}").as_bytes(), &value).await.unwrap();
	}
	tree.flush().await.unwrap();

	// Spot-check
	for i in [0u32, 100, 500, 999] {
		let expected_byte = (i % 256) as u8;
		let val = tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().unwrap();
		assert_eq!(val.len(), 1024);
		assert_eq!(val[0], expected_byte);
	}

	// Verify many SSTs exist from rotations
	let sst_ids = tree.core.inner.table_store.list_sst_ids().await.unwrap();
	assert!(sst_ids.len() > 5, "Expected many SSTs from rotations, got {}", sst_ids.len());

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_lsm_compact_recover_range() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path().join("wal");
	let obj_dir = temp_dir.path().join("obj");
	std::fs::create_dir_all(&wal_dir).unwrap();
	std::fs::create_dir_all(&obj_dir).unwrap();
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&obj_dir).unwrap());

	// Phase 1: Write, compact, close
	{
		let tree = create_cloud_tree(wal_dir.clone(), Arc::clone(&store), |b| b).await;
		for i in 0..500u32 {
			tree.set(format!("key_{i:03}").as_bytes(), format!("val_{i:03}").as_bytes())
				.await
				.unwrap();
		}
		tree.flush().await.unwrap();
		tree.compact(Arc::new(Strategy::default())).await.unwrap();
		tree.close().await.unwrap();
	}

	// Phase 2: Reopen, range scan
	{
		let tree = create_cloud_tree(wal_dir.clone(), Arc::clone(&store), |b| b).await;
		let snap = tree.new_snapshot();
		let results =
			collect_transaction_all(&mut snap.range(Some(b"key_100"), Some(b"key_400")).unwrap())
				.await
				.unwrap();
		assert_eq!(results.len(), 300);
		// Verify ordering
		for i in 1..results.len() {
			assert!(results[i].0 > results[i - 1].0);
		}
		tree.close().await.unwrap();
	}
}

// =============================================================================
// Tier 2: S3 LSM tests (via MinIO) — only compiled with cloud-tests feature
// =============================================================================

#[cfg(feature = "cloud-tests")]
mod s3_tests {
	use std::path::PathBuf;
	use std::sync::Arc;

	use tempfile::TempDir;
	use test_log::test;

	use crate::test::cloud_test_helpers::{
		cleanup_prefix,
		create_s3_table_store,
		require_s3_config,
	};
	use crate::{LSMIterator, TreeBuilder};

	fn unique_prefix() -> String {
		format!("test/{}/", ulid::Ulid::new())
	}

	/// Create a tree with WAL on local disk, SSTs on S3.
	async fn create_s3_tree<F>(
		wal_path: PathBuf,
		store: Arc<dyn object_store::ObjectStore>,
		prefix: &str,
		configure: F,
	) -> crate::Tree
	where
		F: FnOnce(TreeBuilder) -> TreeBuilder,
	{
		let mut opts = crate::Options::new();
		opts.path = wal_path;
		opts.object_store = store;
		opts.object_store_root = prefix.to_string();
		opts.local_sst_cache = false;
		opts.max_memtable_size = 64 * 1024;

		let builder = TreeBuilder::with_options(opts);
		configure(builder).build().await.unwrap()
	}

	#[test(tokio::test)]
	async fn s3_lsm_basic_write_read() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree =
			create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| b).await;

		tree.set(b"hello", b"world").await.unwrap();
		let val = tree.get(b"hello").await.unwrap().unwrap();
		assert_eq!(val, b"world");

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_flush_to_s3() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree =
			create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| b).await;

		for i in 0..100u32 {
			tree.set(format!("key_{i:05}").as_bytes(), format!("val_{i:05}").as_bytes())
				.await
				.unwrap();
		}
		tree.flush().await.unwrap();

		let sst_ids = tree.core.inner.table_store.list_sst_ids().await.unwrap();
		assert!(!sst_ids.is_empty(), "Expected SSTs in S3 after flush");

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_recovery_from_s3() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path().join("wal");
		std::fs::create_dir_all(&wal_dir).unwrap();

		// Phase 1: Write, flush, close
		{
			let tree = create_s3_tree(wal_dir.clone(), Arc::clone(&store), &prefix, |b| b).await;
			tree.set(b"key1", b"val1").await.unwrap();
			tree.set(b"key2", b"val2").await.unwrap();
			tree.flush().await.unwrap();
			tree.close().await.unwrap();
		}

		// Phase 2: Reopen — recovery reads SSTs from S3
		{
			let tree = create_s3_tree(wal_dir.clone(), Arc::clone(&store), &prefix, |b| b).await;
			assert_eq!(tree.get(b"key1").await.unwrap().unwrap(), b"val1");
			assert_eq!(tree.get(b"key2").await.unwrap().unwrap(), b"val2");
			tree.close().await.unwrap();
		}

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_compaction() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree =
			create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| b).await;

		for batch_num in 0..10u32 {
			for i in 0..50u32 {
				let key = format!("key_{i:05}");
				let value = format!("val_{batch_num}_{i:05}");
				tree.set(key.as_bytes(), value.as_bytes()).await.unwrap();
			}
			tree.flush().await.unwrap();
		}

		for i in 0..50u32 {
			let key = format!("key_{i:05}");
			let expected = format!("val_9_{i:05}");
			let val = tree.get(key.as_bytes()).await.unwrap().unwrap();
			assert_eq!(val, expected.as_bytes());
		}

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_many_keys() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree =
			create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| b).await;

		let num_keys = 10_000u32;
		for i in 0..num_keys {
			tree.set(format!("key_{i:08}").as_bytes(), format!("val_{i:08}").as_bytes())
				.await
				.unwrap();
		}
		tree.flush().await.unwrap();

		for i in [0, 100, 5000, 9999] {
			let key = format!("key_{i:08}");
			let expected = format!("val_{i:08}");
			let val = tree.get(key.as_bytes()).await.unwrap().unwrap();
			assert_eq!(val, expected.as_bytes());
		}

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_snapshot_survives_flush() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree =
			create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| b).await;

		for i in 0..10u32 {
			tree.set(format!("key_{i:05}").as_bytes(), b"v1").await.unwrap();
		}
		tree.flush().await.unwrap();

		let snapshot = tree.new_snapshot();

		for i in 0..10u32 {
			tree.set(format!("key_{i:05}").as_bytes(), b"v2").await.unwrap();
		}
		tree.flush().await.unwrap();

		let val = snapshot.get(b"key_00005").await.unwrap().unwrap();
		assert_eq!(val.0, b"v1");
		assert_eq!(tree.get(b"key_00005").await.unwrap().unwrap(), b"v2");

		let results = crate::test::collect_transaction_all(
			&mut snapshot.range(Some(b"key_00000"), Some(b"key_99999")).unwrap(),
		)
		.await
		.unwrap();
		assert_eq!(results.len(), 10);
		for (_, v) in &results {
			assert_eq!(v, b"v1");
		}

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_versioned_history_after_flush() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree = create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| {
			b.with_versioning(true, 0)
		})
		.await;

		tree.set_at(b"key_a", b"value_v1", 100).await.unwrap();
		tree.flush().await.unwrap();
		tree.set_at(b"key_a", b"value_v2", 200).await.unwrap();
		tree.flush().await.unwrap();

		let snap = tree.new_snapshot();
		let mut iter =
			snap.history_iter(Some(b"key_a"), Some(b"key_b"), false, None, None).unwrap();
		iter.seek_first().await.unwrap();
		let mut results = Vec::new();
		while iter.valid() {
			let key_ref = iter.key();
			let value = iter.value().unwrap();
			results.push((key_ref.user_key().to_vec(), value, key_ref.timestamp()));
			iter.next().await.unwrap();
		}
		assert_eq!(results.len(), 2);
		assert_eq!(results[0].1, b"value_v2");
		assert_eq!(results[0].2, 200);
		assert_eq!(results[1].1, b"value_v1");
		assert_eq!(results[1].2, 100);

		let val = snap.get_at(b"key_a", 100).await.unwrap().unwrap();
		assert_eq!(val, b"value_v1");
		let val = snap.get_at(b"key_a", 200).await.unwrap().unwrap();
		assert_eq!(val, b"value_v2");
		let val = snap.get_at(b"key_a", 150).await.unwrap().unwrap();
		assert_eq!(val, b"value_v1");

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_range_scan_across_ssts() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree =
			create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| b).await;

		for i in 0..100u32 {
			tree.set(format!("key_{i:03}").as_bytes(), format!("batch_0_{i:03}").as_bytes())
				.await
				.unwrap();
		}
		tree.flush().await.unwrap();
		for i in 100..200u32 {
			tree.set(format!("key_{i:03}").as_bytes(), format!("batch_1_{i:03}").as_bytes())
				.await
				.unwrap();
		}
		tree.flush().await.unwrap();
		for i in 200..300u32 {
			tree.set(format!("key_{i:03}").as_bytes(), format!("batch_2_{i:03}").as_bytes())
				.await
				.unwrap();
		}
		tree.flush().await.unwrap();

		let snap = tree.new_snapshot();
		let results = crate::test::collect_transaction_all(
			&mut snap.range(Some(b"key_050"), Some(b"key_250")).unwrap(),
		)
		.await
		.unwrap();
		assert_eq!(results.len(), 200);
		for i in 1..results.len() {
			assert!(results[i].0 > results[i - 1].0);
		}

		let results_rev = crate::test::collect_transaction_reverse(
			&mut snap.range(Some(b"key_050"), Some(b"key_250")).unwrap(),
		)
		.await
		.unwrap();
		assert_eq!(results_rev.len(), 200);
		for i in 1..results_rev.len() {
			assert!(results_rev[i].0 < results_rev[i - 1].0);
		}

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_compaction_removes_old_ssts() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree =
			create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| b).await;

		for batch in 0..5u32 {
			for i in 0..50u32 {
				tree.set(
					format!("key_{i:05}").as_bytes(),
					format!("val_{batch}_{i:05}").as_bytes(),
				)
				.await
				.unwrap();
			}
			tree.flush().await.unwrap();
		}

		let sst_count_before = tree.core.inner.table_store.list_sst_ids().await.unwrap().len();
		assert!(sst_count_before >= 5);

		tree.compact(Arc::new(crate::compaction::leveled::Strategy::default())).await.unwrap();

		let sst_count_after = tree.core.inner.table_store.list_sst_ids().await.unwrap().len();
		assert!(sst_count_after < sst_count_before);

		for i in 0..50u32 {
			let val = tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().unwrap();
			assert_eq!(val, format!("val_4_{i:05}").as_bytes());
		}

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_recovery_after_compaction() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path().join("wal");
		std::fs::create_dir_all(&wal_dir).unwrap();

		// Phase 1: Write, compact, close
		{
			let tree = create_s3_tree(wal_dir.clone(), Arc::clone(&store), &prefix, |b| b).await;
			for batch in 0..5u32 {
				for i in 0..50u32 {
					tree.set(
						format!("key_{i:05}").as_bytes(),
						format!("val_{batch}_{i:05}").as_bytes(),
					)
					.await
					.unwrap();
				}
				tree.flush().await.unwrap();
			}
			tree.compact(Arc::new(crate::compaction::leveled::Strategy::default())).await.unwrap();
			tree.close().await.unwrap();
		}

		// Phase 2: Reopen
		{
			let tree = create_s3_tree(wal_dir.clone(), Arc::clone(&store), &prefix, |b| b).await;
			for i in 0..50u32 {
				let expected = format!("val_4_{i:05}");
				let val = tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().unwrap();
				assert_eq!(val, expected.as_bytes());
			}
			tree.close().await.unwrap();
		}

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_snapshot_survives_compaction() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree =
			create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| b).await;

		for i in 0..50u32 {
			tree.set(format!("key_{i:05}").as_bytes(), b"old_value").await.unwrap();
		}
		tree.flush().await.unwrap();

		let snapshot = tree.new_snapshot();

		for i in 0..50u32 {
			tree.set(format!("key_{i:05}").as_bytes(), b"new_value").await.unwrap();
		}
		tree.flush().await.unwrap();

		tree.compact(Arc::new(crate::compaction::leveled::Strategy::default())).await.unwrap();

		let val = snapshot.get(b"key_00025").await.unwrap().unwrap();
		assert_eq!(val.0, b"old_value");
		assert_eq!(tree.get(b"key_00025").await.unwrap().unwrap(), b"new_value");

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_mixed_batch_operations() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree =
			create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| b).await;

		for i in 0..20u32 {
			tree.set(format!("key_{i:05}").as_bytes(), b"initial").await.unwrap();
		}
		tree.flush().await.unwrap();

		let mut batch = tree.new_batch();
		for i in 0..10u32 {
			batch.delete(format!("key_{i:05}").as_bytes()).unwrap();
		}
		for i in 20..30u32 {
			batch.set(format!("key_{i:05}").as_bytes(), b"new_value").unwrap();
		}
		tree.apply(batch, false).await.unwrap();
		tree.flush().await.unwrap();

		for i in 0..10u32 {
			assert!(tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().is_none());
		}
		for i in 10..30u32 {
			assert!(tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().is_some());
		}

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_large_batch_many_rotations() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let tree = create_s3_tree(temp_dir.path().join("wal"), Arc::clone(&store), &prefix, |b| {
			b.with_max_memtable_size(32 * 1024)
		})
		.await;

		for i in 0..1000u32 {
			let value = vec![((i % 256) as u8); 1024];
			tree.set(format!("key_{i:05}").as_bytes(), &value).await.unwrap();
		}
		tree.flush().await.unwrap();

		for i in [0u32, 100, 500, 999] {
			let expected_byte = (i % 256) as u8;
			let val = tree.get(format!("key_{i:05}").as_bytes()).await.unwrap().unwrap();
			assert_eq!(val.len(), 1024);
			assert_eq!(val[0], expected_byte);
		}

		let sst_ids = tree.core.inner.table_store.list_sst_ids().await.unwrap();
		assert!(sst_ids.len() > 5);

		tree.close().await.unwrap();
		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_lsm_compact_recover_range() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, _ts) = create_s3_table_store(&prefix, None).unwrap();

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path().join("wal");
		std::fs::create_dir_all(&wal_dir).unwrap();

		// Phase 1: Write, compact, close
		{
			let tree = create_s3_tree(wal_dir.clone(), Arc::clone(&store), &prefix, |b| b).await;
			for i in 0..500u32 {
				tree.set(format!("key_{i:03}").as_bytes(), format!("val_{i:03}").as_bytes())
					.await
					.unwrap();
			}
			tree.flush().await.unwrap();
			tree.compact(Arc::new(crate::compaction::leveled::Strategy::default())).await.unwrap();
			tree.close().await.unwrap();
		}

		// Phase 2: Reopen, range scan
		{
			let tree = create_s3_tree(wal_dir.clone(), Arc::clone(&store), &prefix, |b| b).await;
			let snap = tree.new_snapshot();
			let results = crate::test::collect_transaction_all(
				&mut snap.range(Some(b"key_100"), Some(b"key_400")).unwrap(),
			)
			.await
			.unwrap();
			assert_eq!(results.len(), 300);
			for i in 1..results.len() {
				assert!(results[i].0 > results[i - 1].0);
			}
			tree.close().await.unwrap();
		}

		cleanup_prefix(&store, &prefix).await.unwrap();
	}
}
