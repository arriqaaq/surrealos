//! LSM Recovery Integration Tests

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::test::recovery_test_helpers::RecoveryTestHelper;
use crate::{LSMIterator, TreeBuilder};

/// Create a tree with custom options and a shared object store
async fn create_tree<F>(
	path: PathBuf,
	store: Arc<dyn object_store::ObjectStore>,
	configure: F,
) -> crate::Tree
where
	F: FnOnce(TreeBuilder) -> TreeBuilder,
{
	let builder = TreeBuilder::new()
		.with_path(path)
		.with_max_memtable_size(1024 * 1024) // 1MB default
		.with_object_store(store);

	configure(builder).build().await.unwrap()
}

// ============================================================================
// Core Recovery Tests
// ============================================================================

/// Test 1: Basic Recovery
#[test(tokio::test)]
async fn test_basic_recovery() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	// Phase 1: Write and close
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		tree.set(b"foo", b"v1").await.unwrap();
		tree.set(b"baz", b"v5").await.unwrap();

		tree.close().await.unwrap();
	}

	// Phase 2: Reopen and verify
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		RecoveryTestHelper::verify_key(&tree, "foo", "v1").await;
		RecoveryTestHelper::verify_key(&tree, "baz", "v5").await;

		// Write more data
		tree.set(b"bar", b"v2").await.unwrap();
		tree.set(b"foo", b"v3").await.unwrap();

		tree.close().await.unwrap();
	}

	// Phase 3: Reopen again and verify all data
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		RecoveryTestHelper::verify_key(&tree, "foo", "v3").await;
		RecoveryTestHelper::verify_key(&tree, "bar", "v2").await;
		RecoveryTestHelper::verify_key(&tree, "baz", "v5").await;

		tree.close().await.unwrap();
	}
}

/// Test 2: Recovery With Existing SST Files
#[test(tokio::test)]
async fn test_recover_with_existing_ssts() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let sst_dir = path.join("sst");
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(path.clone()).unwrap());

	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| {
			b.with_max_memtable_size(64 * 1024) // Explicit flushes, no size pressure needed
		})
		.await;

		// First batch - will be flushed
		tree.set(b"foo", b"v1").await.unwrap();
		tree.set(b"bar", b"v2").await.unwrap();

		tree.flush().await.unwrap();
		let sst_count_1 = RecoveryTestHelper::count_sst_files(&sst_dir);
		assert_eq!(sst_count_1, 1, "Should have 1 SST after first flush");

		// Second batch - will be flushed
		tree.set(b"foo", b"v3").await.unwrap();
		tree.set(b"bar", b"v4").await.unwrap();

		tree.flush().await.unwrap();
		let sst_count_2 = RecoveryTestHelper::count_sst_files(&sst_dir);
		assert_eq!(sst_count_2, 2, "Should have 2 SSTs after second flush");

		// Third write - stays in WAL only
		tree.set(b"big", b"large_value_not_flushed").await.unwrap();

		// Close without flushing the last write
		tree.close().await.unwrap();
	}

	// Reopen and verify
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		// Data from SSTs
		RecoveryTestHelper::verify_key(&tree, "foo", "v3").await;
		RecoveryTestHelper::verify_key(&tree, "bar", "v4").await;

		// Data from WAL recovery
		RecoveryTestHelper::verify_key(&tree, "big", "large_value_not_flushed").await;

		tree.close().await.unwrap();
	}
}

/// Test 3: Recovery Without Flush (Multiple WALs)
#[test(tokio::test)]
async fn test_recover_multiple_wals_without_flush() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
	let keys_to_write = 100;

	// Phase 1: Write data (will create WAL, on close will flush creating SST)
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		for i in 0..keys_to_write {
			let key = format!("key_{:05}", i);
			let value = format!("value_{:05}", i);

			tree.set(key.as_bytes(), value.as_bytes()).await.unwrap();
		}

		// Note: Close will auto-flush
		tree.close().await.unwrap();
	}

	// Phase 2: Reopen and verify all WALs replayed
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		// Verify all keys recovered from multiple WAL segments
		for i in 0..keys_to_write {
			let key = format!("key_{:05}", i);
			let expected_value = format!("value_{:05}", i);
			RecoveryTestHelper::verify_key(&tree, &key, &expected_value).await;
		}

		// Write more data (new WAL)
		tree.set(b"new_key", b"new_value").await.unwrap();

		tree.close().await.unwrap();
	}

	// Phase 3: Reopen again, verify original + new data
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		// Original data
		for i in 0..keys_to_write {
			let key = format!("key_{:05}", i);
			let expected_value = format!("value_{:05}", i);
			RecoveryTestHelper::verify_key(&tree, &key, &expected_value).await;
		}

		// New data
		RecoveryTestHelper::verify_key(&tree, "new_key", "new_value").await;

		// Now flush everything
		tree.flush().await.unwrap();

		tree.close().await.unwrap();
	}

	// Phase 4: Reopen and verify data from SST
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		for i in 0..keys_to_write {
			let key = format!("key_{:05}", i);
			let expected_value = format!("value_{:05}", i);
			RecoveryTestHelper::verify_key(&tree, &key, &expected_value).await;
		}
		RecoveryTestHelper::verify_key(&tree, "new_key", "new_value").await;

		tree.close().await.unwrap();
	}
}

/// Test 4: Recovery With Large WAL
#[test(tokio::test)]
async fn test_recover_with_large_wal() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	let large_entry_count = 1000;

	// Write large amount of data
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| {
			b.with_max_memtable_size(10 * 1024 * 1024) // 10MB - avoid rotation
		})
		.await;

		for i in 0..large_entry_count {
			let key = format!("large_key_{:06}", i);
			let value = format!("large_value_{:06}_with_padding", i);

			tree.set(key.as_bytes(), value.as_bytes()).await.unwrap();
		}

		tree.close().await.unwrap();
	}

	// Reopen and verify all data recovered
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		for i in 0..large_entry_count {
			let key = format!("large_key_{:06}", i);
			let result = tree.get(key.as_bytes()).await.unwrap();
			assert!(result.is_some(), "Key {} should exist", key);
		}

		tree.close().await.unwrap();
	}
}

/// Test 5: Recovery With Empty WAL
#[test(tokio::test)]
async fn test_recovery_with_empty_wal() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		// Write and flush
		tree.set(b"foo", b"v1").await.unwrap();

		tree.flush().await.unwrap();

		// WAL is now empty (all data flushed)
		tree.close().await.unwrap();
	}

	// Reopen - should handle empty WAL gracefully
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		RecoveryTestHelper::verify_key(&tree, "foo", "v1").await;

		tree.close().await.unwrap();
	}
}

/// Test 6: Verify File Count After Recovery
#[test(tokio::test)]
async fn test_file_count_after_recovery() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let wal_dir = path.join("wal");
	let sst_dir = path.join("sst");
	let store: Arc<dyn object_store::ObjectStore> =
		Arc::new(object_store::local::LocalFileSystem::new_with_prefix(path.clone()).unwrap());

	// Write data causing multiple flushes
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| {
			b.with_max_memtable_size(4 * 1024) // Small enough to trigger ~5-6 automatic flushes with 150
			                          // entries
		})
		.await;

		for i in 0..150 {
			let key = format!("key_{:04}", i);
			let value = format!("value_{:04}", i);

			tree.set(key.as_bytes(), value.as_bytes()).await.unwrap();
		}

		let sst_count = RecoveryTestHelper::count_sst_files(&sst_dir);
		assert!(sst_count >= 1, "Should have created SST files");

		tree.close().await.unwrap();
	}

	// Reopen and verify file counts
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		let sst_count_after = RecoveryTestHelper::count_sst_files(&sst_dir);
		let wal_count_after = RecoveryTestHelper::count_wal_files(&wal_dir);

		// Verify SSTs remain
		assert!(sst_count_after >= 1, "SST files should exist after recovery");

		// Verify WAL files exist (unflushed data)
		assert!(wal_count_after >= 1, "WAL files should exist");

		// Verify all data accessible
		for i in 0..150 {
			let key = format!("key_{:04}", i);
			let result = tree.get(key.as_bytes()).await.unwrap();
			assert!(result.is_some(), "Key {} should exist after recovery", key);
		}

		tree.close().await.unwrap();
	}
}

/// Test 7: WAL Cleanup After Recovery Without Flush
#[test(tokio::test)]
async fn test_wal_cleanup_after_recovery_without_flush() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let wal_dir = path.join("wal");
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		tree.set(b"foo", b"v1").await.unwrap();

		tree.close().await.unwrap();
	}

	// Reopen - WAL replayed but not flushed
	let wal_count_before_flush;
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		wal_count_before_flush = RecoveryTestHelper::count_wal_files(&wal_dir);
		assert!(wal_count_before_flush > 0, "WAL files should exist after recovery");

		// Now flush
		tree.flush().await.unwrap();

		tree.close().await.unwrap();
	}

	// Reopen and verify old WALs cleaned up
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		let wal_count_after_flush = RecoveryTestHelper::count_wal_files(&wal_dir);

		// Old WALs should be cleaned (or at least not growing)
		assert!(
			wal_count_after_flush <= wal_count_before_flush + 1,
			"Old WALs should be cleaned up after flush"
		);

		RecoveryTestHelper::verify_key(&tree, "foo", "v1").await;

		tree.close().await.unwrap();
	}
}

/// Test 8: Mixed Flushed and Unflushed WALs
#[test(tokio::test)]
async fn test_mixed_flushed_and_unflushed_wals() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	{
		let tree =
			create_tree(path.clone(), Arc::clone(&store), |b| b.with_max_memtable_size(64 * 1024))
				.await; // Explicit flushes, no size pressure needed

		// Batch A - will be flushed
		for i in 0..10 {
			tree.set(format!("batch_a_{}", i).as_bytes(), b"value_a").await.unwrap();
		}
		tree.flush().await.unwrap();

		let log_number_after_flush = RecoveryTestHelper::get_manifest_log_number(&tree);

		// Batch B - stays in WAL segment after flush
		for i in 0..10 {
			tree.set(format!("batch_b_{}", i).as_bytes(), b"value_b").await.unwrap();
		}

		// Trigger rotation (creating new WAL segment)
		tree.flush().await.unwrap();

		// Batch C - stays in newest WAL segment
		for i in 0..10 {
			tree.set(format!("batch_c_{}", i).as_bytes(), b"value_c").await.unwrap();
		}

		// Close without flushing B and C
		tree.close().await.unwrap();

		log::info!("Log number after flush: {}", log_number_after_flush);
	}

	// Reopen and verify
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		// Batch A from SST
		for i in 0..10 {
			let key = format!("batch_a_{}", i);
			RecoveryTestHelper::verify_key(&tree, &key, "value_a").await;
		}

		// Batches B and C from WAL
		for i in 0..10 {
			let key = format!("batch_b_{}", i);
			RecoveryTestHelper::verify_key(&tree, &key, "value_b").await;
		}

		for i in 0..10 {
			let key = format!("batch_c_{}", i);
			RecoveryTestHelper::verify_key(&tree, &key, "value_c").await;
		}

		tree.close().await.unwrap();
	}
}

/// Test 9: Orphaned SST Cleanup
/// Note: This test verifies that orphaned SSTs don't break recovery
#[test(tokio::test)]
async fn test_orphaned_sst_doesnt_break_recovery() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let sst_dir = path.join("sstables");
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	// Phase 1: Create database and write data
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		tree.set(b"real_key", b"real_value").await.unwrap();

		tree.flush().await.unwrap();
		tree.close().await.unwrap();
	}

	// Manually create orphaned SST file (not in manifest)
	fs::create_dir_all(&sst_dir).ok();
	let orphan_sst_path = sst_dir.join("99999999999999999999.sst");
	fs::write(&orphan_sst_path, b"orphaned_data").unwrap();

	// Reopen - should handle orphan gracefully (cleanup or ignore)
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		// Real data should be there (orphan shouldn't affect recovery)
		RecoveryTestHelper::verify_key(&tree, "real_key", "real_value").await;

		tree.close().await.unwrap();
	}
}

/// Test 10: Manifest Log Number Progression
#[test(tokio::test)]
async fn test_manifest_log_number_progression() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	// Cycle 1: Write and flush
	{
		let tree =
			create_tree(path.clone(), Arc::clone(&store), |b| b.with_max_memtable_size(64 * 1024))
				.await; // Explicit flushes, no size pressure needed

		let log_num_initial = RecoveryTestHelper::get_manifest_log_number(&tree);

		for i in 0..10 {
			tree.set(format!("c1_key_{}", i).as_bytes(), b"cycle1").await.unwrap();
		}

		tree.flush().await.unwrap();

		let log_num_after_flush = RecoveryTestHelper::get_manifest_log_number(&tree);
		assert!(log_num_after_flush > log_num_initial, "Log number should advance after flush");

		tree.close().await.unwrap();
	}

	// Cycle 2: Reopen, write and flush
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		let log_num_before = RecoveryTestHelper::get_manifest_log_number(&tree);

		for i in 0..10 {
			tree.set(format!("c2_key_{}", i).as_bytes(), b"cycle2").await.unwrap();
		}

		tree.flush().await.unwrap();

		let log_num_after = RecoveryTestHelper::get_manifest_log_number(&tree);
		assert!(log_num_after > log_num_before, "Log number should advance again");

		tree.close().await.unwrap();
	}

	// Cycle 3: Reopen, write and flush again
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		let log_num_before = RecoveryTestHelper::get_manifest_log_number(&tree);

		for i in 0..10 {
			tree.set(format!("c3_key_{}", i).as_bytes(), b"cycle3").await.unwrap();
		}

		tree.flush().await.unwrap();

		let log_num_after = RecoveryTestHelper::get_manifest_log_number(&tree);

		// Log number should advance with each flush
		assert!(log_num_after > log_num_before, "Log number should advance with flush in cycle 3");

		tree.close().await.unwrap();
	}

	// Final reopen and verify all cycles
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		// Verify all data from all cycles
		for i in 0..10 {
			RecoveryTestHelper::verify_key(&tree, &format!("c1_key_{}", i), "cycle1").await;
			RecoveryTestHelper::verify_key(&tree, &format!("c2_key_{}", i), "cycle2").await;
			RecoveryTestHelper::verify_key(&tree, &format!("c3_key_{}", i), "cycle3").await;
		}

		tree.close().await.unwrap();
	}
}

/// Test 11: Recovery With No WAL Files (SST Only)
#[test(tokio::test)]
async fn test_recovery_with_no_wal_files() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let wal_dir = path.join("wal");
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	// Create data and flush
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		for i in 0..20 {
			tree.set(format!("key_{}", i).as_bytes(), b"value").await.unwrap();
		}

		tree.flush().await.unwrap();
		tree.close().await.unwrap();
	}

	// Manually delete all WAL files (simulating cleanup)
	if wal_dir.exists() {
		for entry in fs::read_dir(&wal_dir).unwrap().flatten() {
			if entry.path().extension().and_then(|s| s.to_str()) == Some("wal") {
				fs::remove_file(entry.path()).ok();
			}
		}
	}

	let wal_count_after_delete = RecoveryTestHelper::count_wal_files(&wal_dir);
	assert_eq!(wal_count_after_delete, 0, "All WAL files should be deleted");

	// Reopen - should recover from SST only
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		// Verify all data from SST
		for i in 0..20 {
			let key = format!("key_{}", i);
			RecoveryTestHelper::verify_key(&tree, &key, "value").await;
		}

		// New WAL should be created for new writes
		tree.set(b"new_key", b"new_value").await.unwrap();

		let wal_count_after_write = RecoveryTestHelper::count_wal_files(&wal_dir);
		assert!(wal_count_after_write > 0, "New WAL should be created");

		tree.close().await.unwrap();
	}
}

/// Test 12: Corrupted WAL But SST Intact
#[test(tokio::test)]
async fn test_corrupted_wal_with_valid_sst() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let wal_dir = path.join("wal");
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		// Write and flush to SST (data safe)
		for i in 0..10 {
			tree.set(format!("sst_key_{}", i).as_bytes(), b"safe_in_sst").await.unwrap();
		}
		tree.flush().await.unwrap();

		// Write more data to WAL
		for i in 0..5 {
			tree.set(format!("wal_key_{}", i).as_bytes(), b"in_wal").await.unwrap();
		}

		tree.close().await.unwrap();
	}

	// Corrupt the WAL file
	if let Some(wal_file) = fs::read_dir(&wal_dir)
		.ok()
		.and_then(|mut entries| entries.find_map(|e| e.ok()))
		.filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("wal"))
	{
		let mut file = fs::OpenOptions::new().write(true).open(wal_file.path()).unwrap();
		use std::io::{Seek, SeekFrom, Write};
		file.seek(SeekFrom::Start(50)).unwrap();
		file.write_all(&[0xFF, 0xAA, 0x55]).unwrap();
	}

	// Reopen - should handle corruption
	{
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| b).await;

		// SST data should be recovered successfully
		for i in 0..10 {
			let key = format!("sst_key_{}", i);
			RecoveryTestHelper::verify_key(&tree, &key, "safe_in_sst").await;
		}

		// WAL data may be partially recovered or repaired
		// At minimum, SST data should be intact

		tree.close().await.unwrap();
	}
}

// =============================================================================
// Recovery Tests During Compaction
// =============================================================================

/// Test: data integrity survives compaction + close + reopen.
/// Uses small memtable to force multiple flushes and compaction, then
/// verifies all keys survive recovery.
#[test(tokio::test)]
async fn test_recovery_after_compaction() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	let mut model: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();

	// Phase 1: Write enough data to trigger compaction (small memtable = 4KB)
	{
		let tree =
			create_tree(path.clone(), Arc::clone(&store), |b| b.with_max_memtable_size(4 * 1024))
				.await;

		for i in 0..100u32 {
			let key = format!("key_{i:04}");
			let value = format!("val_{i:04}");
			tree.set(key.as_bytes(), value.as_bytes()).await.unwrap();
			model.insert(key, value);
		}

		tree.close().await.unwrap();
	}

	// Phase 2: Reopen and verify ALL keys
	{
		let tree =
			create_tree(path.clone(), Arc::clone(&store), |b| b.with_max_memtable_size(4 * 1024))
				.await;

		for (key, value) in &model {
			RecoveryTestHelper::verify_key(&tree, key, value).await;
		}

		// Also verify via range scan
		let snap = tree.new_snapshot();
		let mut iter = snap.range(None, None).unwrap();
		iter.seek_first().await.unwrap();
		let mut count = 0;
		while iter.valid() {
			count += 1;
			iter.next().await.unwrap();
		}
		assert_eq!(count, model.len(), "Range scan count should match model after recovery");

		tree.close().await.unwrap();
	}
}

/// Multi-cycle recovery fuzz: random operations across 15 cycles with
/// close+reopen between each, verifying data integrity after each cycle.
#[test(tokio::test)]
async fn test_multi_cycle_recovery_fuzz() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().to_path_buf();
	let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

	let mut model: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = std::collections::BTreeMap::new();
	let mut rng = fastrand::Rng::with_seed(42);

	for cycle in 0..15u32 {
		// Open store
		let tree = create_tree(path.clone(), Arc::clone(&store), |b| {
			b.with_max_memtable_size(4 * 1024) // Small memtable to trigger flushes
		})
		.await;

		// Random operations
		let num_ops = rng.usize(10..=30);
		for _ in 0..num_ops {
			let op = rng.u8(0..100);
			let key_num = rng.u8(0..50);
			let key = format!("k{key_num:02}").into_bytes();

			if op < 70 {
				// SET
				let value = format!("v{cycle}_{key_num}").into_bytes();
				tree.set(&key, &value).await.unwrap();
				model.insert(key, value);
			} else if op < 85 {
				// DELETE
				tree.delete(&key).await.unwrap();
				model.remove(&key);
			} else {
				// GET + verify
				let tree_val = tree.get(&key).await.unwrap();
				let model_val = model.get(&key);
				match (tree_val.as_ref(), model_val) {
					(Some(tv), Some(mv)) => assert_eq!(
						tv.as_slice(),
						mv.as_slice(),
						"cycle {cycle}: get mismatch for key {:?}",
						String::from_utf8_lossy(&key)
					),
					(None, None) => {}
					(tv, mv) => panic!(
						"cycle {cycle}: existence mismatch for key {:?}: tree={}, model={}",
						String::from_utf8_lossy(&key),
						tv.is_some(),
						mv.is_some()
					),
				}
			}
		}

		// Close (graceful shutdown)
		tree.close().await.unwrap();

		// Reopen and verify 5 random keys from model
		let tree2 =
			create_tree(path.clone(), Arc::clone(&store), |b| b.with_max_memtable_size(4 * 1024))
				.await;

		let model_keys: Vec<_> = model.keys().cloned().collect();
		let check_count = 5.min(model_keys.len());
		for _ in 0..check_count {
			let idx = rng.usize(0..model_keys.len());
			let key = &model_keys[idx];
			let expected = &model[key];
			let actual = tree2.get(key).await.unwrap();
			assert_eq!(
				actual.as_deref(),
				Some(expected.as_slice()),
				"cycle {cycle}: recovery mismatch for key {:?}",
				String::from_utf8_lossy(key)
			);
		}

		tree2.close().await.unwrap();
	}

	// Final cycle: full scan comparison against model
	let tree =
		create_tree(path.clone(), Arc::clone(&store), |b| b.with_max_memtable_size(4 * 1024)).await;

	for (key, value) in &model {
		let actual = tree.get(key).await.unwrap();
		assert_eq!(
			actual.as_deref(),
			Some(value.as_slice()),
			"final: mismatch for key {:?}",
			String::from_utf8_lossy(key)
		);
	}

	// Verify no extra keys via scan
	let snap = tree.new_snapshot();
	let mut iter = snap.range(None, None).unwrap();
	iter.seek_first().await.unwrap();
	let mut count = 0;
	while iter.valid() {
		let key = iter.key().user_key().to_vec();
		assert!(
			model.contains_key(&key),
			"final: unexpected key in tree: {:?}",
			String::from_utf8_lossy(&key)
		);
		count += 1;
		iter.next().await.unwrap();
	}
	assert_eq!(count, model.len(), "final: scan count mismatch");

	tree.close().await.unwrap();
}
