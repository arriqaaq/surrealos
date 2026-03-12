//! Tests for the versioned iterator functionality.
//!
//! Tests run with LSM-only iteration to verify versioned query behavior.

use tempdir::TempDir;
use test_log::test;

use crate::{Key, LSMIterator, Options, Result, TreeBuilder, Value};

fn create_temp_directory() -> TempDir {
	TempDir::new("test").unwrap()
}

/// Create a store with versioning enabled
async fn create_versioned_store() -> (crate::Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let opts = Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().await.unwrap();
	(tree, temp_dir)
}

/// Create a store without versioning enabled
async fn create_store_no_versioning() -> (crate::Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let opts = Options::new().with_path(temp_dir.path().to_path_buf());
	let tree = TreeBuilder::with_options(opts).build().await.unwrap();
	(tree, temp_dir)
}

/// Collects all entries from a history iterator
async fn collect_history_all(iter: &mut impl LSMIterator) -> Result<Vec<(Key, Value, u64, bool)>> {
	iter.seek_first().await?;
	let mut result = Vec::new();
	while iter.valid() {
		let key_ref = iter.key();
		let is_tombstone = key_ref.is_tombstone();
		// Tombstones have no value, so use empty vec
		let value = if is_tombstone {
			Vec::new()
		} else {
			iter.value()?
		};
		result.push((key_ref.user_key().to_vec(), value, key_ref.timestamp(), is_tombstone));
		iter.next().await?;
	}
	Ok(result)
}

/// Collects all entries from a history iterator in reverse
async fn collect_history_reverse(
	iter: &mut impl LSMIterator,
) -> Result<Vec<(Key, Value, u64, bool)>> {
	iter.seek_last().await?;
	let mut result = Vec::new();
	while iter.valid() {
		let key_ref = iter.key();
		let is_tombstone = key_ref.is_tombstone();
		// Tombstones have no value, so use empty vec
		let value = if is_tombstone {
			Vec::new()
		} else {
			iter.value()?
		};
		result.push((key_ref.user_key().to_vec(), value, key_ref.timestamp(), is_tombstone));
		if !iter.prev().await? {
			break;
		}
	}
	Ok(result)
}

// ============================================================================
// Test 1: Multiple Versions Per Key
// ============================================================================

#[test(tokio::test)]
async fn test_history_multiple_versions_single_key() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 three times with different values
	store.set_at(b"key1", b"value1_v1", 100).await.unwrap();
	store.set_at(b"key1", b"value1_v2", 200).await.unwrap();
	store.set_at(b"key1", b"value1_v3", 300).await.unwrap();

	// Query versioned range
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	// Should have all 3 versions, ordered by seq_num descending (newest first)
	assert_eq!(results.len(), 3, "Should have 3 versions");
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[0].1, b"value1_v3");
	assert_eq!(results[0].2, 300);
	assert_eq!(results[1].0, b"key1");
	assert_eq!(results[1].1, b"value1_v2");
	assert_eq!(results[1].2, 200);
	assert_eq!(results[2].0, b"key1");
	assert_eq!(results[2].1, b"value1_v1");
	assert_eq!(results[2].2, 100);
}

// ============================================================================
// Test 2: Multiple Keys with Multiple Versions
// ============================================================================

#[test(tokio::test)]
async fn test_history_multiple_keys_multiple_versions() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 (2 versions)
	store.set_at(b"key1", b"key1_v1", 100).await.unwrap();
	store.set_at(b"key1", b"key1_v2", 200).await.unwrap();

	// Insert key2 (1 version)
	store.set_at(b"key2", b"key2_v1", 150).await.unwrap();

	// Insert key3 (3 versions)
	store.set_at(b"key3", b"key3_v1", 50).await.unwrap();
	store.set_at(b"key3", b"key3_v2", 250).await.unwrap();
	store.set_at(b"key3", b"key3_v3", 350).await.unwrap();

	// Query all keys
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	// Should have 6 total versions
	assert_eq!(results.len(), 6, "Should have 6 total versions");

	// Verify ordering: grouped by key, newest first within each key
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[0].1, b"key1_v2");
	assert_eq!(results[1].0, b"key1");
	assert_eq!(results[1].1, b"key1_v1");
	assert_eq!(results[2].0, b"key2");
	assert_eq!(results[2].1, b"key2_v1");
	assert_eq!(results[3].0, b"key3");
	assert_eq!(results[3].1, b"key3_v3");
	assert_eq!(results[4].0, b"key3");
	assert_eq!(results[4].1, b"key3_v2");
	assert_eq!(results[5].0, b"key3");
	assert_eq!(results[5].1, b"key3_v1");
}

// ============================================================================
// Test 3: Tombstones - Without Include Tombstones
// ============================================================================

#[test(tokio::test)]
async fn test_history_excludes_tombstones() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 v1
	store.set_at(b"key1", b"value1", 100).await.unwrap();

	// Delete key1
	store.delete(b"key1").await.unwrap();

	// Insert key2
	store.set_at(b"key2", b"value2", 200).await.unwrap();

	// Query with history (excludes tombstones by default)
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	// Hard delete hides ALL versions of key1, only key2 visible
	assert_eq!(results.len(), 1, "Only key2 visible");

	for result in &results {
		assert!(!result.3, "No entry should be a tombstone");
	}

	assert_eq!(results[0].0, b"key2");
	assert_eq!(results[0].1, b"value2");
}

// ============================================================================
// Test 4: Tombstones - With Include Tombstones
// ============================================================================

#[test(tokio::test)]
async fn test_history_with_tombstones() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 v1
	store.set_at(b"key1", b"value1", 100).await.unwrap();

	// Delete key1 (creates tombstone)
	store.delete(b"key1").await.unwrap();

	// Insert key2
	store.set_at(b"key2", b"value2", 200).await.unwrap();

	// Query with history (includes tombstones)
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), true, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	// Hard delete hides ALL versions of key1, even with include_tombstones=true
	assert_eq!(results.len(), 1, "Only key2 visible");
	assert_eq!(results[0].0, b"key2");
	assert!(!results[0].3, "key2 should not be a tombstone");
}

// ============================================================================
// Test 5: Multiple Versions
// ============================================================================

#[test(tokio::test)]
async fn test_history_multiple_versions() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1=v1
	store.set_at(b"key1", b"v1", 100).await.unwrap();

	// Update key1=v2
	store.set_at(b"key1", b"v2", 200).await.unwrap();

	// Update key1=v3
	store.set_at(b"key1", b"v3", 300).await.unwrap();

	// Query all versions
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert_eq!(results.len(), 3, "All 3 versions visible");
	assert_eq!(results[0].1, b"v3");
	assert_eq!(results[1].1, b"v2");
	assert_eq!(results[2].1, b"v1");
}

// ============================================================================
// Test 7: Bounds - Inclusive Start, Exclusive End
// ============================================================================

#[test(tokio::test)]
async fn test_history_bounds() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert keys a through e
	for key in [b"key_a", b"key_b", b"key_c", b"key_d", b"key_e"] {
		store.set_at(key, b"value", 100).await.unwrap();
	}

	// Query range [key_b, key_d) - should include key_b and key_c, NOT key_d
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key_b"), Some(b"key_d"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert_eq!(results.len(), 2, "Should have 2 entries");
	assert_eq!(results[0].0, b"key_b");
	assert_eq!(results[1].0, b"key_c");

	store.close().await.unwrap();
}

// ============================================================================
// Test 8: Bounds - Empty Range
// ============================================================================

#[test(tokio::test)]
async fn test_history_empty_range() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key_a and key_b
	let mut batch = store.new_batch();
	batch.set_at(b"key_a", b"value_a", 100).unwrap();
	batch.set_at(b"key_b", b"value_b", 100).unwrap();
	store.apply(batch, false).await.unwrap();

	// Query range [key_c, key_d) - no matching keys
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key_c"), Some(b"key_d"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert!(results.is_empty(), "Should have no entries");

	store.close().await.unwrap();
}

// ============================================================================
// Test 9: Bounds - Single Key Match
// ============================================================================

#[test(tokio::test)]
async fn test_history_single_key_match() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key_a (2 versions)
	store.set_at(b"key_a", b"v1", 100).await.unwrap();
	store.set_at(b"key_a", b"v2", 200).await.unwrap();

	// Insert key_b and key_c
	let mut batch = store.new_batch();
	batch.set_at(b"key_b", b"value_b", 150).unwrap();
	batch.set_at(b"key_c", b"value_c", 150).unwrap();
	store.apply(batch, false).await.unwrap();

	// Query range that only matches key_a
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key_a"), Some(b"key_b"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert_eq!(results.len(), 2, "Should have both versions");
	assert_eq!(results[0].0, b"key_a");
	assert_eq!(results[0].1, b"v2");
	assert_eq!(results[1].0, b"key_a");
	assert_eq!(results[1].1, b"v1");
}

// ============================================================================
// Test 10: Interleaved Iteration - Forward/Backward
// ============================================================================

#[test(tokio::test)]
async fn test_history_interleaved_iteration() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 (2 versions) and key2 (2 versions)
	store.set_at(b"key1", b"key1_v1", 100).await.unwrap();
	store.set_at(b"key1", b"key1_v2", 200).await.unwrap();
	store.set_at(b"key2", b"key2_v1", 150).await.unwrap();
	store.set_at(b"key2", b"key2_v2", 250).await.unwrap();

	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();

	// Test forward iteration
	iter.seek_first().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");
	assert_eq!(iter.value().unwrap(), b"key1_v2".to_vec());

	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");
	assert_eq!(iter.value().unwrap(), b"key1_v1".to_vec());

	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v2".to_vec());

	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v1".to_vec());

	// Test backward iteration starting from last
	iter.seek_last().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v1".to_vec());

	iter.prev().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v2".to_vec());

	iter.prev().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");
	assert_eq!(iter.value().unwrap(), b"key1_v1".to_vec());
}

// ============================================================================
// Test 11: Interleaved Iteration - Seek in Middle
// ============================================================================

#[test(tokio::test)]
async fn test_history_seek_middle() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert 5 keys
	for i in 1..=5 {
		let key = format!("key{i}");
		let value = format!("value{i}");
		store.set_at(key.as_bytes(), value.as_bytes(), i as u64 * 100).await.unwrap();
	}

	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();

	// Forward iteration should return keys in order
	let results = collect_history_all(&mut iter).await.unwrap();
	assert_eq!(results.len(), 5);
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[1].0, b"key2");
	assert_eq!(results[2].0, b"key3");
	assert_eq!(results[3].0, b"key4");
	assert_eq!(results[4].0, b"key5");

	// Test seek_first after collection
	iter.seek_first().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");

	// Navigate forward
	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");

	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key3");
}

// ============================================================================
// Test 12: Backward Iteration Only
// ============================================================================

#[test(tokio::test)]
async fn test_history_backward_iteration() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert multiple keys with multiple versions
	store.set_at(b"key1", b"key1_v1", 100).await.unwrap();
	store.set_at(b"key1", b"key1_v2", 200).await.unwrap();
	store.set_at(b"key2", b"key2_v1", 150).await.unwrap();

	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();
	let results = collect_history_reverse(&mut iter).await.unwrap();

	assert_eq!(results.len(), 3, "Should have 3 entries");

	// Reverse order: key2_v1, key1_v1, key1_v2
	assert_eq!(results[0].0, b"key2");
	assert_eq!(results[0].1, b"key2_v1");
	assert_eq!(results[1].0, b"key1");
	assert_eq!(results[1].1, b"key1_v1");
	assert_eq!(results[2].0, b"key1");
	assert_eq!(results[2].1, b"key1_v2");
}

// ============================================================================
// Test 13: Snapshot Isolation - Versions Not Visible to Old Snapshots
// ============================================================================

#[test(tokio::test)]
async fn test_history_snapshot_isolation() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 v1
	store.set_at(b"key1", b"v1", 100).await.unwrap();

	// Take snapshot1
	let snap1 = store.new_snapshot();

	// Insert key1 v2
	store.set_at(b"key1", b"v2", 200).await.unwrap();

	// Take snapshot2
	let snap2 = store.new_snapshot();

	// snapshot1 should only see v1
	{
		let mut iter = snap1.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
		let results = collect_history_all(&mut iter).await.unwrap();
		assert_eq!(results.len(), 1, "snapshot1 should see 1 version");
		assert_eq!(results[0].1, b"v1");
	}

	// snapshot2 should see both v1 and v2
	{
		let mut iter = snap2.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
		let results = collect_history_all(&mut iter).await.unwrap();
		assert_eq!(results.len(), 2, "snapshot2 should see 2 versions");
		assert_eq!(results[0].1, b"v2");
		assert_eq!(results[1].1, b"v1");
	}
	store.close().await.unwrap();
}

// ============================================================================
// Test 14: Large Number of Versions
// ============================================================================

#[test(tokio::test)]
async fn test_history_many_versions() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert same key 100 times with different values
	for i in 1..=100 {
		let value = format!("value_{i:03}");
		store.set_at(b"key1", value.as_bytes(), i as u64 * 10).await.unwrap();
	}

	// Query all versions
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert_eq!(results.len(), 100, "Should have all 100 versions");
	assert_eq!(results[0].1, b"value_100");
	assert_eq!(results[99].1, b"value_001");

	store.close().await.unwrap();
}

// ============================================================================
// Test 15: Mixed Timestamps
// ============================================================================

#[test(tokio::test)]
async fn test_history_timestamps() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key with timestamp 50, then 100, then 200
	store.set_at(b"key1", b"ts_50", 50).await.unwrap();
	store.set_at(b"key1", b"ts_100", 100).await.unwrap();
	store.set_at(b"key1", b"ts_200", 200).await.unwrap();

	// Query versions
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert_eq!(results.len(), 3, "Should have 3 versions");

	// Verify ordering is by timestamp descending (B+tree uses TimestampComparator)
	assert_eq!(results[0].1, b"ts_200");
	assert_eq!(results[0].2, 200);
	assert_eq!(results[1].1, b"ts_100");
	assert_eq!(results[1].2, 100);
	assert_eq!(results[2].1, b"ts_50");
	assert_eq!(results[2].2, 50);

	store.close().await.unwrap();
}

// ============================================================================
// Test 17: get_at Fallback
// ============================================================================

#[test(tokio::test)]
async fn test_get_at_fallback() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key at ts=100
	store.set_at(b"key1", b"value_100", 100).await.unwrap();

	// Insert key at ts=200
	store.set_at(b"key1", b"value_200", 200).await.unwrap();

	let snap = store.new_snapshot();

	let result = snap.get_at(b"key1", 150).await.unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_100");

	let result = snap.get_at(b"key1", 200).await.unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_200");

	let result = snap.get_at(b"key1", 50).await.unwrap();
	assert!(result.is_none());

	let result = snap.get_at(b"key1", 250).await.unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_200");

	store.close().await.unwrap();
}

// ============================================================================
// Test 19: Error Handling - Versioning Disabled
// ============================================================================

#[test(tokio::test)]
async fn test_history_requires_versioning() {
	// Create store WITHOUT versioning enabled
	let (store, _temp_dir) = create_store_no_versioning().await;

	// Insert some data
	store.set(b"key1", b"value1").await.unwrap();

	// Try to call history - should return error
	let snap = store.new_snapshot();
	let result = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None);

	assert!(result.is_err(), "history should fail without versioning enabled");
}

// ============================================================================
// Test: Versions survive memtable flush (bug detector)
// ============================================================================

#[test(tokio::test)]
async fn test_history_survives_memtable_flush() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		// Insert key1 three times with different values
		for i in 1..=3 {
			let value = format!("v{i}");
			store.set_at(b"key1", value.as_bytes(), i as u64 * 100).await.unwrap();
		}

		// Verify all 3 versions exist in memtable BEFORE flush
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();
			assert_eq!(results.len(), 3, "Should have 3 versions before flush");
		}

		// FORCE MEMTABLE FLUSH
		{
			// Query versions AFTER flush
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 3, "All 3 versions should survive flush");
			assert_eq!(results[0].1, b"v3");
			assert_eq!(results[1].1, b"v2");
			assert_eq!(results[2].1, b"v1");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: Versions survive compaction (L0 -> L1)
// ============================================================================

#[test(tokio::test)]
async fn test_versions_survive_compaction() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		// Insert key1 with multiple versions
		for i in 1..=5 {
			let value = format!("v{i}");
			store.set_at(b"key1", value.as_bytes(), i as u64 * 100).await.unwrap();
		}

		// First flush (memtable -> L0)
		// Verify versions after first flush
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();
			assert_eq!(results.len(), 5, "All 5 versions should survive L0 flush");
		}

		// Insert more versions to trigger another flush
		for i in 6..=10 {
			let value = format!("v{i}");
			store.set_at(b"key1", value.as_bytes(), i as u64 * 100).await.unwrap();
		}

		// Second flush
		{
			// Verify all 10 versions exist across L0 files
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 10, "All 10 versions should survive");
			assert_eq!(results[0].1, b"v10");
			assert_eq!(results[9].1, b"v1");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: history() forward and backward iteration
// ============================================================================

#[test(tokio::test)]
async fn test_history_bidirectional_iteration() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		// Insert multiple keys with multiple versions
		store.set_at(b"key1", b"key1_v1", 100).await.unwrap();
		store.set_at(b"key1", b"key1_v2", 200).await.unwrap();
		store.set_at(b"key2", b"key2_v1", 150).await.unwrap();
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();

			let forward_results = collect_history_all(&mut iter).await.unwrap();
			assert_eq!(forward_results.len(), 3);

			let reverse_results = collect_history_reverse(&mut iter).await.unwrap();
			assert_eq!(reverse_results.len(), 3);

			assert_eq!(forward_results[0].0, reverse_results[2].0);
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: ts_range filtering
// ============================================================================

#[test(tokio::test)]
async fn test_history_ts_range_forward() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		store.set_at(b"key1", b"v100", 100).await.unwrap();
		store.set_at(b"key1", b"v200", 200).await.unwrap();
		store.set_at(b"key1", b"v300", 300).await.unwrap();
		store.set_at(b"key1", b"v400", 400).await.unwrap();
		{
			// Query with ts_range [150, 350] - should only return v200 and v300
			let snap = store.new_snapshot();
			let mut iter = snap
				.history_iter(Some(b"key1"), Some(b"key2"), false, Some((150, 350)), None)
				.unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 2, "Should have 2 versions in range");
			assert_eq!(results[0].2, 300);
			assert_eq!(results[1].2, 200);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_ts_range_backward() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		store.set_at(b"key1", b"v100", 100).await.unwrap();
		store.set_at(b"key1", b"v200", 200).await.unwrap();
		store.set_at(b"key1", b"v300", 300).await.unwrap();
		{
			// Query with ts_range [150, 250] - should only return v200
			let snap = store.new_snapshot();
			let mut iter = snap
				.history_iter(Some(b"key1"), Some(b"key2"), false, Some((150, 250)), None)
				.unwrap();
			let results = collect_history_reverse(&mut iter).await.unwrap();

			assert_eq!(results.len(), 1, "Should have 1 version in range");
			assert_eq!(results[0].2, 200);
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: limit filtering
// ============================================================================

#[test(tokio::test)]
async fn test_history_limit_forward() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		for i in 1..=5 {
			store.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).await.unwrap();
		}
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, Some(3)).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 3, "Should have 3 entries");
			assert_eq!(results[0].2, 500);
			assert_eq!(results[1].2, 400);
			assert_eq!(results[2].2, 300);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_limit_backward() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		for i in 1..=5 {
			store.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).await.unwrap();
		}
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, Some(2)).unwrap();
			let results = collect_history_reverse(&mut iter).await.unwrap();

			assert_eq!(results.len(), 2, "Should have 2 entries");
			assert_eq!(results[0].2, 100);
			assert_eq!(results[1].2, 200);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_limit_multiple_keys() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		store.set_at(b"key1", b"v1", 100).await.unwrap();
		store.set_at(b"key1", b"v2", 200).await.unwrap();
		store.set_at(b"key2", b"v1", 150).await.unwrap();
		store.set_at(b"key2", b"v2", 250).await.unwrap();
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, Some(3)).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 3, "Should have 3 entries across keys");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: ts_range + limit combination
// ============================================================================

#[test(tokio::test)]
async fn test_history_ts_range_with_limit() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		for i in 1..=10 {
			store.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).await.unwrap();
		}
		{
			let snap = store.new_snapshot();
			let mut iter = snap
				.history_iter(Some(b"key1"), Some(b"key2"), false, Some((300, 800)), Some(3))
				.unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 3, "Should have 3 entries (limited)");
			assert_eq!(results[0].2, 800);
			assert_eq!(results[1].2, 700);
			assert_eq!(results[2].2, 600);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_limit_zero() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		store.set_at(b"key1", b"v1", 100).await.unwrap();
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, Some(0)).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 0, "Should have 0 entries");
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_ts_range_empty_result() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		store.set_at(b"key1", b"v100", 100).await.unwrap();
		store.set_at(b"key1", b"v200", 200).await.unwrap();
		{
			let snap = store.new_snapshot();
			let mut iter = snap
				.history_iter(Some(b"key1"), Some(b"key2"), false, Some((500, 600)), None)
				.unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 0, "Should have 0 entries");
		}

		store.close().await.unwrap();
	}
}

// ==================== RYOW (Read Your Own Writes) Tests ====================

/// Test get_at RYOW: uncommitted writes should be visible
/// Test get_at RYOW: future timestamp writes should not be visible
/// Test get_at RYOW with tombstone
/// Test history iterator RYOW: uncommitted writes appear in history
/// Test history RYOW with timestamp collision: write set wins
/// Test history RYOW with timestamp range filtering
/// Test history RYOW soft delete (tombstone) handling
/// Test history RYOW hard delete handling - wipes all history
/// Test get_at RYOW with hard delete - returns None regardless of timestamp
///
/// History Iterator Bounds Tests
///
/// Test that forward iteration respects lower bound when keys exist before the range.
/// Regression test: keys with user_key lexicographically before lower bound should not be returned.
/// Test that backward iteration respects upper bound when keys exist after the range.
/// Regression test: keys with user_key lexicographically after upper bound should not be returned.
/// Test that both bounds are respected with keys outside both ends of the range.
/// Test bounds with timestamp ranges - keys outside range but within timestamp should be excluded.
/// Test direction switching (forward to backward) respects bounds.
/// Test direction switching (backward to forward) respects bounds.
/// Test direction switching with keys that have many versions.
/// Verifies that prev/next correctly navigate across multi-version keys.
/// Test seeking to a key below lower_bound.
/// Test seeking to a key at or above upper_bound.
/// Test seeking to exact bound values.
/// Test key exactly at lower_bound is included (inclusive).
/// Test key exactly at upper_bound is excluded (exclusive).
/// Test adjacent byte boundaries with special byte values.
/// Test tombstone (soft delete) at lower bound.
/// Test hard delete at boundary.
/// Test replace operation at boundary.
/// Test equal lower and upper bounds (empty range).
/// Test inverted bounds (lower > upper).
/// Test single key in range.
/// Test all keys outside range.
/// Test bounds with limit (forward).
/// Test bounds with limit (backward).
/// Test bounds with timestamp range and limit combined.
/// Test prefix pattern iteration (common use case).
/// Test keys and bounds containing null bytes.
/// Test bounds with maximum byte values (0xFF).
// Test for https://github.com/surrealdb/surrealkv/issues/364
#[tokio::test(flavor = "current_thread")]
async fn repro() {
	let tmp = tempfile::Builder::new().prefix("tc-").tempdir().unwrap();

	let dir = std::path::PathBuf::from(tmp.path());

	let opts = Options::new().with_path(dir).with_versioning(true, 0).with_l0_no_compression();

	let store = TreeBuilder::with_options(opts).build().await.unwrap();

	let sync_key = b"#@prefix:sync\x00\x00\x00\x00\x00\x00\x00\x02****************";
	let table_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10cccccccccccccccc";
	let before_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10kkkkkkkkkkkkkkkk";
	let after_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10MMMMMMMMMMMMMMMM";

	store.set_at(before_key, b"value", 1).await.unwrap();

	{
		let mut batch = store.new_batch();
		batch.set_at(sync_key, b"value", 2).unwrap();
		batch.set_at(table_key, b"value", 2).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	store.set_at(after_key, b"value", 3).await.unwrap();

	let snap = store.new_snapshot();

	let lower = b"@prefix:\x00";
	let upper = b"@prefix:\xFF";
	let mut it = snap
		.history_iter(Some(lower.as_slice()), Some(upper.as_slice()), true, Some((2, 2)), None)
		.unwrap();

	let mut seen = vec![];

	if it.seek_first().await.unwrap() {
		while it.valid() {
			let key = it.key();
			let user_key = String::from_utf8_lossy(key.user_key()).to_string();
			eprintln!("\t{}: {}", key.timestamp(), user_key);
			seen.push(user_key);

			it.next().await.unwrap();
		}
	}

	// Expected: only the @prefix key should be returned for this range.
	// Current behavior: this also returns #@prefix:* and reproduces the bug.
	assert_eq!(seen.len(), 1, "unexpected keys at ts=2 in scope range: {seen:?}");
}

// ============================================================================
// History Iterator Bounds Tests
// ============================================================================

/// Test that forward iteration respects lower bound when keys exist before the range.
/// Regression test: keys with user_key lexicographically before lower bound should not be returned.
#[test(tokio::test)]
async fn test_history_forward_respects_lower_bound() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Key starting with '#' (ASCII 35) comes before '@' (ASCII 64)
	let sync_key = b"#@prefix:sync\x00\x00\x00\x00\x00\x00\x00\x02****************";
	let table_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10cccccccccccccccc";
	let before_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10kkkkkkkkkkkkkkkk";
	let after_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10MMMMMMMMMMMMMMMM";

	store.set_at(before_key, b"value", 1).await.unwrap();

	{
		let mut batch = store.new_batch();
		batch.set_at(sync_key, b"value", 2).unwrap();
		batch.set_at(table_key, b"value", 2).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	store.set_at(after_key, b"value", 3).await.unwrap();

	let snap = store.new_snapshot();

	// Query range starting with '@' should NOT return '#@prefix:sync'
	let lower = b"@prefix:\x00";
	let upper = b"@prefix:\xFF";
	let mut iter = snap
		.history_iter(Some(lower.as_slice()), Some(upper.as_slice()), true, Some((2, 2)), None)
		.unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();

	// Only table_key should be returned (it's the only key within range at ts=2)
	assert_eq!(
		results.len(),
		1,
		"Only keys within range should be returned, got: {:?}",
		results.iter().map(|(k, _, _, _)| String::from_utf8_lossy(k)).collect::<Vec<_>>()
	);
	assert!(results[0].0.starts_with(b"@prefix:"), "Key should start with @prefix:");

	store.close().await.unwrap();
}

/// Test that backward iteration respects upper bound when keys exist after the range.
/// Regression test: keys with user_key lexicographically after upper bound should not be returned.
#[test(tokio::test)]
async fn test_history_backward_respects_upper_bound() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key_a", b"value_a", 1).unwrap();
		batch.set_at(b"key_b", b"value_b", 1).unwrap();
		batch.set_at(b"key_z", b"value_z", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();

	// Query range [key_a, key_c) should NOT return key_z
	let mut iter = snap
		.history_iter(Some(b"key_a".as_slice()), Some(b"key_c".as_slice()), true, None, None)
		.unwrap();

	// Iterate backward from the end
	iter.seek_last().await.unwrap();
	let mut results = Vec::new();
	while iter.valid() {
		let key_ref = iter.key();
		results.push(key_ref.user_key().to_vec());
		if !iter.prev().await.unwrap() {
			break;
		}
	}

	assert_eq!(
		results.len(),
		2,
		"Only keys within range should be returned, got: {:?}",
		results.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>()
	);
	// Backward iteration returns newest first, so key_b then key_a
	assert_eq!(results[0], b"key_b".to_vec(), "First should be key_b");
	assert_eq!(results[1], b"key_a".to_vec(), "Second should be key_a");

	store.close().await.unwrap();
}

/// Test that both bounds are respected with keys outside both ends of the range.
#[test(tokio::test)]
async fn test_history_respects_both_bounds() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"aa_before", b"v", 1).unwrap();
		batch.set_at(b"ab_before", b"v", 1).unwrap();
		batch.set_at(b"ma_inside", b"v", 1).unwrap();
		batch.set_at(b"mb_inside", b"v", 1).unwrap();
		batch.set_at(b"mc_inside", b"v", 1).unwrap();
		batch.set_at(b"za_after", b"v", 1).unwrap();
		batch.set_at(b"zb_after", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();

	// Query range [m, z) should only return keys starting with 'm'
	let mut iter =
		snap.history_iter(Some(b"m".as_slice()), Some(b"z".as_slice()), true, None, None).unwrap();

	// Test forward iteration
	let results = collect_history_all(&mut iter).await.unwrap();
	assert_eq!(
		results.len(),
		3,
		"Forward should return 3 keys in range, got: {:?}",
		results.iter().map(|(k, _, _, _)| String::from_utf8_lossy(k)).collect::<Vec<_>>()
	);
	for (key, _, _, _) in &results {
		assert!(
			key.as_slice() >= b"m".as_slice() && key.as_slice() < b"z".as_slice(),
			"Key {:?} should be in range [m, z)",
			String::from_utf8_lossy(key)
		);
	}

	// Test backward iteration
	let mut iter =
		snap.history_iter(Some(b"m".as_slice()), Some(b"z".as_slice()), true, None, None).unwrap();
	iter.seek_last().await.unwrap();
	let mut backward_results = Vec::new();
	while iter.valid() {
		backward_results.push(iter.key().user_key().to_vec());
		if !iter.prev().await.unwrap() {
			break;
		}
	}
	assert_eq!(
		backward_results.len(),
		3,
		"Backward should return 3 keys in range, got: {:?}",
		backward_results.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>()
	);
	for key in &backward_results {
		assert!(
			key.as_slice() >= b"m".as_slice() && key.as_slice() < b"z".as_slice(),
			"Key {:?} should be in range [m, z)",
			String::from_utf8_lossy(key)
		);
	}

	store.close().await.unwrap();
}

/// Test bounds with timestamp ranges - keys outside range but within timestamp should be excluded.
#[test(tokio::test)]
async fn test_history_bounds_with_timestamp_range() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		// All keys at timestamp 5
		batch.set_at(b"aaa_before", b"v", 5).unwrap();
		batch.set_at(b"mmm_inside", b"v", 5).unwrap();
		batch.set_at(b"zzz_after", b"v", 5).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();

	// Query with timestamp filter - should still respect bounds
	let mut iter = snap
		.history_iter(Some(b"m".as_slice()), Some(b"z".as_slice()), true, Some((5, 5)), None)
		.unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();
	assert_eq!(
		results.len(),
		1,
		"Only key_inside should be returned, got: {:?}",
		results.iter().map(|(k, _, _, _)| String::from_utf8_lossy(k)).collect::<Vec<_>>()
	);
	assert_eq!(results[0].0, b"mmm_inside".to_vec(), "Should return mmm_inside");

	store.close().await.unwrap();
}

/// Test direction switching (forward to backward) respects bounds.
#[test(tokio::test)]
async fn test_history_bounds_direction_switch_forward_to_backward() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		for key in [b"aa_before" as &[u8], b"mm_in1", b"mm_in2", b"mm_in3", b"zz_after"] {
			batch.set_at(key, b"value", 1).unwrap();
		}
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let lower: &[u8] = b"m";
	let upper: &[u8] = b"z";
	let mut iter = snap.history_iter(Some(lower), Some(upper), true, None, None).unwrap();

	// Forward: get first key
	iter.seek_first().await.unwrap();
	assert!(iter.valid(), "Should be valid after seek_first");
	let first_key = iter.key().user_key().to_vec();
	assert_eq!(first_key, b"mm_in1".to_vec(), "First key should be mm_in1");

	// Move forward one more
	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"mm_in2", "Second should be mm_in2");

	// Switch direction: go backward
	iter.prev().await.unwrap();
	assert!(iter.valid());
	let back_key = iter.key().user_key().to_vec();
	assert_eq!(back_key, b"mm_in1".to_vec(), "After prev should be mm_in1");

	// Continue backward - should become invalid (no more in-range keys)
	let has_more = iter.prev().await.unwrap();
	assert!(!has_more || !iter.valid(), "Should have no more keys before mm_in1");

	store.close().await.unwrap();
}

/// Test direction switching (backward to forward) respects bounds.
#[test(tokio::test)]
async fn test_history_bounds_direction_switch_backward_to_forward() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		for key in [b"aa_before" as &[u8], b"mm_in1", b"mm_in2", b"mm_in3", b"zz_after"] {
			batch.set_at(key, b"value", 1).unwrap();
		}
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let lower: &[u8] = b"m";
	let upper: &[u8] = b"z";
	let mut iter = snap.history_iter(Some(lower), Some(upper), true, None, None).unwrap();

	// Backward: get last key in range
	iter.seek_last().await.unwrap();
	assert!(iter.valid(), "Should be valid after seek_last");
	let last_key = iter.key().user_key().to_vec();
	assert_eq!(last_key, b"mm_in3".to_vec(), "Last key should be mm_in3");

	// Move backward one
	iter.prev().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"mm_in2", "Should be at mm_in2");

	// Switch direction: go forward
	iter.next().await.unwrap();
	assert!(iter.valid());
	let fwd_key = iter.key().user_key().to_vec();
	assert_eq!(fwd_key, b"mm_in3".to_vec(), "After next should be mm_in3");

	// Continue forward - should become invalid (no more in-range keys)
	let has_more = iter.next().await.unwrap();
	assert!(!has_more || !iter.valid(), "Should have no more keys after mm_in3");

	store.close().await.unwrap();
}

/// Test direction switching with keys that have many versions.
/// Verifies that prev/next correctly navigate across multi-version keys.
#[test(tokio::test)]
async fn test_history_direction_switch_multi_version_keys() {
	const VERSIONS_PER_KEY: u64 = 5;
	let (store, _temp_dir) = create_versioned_store().await;

	// Create 3 keys, each with many versions
	for key in [b"key_a" as &[u8], b"key_b", b"key_c"] {
		for v in 1..=VERSIONS_PER_KEY {
			store
				.set_at(
					key,
					format!("{}_v{}", std::str::from_utf8(key).unwrap(), v).as_bytes(),
					v * 100,
				)
				.await
				.unwrap();
		}
	}

	let snap = store.new_snapshot();
	let mut iter = snap
		.history_iter(Some(b"key_a".as_slice()), Some(b"key_d".as_slice()), true, None, None)
		.unwrap();

	// --- Forward: advance past key_a (5 versions) to key_b
	iter.seek_first().await.unwrap();
	assert_eq!(iter.key().user_key(), b"key_a", "First should be key_a");
	for _ in 0..VERSIONS_PER_KEY {
		iter.next().await.unwrap();
	}
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key_b", "After key_a versions should be key_b");

	// --- Switch backward: prev should go to last version of key_a
	iter.prev().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key_a", "After prev from key_b should be key_a");
	assert_eq!(iter.value().unwrap(), b"key_a_v1", "Should be oldest key_a");

	// --- Switch forward: next advances one entry (key_a v1 -> v2). Advance through key_a to
	// key_b.
	for _ in 0..VERSIONS_PER_KEY {
		iter.next().await.unwrap();
	}
	assert!(iter.valid());
	assert_eq!(
		iter.key().user_key(),
		b"key_b",
		"After advancing through key_a versions should be key_b"
	);

	// --- Forward: advance through key_b to key_c
	for _ in 0..VERSIONS_PER_KEY {
		iter.next().await.unwrap();
	}
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key_c", "After key_b versions should be key_c");

	// --- Backward: seek_last, then prev through key_c to key_b
	iter.seek_last().await.unwrap();
	assert_eq!(iter.key().user_key(), b"key_c", "Last should be key_c");
	for _ in 0..VERSIONS_PER_KEY {
		iter.prev().await.unwrap();
	}
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key_b", "After key_c versions should be key_b");

	// --- Switch forward: next advances one entry. Advance through key_b to key_c.
	for _ in 0..VERSIONS_PER_KEY {
		iter.next().await.unwrap();
	}
	assert!(iter.valid());
	assert_eq!(
		iter.key().user_key(),
		b"key_c",
		"After advancing through key_b versions should be key_c"
	);

	store.close().await.unwrap();
}

/// Test seeking to a key below lower_bound.
#[test(tokio::test)]
async fn test_history_seek_outside_lower_bound() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"aaa", b"v", 1).unwrap();
		batch.set_at(b"mmm", b"v", 1).unwrap();
		batch.set_at(b"zzz", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let mut iter =
		snap.history_iter(Some(b"m".as_slice()), Some(b"z".as_slice()), true, None, None).unwrap();

	// Seek to key below lower_bound - should position at first in-range key or become invalid
	iter.seek(b"aaa").await.unwrap();
	if iter.valid() {
		let key = iter.key().user_key();
		assert!(
			key >= b"m".as_slice(),
			"After seek below lower, key should be >= lower_bound, got: {:?}",
			String::from_utf8_lossy(key)
		);
	}

	store.close().await.unwrap();
}

/// Test seeking to a key at or above upper_bound.
#[test(tokio::test)]
async fn test_history_seek_outside_upper_bound() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"aaa", b"v", 1).unwrap();
		batch.set_at(b"mmm", b"v", 1).unwrap();
		batch.set_at(b"zzz", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();

	// Seek to key at upper_bound - should be invalid (upper is exclusive)
	let mut iter =
		snap.history_iter(Some(b"m".as_slice()), Some(b"z".as_slice()), true, None, None).unwrap();
	iter.seek(b"z").await.unwrap();
	assert!(!iter.valid(), "Seek to upper_bound should make iterator invalid");

	// Seek to key above upper_bound
	let mut iter =
		snap.history_iter(Some(b"m".as_slice()), Some(b"z".as_slice()), true, None, None).unwrap();
	iter.seek(b"zzz").await.unwrap();
	assert!(!iter.valid(), "Seek above upper_bound should make iterator invalid");

	store.close().await.unwrap();
}

/// Test seeking to exact bound values.
#[test(tokio::test)]
async fn test_history_seek_to_exact_bounds() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key_a", b"v", 1).unwrap();
		batch.set_at(b"key_m", b"v", 1).unwrap();
		batch.set_at(b"key_z", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();

	// Seek to exact lower_bound where key exists
	let mut iter = snap
		.history_iter(Some(b"key_m".as_slice()), Some(b"key_z".as_slice()), true, None, None)
		.unwrap();
	iter.seek(b"key_m").await.unwrap();
	assert!(iter.valid(), "Seek to lower_bound should be valid");
	assert_eq!(iter.key().user_key(), b"key_m", "Should be at key_m");

	// Seek to exact upper_bound - should be invalid (exclusive)
	iter.seek(b"key_z").await.unwrap();
	assert!(!iter.valid(), "Seek to upper_bound should be invalid (exclusive)");

	store.close().await.unwrap();
}

/// Test key exactly at lower_bound is included (inclusive).
#[test(tokio::test)]
async fn test_history_key_at_exact_lower_bound() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"lower", b"v", 1).unwrap();
		batch.set_at(b"middle", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let mut iter = snap
		.history_iter(Some(b"lower".as_slice()), Some(b"upper".as_slice()), true, None, None)
		.unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();
	assert!(
		results.iter().any(|(k, _, _, _)| k == b"lower"),
		"Key exactly at lower_bound should be included"
	);

	store.close().await.unwrap();
}

/// Test key exactly at upper_bound is excluded (exclusive).
#[test(tokio::test)]
async fn test_history_key_at_exact_upper_bound() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"middle", b"v", 1).unwrap();
		batch.set_at(b"upper", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let mut iter = snap
		.history_iter(Some(b"lower".as_slice()), Some(b"upper".as_slice()), true, None, None)
		.unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();
	assert!(
		!results.iter().any(|(k, _, _, _)| k == b"upper"),
		"Key exactly at upper_bound should be excluded"
	);
	assert!(
		results.iter().any(|(k, _, _, _)| k == b"middle"),
		"Key before upper_bound should be included"
	);

	store.close().await.unwrap();
}

/// Test adjacent byte boundaries with special byte values.
#[test(tokio::test)]
async fn test_history_adjacent_byte_boundaries() {
	let (store, _temp_dir) = create_versioned_store().await;

	let key_before = b"key";
	let key_at_lower = b"key\x00";
	let key_in_range = b"key\x01";
	let key_at_upper = b"key\x10";

	{
		let mut batch = store.new_batch();
		batch.set_at(key_before, b"v", 1).unwrap();
		batch.set_at(key_at_lower, b"v", 1).unwrap();
		batch.set_at(key_in_range, b"v", 1).unwrap();
		batch.set_at(key_at_upper, b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	// Range: [key\x00, key\x10)
	let mut iter = snap
		.history_iter(Some(b"key\x00".as_slice()), Some(b"key\x10".as_slice()), true, None, None)
		.unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();

	// key\x00 should be included (at lower bound, inclusive)
	assert!(
		results.iter().any(|(k, _, _, _)| k == key_at_lower),
		"Key at lower bound should be included"
	);
	// key\x01 should be included (in range)
	assert!(
		results.iter().any(|(k, _, _, _)| k == key_in_range),
		"Key in range should be included"
	);
	// key (without suffix) should be excluded (before lower)
	assert!(
		!results.iter().any(|(k, _, _, _)| k == key_before),
		"Key before lower bound should be excluded"
	);
	// key\x10 should be excluded (at upper, exclusive)
	assert!(
		!results.iter().any(|(k, _, _, _)| k == key_at_upper),
		"Key at upper bound should be excluded"
	);

	store.close().await.unwrap();
}

/// Test hard delete at lower bound hides the key from history.
#[test(tokio::test)]
async fn test_history_delete_at_lower_bound() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key_a", b"v", 1).unwrap();
		batch.set_at(b"key_b", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Hard delete key_a
	store.delete(b"key_a").await.unwrap();

	let snap = store.new_snapshot();

	// key_a should be completely hidden (hard delete hides all versions)
	let mut iter = snap
		.history_iter(Some(b"key_a".as_slice()), Some(b"key_z".as_slice()), true, None, None)
		.unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();
	assert!(
		!results.iter().any(|(k, _, _, _)| k == b"key_a"),
		"Hard deleted key at boundary should not appear"
	);
	assert!(results.iter().any(|(k, _, _, _)| k == b"key_b"), "Other keys should still appear");

	store.close().await.unwrap();
}

/// Test hard delete at boundary.
#[test(tokio::test)]
async fn test_history_hard_delete_at_boundary() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key_a", b"v1", 1).unwrap();
		batch.set_at(b"key_b", b"v1", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Hard delete key_a
	store.delete(b"key_a").await.unwrap();

	let snap = store.new_snapshot();
	let mut iter = snap
		.history_iter(Some(b"key_a".as_slice()), Some(b"key_z".as_slice()), true, None, None)
		.unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();
	// Hard delete should completely remove the key from history
	assert!(
		!results.iter().any(|(k, _, _, _)| k == b"key_a"),
		"Hard deleted key at boundary should not appear"
	);
	assert!(results.iter().any(|(k, _, _, _)| k == b"key_b"), "Other keys should still appear");

	store.close().await.unwrap();
}

/// Test equal lower and upper bounds (empty range).
#[test(tokio::test)]
async fn test_history_bounds_equal_lower_upper() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key_a", b"v", 1).unwrap();
		batch.set_at(b"key_b", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	// Range [key_a, key_a) is empty since upper is exclusive
	let mut iter = snap
		.history_iter(Some(b"key_a".as_slice()), Some(b"key_a".as_slice()), true, None, None)
		.unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();
	assert_eq!(results.len(), 0, "Equal lower and upper bounds should return empty");

	store.close().await.unwrap();
}

/// Test inverted bounds (lower > upper).
#[test(tokio::test)]
async fn test_history_bounds_inverted() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key_a", b"v", 1).unwrap();
		batch.set_at(b"key_b", b"v", 1).unwrap();
		batch.set_at(b"key_c", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	// Range [key_z, key_a) is inverted
	let mut iter = snap
		.history_iter(Some(b"key_z".as_slice()), Some(b"key_a".as_slice()), true, None, None)
		.unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();
	assert_eq!(results.len(), 0, "Inverted bounds should return empty");

	store.close().await.unwrap();
}

/// Test single key in range.
#[test(tokio::test)]
async fn test_history_single_key_in_range() {
	let (store, _temp_dir) = create_versioned_store().await;

	store.set_at(b"only_key", b"v", 1).await.unwrap();

	let snap = store.new_snapshot();
	let lower: &[u8] = b"only";
	let upper: &[u8] = b"only_z";

	// Forward
	let mut iter = snap.history_iter(Some(lower), Some(upper), true, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();
	assert_eq!(results.len(), 1, "Should have exactly 1 key");
	assert_eq!(results[0].0, b"only_key".to_vec());

	// Backward
	let mut iter = snap.history_iter(Some(lower), Some(upper), true, None, None).unwrap();
	iter.seek_last().await.unwrap();
	assert!(iter.valid(), "seek_last should be valid");
	assert_eq!(iter.key().user_key(), b"only_key");

	let has_more = iter.prev().await.unwrap();
	assert!(!has_more, "Should have no more keys");

	store.close().await.unwrap();
}

/// Test all keys outside range.
#[test(tokio::test)]
async fn test_history_all_keys_outside_range() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"aaa", b"v", 1).unwrap();
		batch.set_at(b"bbb", b"v", 1).unwrap();
		batch.set_at(b"zzz", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();

	// Forward - Range [m, n) has no keys
	let mut iter =
		snap.history_iter(Some(b"m".as_slice()), Some(b"n".as_slice()), true, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();
	assert_eq!(results.len(), 0, "Forward should return empty");

	// Backward
	let mut iter =
		snap.history_iter(Some(b"m".as_slice()), Some(b"n".as_slice()), true, None, None).unwrap();
	iter.seek_last().await.unwrap();
	assert!(!iter.valid(), "seek_last should be invalid for empty range");

	store.close().await.unwrap();
}

/// Test bounds with limit (forward).
#[test(tokio::test)]
async fn test_history_bounds_with_limit() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key_1", b"v", 1).unwrap();
		batch.set_at(b"key_2", b"v", 1).unwrap();
		batch.set_at(b"key_3", b"v", 1).unwrap();
		batch.set_at(b"key_4", b"v", 1).unwrap();
		batch.set_at(b"key_5", b"v", 1).unwrap();
		batch.set_at(b"aaa", b"v", 1).unwrap();
		batch.set_at(b"zzz", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let lower: &[u8] = b"key";
	let upper: &[u8] = b"key_z";
	let mut iter = snap.history_iter(Some(lower), Some(upper), true, None, Some(3)).unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();
	assert_eq!(results.len(), 3, "Limit should cap results to 3");
	for (key, _, _, _) in &results {
		assert!(key.as_slice() >= lower && key.as_slice() < upper, "All keys should be in range");
	}

	store.close().await.unwrap();
}

/// Test bounds with limit (backward).
#[test(tokio::test)]
async fn test_history_bounds_with_limit_backward() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key_1", b"v", 1).unwrap();
		batch.set_at(b"key_2", b"v", 1).unwrap();
		batch.set_at(b"key_3", b"v", 1).unwrap();
		batch.set_at(b"key_4", b"v", 1).unwrap();
		batch.set_at(b"key_5", b"v", 1).unwrap();
		batch.set_at(b"zzz", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let lower: &[u8] = b"key";
	let upper: &[u8] = b"key_z";
	let mut iter = snap.history_iter(Some(lower), Some(upper), true, None, Some(3)).unwrap();

	// Backward iteration with limit
	iter.seek_last().await.unwrap();
	let mut results = Vec::new();
	while iter.valid() && results.len() < 3 {
		let key = iter.key().user_key().to_vec();
		assert!(
			key.as_slice() >= lower && key.as_slice() < upper,
			"Key should be in range, got: {:?}",
			String::from_utf8_lossy(&key)
		);
		results.push(key);
		if !iter.prev().await.unwrap() {
			break;
		}
	}

	assert!(results.len() <= 3, "Should respect limit");

	store.close().await.unwrap();
}

/// Test bounds with timestamp range and limit combined.
#[test(tokio::test)]
async fn test_history_bounds_ts_range_and_limit() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key_a", b"v_ts1", 1).unwrap();
		batch.set_at(b"key_b", b"v_ts1", 1).unwrap();
		batch.set_at(b"key_c", b"v_ts1", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key_a", b"v_ts5", 5).unwrap();
		batch.set_at(b"key_b", b"v_ts5", 5).unwrap();
		batch.set_at(b"key_c", b"v_ts5", 5).unwrap();
		batch.set_at(b"key_d", b"v_ts5", 5).unwrap();
		batch.set_at(b"key_e", b"v_ts5", 5).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Key outside range
	store.set_at(b"zzz", b"v_ts5", 5).await.unwrap();

	let snap = store.new_snapshot();
	// All three constraints: key bounds [key, key_z), ts_range [5,5], limit 3
	let lower: &[u8] = b"key";
	let upper: &[u8] = b"key_z";
	let mut iter =
		snap.history_iter(Some(lower), Some(upper), true, Some((5, 5)), Some(3)).unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();

	// Should have at most 3 results
	assert!(results.len() <= 3, "Limit should be respected, got {}", results.len());

	// All results should be in key range and ts range
	for (key, _, ts, _) in &results {
		assert!(key.as_slice() >= lower && key.as_slice() < upper, "Key should be in range");
		assert_eq!(*ts, 5, "Timestamp should be 5");
	}

	store.close().await.unwrap();
}

/// Test prefix pattern iteration (common use case).
#[test(tokio::test)]
async fn test_history_bounds_prefix_pattern() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"user:alice", b"v", 1).unwrap();
		batch.set_at(b"user:bob", b"v", 1).unwrap();
		batch.set_at(b"user:charlie", b"v", 1).unwrap();
		batch.set_at(b"admin:root", b"v", 1).unwrap();
		batch.set_at(b"users_count", b"v", 1).unwrap();
		batch.set_at(b"userz", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	// Range for prefix "user:" is [user:, user;) since ';' is byte after ':'
	let mut iter = snap
		.history_iter(Some(b"user:".as_slice()), Some(b"user;".as_slice()), true, None, None)
		.unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();
	assert_eq!(results.len(), 3, "Should have 3 keys with prefix 'user:'");

	for (key, _, _, _) in &results {
		assert!(
			key.starts_with(b"user:"),
			"All keys should start with 'user:', got: {:?}",
			String::from_utf8_lossy(key)
		);
	}

	store.close().await.unwrap();
}

/// Test keys and bounds containing null bytes.
#[test(tokio::test)]
async fn test_history_bounds_null_bytes() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key\x00a", b"v", 1).unwrap();
		batch.set_at(b"key\x00b", b"v", 1).unwrap();
		batch.set_at(b"key\x00c", b"v", 1).unwrap();
		batch.set_at(b"key", b"v", 1).unwrap();
		batch.set_at(b"key\x01", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	// Range with null byte bounds: [key\x00, key\x01)
	let lower: &[u8] = b"key\x00";
	let upper: &[u8] = b"key\x01";
	let mut iter = snap.history_iter(Some(lower), Some(upper), true, None, None).unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();

	// Should include key\x00a, key\x00b, key\x00c but not "key" or "key\x01"
	assert_eq!(results.len(), 3, "Should have 3 keys in null byte range");

	for (key, _, _, _) in &results {
		assert!(key.starts_with(b"key\x00"), "All keys should start with 'key\\x00'");
	}

	assert!(
		!results.iter().any(|(k, _, _, _)| k == b"key"),
		"'key' without null should not be included"
	);

	store.close().await.unwrap();
}

/// Test bounds with maximum byte values (0xFF).
#[test(tokio::test)]
async fn test_history_bounds_max_byte_values() {
	let (store, _temp_dir) = create_versioned_store().await;

	{
		let mut batch = store.new_batch();
		batch.set_at(b"key\xfe", b"v", 1).unwrap();
		batch.set_at(b"key\xff", b"v", 1).unwrap();
		batch.set_at(b"key\xff\x00", b"v", 1).unwrap();
		batch.set_at(b"kez", b"v", 1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	// Range [key\xff, key\xff\xff) - keys starting with key\xff
	let lower: &[u8] = b"key\xff";
	let upper: &[u8] = b"key\xff\xff";
	let mut iter = snap.history_iter(Some(lower), Some(upper), true, None, None).unwrap();

	let results = collect_history_all(&mut iter).await.unwrap();

	// Should include key\xff and key\xff\x00
	assert_eq!(results.len(), 2, "Should have 2 keys starting with 0xFF");

	// Verify key\xfe is not included
	assert!(!results.iter().any(|(k, _, _, _)| k == b"key\xfe"), "key\\xfe should not be included");

	store.close().await.unwrap();
}
