//! E2E tests for CloudStore over real backends (LocalFileSystem and S3).
//!
//! Tier 1: LocalFileSystem tests — always run in `cargo test`.
//! Tier 2: S3 tests — run via `cargo test --features cloud-tests -- --ignored`
//!         after starting MinIO locally.

use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::error::Error;
use crate::sstable::table::TableWriter;
use crate::test::cloud_test_helpers::*;
use crate::test::test_sst_id;
use crate::{InternalKey, InternalKeyKind, Options};

// =============================================================================
// Helper: build a minimal SST in-memory
// =============================================================================

fn build_test_sst(num_keys: u64) -> Vec<u8> {
	let opts = Arc::new(Options::new());
	let id = test_sst_id(99);
	let mut buf = Vec::new();
	let mut writer = TableWriter::new(&mut buf, id, opts, 0);
	for i in 0..num_keys {
		let key = format!("key_{i:05}");
		let value = format!("val_{i:05}");
		let ik = InternalKey::new(key.as_bytes().to_vec(), i + 1, InternalKeyKind::Set, 0);
		writer.add(ik, value.as_bytes()).unwrap();
	}
	writer.finish().unwrap();
	buf
}

// =============================================================================
// Tier 1: LocalFileSystem CloudStore tests
// =============================================================================

#[test(tokio::test)]
async fn local_fs_write_and_read_sst() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let id = test_sst_id(1);
	let data = bytes::Bytes::from(vec![42u8; 1024]);
	ts.write_sst(&id, data.clone()).await.unwrap();

	let result = ts.read_range(&id, 0..1024).await.unwrap();
	assert_eq!(result, data);
}

#[test(tokio::test)]
async fn local_fs_read_range_partial() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let id = test_sst_id(1);
	let data: Vec<u8> = (0..=255u8).cycle().take(1024).collect();
	ts.write_sst(&id, bytes::Bytes::from(data.clone())).await.unwrap();

	let result = ts.read_range(&id, 100..200).await.unwrap();
	assert_eq!(result.as_ref(), &data[100..200]);
}

#[test(tokio::test)]
async fn local_fs_list_sst_ids() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let ids = [test_sst_id(1), test_sst_id(2), test_sst_id(3)];
	for id in &ids {
		ts.write_sst(id, bytes::Bytes::from(vec![1u8; 64])).await.unwrap();
	}

	let listed = ts.list_sst_ids().await.unwrap();
	assert_eq!(listed.len(), 3);
	for id in &ids {
		assert!(listed.contains(id), "Expected {id} in listed SST IDs");
	}
}

#[test(tokio::test)]
async fn local_fs_delete_sst() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let id = test_sst_id(1);
	ts.write_sst(&id, bytes::Bytes::from(vec![1u8; 64])).await.unwrap();
	ts.delete_sst(&id).await.unwrap();

	let err = ts.read_range(&id, 0..64).await.unwrap_err();
	assert!(matches!(err, Error::ObjectStoreError(_)));
}

#[test(tokio::test)]
async fn local_fs_overwrite_sst() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let id = test_sst_id(1);
	ts.write_sst(&id, bytes::Bytes::from(vec![1u8; 64])).await.unwrap();
	ts.write_sst(&id, bytes::Bytes::from(vec![2u8; 64])).await.unwrap();

	let result = ts.read_range(&id, 0..64).await.unwrap();
	assert_eq!(result[0], 2u8);
}

#[test(tokio::test)]
async fn local_fs_manifest_roundtrip() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	ts.write_manifest(1, bytes::Bytes::from(b"manifest_v1".to_vec())).await.unwrap();
	ts.write_manifest(2, bytes::Bytes::from(b"manifest_v2".to_vec())).await.unwrap();

	let data = ts.read_manifest(1).await.unwrap();
	assert_eq!(data.as_ref(), b"manifest_v1");

	let data = ts.read_manifest(2).await.unwrap();
	assert_eq!(data.as_ref(), b"manifest_v2");

	let ids = ts.list_manifest_ids().await.unwrap();
	assert_eq!(ids, vec![1, 2]);
}

#[test(tokio::test)]
async fn local_fs_manifest_cas() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let data = bytes::Bytes::from(b"manifest_v1".to_vec());

	// First CAS write succeeds
	ts.write_manifest_if_absent(1, data.clone()).await.unwrap();

	// Second CAS write to same id fails
	let err = ts.write_manifest_if_absent(1, data.clone()).await.unwrap_err();
	assert!(matches!(err, Error::ManifestVersionExists));

	// Unconditional write_manifest always succeeds (overwrites)
	ts.write_manifest(1, data).await.unwrap();
}

#[test(tokio::test)]
async fn local_fs_read_latest_manifest() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	// No manifests → None
	assert!(ts.read_latest_manifest().await.unwrap().is_none());

	// Write manifests with IDs 1, 5, 3 — read_latest should return ID 5
	ts.write_manifest(1, bytes::Bytes::from(b"m1".to_vec())).await.unwrap();
	ts.write_manifest(5, bytes::Bytes::from(b"m5".to_vec())).await.unwrap();
	ts.write_manifest(3, bytes::Bytes::from(b"m3".to_vec())).await.unwrap();

	let (id, data) = ts.read_latest_manifest().await.unwrap().unwrap();
	assert_eq!(id, 5);
	assert_eq!(data.as_ref(), b"m5");
}

#[test(tokio::test)]
async fn local_fs_read_footer() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let sst_data = build_test_sst(10);
	let id = test_sst_id(1);
	let file_size = sst_data.len() as u64;
	ts.write_sst(&id, bytes::Bytes::from(sst_data)).await.unwrap();

	let footer = ts.read_footer(&id, file_size).await.unwrap();
	// Footer should have valid index and meta_index block handles
	assert!(footer.index.size() > 0);
}

#[test(tokio::test)]
async fn local_fs_read_table_block() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let sst_data = build_test_sst(10);
	let id = test_sst_id(1);
	let file_size = sst_data.len() as u64;
	ts.write_sst(&id, bytes::Bytes::from(sst_data)).await.unwrap();

	let opts = Arc::new(Options::new());
	let footer = ts.read_footer(&id, file_size).await.unwrap();
	// Read the index block — validates checksum verification path
	let block = ts
		.read_table_block(&id, &footer.index, Arc::clone(&opts.internal_comparator))
		.await
		.unwrap();
	assert!(block.size() > 0);
}

#[test(tokio::test)]
async fn local_fs_write_local_sst() {
	let temp_dir = TempDir::new().unwrap();
	let cache_dir = temp_dir.path().join("sst_cache");
	std::fs::create_dir_all(&cache_dir).unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), Some(cache_dir.clone()));

	let id = test_sst_id(1);
	ts.write_local_sst(&id, &vec![1u8; 512]).unwrap();

	// Verify atomic rename: final file exists, tmp does not
	assert!(cache_dir.join(format!("{id}.sst")).exists());
	assert!(!cache_dir.join(format!("{id}.sst.tmp")).exists());
}

#[test(tokio::test)]
async fn local_fs_read_prefers_local_cache() {
	let temp_dir = TempDir::new().unwrap();
	let cache_dir = temp_dir.path().join("sst_cache");
	std::fs::create_dir_all(&cache_dir).unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), Some(cache_dir.clone()));

	let id = test_sst_id(1);

	// Write to object store (remote)
	ts.write_sst(&id, bytes::Bytes::from(vec![1u8; 512])).await.unwrap();

	// Write DIFFERENT data to local cache
	ts.write_local_sst(&id, &vec![2u8; 512]).unwrap();

	// read_range prefers local cache → returns 2s not 1s
	let result = ts.read_range(&id, 0..512).await.unwrap();
	assert_eq!(result[0], 2u8);
}

#[test(tokio::test)]
async fn local_fs_delete_cleans_local_cache() {
	let temp_dir = TempDir::new().unwrap();
	let cache_dir = temp_dir.path().join("sst_cache");
	std::fs::create_dir_all(&cache_dir).unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), Some(cache_dir.clone()));

	let id = test_sst_id(1);
	ts.write_sst(&id, bytes::Bytes::from(vec![1u8; 64])).await.unwrap();
	ts.write_local_sst(&id, &[1u8; 64]).unwrap();
	assert!(cache_dir.join(format!("{id}.sst")).exists());

	// delete_sst removes from object store AND local cache
	ts.delete_sst(&id).await.unwrap();
	assert!(!cache_dir.join(format!("{id}.sst")).exists());
}

#[test(tokio::test)]
async fn local_fs_write_local_sst_noop_without_cache() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let id = test_sst_id(1);
	// write_local_sst is a no-op when local_sst_dir is None
	ts.write_local_sst(&id, &[1u8; 64]).unwrap();

	// No local file created — read_range fails since we didn't call write_sst
	assert!(ts.read_range(&id, 0..64).await.is_err());
}

#[test(tokio::test)]
async fn local_fs_large_sst() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let id = test_sst_id(1);
	// 1MB+ SST upload/download
	let data = vec![0xABu8; 1024 * 1024 + 512];
	ts.write_sst(&id, bytes::Bytes::from(data.clone())).await.unwrap();

	let result = ts.read_range(&id, 0..data.len() as u64).await.unwrap();
	assert_eq!(result.len(), data.len());
	assert_eq!(result[0], 0xAB);
	assert_eq!(result[result.len() - 1], 0xAB);
}

#[test(tokio::test)]
async fn local_fs_delete_nonexistent_sst() {
	let temp_dir = TempDir::new().unwrap();
	let (_store, ts) = create_local_table_store(temp_dir.path(), None);

	let fake_id = test_sst_id(99999);
	// LocalFileSystem returns NotFound for non-existent files (unlike S3 which is idempotent)
	let result = ts.delete_sst(&fake_id).await;
	assert!(result.is_err());
}

// =============================================================================
// Tier 2: S3 tests (via MinIO) — only compiled with cloud-tests feature
// =============================================================================

#[cfg(feature = "cloud-tests")]
mod s3_tests {
	use std::sync::Arc;

	use test_log::test;

	use super::build_test_sst;
	use crate::error::Error;
	use crate::test::cloud_test_helpers::{
		cleanup_prefix,
		create_s3_table_store,
		require_s3_config,
	};
	use crate::test::test_sst_id;
	use crate::Options;

	fn unique_prefix() -> String {
		format!("test/{}/", ulid::Ulid::new())
	}

	#[test(tokio::test)]
	async fn s3_write_and_read_sst() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		let id = test_sst_id(1);
		let data = bytes::Bytes::from(vec![42u8; 1024]);
		ts.write_sst(&id, data.clone()).await.unwrap();

		let result = ts.read_range(&id, 0..1024).await.unwrap();
		assert_eq!(result, data);

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_read_range_partial() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		let id = test_sst_id(1);
		let data: Vec<u8> = (0..=255u8).cycle().take(1024).collect();
		ts.write_sst(&id, bytes::Bytes::from(data.clone())).await.unwrap();

		let result = ts.read_range(&id, 100..200).await.unwrap();
		assert_eq!(result.as_ref(), &data[100..200]);

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_delete_sst() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		let id = test_sst_id(1);
		ts.write_sst(&id, bytes::Bytes::from(vec![1u8; 64])).await.unwrap();
		ts.delete_sst(&id).await.unwrap();

		let err = ts.read_range(&id, 0..64).await.unwrap_err();
		assert!(matches!(err, Error::ObjectStoreError(_)));

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_list_sst_ids() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		let ids = [test_sst_id(1), test_sst_id(2), test_sst_id(3)];
		for id in &ids {
			ts.write_sst(id, bytes::Bytes::from(vec![1u8; 64])).await.unwrap();
		}

		let listed = ts.list_sst_ids().await.unwrap();
		assert_eq!(listed.len(), 3);

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_manifest_roundtrip() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		ts.write_manifest(1, bytes::Bytes::from(b"m1".to_vec())).await.unwrap();
		ts.write_manifest(2, bytes::Bytes::from(b"m2".to_vec())).await.unwrap();

		let data = ts.read_manifest(1).await.unwrap();
		assert_eq!(data.as_ref(), b"m1");

		let ids = ts.list_manifest_ids().await.unwrap();
		assert_eq!(ids, vec![1, 2]);

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_manifest_cas() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		let data = bytes::Bytes::from(b"manifest_v1".to_vec());
		ts.write_manifest_if_absent(1, data.clone()).await.unwrap();

		let err = ts.write_manifest_if_absent(1, data.clone()).await.unwrap_err();
		assert!(matches!(err, Error::ManifestVersionExists));

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_read_latest_manifest() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		assert!(ts.read_latest_manifest().await.unwrap().is_none());

		ts.write_manifest(1, bytes::Bytes::from(b"m1".to_vec())).await.unwrap();
		ts.write_manifest(5, bytes::Bytes::from(b"m5".to_vec())).await.unwrap();
		ts.write_manifest(3, bytes::Bytes::from(b"m3".to_vec())).await.unwrap();

		let (id, data) = ts.read_latest_manifest().await.unwrap().unwrap();
		assert_eq!(id, 5);
		assert_eq!(data.as_ref(), b"m5");

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_read_footer() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		let sst_data = build_test_sst(10);
		let id = test_sst_id(1);
		let file_size = sst_data.len() as u64;
		ts.write_sst(&id, bytes::Bytes::from(sst_data)).await.unwrap();

		let footer = ts.read_footer(&id, file_size).await.unwrap();
		assert!(footer.index.size() > 0);

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_read_table_block() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		let sst_data = build_test_sst(10);
		let id = test_sst_id(1);
		let file_size = sst_data.len() as u64;
		ts.write_sst(&id, bytes::Bytes::from(sst_data)).await.unwrap();

		let opts = Arc::new(Options::new());
		let footer = ts.read_footer(&id, file_size).await.unwrap();
		let block = ts
			.read_table_block(&id, &footer.index, Arc::clone(&opts.internal_comparator))
			.await
			.unwrap();
		assert!(block.size() > 0);

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_overwrite_sst() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		let id = test_sst_id(1);
		ts.write_sst(&id, bytes::Bytes::from(vec![1u8; 64])).await.unwrap();
		ts.write_sst(&id, bytes::Bytes::from(vec![2u8; 64])).await.unwrap();

		let result = ts.read_range(&id, 0..64).await.unwrap();
		assert_eq!(result[0], 2u8);

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_large_sst() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		let id = test_sst_id(1);
		let data = vec![0xABu8; 1024 * 1024 + 512];
		ts.write_sst(&id, bytes::Bytes::from(data.clone())).await.unwrap();

		let result = ts.read_range(&id, 0..data.len() as u64).await.unwrap();
		assert_eq!(result.len(), data.len());
		assert_eq!(result[0], 0xAB);

		cleanup_prefix(&store, &prefix).await.unwrap();
	}

	#[test(tokio::test)]
	async fn s3_delete_nonexistent_sst() {
		require_s3_config!();
		let prefix = unique_prefix();
		let (store, ts) = create_s3_table_store(&prefix, None).unwrap();

		let fake_id = test_sst_id(99999);
		// S3 DELETE is idempotent — should succeed for non-existent SSTs
		ts.delete_sst(&fake_id).await.unwrap();

		cleanup_prefix(&store, &prefix).await.unwrap();
	}
}
