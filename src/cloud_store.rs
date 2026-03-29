use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use integer_encoding::FixedInt;
use object_store::ObjectStore;

use crate::disk_cache::{DiskCache, PopulateMsg};
use crate::error::{Error, Result};
use crate::metrics::DbStats;
use crate::paths::StorePaths;
use crate::sstable::block::{Block, BlockHandle};
use crate::sstable::error::SSTableError;
use crate::sstable::table::{
	decompress_block,
	unmask,
	Footer,
	BLOCK_CKSUM_LEN,
	BLOCK_COMPRESS_LEN,
	TABLE_FULL_FOOTER_LENGTH,
};
use crate::sstable::SstId;
use crate::{Comparator, CompressionType};

/// Central abstraction for SST lifecycle management over object store.
///
/// Replaces all direct file I/O for SSTs. Handles:
/// - Atomic uploads (memtable flush)
/// - Streaming writes (compaction)
/// - Block reads with cache integration
/// - Point lookups (bloom → index → data block)
/// - SST deletion and listing
pub(crate) struct CloudStore {
	object_store: Arc<dyn ObjectStore>,
	path_resolver: StorePaths,
	/// When Some, SST files are cached on local disk and reads prefer local files.
	local_sst_dir: Option<PathBuf>,
	/// Bounded, LRU-evicting local disk cache for SSTs.
	/// When set, `read_range()` checks this cache before the object store,
	/// and on a miss triggers background population of the full SST.
	disk_cache: Option<Arc<DiskCache>>,
	/// Optional metrics counters for observability.
	/// None for temporary CloudStore instances (e.g., GC, branch recovery).
	db_stats: Option<Arc<DbStats>>,
	/// Counts object-store GET requests (excluding local-disk cache hits).
	/// Only present in test builds; used to assert cloud read counts in tests.
	#[cfg(test)]
	pub(crate) read_range_count: Arc<AtomicUsize>,
}

impl CloudStore {
	pub(crate) fn new(
		object_store: Arc<dyn ObjectStore>,
		path_resolver: StorePaths,
		local_sst_dir: Option<PathBuf>,
	) -> Self {
		Self {
			object_store,
			path_resolver,
			local_sst_dir,
			disk_cache: None,
			db_stats: None,
			#[cfg(test)]
			read_range_count: Arc::new(AtomicUsize::new(0)),
		}
	}

	/// Creates a new CloudStore with metrics counters attached.
	pub(crate) fn with_stats(
		object_store: Arc<dyn ObjectStore>,
		path_resolver: StorePaths,
		local_sst_dir: Option<PathBuf>,
		db_stats: Arc<DbStats>,
	) -> Self {
		Self {
			object_store,
			path_resolver,
			local_sst_dir,
			disk_cache: None,
			db_stats: Some(db_stats),
			#[cfg(test)]
			read_range_count: Arc::new(AtomicUsize::new(0)),
		}
	}

	/// Creates a new CloudStore with metrics counters and disk cache attached.
	pub(crate) fn with_stats_and_cache(
		object_store: Arc<dyn ObjectStore>,
		path_resolver: StorePaths,
		local_sst_dir: Option<PathBuf>,
		db_stats: Arc<DbStats>,
		disk_cache: Option<Arc<DiskCache>>,
	) -> Self {
		Self {
			object_store,
			path_resolver,
			local_sst_dir,
			disk_cache,
			db_stats: Some(db_stats),
			#[cfg(test)]
			read_range_count: Arc::new(AtomicUsize::new(0)),
		}
	}

	/// Delete an SST from the object store (and local cache if present).
	pub(crate) async fn delete_sst(&self, id: &SstId) -> Result<()> {
		let path = self.path_resolver.table_path(id);
		if let Some(ref stats) = self.db_stats {
			DbStats::inc(&stats.object_store_deletes);
		}
		self.object_store.delete(&path).await.map_err(|e| {
			if let Some(ref stats) = self.db_stats {
				DbStats::inc(&stats.object_store_errors);
			}
			e
		})?;
		if let Some(ref sst_dir) = self.local_sst_dir {
			let _ = std::fs::remove_file(sst_dir.join(format!("{id}.sst")));
		}
		if let Some(ref cache) = self.disk_cache {
			cache.remove(id);
		}
		Ok(())
	}

	/// Delete an SST from local cache only (not from object store).
	/// Used when branching is enabled — object store deletion is deferred
	/// to the cross-branch purger.
	pub(crate) fn delete_local_sst(&self, id: &SstId) {
		if let Some(ref sst_dir) = self.local_sst_dir {
			let _ = std::fs::remove_file(sst_dir.join(format!("{id}.sst")));
		}
		if let Some(ref cache) = self.disk_cache {
			cache.remove(id);
		}
	}

	/// Read a byte range from an SST file.
	///
	/// Check order:
	/// 1. Disk cache (bounded, LRU-evicted)
	/// 2. Legacy local_sst_dir (unbounded, backward compat)
	/// 3. Object store (remote)
	///
	/// On an object store hit, a background task is enqueued to fetch the
	/// full SST and populate the disk cache.  If the background channel is
	/// full the request is silently dropped (backpressure).
	pub(crate) async fn read_range(
		&self,
		id: &SstId,
		range: std::ops::Range<u64>,
	) -> Result<Bytes> {
		// 1. Check disk cache
		if let Some(ref cache) = self.disk_cache {
			if cache.contains(id) {
				let path = cache.get_path(id);
				if let Ok(data) = Self::read_local_range(&path, &range) {
					cache.touch(id);
					return Ok(data);
				}
				// File disappeared (race with eviction) — fall through
			}
		}

		// 2. Legacy local_sst_dir (backward compat)
		if let Some(ref sst_dir) = self.local_sst_dir {
			let local_path = sst_dir.join(format!("{id}.sst"));
			if let Ok(data) = Self::read_local_range(&local_path, &range) {
				return Ok(data);
			}
		}

		// 3. Object store read
		let path = self.path_resolver.table_path(id);
		#[cfg(test)]
		self.read_range_count.fetch_add(1, Ordering::Relaxed);
		if let Some(ref stats) = self.db_stats {
			DbStats::inc(&stats.object_store_reads);
		}
		let data = self.object_store.get_range(&path, range).await.map_err(|e| {
			if let Some(ref stats) = self.db_stats {
				DbStats::inc(&stats.object_store_errors);
			}
			e
		})?;

		// 4. Enqueue background population of full SST into disk cache. Uses try_send for
		//    backpressure — if the channel is full we simply skip caching rather than stalling the
		//    foreground read.
		if let Some(ref cache) = self.disk_cache {
			if !cache.contains(id) {
				let _ = cache.try_populate(PopulateMsg {
					id: *id,
					object_store: Arc::clone(&self.object_store),
					sst_path: self.path_resolver.table_path(id),
				});
			}
		}

		Ok(data)
	}

	/// Read a byte range from a local SST file.
	fn read_local_range(
		path: &std::path::Path,
		range: &std::ops::Range<u64>,
	) -> std::io::Result<Bytes> {
		use std::io::{Read, Seek, SeekFrom};
		let mut file = std::fs::File::open(path)?;
		file.seek(SeekFrom::Start(range.start))?;
		let len = (range.end - range.start) as usize;
		let mut buf = vec![0u8; len];
		file.read_exact(&mut buf)?;
		Ok(Bytes::from(buf))
	}

	/// Upload an SST as a single atomic put.
	pub(crate) async fn write_sst(&self, id: &SstId, data: Bytes) -> Result<()> {
		let path = self.path_resolver.table_path(id);
		if let Some(ref stats) = self.db_stats {
			DbStats::inc(&stats.object_store_writes);
		}
		self.object_store.put(&path, data.into()).await.map_err(|e| {
			if let Some(ref stats) = self.db_stats {
				DbStats::inc(&stats.object_store_errors);
			}
			e
		})?;
		Ok(())
	}

	/// Write SST data to local disk cache atomically (write-to-tmp + rename).
	/// No-op when local SST caching is not enabled.
	pub(crate) fn write_local_sst(&self, id: &SstId, data: &[u8]) -> Result<()> {
		if let Some(ref sst_dir) = self.local_sst_dir {
			let final_path = sst_dir.join(format!("{id}.sst"));
			let tmp_path = sst_dir.join(format!("{id}.sst.tmp"));
			std::fs::write(&tmp_path, data)?;
			std::fs::rename(&tmp_path, &final_path)?;
		}
		Ok(())
	}

	/// Read the footer from the end of an SST file.
	pub(crate) async fn read_footer(&self, id: &SstId, file_size: u64) -> Result<Footer> {
		if (file_size as usize) < TABLE_FULL_FOOTER_LENGTH {
			return Err(Error::from(SSTableError::FileTooSmall {
				file_size: file_size as usize,
				min_size: TABLE_FULL_FOOTER_LENGTH,
			}));
		}
		let offset = file_size - TABLE_FULL_FOOTER_LENGTH as u64;
		let buf = self.read_range(id, offset..file_size).await?;
		Footer::decode(&buf)
	}

	/// Read raw bytes at a block handle location.
	pub(crate) async fn read_block_bytes(
		&self,
		id: &SstId,
		location: &BlockHandle,
	) -> Result<Bytes> {
		let start = location.offset() as u64;
		let end = start + location.size() as u64;
		self.read_range(id, start..end).await
	}

	/// Write a manifest to the object store (unconditional put).
	/// Used by the async uploader for best-effort background sync.
	pub(crate) async fn write_manifest(&self, id: u64, data: Bytes) -> Result<()> {
		let path = self.path_resolver.manifest_path(id);
		if let Some(ref stats) = self.db_stats {
			DbStats::inc(&stats.object_store_writes);
		}
		self.object_store.put(&path, data.into()).await.map_err(|e| {
			if let Some(ref stats) = self.db_stats {
				DbStats::inc(&stats.object_store_errors);
			}
			e
		})?;
		Ok(())
	}

	/// Write a manifest to the object store using CAS (put-if-not-exists).
	/// Returns `Error::ManifestVersionExists` if another writer already wrote this version.
	pub(crate) async fn write_manifest_if_absent(&self, id: u64, data: Bytes) -> Result<()> {
		let path = self.path_resolver.manifest_path(id);
		let opts = object_store::PutOptions::from(object_store::PutMode::Create);
		if let Some(ref stats) = self.db_stats {
			DbStats::inc(&stats.object_store_writes);
		}
		self.object_store.put_opts(&path, data.into(), opts).await.map_err(|e| {
			if let Some(ref stats) = self.db_stats {
				// AlreadyExists is expected for CAS, not a true error
				if !matches!(e, object_store::Error::AlreadyExists { .. }) {
					DbStats::inc(&stats.object_store_errors);
				}
			}
			match e {
				object_store::Error::AlreadyExists {
					..
				} => Error::ManifestVersionExists,
				other => Error::from(other),
			}
		})?;
		Ok(())
	}

	/// Read the latest manifest from the object store by listing and picking the highest ID.
	/// Returns `None` if no manifests exist.
	pub(crate) async fn read_latest_manifest(&self) -> Result<Option<(u64, Bytes)>> {
		let ids = self.list_manifest_ids().await?;
		match ids.last() {
			Some(&id) => {
				let data = self.read_manifest(id).await?;
				Ok(Some((id, data)))
			}
			None => Ok(None),
		}
	}

	/// Read a manifest from the object store.
	pub(crate) async fn read_manifest(&self, id: u64) -> Result<Bytes> {
		let path = self.path_resolver.manifest_path(id);
		if let Some(ref stats) = self.db_stats {
			DbStats::inc(&stats.object_store_reads);
		}
		let result = self.object_store.get(&path).await.map_err(|e| {
			if let Some(ref stats) = self.db_stats {
				DbStats::inc(&stats.object_store_errors);
			}
			e
		})?;
		Ok(result.bytes().await?)
	}

	/// List all manifest IDs in the object store, sorted ascending.
	pub(crate) async fn list_manifest_ids(&self) -> Result<Vec<u64>> {
		let prefix = self.path_resolver.manifest_prefix();
		if let Some(ref stats) = self.db_stats {
			DbStats::inc(&stats.object_store_reads);
		}
		let list_result =
			self.object_store.list_with_delimiter(Some(&prefix)).await.map_err(|e| {
				if let Some(ref stats) = self.db_stats {
					DbStats::inc(&stats.object_store_errors);
				}
				e
			})?;
		let mut ids = Vec::new();
		for obj in list_result.objects {
			if let Some(name) = obj.location.filename() {
				if let Some(id_str) = name.strip_suffix(".manifest") {
					if let Ok(id) = id_str.parse::<u64>() {
						ids.push(id);
					}
				}
			}
		}
		ids.sort();
		Ok(ids)
	}

	/// Delete a manifest from the object store.
	pub(crate) async fn delete_manifest(&self, id: u64) -> Result<()> {
		let path = self.path_resolver.manifest_path(id);
		if let Some(ref stats) = self.db_stats {
			DbStats::inc(&stats.object_store_deletes);
		}
		self.object_store.delete(&path).await.map_err(|e| {
			if let Some(ref stats) = self.db_stats {
				DbStats::inc(&stats.object_store_errors);
			}
			e
		})?;
		Ok(())
	}

	/// List all SST IDs in the object store.
	pub(crate) async fn list_sst_ids(&self) -> Result<Vec<SstId>> {
		let prefix = self.path_resolver.sst_path();
		if let Some(ref stats) = self.db_stats {
			DbStats::inc(&stats.object_store_reads);
		}
		let list_result =
			self.object_store.list_with_delimiter(Some(&prefix)).await.map_err(|e| {
				if let Some(ref stats) = self.db_stats {
					DbStats::inc(&stats.object_store_errors);
				}
				e
			})?;
		let mut ids = Vec::new();
		for obj in list_result.objects {
			let filename = obj.location.filename().unwrap_or_default();
			if let Some(stem) = filename.strip_suffix(".sst") {
				if let Ok(id) = stem.parse::<SstId>() {
					ids.push(id);
				}
			}
		}
		Ok(ids)
	}

	/// Read and verify a table block from the object store.
	///
	/// Performs a single range read for the block data + compression byte + checksum,
	/// avoiding 3 separate round-trips.
	pub(crate) async fn read_table_block(
		&self,
		id: &SstId,
		location: &BlockHandle,
		comparator: Arc<dyn Comparator>,
	) -> Result<Block> {
		// Single range read: block_data | compress_byte | crc32
		let total_size = location.size() + BLOCK_COMPRESS_LEN + BLOCK_CKSUM_LEN;
		let start = location.offset() as u64;
		let end = start + total_size as u64;
		let buf = self.read_range(id, start..end).await?;

		let block_data = &buf[..location.size()];
		let compress_byte = buf[location.size()];
		let cksum_bytes = &buf[location.size() + BLOCK_COMPRESS_LEN..];

		// Verify checksum
		let stored_cksum = unmask(u32::decode_fixed(cksum_bytes).unwrap());
		let mut hasher = crc32fast::Hasher::new();
		hasher.update(block_data);
		hasher.update(&[compress_byte; BLOCK_COMPRESS_LEN]);
		if hasher.finalize() != stored_cksum {
			return Err(Error::from(SSTableError::ChecksumVerificationFailed {
				block_offset: location.offset() as u64,
			}));
		}

		// Decompress
		let decompressed = decompress_block(block_data, CompressionType::try_from(compress_byte)?)?;
		Ok(Block::new(decompressed, comparator))
	}
}
