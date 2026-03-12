use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use integer_encoding::FixedInt;
use object_store::ObjectStore;

use crate::error::{Error, Result};
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
		}
	}

	/// Delete an SST from the object store (and local cache if present).
	pub(crate) async fn delete_sst(&self, id: &SstId) -> Result<()> {
		let path = self.path_resolver.table_path(id);
		self.object_store.delete(&path).await?;
		if let Some(ref sst_dir) = self.local_sst_dir {
			let _ = std::fs::remove_file(sst_dir.join(format!("{id}.sst")));
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
	}

	/// Read a byte range from an SST file.
	/// When local SST caching is enabled, reads from local disk first,
	/// falling back to the object store on miss.
	pub(crate) async fn read_range(
		&self,
		id: &SstId,
		range: std::ops::Range<u64>,
	) -> Result<Bytes> {
		if let Some(ref sst_dir) = self.local_sst_dir {
			let local_path = sst_dir.join(format!("{id}.sst"));
			if let Ok(data) = Self::read_local_range(&local_path, &range) {
				return Ok(data);
			}
		}
		let path = self.path_resolver.table_path(id);
		Ok(self.object_store.get_range(&path, range).await?)
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
		self.object_store.put(&path, data.into()).await?;
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
		self.object_store.put(&path, data.into()).await?;
		Ok(())
	}

	/// Write a manifest to the object store using CAS (put-if-not-exists).
	/// Returns `Error::ManifestVersionExists` if another writer already wrote this version.
	pub(crate) async fn write_manifest_if_absent(&self, id: u64, data: Bytes) -> Result<()> {
		let path = self.path_resolver.manifest_path(id);
		let opts = object_store::PutOptions::from(object_store::PutMode::Create);
		self.object_store.put_opts(&path, data.into(), opts).await.map_err(|e| match e {
			object_store::Error::AlreadyExists {
				..
			} => Error::ManifestVersionExists,
			other => Error::from(other),
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
		let result = self.object_store.get(&path).await?;
		Ok(result.bytes().await?)
	}

	/// List all manifest IDs in the object store, sorted ascending.
	pub(crate) async fn list_manifest_ids(&self) -> Result<Vec<u64>> {
		let prefix = self.path_resolver.manifest_prefix();
		let list_result = self.object_store.list_with_delimiter(Some(&prefix)).await?;
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

	/// List all SST IDs in the object store.
	pub(crate) async fn list_sst_ids(&self) -> Result<Vec<SstId>> {
		let prefix = self.path_resolver.sst_path();
		let list_result = self.object_store.list_with_delimiter(Some(&prefix)).await?;
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
