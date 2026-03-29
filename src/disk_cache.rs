use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use rand::Rng;
use tokio::sync::mpsc;

use crate::error::Result;
use crate::sstable::SstId;

/// Configuration for the bounded local disk cache.
#[derive(Clone, Debug)]
pub struct DiskCacheConfig {
	/// Directory path for cached SST files.
	pub path: PathBuf,
	/// Maximum total size of cached files in bytes.
	/// Default: 10 GB.
	pub max_size_bytes: u64,
}

impl Default for DiskCacheConfig {
	fn default() -> Self {
		Self {
			path: PathBuf::from("disk_cache"),
			max_size_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
		}
	}
}

/// Metadata for a single cached SST file.
struct CacheEntry {
	size: u64,
	/// Monotonic counter for approximate LRU ordering.
	/// Atomic so `touch()` can update under a read-lock without UB.
	last_access_epoch: AtomicU64,
}

/// Message sent to the background cache-population task.
pub(crate) struct PopulateMsg {
	pub id: SstId,
	pub object_store: Arc<dyn object_store::ObjectStore>,
	pub sst_path: object_store::path::Path,
}

/// A bounded, LRU-evicting local disk cache for SST files.
///
/// On startup the cache directory is scanned to rebuild the index so that
/// cached files survive process restarts.  Eviction uses a Pick-of-2 LRU
/// approximation: two random entries are sampled and the one with the
/// older access epoch is removed.  This avoids the cost of sorting
/// the full entry set on every eviction pass.
pub(crate) struct DiskCache {
	config: DiskCacheConfig,
	entries: RwLock<HashMap<SstId, CacheEntry>>,
	current_size: AtomicU64,
	access_counter: AtomicU64,
	/// Bounded channel for background cache population.
	/// `try_send` provides backpressure — if the channel is full
	/// the foreground read simply skips caching.
	populate_tx: mpsc::Sender<PopulateMsg>,
}

impl DiskCache {
	/// Create a new `DiskCache`, scanning `config.path` on startup to rebuild
	/// the in-memory index from any `.sst` files left from a previous run.
	///
	/// A background tokio task is spawned to drain the population channel.
	pub fn new(config: DiskCacheConfig) -> Arc<Self> {
		std::fs::create_dir_all(&config.path).ok();

		let mut entries = HashMap::new();
		let mut total_size: u64 = 0;

		// Scan cache dir and rebuild entries from existing .sst files.
		if let Ok(dir) = std::fs::read_dir(&config.path) {
			for entry in dir.flatten() {
				let path = entry.path();
				if path.extension().is_some_and(|ext| ext == "sst") {
					if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
						if let Ok(id) = stem.parse::<SstId>() {
							if let Ok(meta) = std::fs::metadata(&path) {
								let size = meta.len();
								entries.insert(
									id,
									CacheEntry {
										size,
										last_access_epoch: AtomicU64::new(0),
									},
								);
								total_size += size;
							}
						}
					}
				}
			}
		}

		// Bounded channel — capacity 64 keeps a reasonable backlog without
		// unbounded memory growth.
		let (populate_tx, populate_rx) = mpsc::channel::<PopulateMsg>(64);

		let cache = Arc::new(Self {
			config,
			entries: RwLock::new(entries),
			current_size: AtomicU64::new(total_size),
			access_counter: AtomicU64::new(1), // start at 1 so startup entries (epoch 0) are oldest
			populate_tx,
		});

		// Spawn the background population task.
		{
			let cache = Arc::clone(&cache);
			tokio::spawn(Self::populate_loop(cache, populate_rx));
		}

		cache
	}

	/// Return the local file path for a cached SST.
	pub fn get_path(&self, id: &SstId) -> PathBuf {
		self.config.path.join(format!("{id}.sst"))
	}

	/// Check whether the cache index contains the given SST.
	/// This is a fast in-memory check with no disk I/O.
	pub fn contains(&self, id: &SstId) -> bool {
		self.entries.read().contains_key(id)
	}

	/// Record a read-hit by bumping the access epoch for the given SST.
	pub fn touch(&self, id: &SstId) {
		let epoch = self.access_counter.fetch_add(1, Ordering::Relaxed);
		let entries = self.entries.read();
		if let Some(entry) = entries.get(id) {
			entry.last_access_epoch.store(epoch, Ordering::Relaxed);
		}
	}

	/// Atomically write an SST to the cache directory (tmp + rename) and
	/// update the in-memory index.  Triggers eviction if the cache exceeds
	/// the configured size limit.
	pub fn insert(&self, id: &SstId, data: &[u8]) -> Result<()> {
		let final_path = self.get_path(id);
		let tmp_path = self.config.path.join(format!("{id}.sst.tmp"));
		std::fs::write(&tmp_path, data)?;
		std::fs::rename(&tmp_path, &final_path)?;

		let size = data.len() as u64;
		let epoch = self.access_counter.fetch_add(1, Ordering::Relaxed);

		{
			let mut entries = self.entries.write();
			// If the entry was already inserted by a concurrent task, remove
			// the old size accounting first.
			if let Some(old) = entries.insert(
				*id,
				CacheEntry {
					size,
					last_access_epoch: AtomicU64::new(epoch),
				},
			) {
				self.current_size.fetch_sub(old.size, Ordering::Relaxed);
			}
		}
		self.current_size.fetch_add(size, Ordering::Relaxed);

		self.evict_if_needed();
		Ok(())
	}

	/// Remove a cached SST (e.g. after compaction deletes it).
	pub fn remove(&self, id: &SstId) {
		let path = self.get_path(id);
		let _ = std::fs::remove_file(&path);
		let mut entries = self.entries.write();
		if let Some(removed) = entries.remove(id) {
			self.current_size.fetch_sub(removed.size, Ordering::Relaxed);
		}
	}

	/// Try to enqueue a background cache-population request.
	/// Returns `true` if the message was enqueued, `false` if the channel
	/// is full (backpressure — the foreground read is not blocked).
	pub fn try_populate(&self, msg: PopulateMsg) -> bool {
		self.populate_tx.try_send(msg).is_ok()
	}

	/// Pick-of-2 LRU eviction: repeatedly sample two random entries and
	/// remove the one with the older access epoch until the cache size
	/// drops below 80 % of `max_size_bytes`.
	fn evict_if_needed(&self) {
		let threshold = self.config.max_size_bytes * 80 / 100;
		let mut entries = self.entries.write();
		let mut rng = rand::rng();

		while self.current_size.load(Ordering::Relaxed) > threshold {
			if entries.len() < 2 {
				break;
			}

			let keys: Vec<SstId> = entries.keys().copied().collect();
			let i = rng.random_range(0..keys.len());
			let j = loop {
				let x = rng.random_range(0..keys.len());
				if x != i {
					break x;
				}
			};

			let epoch_i = entries[&keys[i]].last_access_epoch.load(Ordering::Relaxed);
			let epoch_j = entries[&keys[j]].last_access_epoch.load(Ordering::Relaxed);
			let older = if epoch_i < epoch_j {
				i
			} else {
				j
			};

			let evicted_key = keys[older];
			let path = self.config.path.join(format!("{evicted_key}.sst"));
			let _ = std::fs::remove_file(&path);

			if let Some(removed) = entries.remove(&evicted_key) {
				self.current_size.fetch_sub(removed.size, Ordering::Relaxed);
			}
		}
	}

	/// Background loop that drains the populate channel and writes full
	/// SSTs into the cache.
	async fn populate_loop(cache: Arc<DiskCache>, mut rx: mpsc::Receiver<PopulateMsg>) {
		while let Some(msg) = rx.recv().await {
			// Skip if already cached (another task may have raced).
			if cache.contains(&msg.id) {
				continue;
			}
			if let Ok(result) = msg.object_store.get(&msg.sst_path).await {
				if let Ok(bytes) = result.bytes().await {
					let _ = cache.insert(&msg.id, &bytes);
				}
			}
		}
	}
}
