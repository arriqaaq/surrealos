use std::sync::atomic::{AtomicU64, Ordering};

/// Live metrics counters for the LSM storage engine. All fields are atomically updated.
/// Use `snapshot()` to get a plain copy for reporting.
pub struct DbStats {
	// Write path
	pub commits: AtomicU64,
	pub bytes_written: AtomicU64,
	pub memtable_flushes: AtomicU64,
	pub memtable_rotations: AtomicU64,
	// Read path
	pub point_lookups: AtomicU64,
	pub range_scans: AtomicU64,
	// Cache
	pub cache_data_hits: AtomicU64,
	pub cache_data_misses: AtomicU64,
	pub cache_index_hits: AtomicU64,
	pub cache_index_misses: AtomicU64,
	// Compaction
	pub compactions_completed: AtomicU64,
	pub compaction_bytes_read: AtomicU64,
	pub compaction_bytes_written: AtomicU64,
	// Object store
	pub object_store_reads: AtomicU64,
	pub object_store_writes: AtomicU64,
	pub object_store_deletes: AtomicU64,
	pub object_store_errors: AtomicU64,
	// Stalls
	pub write_stalls: AtomicU64,
	// WAL
	pub wal_rotations: AtomicU64,
	pub wal_segments_cleaned: AtomicU64,
}

impl DbStats {
	pub fn new() -> Self {
		Self {
			commits: AtomicU64::new(0),
			bytes_written: AtomicU64::new(0),
			memtable_flushes: AtomicU64::new(0),
			memtable_rotations: AtomicU64::new(0),
			point_lookups: AtomicU64::new(0),
			range_scans: AtomicU64::new(0),
			cache_data_hits: AtomicU64::new(0),
			cache_data_misses: AtomicU64::new(0),
			cache_index_hits: AtomicU64::new(0),
			cache_index_misses: AtomicU64::new(0),
			compactions_completed: AtomicU64::new(0),
			compaction_bytes_read: AtomicU64::new(0),
			compaction_bytes_written: AtomicU64::new(0),
			object_store_reads: AtomicU64::new(0),
			object_store_writes: AtomicU64::new(0),
			object_store_deletes: AtomicU64::new(0),
			object_store_errors: AtomicU64::new(0),
			write_stalls: AtomicU64::new(0),
			wal_rotations: AtomicU64::new(0),
			wal_segments_cleaned: AtomicU64::new(0),
		}
	}

	/// Take a point-in-time snapshot of all counters.
	pub fn snapshot(&self) -> StatsSnapshot {
		StatsSnapshot {
			commits: self.commits.load(Ordering::Relaxed),
			bytes_written: self.bytes_written.load(Ordering::Relaxed),
			memtable_flushes: self.memtable_flushes.load(Ordering::Relaxed),
			memtable_rotations: self.memtable_rotations.load(Ordering::Relaxed),
			point_lookups: self.point_lookups.load(Ordering::Relaxed),
			range_scans: self.range_scans.load(Ordering::Relaxed),
			cache_data_hits: self.cache_data_hits.load(Ordering::Relaxed),
			cache_data_misses: self.cache_data_misses.load(Ordering::Relaxed),
			cache_index_hits: self.cache_index_hits.load(Ordering::Relaxed),
			cache_index_misses: self.cache_index_misses.load(Ordering::Relaxed),
			compactions_completed: self.compactions_completed.load(Ordering::Relaxed),
			compaction_bytes_read: self.compaction_bytes_read.load(Ordering::Relaxed),
			compaction_bytes_written: self.compaction_bytes_written.load(Ordering::Relaxed),
			object_store_reads: self.object_store_reads.load(Ordering::Relaxed),
			object_store_writes: self.object_store_writes.load(Ordering::Relaxed),
			object_store_deletes: self.object_store_deletes.load(Ordering::Relaxed),
			object_store_errors: self.object_store_errors.load(Ordering::Relaxed),
			write_stalls: self.write_stalls.load(Ordering::Relaxed),
			wal_rotations: self.wal_rotations.load(Ordering::Relaxed),
			wal_segments_cleaned: self.wal_segments_cleaned.load(Ordering::Relaxed),
		}
	}

	/// Increment an atomic counter by 1.
	#[inline]
	pub(crate) fn inc(field: &AtomicU64) {
		field.fetch_add(1, Ordering::Relaxed);
	}

	/// Add a value to an atomic counter.
	#[inline]
	pub(crate) fn add(field: &AtomicU64, val: u64) {
		field.fetch_add(val, Ordering::Relaxed);
	}
}

impl Default for DbStats {
	fn default() -> Self {
		Self::new()
	}
}

/// Plain struct snapshot of all metrics at a point in time.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
	// Write path
	pub commits: u64,
	pub bytes_written: u64,
	pub memtable_flushes: u64,
	pub memtable_rotations: u64,
	// Read path
	pub point_lookups: u64,
	pub range_scans: u64,
	// Cache
	pub cache_data_hits: u64,
	pub cache_data_misses: u64,
	pub cache_index_hits: u64,
	pub cache_index_misses: u64,
	// Compaction
	pub compactions_completed: u64,
	pub compaction_bytes_read: u64,
	pub compaction_bytes_written: u64,
	// Object store
	pub object_store_reads: u64,
	pub object_store_writes: u64,
	pub object_store_deletes: u64,
	pub object_store_errors: u64,
	// Stalls
	pub write_stalls: u64,
	// WAL
	pub wal_rotations: u64,
	pub wal_segments_cleaned: u64,
}
