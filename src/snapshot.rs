use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use crossbeam_skiplist::SkipSet;

use crate::error::{Error, Result};
use crate::iter::BoxedLSMIterator;
use crate::levels::Levels;
use crate::lsm::Core;
use crate::memtable::MemTable;
use crate::merge_operator::MergeOperator;
use crate::sstable::table::Table;
use crate::{
	BytewiseComparator,
	Comparator,
	InternalKey,
	InternalKeyComparator,
	InternalKeyKind,
	InternalKeyRange,
	InternalKeyRef,
	LSMIterator,
	TimestampComparator,
	Value,
};

// ===== History Options =====

/// Options for history (versioned) iteration.
/// Controls what versions and tombstones are included in the iteration.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HistoryOptions {
	/// Whether to include tombstones (deleted entries) in the iteration.
	/// Default: false
	pub include_tombstones: bool,
	/// Optional timestamp range filter (start_ts, end_ts) inclusive.
	/// Only versions within this range are returned.
	/// Default: None (no timestamp filtering)
	pub ts_range: Option<(u64, u64)>,
	/// Optional limit on the total number of entries/versions to return.
	/// Default: None (no limit)
	pub limit: Option<usize>,
}

impl HistoryOptions {
	/// Creates a new HistoryOptions with default values (no tombstones, no filters).
	pub fn new() -> Self {
		Self::default()
	}

	/// Include tombstones (soft-deleted entries) in the iteration.
	pub fn with_tombstones(mut self, include: bool) -> Self {
		self.include_tombstones = include;
		self
	}

	/// Set a timestamp range filter. Only versions within [start_ts, end_ts] are returned.
	pub fn with_ts_range(mut self, start_ts: u64, end_ts: u64) -> Self {
		self.ts_range = Some((start_ts, end_ts));
		self
	}

	/// Set a limit on the total number of entries/versions to return.
	pub fn with_limit(mut self, limit: usize) -> Self {
		self.limit = Some(limit);
		self
	}
}

// ===== Snapshot Tracker =====
/// Tracks active snapshot sequence numbers in the system.
///
/// This tracker maintains the actual sequence numbers of active snapshots,
/// enabling snapshot-aware compaction. During compaction, versions that are
/// visible to any active snapshot must be preserved.
///
/// # Compaction Integration
///
/// The compaction iterator uses `get_all_snapshots()` to obtain a sorted list
/// of active snapshot sequence numbers. For each version being considered for
/// removal, it checks if the version is visible to any snapshot using binary
/// search. Versions visible to snapshots are preserved unless hidden by a newer
/// version in the same visibility boundary.
pub(crate) struct SnapshotTracker {
	snapshots: Arc<SkipSet<u64>>,
}

impl Clone for SnapshotTracker {
	fn clone(&self) -> Self {
		Self {
			snapshots: Arc::clone(&self.snapshots),
		}
	}
}

impl Default for SnapshotTracker {
	fn default() -> Self {
		Self::new()
	}
}

impl std::fmt::Debug for SnapshotTracker {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SnapshotTracker").field("snapshots", &self.get_all_snapshots()).finish()
	}
}

impl SnapshotTracker {
	/// Creates a new empty snapshot tracker.
	pub(crate) fn new() -> Self {
		Self {
			snapshots: Arc::new(SkipSet::new()),
		}
	}

	/// Registers a new snapshot with the given sequence number.
	///
	/// Called when a new snapshot is created. The sequence number is added
	/// to the tracking set, ensuring compaction will preserve versions
	/// visible to this snapshot.
	pub(crate) fn register(&self, seq_num: u64) {
		self.snapshots.insert(seq_num);
	}

	/// Unregisters a snapshot with the given sequence number.
	///
	/// Called when a snapshot is dropped. Once all snapshots at or above
	/// a certain sequence number are dropped, older versions become eligible
	/// for garbage collection during compaction.
	pub(crate) fn unregister(&self, seq_num: u64) {
		self.snapshots.remove(&seq_num);
	}

	/// Returns all active snapshots as a sorted vector.
	///
	/// This is the primary method used by compaction. The returned vector
	/// is sorted in ascending order.
	pub(crate) fn get_all_snapshots(&self) -> Vec<u64> {
		self.snapshots.iter().map(|entry| *entry).collect()
	}
}

// ===== Iterator State =====
/// Holds references to all LSM tree components needed for iteration.
pub(crate) struct IterState {
	/// The active memtable receiving current writes
	pub active: Arc<MemTable>,
	/// Immutable memtables waiting to be flushed
	pub immutable: Vec<Arc<MemTable>>,
	/// All levels containing SSTables
	pub levels: Levels,
}

// ===== Snapshot Implementation =====
/// A consistent point-in-time view of the store.
///
/// # Snapshot Isolation
///
/// Snapshots provide consistent reads by fixing a sequence number at creation
/// time. All reads through the snapshot only see data with sequence numbers
/// less than or equal to the snapshot's sequence number.
///
/// Create a snapshot using `Store::new_snapshot()`.
pub struct Snapshot {
	/// Reference to the LSM tree core
	core: Arc<Core>,

	/// Sequence number defining this snapshot's view of the data
	/// Only data with seq_num <= this value is visible
	pub(crate) seq_num: u64,
}

impl Snapshot {
	/// Creates a new snapshot at the current visible sequence number.
	pub(crate) fn new(core: Arc<Core>) -> Self {
		let seq_num = core.inner.visible_seq_num.load(std::sync::atomic::Ordering::Acquire);
		// Register this snapshot's sequence number so compaction knows
		// to preserve versions visible to this snapshot
		core.snapshot_tracker.register(seq_num);

		Self {
			core,
			seq_num,
		}
	}

	/// Collects the iterator state from all LSM components
	/// This is a helper method used by both iterators and optimized operations
	/// like count
	pub(crate) fn collect_iter_state(&self) -> Result<IterState> {
		let active = guardian::ArcRwLockReadGuardian::take(Arc::clone(&self.core.active_memtable))?;
		let immutable =
			guardian::ArcRwLockReadGuardian::take(Arc::clone(&self.core.immutable_memtables))?;
		let manifest =
			guardian::ArcRwLockReadGuardian::take(Arc::clone(&self.core.level_manifest))?;

		Ok(IterState {
			active: active.clone(),
			immutable: immutable.iter().map(|entry| Arc::clone(&entry.memtable)).collect(),
			levels: manifest.levels.clone(),
		})
	}

	/// Gets a single key from the snapshot.
	///
	/// # Read Path in LSM Trees
	///
	/// The read path checks multiple locations in order:
	/// 1. **Active Memtable**: Most recent writes, in memory
	/// 2. **Immutable Memtables**: Recent writes being flushed
	/// 3. **Level**: From SSTables
	///
	/// The search stops at the first Set/Delete version found with
	/// seq_num <= snapshot seq_num. When Merge entries are encountered,
	/// scanning continues to collect all Merge operands until a Set,
	/// Delete, or end of data is reached, then the merge operator is
	/// applied.
	pub(crate) async fn get(&self, key: &[u8]) -> crate::Result<Option<(Value, u64)>> {
		crate::metrics::DbStats::inc(&self.core.inner.db_stats.point_lookups);
		// Batch size for partial_merge to avoid unbounded memory.
		const MERGE_BATCH_SIZE: usize = 100;

		/// Result of checking a single source for a key.
		enum LookupResult {
			/// Found a Set entry — return or use as merge base.
			Set(Value, u64),
			/// Found a Delete entry — key is deleted.
			Deleted(u64),
			/// Found a Merge entry — collect operand and keep searching.
			Merge(Value, u64),
		}

		// Collect merge operands (oldest-to-newest; we'll reverse at the end
		// since we scan newest-first).
		let mut merge_operands: Vec<Value> = Vec::new();
		let mut merge_seq: Option<u64> = None;
		let merge_op: Option<&dyn MergeOperator> = self.core.opts.merge_operator.as_deref();

		/// Helper: apply batched partial_merge to keep operand list bounded.
		fn batch_partial_merge(
			merge_op: &dyn MergeOperator,
			key: &[u8],
			operands: &mut Vec<Value>,
		) -> crate::Result<()> {
			if operands.len() >= MERGE_BATCH_SIZE {
				// operands are in newest-first order at this point; reverse
				// for the merge call (oldest-first).
				operands.reverse();
				let refs: Vec<&[u8]> = operands.iter().map(|v| v.as_slice()).collect();
				let merged = merge_op.partial_merge(key, &refs)?;
				operands.clear();
				operands.push(merged);
			}
			Ok(())
		}

		// ----- Helper closure to process a lookup result -----
		// Returns: Some(final_answer) when search should stop,
		//          None when search should continue.
		macro_rules! handle_result {
			($result:expr, $key:expr) => {
				match $result {
					LookupResult::Set(value, seq) => {
						if merge_operands.is_empty() {
							return Ok(Some((value, seq)));
						}
						// We have accumulated merge operands; apply full_merge.
						let op = merge_op.ok_or(Error::MergeOperatorRequired)?;
						// Operands are in newest-first order; reverse to oldest-first.
						merge_operands.reverse();
						let refs: Vec<&[u8]> =
							merge_operands.iter().map(|v| v.as_slice()).collect();
						let merged = op.full_merge($key, &value, &refs)?;
						return Ok(Some((merged, merge_seq.unwrap_or(seq))));
					}
					LookupResult::Deleted(seq) => {
						if merge_operands.is_empty() {
							return Ok(None);
						}
						// Treat Delete as "no base" — use partial_merge.
						let op = merge_op.ok_or(Error::MergeOperatorRequired)?;
						merge_operands.reverse();
						let refs: Vec<&[u8]> =
							merge_operands.iter().map(|v| v.as_slice()).collect();
						let merged = op.partial_merge($key, &refs)?;
						return Ok(Some((merged, merge_seq.unwrap_or(seq))));
					}
					LookupResult::Merge(operand, seq) => {
						let _op = merge_op.ok_or(Error::MergeOperatorRequired)?;
						if merge_seq.is_none() {
							merge_seq = Some(seq);
						}
						merge_operands.push(operand);
						// Batch partial merge to bound memory.
						batch_partial_merge(_op, $key, &mut merge_operands)?;
					}
				}
			};
		}

		// ===== Search active memtable =====
		{
			let memtable_lock = self.core.active_memtable.read()?;
			if let Some(item) = memtable_lock.get(key.as_ref(), Some(self.seq_num)) {
				let result = match item.0.kind() {
					InternalKeyKind::Delete | InternalKeyKind::RangeDelete => {
						LookupResult::Deleted(item.0.seq_num())
					}
					InternalKeyKind::Merge => LookupResult::Merge(item.1, item.0.seq_num()),
					_ => LookupResult::Set(item.1, item.0.seq_num()),
				};
				handle_result!(result, key);
			}
		}

		// ===== Search immutable memtables =====
		{
			let memtable_lock = self.core.immutable_memtables.read()?;
			for entry in memtable_lock.iter().rev() {
				let memtable = &entry.memtable;
				if let Some(item) = memtable.get(key.as_ref(), Some(self.seq_num)) {
					let result = match item.0.kind() {
						InternalKeyKind::Delete | InternalKeyKind::RangeDelete => {
							LookupResult::Deleted(item.0.seq_num())
						}
						InternalKeyKind::Merge => LookupResult::Merge(item.1, item.0.seq_num()),
						_ => LookupResult::Set(item.1, item.0.seq_num()),
					};
					handle_result!(result, key);
				}
			}
		}

		let ikey = InternalKey::new(key.to_vec(), self.seq_num, InternalKeyKind::Set, 0);

		// Collect candidate tables under the lock, then drop it before async I/O
		let candidate_tables: Vec<(usize, Arc<Table>)> = {
			let level_manifest = self.core.level_manifest.read()?;

			let mut candidates = Vec::new();
			for (level_idx, level) in (&level_manifest.levels).into_iter().enumerate() {
				if level_idx == 0 {
					for table in level.tables.iter() {
						if table.is_key_in_key_range(&ikey) {
							candidates.push((level_idx, Arc::clone(table)));
						}
					}
				} else {
					let query_range = crate::user_range_to_internal_range(
						Bound::Included(key),
						Bound::Included(key),
					);
					let start_idx = level.find_first_overlapping_table(&query_range);
					let end_idx = level.find_last_overlapping_table(&query_range);
					for table in &level.tables[start_idx..end_idx] {
						candidates.push((level_idx, Arc::clone(table)));
					}
				}
			}
			candidates
		}; // level_manifest lock dropped here

		// ===== Search SSTables =====
		for (_level_idx, table) in &candidate_tables {
			let maybe_item = table.get(&ikey).await?;

			if let Some(item) = maybe_item {
				let result = match item.0.kind() {
					InternalKeyKind::Delete | InternalKeyKind::RangeDelete => {
						LookupResult::Deleted(item.0.seq_num())
					}
					InternalKeyKind::Merge => LookupResult::Merge(item.1, item.0.seq_num()),
					_ => LookupResult::Set(item.1, item.0.seq_num()),
				};
				handle_result!(result, key);
			}
		}

		// ===== End of all sources =====
		// If we have accumulated merge operands but no base value, use partial_merge.
		if !merge_operands.is_empty() {
			let op = merge_op.ok_or(Error::MergeOperatorRequired)?;
			merge_operands.reverse();
			let refs: Vec<&[u8]> = merge_operands.iter().map(|v| v.as_slice()).collect();
			let merged = op.partial_merge(key, &refs)?;
			return Ok(Some((merged, merge_seq.unwrap_or(0))));
		}

		Ok(None)
	}

	/// Creates an iterator over all keys in the snapshot.
	///
	/// The iterator traverses keys in ascending order.
	pub fn new_iter(&self) -> Result<SnapshotIterator<'_>> {
		self.range(None, None)
	}

	/// Creates an iterator for a range scan within the snapshot.
	///
	/// # Arguments
	/// * `lower` - Optional lower bound (inclusive)
	/// * `upper` - Optional upper bound (exclusive)
	///
	/// Returns a SnapshotIterator that implements LSMIterator.
	pub fn range(
		&self,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
	) -> Result<SnapshotIterator<'_>> {
		let internal_range = crate::user_range_to_internal_range(
			lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
			upper.map(Bound::Excluded).unwrap_or(Bound::Unbounded),
		);
		SnapshotIterator::new_from(Arc::clone(&self.core), self.seq_num, internal_range)
	}

	/// Creates a history iterator that scans all versions of keys.
	///
	/// Merges memtable iterators with SSTable iterators via KMergeIterator.
	///
	/// # Arguments
	/// * `lower` - Optional lower bound key (inclusive)
	/// * `upper` - Optional upper bound key (exclusive)
	/// * `include_tombstones` - Whether to include tombstones in the iteration
	/// * `ts_range` - Optional timestamp range filter (start_ts, end_ts) inclusive
	/// * `limit` - Optional limit on total entries returned
	///
	/// # Errors
	/// Returns an error if versioning is not enabled.
	pub fn history_iter(
		&self,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
		include_tombstones: bool,
		ts_range: Option<(u64, u64)>,
		limit: Option<usize>,
	) -> Result<SnapshotIterator<'_>> {
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioning not enabled".to_string()));
		}

		let range = crate::user_range_to_internal_range(
			lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
			upper.map(Bound::Excluded).unwrap_or(Bound::Unbounded),
		);
		let iter_state = self.collect_iter_state()?;

		Ok(SnapshotIterator::new_history(
			self.seq_num,
			iter_state,
			range,
			include_tombstones,
			ts_range,
			limit,
			lower,
			upper,
		))
	}

	/// Queries for a specific key at a specific timestamp.
	/// Only returns data visible to this snapshot (seq_num <= snapshot.seq_num).
	///
	/// Note: This uses HistoryIterator which respects HARD_DELETE barriers.
	/// If a key has been hard-deleted, all historical versions are inaccessible.
	pub async fn get_at(&self, key: &[u8], timestamp: u64) -> Result<Option<Value>> {
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioning not enabled".to_string()));
		}

		// Use history iterator - respects HARD_DELETE barriers
		let mut iter = self.history_iter(Some(key), None, true, None, None)?;
		iter.seek_first().await?;

		// Track the best match (latest version at or before requested timestamp)
		let mut best_value: Option<Value> = None;
		let mut best_timestamp: u64 = 0;

		while iter.valid() {
			let entry_key = iter.key();

			// Stop if we've moved past our key
			if entry_key.user_key() != key {
				break;
			}

			let entry_ts = entry_key.timestamp();

			// Only consider versions at or before the requested timestamp
			if entry_ts <= timestamp && entry_ts >= best_timestamp {
				if entry_key.is_tombstone() {
					best_value = None;
				} else {
					best_value = Some(iter.value_encoded()?.to_vec());
				}
				best_timestamp = entry_ts;
			}

			iter.next().await?;
		}

		Ok(best_value)
	}
}

impl Drop for Snapshot {
	fn drop(&mut self) {
		// Unregister this snapshot's sequence number so compaction can
		// clean up versions no longer visible to any snapshot
		self.core.snapshot_tracker.unregister(self.seq_num);
	}
}

/// Direction of iteration for KMergeIterator
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum MergeDirection {
	Forward,
	Backward,
}

/// A merge iterator that sorts by key+seqno.
/// Uses index-based tracking for zero-allocation iteration.
pub(crate) struct KMergeIterator<'iter> {
	/// Array of iterators to merge over.
	///
	/// IMPORTANT: Due to self-referential structs, this must be defined before
	/// `iter_state` in order to ensure it is dropped before `iter_state`.
	iterators: Vec<BoxedLSMIterator<'iter>>,

	// Owned state
	#[allow(dead_code)]
	iter_state: Box<IterState>,

	/// Current winner index (None if exhausted)
	winner: Option<usize>,

	/// Number of active (valid) iterators
	active_count: usize,

	/// Direction of iteration
	direction: MergeDirection,

	/// Whether the iterator has been initialized
	initialized: bool,

	/// Comparator for key comparison
	cmp: Arc<dyn Comparator>,
}

impl<'a> KMergeIterator<'a> {
	/// Creates a new KMergeIterator with InternalKeyComparator (default).
	/// Use this for normal queries where ordering is by seq_num.
	pub(crate) fn new_from(iter_state: IterState, internal_range: InternalKeyRange) -> Self {
		let cmp: Arc<dyn Comparator> =
			Arc::new(InternalKeyComparator::new(Arc::new(BytewiseComparator::default())));
		Self::new_with_comparator(iter_state, internal_range, cmp, None)
	}

	/// Creates a new KMergeIterator with TimestampComparator for history queries.
	/// This enables timestamp-based seek optimization when timestamps are monotonic with seq_nums.
	pub(crate) fn new_for_history(
		iter_state: IterState,
		internal_range: InternalKeyRange,
		ts_range: Option<(u64, u64)>,
	) -> Self {
		let cmp: Arc<dyn Comparator> =
			Arc::new(TimestampComparator::new(Arc::new(BytewiseComparator::default())));
		Self::new_with_comparator(iter_state, internal_range, cmp, ts_range)
	}

	/// Creates a new KMergeIterator with a configurable comparator.
	fn new_with_comparator(
		iter_state: IterState,
		internal_range: InternalKeyRange,
		cmp: Arc<dyn Comparator>,
		ts_range: Option<(u64, u64)>,
	) -> Self {
		let boxed_state = Box::new(iter_state);

		let query_range = Arc::new(internal_range);

		// Pre-allocate capacity for the iterators.
		// 1 active memtable + immutable memtables + level tables.
		let mut iterators: Vec<BoxedLSMIterator<'a>> =
			Vec::with_capacity(1 + boxed_state.immutable.len() + boxed_state.levels.total_tables());

		let state_ref: &'a IterState = unsafe { &*(&*boxed_state as *const IterState) };

		// Extract user key bounds from InternalKeyRange (inclusive lower, exclusive
		// upper)
		let (start_bound, end_bound) = query_range.as_ref();
		let lower = match start_bound {
			Bound::Included(key) | Bound::Excluded(key) => Some(key.user_key.as_slice()),
			Bound::Unbounded => None,
		};
		let upper = match end_bound {
			Bound::Excluded(key) => Some(key.user_key.as_slice()),
			Bound::Included(_) | Bound::Unbounded => None, /* Included upper handled by table
			                                                * iterators */
		};

		// Active memtable
		let active_iter = state_ref.active.range(lower, upper);
		iterators.push(Box::new(active_iter) as BoxedLSMIterator<'a>);

		// Immutable memtables
		for memtable in &state_ref.immutable {
			let iter = memtable.range(lower, upper);
			iterators.push(Box::new(iter) as BoxedLSMIterator<'a>);
		}

		// Tables - these have native seek support
		for (level_idx, level) in (&state_ref.levels).into_iter().enumerate() {
			// Optimization: Skip tables that are completely outside the query range
			if level_idx == 0 {
				// Level 0: Tables can overlap, so we check all but skip those completely
				// outside range
				for table in &level.tables {
					// Skip tables completely before or after the range
					if table.is_before_range(&query_range) || table.is_after_range(&query_range) {
						continue;
					}
					// Skip tables outside timestamp range (if specified)
					if let Some((ts_start, ts_end)) = ts_range {
						let props = &table.meta.properties;
						if let (Some(newest), Some(oldest)) =
							(props.newest_key_time, props.oldest_key_time)
						{
							if newest < ts_start || oldest > ts_end {
								continue;
							}
						}
					}
					// Use custom comparator for table iteration
					if let Ok(table_iter) =
						table.iter_with_comparator(Some((*query_range).clone()), Arc::clone(&cmp))
					{
						iterators.push(Box::new(table_iter) as BoxedLSMIterator<'a>);
					}
				}
			} else {
				// Level 1+: Tables have non-overlapping key ranges, use binary search
				let start_idx = level.find_first_overlapping_table(&query_range);
				let end_idx = level.find_last_overlapping_table(&query_range);

				for table in &level.tables[start_idx..end_idx] {
					// Skip tables outside timestamp range (if specified)
					if let Some((ts_start, ts_end)) = ts_range {
						let props = &table.meta.properties;
						if let (Some(newest), Some(oldest)) =
							(props.newest_key_time, props.oldest_key_time)
						{
							if newest < ts_start || oldest > ts_end {
								continue;
							}
						}
					}
					// Use custom comparator for table iteration
					if let Ok(table_iter) =
						table.iter_with_comparator(Some((*query_range).clone()), Arc::clone(&cmp))
					{
						iterators.push(Box::new(table_iter) as BoxedLSMIterator<'a>);
					}
				}
			}
		}

		Self {
			iterators,
			iter_state: boxed_state,
			winner: None,
			active_count: 0,
			direction: MergeDirection::Forward,
			initialized: false,
			cmp,
		}
	}

	/// Compare two iterators by their current key (zero-copy)
	#[inline]
	fn compare(&self, a: usize, b: usize) -> Ordering {
		let iter_a = &self.iterators[a];
		let iter_b = &self.iterators[b];

		let valid_a = iter_a.valid();
		let valid_b = iter_b.valid();

		match (valid_a, valid_b) {
			(false, false) => Ordering::Equal,
			(true, false) => Ordering::Less, // a wins (valid beats invalid)
			(false, true) => Ordering::Greater, // b wins
			(true, true) => {
				// Both valid - compare keys (zero-copy from iterators)
				let key_a = iter_a.key().encoded();
				let key_b = iter_b.key().encoded();
				let ord = self.cmp.compare(key_a, key_b);
				if self.direction == MergeDirection::Backward {
					ord.reverse()
				} else {
					ord
				}
			}
		}
	}

	/// Find the winner (min for forward, max for backward) among all valid iterators
	fn find_winner(&mut self) {
		if self.iterators.is_empty() || self.active_count == 0 {
			self.winner = None;
			return;
		}

		let mut best_idx = None;
		for i in 0..self.iterators.len() {
			if !self.iterators[i].valid() {
				continue;
			}
			match best_idx {
				None => best_idx = Some(i),
				Some(b) => {
					if self.compare(i, b) == Ordering::Less {
						best_idx = Some(i);
					}
				}
			}
		}

		self.winner = best_idx;
	}

	/// Initialize for forward iteration
	async fn init_forward(&mut self) -> Result<()> {
		self.direction = MergeDirection::Forward;
		self.active_count = 0;

		// Position all iterators at first
		for iter in &mut self.iterators {
			if iter.seek_first().await? {
				self.active_count += 1;
			}
		}

		self.find_winner();
		self.initialized = true;
		Ok(())
	}

	/// Initialize for backward iteration
	async fn init_backward(&mut self) -> Result<()> {
		self.direction = MergeDirection::Backward;
		self.active_count = 0;

		// Position all iterators at last
		for iter in &mut self.iterators {
			if iter.seek_last().await? {
				self.active_count += 1;
			}
		}

		self.find_winner();
		self.initialized = true;
		Ok(())
	}

	/// Switch from backward to forward, positioning just after `target`.
	///
	/// When switching directions, the current iterator just moves forward once.
	/// Non-current iterators need to be positioned at a key strictly greater than
	/// `target` to ensure correct ordering.
	async fn switch_to_forward(&mut self, target: &[u8]) -> Result<()> {
		let current_idx = self.winner;
		self.direction = MergeDirection::Forward;
		self.active_count = 0;

		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if Some(idx) == current_idx {
				// Current iterator: just call next() once
				if iter.next().await? {
					self.active_count += 1;
				}
			} else {
				// Non-current: seek to target, then advance past it
				if iter.seek(target).await? {
					// Advance while key <= target (need to be strictly greater)
					while iter.valid()
						&& self.cmp.compare(iter.key().encoded(), target) != Ordering::Greater
					{
						if !iter.next().await? {
							break;
						}
					}
					if iter.valid() {
						self.active_count += 1;
					}
				}
			}
		}

		self.find_winner();
		Ok(())
	}

	/// Switch from forward to backward, positioning just before `target`.
	///
	/// When switching directions, the current iterator just moves backward once.
	/// Non-current iterators need to be positioned at a key strictly less than
	/// `target` to ensure correct ordering.
	async fn switch_to_backward(&mut self, target: &[u8]) -> Result<()> {
		let current_idx = self.winner;
		self.direction = MergeDirection::Backward;
		self.active_count = 0;

		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if Some(idx) == current_idx {
				// Current iterator: just call prev() once
				if iter.prev().await? {
					self.active_count += 1;
				}
			} else {
				// Non-current: seek to target, then move before it
				if iter.seek(target).await? {
					// Move backward while key >= target (need to be strictly less)
					while iter.valid()
						&& self.cmp.compare(iter.key().encoded(), target) != Ordering::Less
					{
						if !iter.prev().await? {
							break;
						}
					}
					if iter.valid() {
						self.active_count += 1;
					}
				} else {
					// Iterator positioned past all keys, go to last
					if iter.seek_last().await? {
						self.active_count += 1;
					}
				}
			}
		}

		self.find_winner();
		Ok(())
	}

	/// Advance the current winner and find new winner
	async fn advance_winner(&mut self) -> Result<bool> {
		if self.active_count == 0 || self.winner.is_none() {
			return Ok(false);
		}

		let winner_idx = self.winner.unwrap();
		let iter = &mut self.iterators[winner_idx];

		// Advance the winning iterator
		let still_valid = if self.direction == MergeDirection::Forward {
			iter.next().await?
		} else {
			iter.prev().await?
		};

		if !still_valid {
			self.active_count = self.active_count.saturating_sub(1);
		}

		// Find new winner
		self.find_winner();

		Ok(self.winner.is_some())
	}

	/// Check if iterator is positioned on a valid entry
	#[inline]
	pub fn is_valid(&self) -> bool {
		self.winner.is_some() && self.iterators[self.winner.unwrap()].valid()
	}
}

#[async_trait::async_trait]
impl LSMIterator for KMergeIterator<'_> {
	async fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.active_count = 0;

		for iter in &mut self.iterators {
			if iter.seek(target).await? {
				self.active_count += 1;
			}
		}

		self.find_winner();
		self.initialized = true;
		Ok(self.is_valid())
	}

	async fn seek_first(&mut self) -> Result<bool> {
		self.init_forward().await?;
		Ok(self.is_valid())
	}

	async fn seek_last(&mut self) -> Result<bool> {
		self.init_backward().await?;
		Ok(self.is_valid())
	}

	async fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first().await;
		}
		if !self.is_valid() {
			return Ok(false);
		}
		// If we were going backward, switch to forward
		if self.direction != MergeDirection::Forward {
			let target = self.key().encoded().to_vec();
			self.switch_to_forward(&target).await?;
			return Ok(self.is_valid());
		}
		self.advance_winner().await
	}

	async fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last().await;
		}
		if !self.is_valid() {
			return Ok(false);
		}
		// If we were going forward, switch to backward
		if self.direction != MergeDirection::Backward {
			let target = self.key().encoded().to_vec();
			self.switch_to_backward(&target).await?;
			return Ok(self.is_valid());
		}
		self.advance_winner().await
	}

	fn valid(&self) -> bool {
		self.is_valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.is_valid());
		self.iterators[self.winner.unwrap()].key()
	}

	fn value_encoded(&self) -> Result<&[u8]> {
		debug_assert!(self.is_valid());
		self.iterators[self.winner.unwrap()].value_encoded()
	}
}

// ===== Unified Snapshot Iterator =====
// Handles both snapshot (latest-version-only) and history (all-versions) modes
// in a single code path with inline branches, controlled by `history_opts`.

#[derive(Clone)]
struct BufferedEntry {
	key: Vec<u8>,
	value: Vec<u8>,
}

/// Internal options for history mode. When `None`, the iterator operates in
/// snapshot mode (returns only the latest visible version per user key).
struct HistoryOpts {
	include_tombstones: bool,
	ts_range: Option<(u64, u64)>,
	limit: Option<usize>,
}

pub struct SnapshotIterator<'a> {
	merge_iter: KMergeIterator<'a>,
	snapshot_seq_num: u64,
	direction: MergeDirection,
	initialized: bool,

	/// None = snapshot mode (latest version per key),
	/// Some = history mode (all versions with barrier semantics).
	history_opts: Option<HistoryOpts>,

	/// Current user key being processed (used for dedup in snapshot mode
	/// and barrier tracking in history mode).
	current_key: Vec<u8>,

	/// When true, skip all remaining versions of current_key.
	/// Snapshot mode: set after returning an entry or encountering a delete.
	/// History mode: set when DELETE-as-latest triggers full key skip.
	skip_current_key: bool,

	/// History-only forward state (never touched when history_opts is None).
	first_visible_seen: bool,
	latest_is_hard_delete: bool,
	barrier_seen: bool,

	/// Backward buffer (both modes). Snapshot mode: 0-1 entries. History mode: N entries.
	backward_buffer: Vec<BufferedEntry>,
	backward_buffer_index: Option<usize>,

	/// Bounds (both modes; redundant for snapshot but harmless).
	lower_bound: Option<Vec<u8>>,
	upper_bound: Option<Vec<u8>>,

	/// History-only counters (inert when history_opts is None).
	entries_returned: usize,
	limit_reached: bool,
}

impl SnapshotIterator<'_> {
	/// Creates a snapshot-mode iterator over a specific key range.
	fn new_from(core: Arc<Core>, seq_num: u64, range: InternalKeyRange) -> Result<Self> {
		let snapshot = Snapshot {
			core: Arc::clone(&core),
			seq_num,
		};
		let iter_state = snapshot.collect_iter_state()?;
		let merge_iter = KMergeIterator::new_from(iter_state, range);

		Ok(Self {
			merge_iter,
			snapshot_seq_num: seq_num,
			direction: MergeDirection::Forward,
			initialized: false,
			history_opts: None,
			current_key: Vec::new(),
			skip_current_key: false,
			first_visible_seen: false,
			latest_is_hard_delete: false,
			barrier_seen: false,
			backward_buffer: Vec::new(),
			backward_buffer_index: None,
			lower_bound: None,
			upper_bound: None,
			entries_returned: 0,
			limit_reached: false,
		})
	}

	/// Creates a history-mode iterator that returns all versions with barrier semantics.
	#[allow(clippy::too_many_arguments)]
	fn new_history(
		seq_num: u64,
		iter_state: IterState,
		range: InternalKeyRange,
		include_tombstones: bool,
		ts_range: Option<(u64, u64)>,
		limit: Option<usize>,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
	) -> Self {
		let merge_iter = if ts_range.is_some() {
			KMergeIterator::new_for_history(iter_state, range, ts_range)
		} else {
			KMergeIterator::new_from(iter_state, range)
		};

		Self {
			merge_iter,
			snapshot_seq_num: seq_num,
			direction: MergeDirection::Forward,
			initialized: false,
			history_opts: Some(HistoryOpts {
				include_tombstones,
				ts_range,
				limit,
			}),
			current_key: Vec::new(),
			skip_current_key: false,
			first_visible_seen: false,
			latest_is_hard_delete: false,
			barrier_seen: false,
			backward_buffer: Vec::new(),
			backward_buffer_index: None,
			lower_bound: lower.map(|b| b.to_vec()),
			upper_bound: upper.map(|b| b.to_vec()),
			entries_returned: 0,
			limit_reached: false,
		}
	}

	#[inline]
	fn is_history_mode(&self) -> bool {
		self.history_opts.is_some()
	}

	fn reset_forward_state(&mut self) {
		self.current_key.clear();
		self.skip_current_key = false;
		if self.is_history_mode() {
			self.first_visible_seen = false;
			self.latest_is_hard_delete = false;
			self.barrier_seen = false;
		}
	}

	fn reset_all_state(&mut self) {
		self.reset_forward_state();
		self.backward_buffer.clear();
		self.backward_buffer_index = None;
		self.entries_returned = 0;
		self.limit_reached = false;
	}

	// --- Bounds checking ---

	fn within_upper_bound(&self) -> bool {
		if let Some(ref upper) = self.upper_bound {
			if self.merge_iter.valid() {
				self.merge_iter.key().user_key() < upper.as_slice()
			} else {
				false
			}
		} else {
			true
		}
	}

	fn user_key_within_lower_bound(&self, user_key: &[u8]) -> bool {
		match &self.lower_bound {
			Some(lower) => user_key >= lower.as_slice(),
			None => true,
		}
	}

	fn user_key_within_upper_bound(&self, user_key: &[u8]) -> bool {
		match &self.upper_bound {
			Some(upper) => user_key < upper.as_slice(),
			None => true,
		}
	}

	// --- Backward buffer helpers ---

	fn has_buffered_entry(&self) -> bool {
		matches!(self.backward_buffer_index, Some(idx) if idx < self.backward_buffer.len())
	}

	fn buffered_key(&self) -> InternalKeyRef<'_> {
		let idx = self.backward_buffer_index.unwrap();
		InternalKeyRef::from_encoded(&self.backward_buffer[idx].key)
	}

	fn buffered_value(&self) -> &[u8] {
		let idx = self.backward_buffer_index.unwrap();
		&self.backward_buffer[idx].value
	}

	// --- History-only: advance to next user key with optional seek optimization ---

	async fn advance_to_next_user_key(&mut self) -> Result<bool> {
		let ts_end = match self.history_opts.as_ref().and_then(|h| h.ts_range) {
			Some((_, end)) => end,
			None => {
				// Linear scan past current user_key
				let current = self.current_key.clone();
				while self.merge_iter.valid() {
					if self.merge_iter.key().user_key() != current.as_slice() {
						return Ok(true);
					}
					self.merge_iter.next().await?;
				}
				return Ok(false);
			}
		};

		let current = self.current_key.clone();
		while self.merge_iter.valid() {
			let next_key_vec = self.merge_iter.key().user_key().to_vec();
			if next_key_vec != current {
				let seek_key =
					InternalKey::new(next_key_vec, u64::MAX, InternalKeyKind::Set, ts_end);
				self.merge_iter.seek(&seek_key.encode()).await?;
				return Ok(self.merge_iter.valid());
			}
			self.merge_iter.next().await?;
		}
		Ok(false)
	}

	// ===== UNIFIED FORWARD ITERATION =====

	/// Skip to next valid entry in forward direction.
	/// One code path handles both snapshot and history modes via inline branches.
	async fn skip_to_valid_forward(&mut self) -> Result<bool> {
		let is_history = self.is_history_mode();

		while self.merge_iter.valid() {
			// [HISTORY-ONLY] Limit check
			if let Some(ref opts) = self.history_opts {
				if let Some(limit) = opts.limit {
					if self.entries_returned >= limit {
						self.limit_reached = true;
						return Ok(false);
					}
				}
			}

			// [HISTORY-ONLY] Upper bound check in valid position
			if is_history {
				if let Some(ref upper) = self.upper_bound {
					if self.merge_iter.key().user_key() >= upper.as_slice() {
						return Ok(false);
					}
				}
			}

			let key_ref = self.merge_iter.key();
			let user_key = key_ref.user_key();
			let seq_num = key_ref.seq_num();
			let is_delete = key_ref.is_tombstone();

			// [HISTORY-ONLY] Lower bound check
			if is_history && !self.user_key_within_lower_bound(user_key) {
				self.merge_iter.next().await?;
				continue;
			}

			// [BOTH] User key change detection → reset state
			if user_key != self.current_key.as_slice() {
				self.current_key.clear();
				self.current_key.extend_from_slice(user_key);
				self.skip_current_key = false;
				if is_history {
					self.first_visible_seen = false;
					self.latest_is_hard_delete = false;
					self.barrier_seen = false;
				}
			}

			// [BOTH] Skip older versions of current user key
			if self.skip_current_key {
				self.merge_iter.next().await?;
				continue;
			}

			// [BOTH] Visibility check (seq_num)
			if seq_num > self.snapshot_seq_num {
				self.merge_iter.next().await?;
				continue;
			}

			// [HISTORY-ONLY] Timestamp range filtering
			if let Some(ref opts) = self.history_opts {
				if let Some((ts_start, ts_end)) = opts.ts_range {
					let timestamp = key_ref.timestamp();
					if timestamp > ts_end {
						self.merge_iter.next().await?;
						continue;
					}
					if timestamp < ts_start {
						// All remaining entries for this key are below range
						if !self.advance_to_next_user_key().await? {
							return Ok(false);
						}
						continue;
					}
				}
			}

			// ── ENTRY HANDLING (inline mode branch) ──
			if !is_history {
				// ══ SNAPSHOT MODE ══
				if is_delete {
					self.skip_current_key = true;
					self.merge_iter.next().await?;
					continue;
				}
				// Found valid entry. Mark skip so next() skips older versions.
				self.skip_current_key = true;
				return Ok(true);
			} else {
				// ══ HISTORY MODE ══
				if !self.first_visible_seen {
					self.first_visible_seen = true;
					if is_delete {
						self.latest_is_hard_delete = true;
					}
				}

				// DELETE-as-latest → skip entire key
				if self.latest_is_hard_delete {
					self.skip_current_key = true;
					self.merge_iter.next().await?;
					continue;
				}

				// Already past a barrier → skip everything older
				if self.barrier_seen {
					self.merge_iter.next().await?;
					continue;
				}

				// Non-latest DELETE → mark barrier, skip this entry
				if is_delete {
					self.barrier_seen = true;
					self.merge_iter.next().await?;
					continue;
				}

				// Tombstone filtering
				if !self.history_opts.as_ref().unwrap().include_tombstones && is_delete {
					self.merge_iter.next().await?;
					continue;
				}

				// Found valid history entry. Do NOT set skip_current_key.
				self.entries_returned += 1;
				return Ok(true);
			}
		}
		Ok(false)
	}

	// ===== UNIFIED BACKWARD ITERATION =====

	/// Collect versions of current user key going backward, populate backward_buffer.
	/// Snapshot mode: keeps only the latest visible non-tombstone (0-1 entries).
	/// History mode: keeps all visible versions with barrier rules (N entries).
	async fn collect_user_key_backward(&mut self) -> Result<bool> {
		let is_history = self.is_history_mode();

		loop {
			self.backward_buffer.clear();
			self.backward_buffer_index = None;

			if !self.merge_iter.valid() {
				return Ok(false);
			}

			let user_key = self.merge_iter.key().user_key().to_vec();

			// [HISTORY-ONLY] bounds checks
			if is_history {
				if !self.user_key_within_lower_bound(&user_key) {
					return Ok(false);
				}
				if !self.user_key_within_upper_bound(&user_key) {
					while self.merge_iter.valid()
						&& self.merge_iter.key().user_key() == user_key.as_slice()
					{
						self.merge_iter.prev().await?;
					}
					continue;
				}
			}

			// Scan all versions of this user_key via prev()
			// (prev within same user_key gives ascending seq_num: oldest first)
			let mut latest_visible_key: Option<Vec<u8>> = None;
			let mut latest_visible_value: Option<Vec<u8>> = None;

			struct VersionInfo {
				is_delete: bool,
				encoded_key: Vec<u8>,
				value: Vec<u8>,
			}
			let mut versions: Vec<VersionInfo> = Vec::new();

			while self.merge_iter.valid() {
				let key_ref = self.merge_iter.key();
				if key_ref.user_key() != user_key.as_slice() {
					break;
				}

				let visible = key_ref.seq_num() <= self.snapshot_seq_num;

				if !is_history {
					// SNAPSHOT: keep overwriting with each visible entry (last = latest)
					if visible {
						latest_visible_key = Some(key_ref.encoded().to_vec());
						latest_visible_value = Some(self.merge_iter.value_encoded()?.to_vec());
					}
				} else {
					// HISTORY: collect all visible + in-ts-range versions
					let in_ts_range = match self.history_opts.as_ref().and_then(|h| h.ts_range) {
						Some((ts_start, ts_end)) => {
							let ts = key_ref.timestamp();
							ts >= ts_start && ts <= ts_end
						}
						None => true,
					};
					if visible && in_ts_range {
						versions.push(VersionInfo {
							is_delete: key_ref.is_tombstone(),
							encoded_key: key_ref.encoded().to_vec(),
							value: self.merge_iter.value_encoded()?.to_vec(),
						});
					}
				}

				self.merge_iter.prev().await?;
			}

			// Build backward_buffer
			if !is_history {
				// SNAPSHOT: 0 or 1 entry (latest visible, skip if tombstone)
				match (latest_visible_key, latest_visible_value) {
					(Some(k), Some(v)) => {
						let key_ref = InternalKeyRef::from_encoded(&k);
						if key_ref.is_tombstone() {
							continue; // tombstone → try prev user_key
						}
						self.backward_buffer.push(BufferedEntry {
							key: k,
							value: v,
						});
						self.backward_buffer_index = Some(0);
						return Ok(true);
					}
					_ => continue, // no visible version → try prev user_key
				}
			} else {
				// HISTORY: apply barrier rules
				if versions.is_empty() {
					continue;
				}

				// versions are in seq_num ASC order (oldest first, newest last)
				let latest = versions.last().unwrap();

				// DELETE-as-latest → skip entire key
				if latest.is_delete {
					continue;
				}

				// Find first DELETE barrier from newest
				let mut barrier_idx: Option<usize> = None;
				for i in (0..versions.len()).rev() {
					if versions[i].is_delete {
						barrier_idx = Some(i);
						break;
					}
				}

				let valid_start = barrier_idx.map(|i| i + 1).unwrap_or(0);
				let include_tombstones =
					self.history_opts.as_ref().map(|h| h.include_tombstones).unwrap_or(false);

				for v in versions.into_iter().skip(valid_start) {
					if v.is_delete {
						continue;
					}
					if !include_tombstones && v.is_delete {
						continue;
					}
					self.backward_buffer.push(BufferedEntry {
						key: v.encoded_key,
						value: v.value,
					});
				}

				if self.backward_buffer.is_empty() {
					continue;
				}

				// Limit handling
				if let Some(limit) = self.history_opts.as_ref().and_then(|h| h.limit) {
					let remaining = limit.saturating_sub(self.entries_returned);
					if remaining == 0 {
						self.backward_buffer.clear();
						self.limit_reached = true;
						return Ok(false);
					}
					if self.backward_buffer.len() > remaining {
						self.backward_buffer.truncate(remaining);
					}
				}

				self.entries_returned += self.backward_buffer.len();
				self.backward_buffer_index = Some(0);
				return Ok(true);
			}
		}
	}

	/// Advance within the backward buffer, or load the next user key.
	async fn advance_backward(&mut self) -> Result<bool> {
		if let Some(idx) = self.backward_buffer_index {
			if idx + 1 < self.backward_buffer.len() {
				self.backward_buffer_index = Some(idx + 1);
				return Ok(true);
			}
		}
		self.collect_user_key_backward().await
	}

	// ===== DIRECTION SWITCHING =====

	/// Switch from backward to forward direction.
	async fn reverse_to_forward(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.reset_forward_state();

		if !self.has_buffered_entry() {
			self.backward_buffer.clear();
			self.backward_buffer_index = None;
			return Ok(false);
		}

		if !self.is_history_mode() {
			// SNAPSHOT: seek to user_key with MAX_SEQ, then skip via skip_current_key
			let current_user_key = self.buffered_key().user_key().to_vec();
			self.backward_buffer.clear();
			self.backward_buffer_index = None;

			self.current_key.clear();
			self.current_key.extend_from_slice(&current_user_key);
			self.skip_current_key = true;

			let seek_key =
				InternalKey::new(current_user_key, u64::MAX, InternalKeyKind::Set, u64::MAX);
			self.merge_iter.seek(&seek_key.encode()).await?;
			self.skip_to_valid_forward().await
		} else {
			// HISTORY: seek to exact internal key, advance past it
			let current_internal_key = self.buffered_key().encoded().to_vec();
			self.backward_buffer.clear();
			self.backward_buffer_index = None;

			self.merge_iter.seek(&current_internal_key).await?;
			if !self.merge_iter.valid() {
				return Ok(false);
			}
			self.merge_iter.next().await?;
			self.skip_to_valid_forward().await
		}
	}

	/// Switch from forward to backward direction.
	async fn forward_to_backward(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		self.backward_buffer.clear();
		self.backward_buffer_index = None;

		if !self.merge_iter.valid() {
			return Ok(false);
		}

		self.merge_iter.prev().await?;
		self.collect_user_key_backward().await
	}
}

#[async_trait::async_trait]
impl LSMIterator for SnapshotIterator<'_> {
	async fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.reset_all_state();

		let seek_key = InternalKey::new(target.to_vec(), u64::MAX, InternalKeyKind::Set, u64::MAX);
		self.merge_iter.seek(&seek_key.encode()).await?;
		self.initialized = true;
		self.skip_to_valid_forward().await
	}

	async fn seek_first(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.reset_all_state();

		// History mode: use ts_range-aware seeking for optimization
		if let Some(ref opts) = self.history_opts {
			if opts.ts_range.is_some() {
				let ts = opts.ts_range.map(|(_, end)| end).unwrap_or(u64::MAX);
				let seek_key = InternalKey::new(
					self.lower_bound.clone().unwrap_or_default(),
					u64::MAX,
					InternalKeyKind::Set,
					ts,
				);
				self.merge_iter.seek(&seek_key.encode()).await?;
			} else if let Some(ref lower) = self.lower_bound {
				let seek_key =
					InternalKey::new(lower.clone(), u64::MAX, InternalKeyKind::Set, u64::MAX);
				self.merge_iter.seek(&seek_key.encode()).await?;
			} else {
				self.merge_iter.seek_first().await?;
			}
		} else {
			self.merge_iter.seek_first().await?;
		}

		self.initialized = true;
		self.skip_to_valid_forward().await
	}

	async fn seek_last(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		self.reset_all_state();

		self.merge_iter.seek_last().await?;
		self.initialized = true;
		self.collect_user_key_backward().await
	}

	async fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first().await;
		}

		if self.direction == MergeDirection::Backward {
			return self.reverse_to_forward().await;
		}

		if !self.merge_iter.valid() {
			return Ok(false);
		}
		self.merge_iter.next().await?;
		self.skip_to_valid_forward().await
	}

	async fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last().await;
		}

		if self.direction != MergeDirection::Backward {
			return self.forward_to_backward().await;
		}

		if !self.has_buffered_entry() && !self.merge_iter.valid() {
			return Ok(false);
		}
		self.advance_backward().await
	}

	fn valid(&self) -> bool {
		if self.limit_reached {
			return false;
		}
		match self.direction {
			MergeDirection::Forward => self.merge_iter.valid() && self.within_upper_bound(),
			MergeDirection::Backward => self.has_buffered_entry(),
		}
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		match self.direction {
			MergeDirection::Forward => self.merge_iter.key(),
			MergeDirection::Backward => self.buffered_key(),
		}
	}

	fn value_encoded(&self) -> Result<&[u8]> {
		debug_assert!(self.valid());
		match self.direction {
			MergeDirection::Forward => self.merge_iter.value_encoded(),
			MergeDirection::Backward => Ok(self.buffered_value()),
		}
	}
}

/// Type alias for backward compatibility with code referencing HistoryIterator.
pub type HistoryIterator<'a> = SnapshotIterator<'a>;

#[cfg(test)]
mod tests {
	use super::SnapshotTracker;

	#[test]
	fn test_snapshot_tracker_ordering() {
		let tracker = SnapshotTracker::new();

		// Insert snapshots in non-sorted order
		tracker.register(100);
		tracker.register(50);
		tracker.register(200);
		tracker.register(75);
		tracker.register(150);

		// Verify get_all_snapshots returns sorted order
		let snapshots = tracker.get_all_snapshots();
		assert_eq!(snapshots, vec![50, 75, 100, 150, 200]);

		// Unregister some and verify order is maintained
		tracker.unregister(100);
		tracker.unregister(50);

		let snapshots = tracker.get_all_snapshots();
		assert_eq!(snapshots, vec![75, 150, 200]);

		// Add more and verify
		tracker.register(25);
		tracker.register(300);

		let snapshots = tracker.get_all_snapshots();
		assert_eq!(snapshots, vec![25, 75, 150, 200, 300]);
	}

	#[test]
	fn test_snapshot_tracker_empty() {
		let tracker = SnapshotTracker::new();
		assert!(tracker.get_all_snapshots().is_empty());
	}

	#[test]
	fn test_snapshot_tracker_clone_shares_state() {
		let tracker1 = SnapshotTracker::new();
		tracker1.register(100);

		let tracker2 = tracker1.clone();
		tracker2.register(50);

		// Both should see the same snapshots
		assert_eq!(tracker1.get_all_snapshots(), vec![50, 100]);
		assert_eq!(tracker2.get_all_snapshots(), vec![50, 100]);
	}
}
