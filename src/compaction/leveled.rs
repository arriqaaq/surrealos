use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use super::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::levels::{Level, LevelManifest};
use crate::sstable::table::Table;
use crate::sstable::SstId;
use crate::{InternalKeyRange, Options, Result};

/// Compaction priority strategy for selecting files to compact
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompactionPriority {
	#[allow(unused)]
	/// Files whose range hasn't been compacted for the longest
	OldestSmallestSeqFirst,

	/// Files whose latest update is oldest
	#[allow(unused)]
	OldestLargestSeqFirst,

	/// Larger files compensated by deletes (default)
	#[default]
	ByCompensatedSize,
}

pub(crate) struct Strategy {
	// Number of L0 files that trigger compaction
	level0_file_num_trigger: usize,
	// Base size for level 1+ in bytes
	max_bytes_for_level: u64,
	// Size multiplier for each level
	level_multiplier: f64,
	compaction_priority: CompactionPriority,
}

impl Default for Strategy {
	fn default() -> Self {
		let opts = Options::default();
		Self {
			level0_file_num_trigger: opts.level0_max_files,
			max_bytes_for_level: opts.max_bytes_for_level,
			level_multiplier: opts.level_multiplier,
			compaction_priority: CompactionPriority::default(),
		}
	}
}

impl Strategy {
	/// Create a Strategy from Options
	pub(crate) fn from_options(opts: Arc<Options>) -> Self {
		Self {
			level0_file_num_trigger: opts.level0_max_files,
			max_bytes_for_level: opts.max_bytes_for_level,
			level_multiplier: opts.level_multiplier,
			compaction_priority: CompactionPriority::default(),
		}
	}

	/// Create a Strategy from Options with a specific compaction priority
	#[cfg(test)]
	pub(crate) fn from_options_with_priority(
		opts: Arc<Options>,
		priority: CompactionPriority,
	) -> Self {
		Self {
			level0_file_num_trigger: opts.level0_max_files,
			max_bytes_for_level: opts.max_bytes_for_level,
			level_multiplier: opts.level_multiplier,
			compaction_priority: priority,
		}
	}

	/// Calculate max bytes for a level
	fn target_bytes_for_level(&self, level: u8) -> u64 {
		assert!(level >= 1, "L0 should not use target_bytes_for_level");
		(self.max_bytes_for_level as f64 * self.level_multiplier.powi((level - 1) as i32)) as u64
	}

	/// Calculate total bytes in a level
	fn level_bytes(level: &Level) -> u64 {
		level.tables.iter().map(|t| t.file_size).sum()
	}

	/// Get combined key range from an iterator of tables.
	/// Returns the bounding box (smallest, largest) as an InternalKeyRange.
	pub(crate) fn combined_key_range<'a>(
		tables: impl Iterator<Item = &'a Arc<Table>>,
	) -> Option<InternalKeyRange> {
		tables
			.filter_map(|t| {
				let smallest = t.meta.smallest_point.as_ref()?;
				let largest = t.meta.largest_point.as_ref()?;
				Some((smallest.clone(), largest.clone()))
			})
			.fold(None, |acc, (s, l)| match acc {
				None => Some((Bound::Included(s), Bound::Included(l))),
				Some((Bound::Included(acc_s), Bound::Included(acc_l))) => {
					let new_s = if s.user_key < acc_s.user_key {
						s
					} else {
						acc_s
					};
					let new_l = if l.user_key > acc_l.user_key {
						l
					} else {
						acc_l
					};
					Some((Bound::Included(new_s), Bound::Included(new_l)))
				}
				_ => acc,
			})
	}

	/// Expand file selection to include all files sharing boundary user keys.
	pub(crate) fn select_overlapping_ranges(
		level: &Level,
		initial_table_id: SstId,
	) -> Result<Vec<SstId>> {
		// Verify the initial table exists in the level
		if !level.tables.iter().any(|t| t.id == initial_table_id) {
			return Err(crate::error::Error::TableNotFound(initial_table_id));
		}

		let mut selected_ids: HashSet<SstId> = HashSet::new();
		selected_ids.insert(initial_table_id);

		// Keep expanding until no new files are added (fixed point)
		loop {
			let old_size = selected_ids.len();

			// Get combined key range of all currently selected tables
			let range = match Self::combined_key_range(
				level.tables.iter().filter(|t| selected_ids.contains(&t.id)),
			) {
				Some(r) => r,
				None => break,
			};

			// Find all tables that overlap with the combined range
			for table in &level.tables {
				if !selected_ids.contains(&table.id) && table.overlaps_with_range(&range) {
					selected_ids.insert(table.id);
				}
			}

			// No new files added
			if selected_ids.len() == old_size {
				break;
			}
		}

		Ok(selected_ids.into_iter().collect())
	}

	pub(crate) fn select_tables_for_compaction(
		&self,
		source_level: &Level,
		next_level: &Level,
		source_level_num: u8,
	) -> Result<Vec<SstId>> {
		let mut tables = vec![];
		let mut table_id_set: HashSet<SstId> = HashSet::new();

		if source_level.tables.is_empty() {
			return Ok(tables);
		}

		if source_level_num == 0 {
			// L0 → L1: Pick all L0 files since they can overlap
			for table in &source_level.tables {
				if table_id_set.insert(table.id) {
					tables.push(table.id);
				}
			}
		} else {
			// L1+ → L(n+1): Select best table for compaction
			let selected_table_id = self.select_best_table_for_compaction(source_level);
			if let Some(table_id) = selected_table_id {
				// Include all files sharing boundary user keys
				let overlapping_table_ids =
					Self::select_overlapping_ranges(source_level, table_id)?;
				for id in overlapping_table_ids {
					if table_id_set.insert(id) {
						tables.push(id);
					}
				}
			}
		}

		// Get bounds from selected source tables using smallest_point/largest_point
		let source_tables: Vec<_> = if source_level_num == 0 {
			source_level.tables.iter().collect()
		} else {
			// For L1+, use the table(s) we actually selected by ID
			source_level.tables.iter().filter(|t| table_id_set.contains(&t.id)).collect()
		};

		// Build InternalKeyRange from smallest_point/largest_point
		let source_bounds = Self::combined_key_range(source_tables.iter().copied());

		// Find overlapping tables from next level
		if let Some(bounds) = source_bounds {
			let overlapping_tables: Vec<_> = next_level.overlapping_tables(&bounds).collect();

			// Add overlapping tables
			for table in overlapping_tables {
				if table_id_set.insert(table.id) {
					tables.push(table.id);
				}
			}
		}

		Ok(tables)
	}

	fn select_best_table_for_compaction(&self, source_level: &Level) -> Option<SstId> {
		if source_level.tables.is_empty() {
			return None;
		}

		match self.compaction_priority {
			CompactionPriority::OldestSmallestSeqFirst => {
				self.select_oldest_smallest_seq_first(source_level)
			}
			CompactionPriority::OldestLargestSeqFirst => {
				self.select_oldest_largest_seq_first(source_level)
			}
			CompactionPriority::ByCompensatedSize => self.select_by_compensated_size(source_level),
		}
	}

	/// Selects ranges that haven't been compacted for longest
	fn select_oldest_smallest_seq_first(&self, source_level: &Level) -> Option<SstId> {
		if source_level.tables.is_empty() {
			return None;
		}

		#[derive(Debug)]
		struct Choice {
			table_id: SstId,
			smallest_seq: u64,
			file_size: u64,
		}

		let mut choices = Vec::new();

		for source_table in &source_level.tables {
			let source_size = source_table.file_size;
			let smallest_seq = source_table.meta.properties.seqnos.0;

			choices.push(Choice {
				table_id: source_table.id,
				smallest_seq,
				file_size: source_size,
			});
		}

		choices.sort_by(|a, b| match a.smallest_seq.cmp(&b.smallest_seq) {
			std::cmp::Ordering::Equal => b.file_size.cmp(&a.file_size),
			ordering => ordering,
		});

		choices.first().map(|choice| choice.table_id)
	}

	/// Selects files whose latest update is oldest (cold data)
	fn select_oldest_largest_seq_first(&self, source_level: &Level) -> Option<SstId> {
		if source_level.tables.is_empty() {
			return None;
		}

		#[derive(Debug)]
		struct Choice {
			table_id: SstId,
			largest_seq: u64,
			file_size: u64,
		}

		let mut choices = Vec::new();

		for source_table in &source_level.tables {
			let source_size = source_table.file_size;
			let largest_seq = source_table.meta.properties.seqnos.1;

			choices.push(Choice {
				table_id: source_table.id,
				largest_seq,
				file_size: source_size,
			});
		}

		choices.sort_by(|a, b| match a.largest_seq.cmp(&b.largest_seq) {
			std::cmp::Ordering::Equal => b.file_size.cmp(&a.file_size),
			ordering => ordering,
		});

		choices.first().map(|choice| choice.table_id)
	}

	/// Selects files based on compensated size
	pub(crate) fn select_by_compensated_size(&self, source_level: &Level) -> Option<SstId> {
		if source_level.tables.is_empty() {
			return None;
		}

		#[derive(Debug)]
		struct Choice {
			table_id: SstId,
			compensated_size: f64,
		}

		let mut choices = Vec::new();

		for source_table in &source_level.tables {
			let file_size = source_table.file_size;
			let num_entries = source_table.meta.properties.num_entries;
			let num_deletions = source_table.meta.properties.num_deletions;

			let compensated_size = if num_entries > 0 && num_deletions > 0 {
				let delete_ratio = num_deletions as f64 / num_entries as f64;
				file_size as f64 * (1.0 + delete_ratio * 0.5)
			} else {
				file_size as f64
			};

			choices.push(Choice {
				table_id: source_table.id,
				compensated_size,
			});
		}

		choices.sort_by(|a, b| {
			b.compensated_size.partial_cmp(&a.compensated_size).unwrap_or(std::cmp::Ordering::Equal)
		});

		choices.first().map(|choice| choice.table_id)
	}

	/// Compute compaction scores for all levels
	fn compute_compaction_scores(&self, manifest: &LevelManifest) -> Vec<(u8, f64)> {
		let levels = manifest.levels.get_levels();
		let mut scores = Vec::new();

		// L0: score = max(file_count/trigger, total_bytes/max_bytes_for_level_base)
		let l0_file_score = levels[0].tables.len() as f64 / self.level0_file_num_trigger as f64;
		let l0_bytes = Self::level_bytes(&levels[0]);
		let l0_byte_score = l0_bytes as f64 / self.max_bytes_for_level as f64;
		let l0_score = l0_file_score.max(l0_byte_score);
		if l0_score >= 1.0 {
			scores.push((0, l0_score));
		}

		// L1+: score = level_bytes / max_bytes_for_level
		for level in 1..=manifest.last_level_index() {
			let bytes = Self::level_bytes(&levels[level as usize]);
			let target = self.target_bytes_for_level(level);
			if target > 0 {
				let score = bytes as f64 / target as f64;
				if score >= 1.0 {
					scores.push((level, score));
				}
			}
		}

		// Sort descending by score
		scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

		scores
	}

	pub(crate) fn find_compaction_level(&self, manifest: &LevelManifest) -> Option<u8> {
		let scores = self.compute_compaction_scores(manifest);

		if scores.is_empty() {
			return None;
		}

		let (level, _score) = scores[0];
		Some(level)
	}
}

impl CompactionStrategy for Strategy {
	fn pick_levels(&self, manifest: &LevelManifest) -> Result<CompactionChoice> {
		let source_level = match self.find_compaction_level(manifest) {
			Some(level) => level,
			None => return Ok(CompactionChoice::Skip),
		};

		let levels = manifest.levels.get_levels();

		// Allow same-level compaction at the bottom level for tombstone cleanup
		let target_level = if source_level >= manifest.last_level_index() {
			source_level // Same-level compaction
		} else {
			source_level + 1
		};

		let tables_to_merge = self.select_tables_for_compaction(
			&levels[source_level as usize],
			&levels[target_level as usize],
			source_level,
		)?;

		if tables_to_merge.is_empty() {
			return Ok(CompactionChoice::Skip);
		}

		Ok(CompactionChoice::Merge(CompactionInput {
			tables_to_merge,
			source_level,
			target_level,
		}))
	}
}
