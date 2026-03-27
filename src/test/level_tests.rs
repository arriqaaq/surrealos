//! Tests for table range overlap and level table selection logic
//!
//! Add this to src/test/level_range_tests.rs and include in src/test/mod.rs

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::{Arc, RwLock};

use crate::cloud_store::CloudStore;
use crate::compaction::compactor::HiddenTablesGuard;
use crate::levels::{Level, LevelManifest};
use crate::manifest::{ManifestChangeSet, MANIFEST_FORMAT_VERSION_V1};
use crate::sstable::table::TableWriter;
use crate::sstable::SstId;
use crate::test::{new_test_table, new_test_table_with_store, test_sst_id, test_table_store};
use crate::{
	InternalKey,
	InternalKeyKind,
	InternalKeyRange,
	Options,
	INTERNAL_KEY_SEQ_NUM_MAX,
	INTERNAL_KEY_TIMESTAMP_MAX,
};

/// Helper to create an InternalKeyRange
fn make_range(
	lower: Option<(&[u8], bool)>, // (key, inclusive)
	upper: Option<(&[u8], bool)>, // (key, inclusive)
) -> InternalKeyRange {
	let start = match lower {
		None => Bound::Unbounded,
		Some((k, true)) => Bound::Included(InternalKey::new(
			k.to_vec(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Max,
			INTERNAL_KEY_TIMESTAMP_MAX,
		)),
		Some((k, false)) => {
			Bound::Excluded(InternalKey::new(k.to_vec(), 0, InternalKeyKind::Set, 0))
		}
	};
	let end = match upper {
		None => Bound::Unbounded,
		Some((k, true)) => {
			Bound::Included(InternalKey::new(k.to_vec(), 0, InternalKeyKind::Set, 0))
		}
		Some((k, false)) => Bound::Excluded(InternalKey::new(
			k.to_vec(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Max,
			INTERNAL_KEY_TIMESTAMP_MAX,
		)),
	};
	(start, end)
}

/// Creates a table with specific key range for testing
async fn create_test_table(
	id: u64,
	keys: &[&str],
	opts: Arc<Options>,
) -> Arc<crate::sstable::table::Table> {
	let sst_id = test_sst_id(id);
	let mut buf = Vec::new();
	let mut writer = TableWriter::new(&mut buf, sst_id, Arc::clone(&opts), 0);

	for (i, key) in keys.iter().enumerate() {
		let ikey =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(ikey, b"value").unwrap();
	}

	let _size = writer.finish().unwrap();

	Arc::new(new_test_table(sst_id, opts, buf).await.unwrap())
}

/// Creates a level with multiple non-overlapping tables for testing
async fn create_test_level(table_ranges: &[(&str, &str)], opts: Arc<Options>) -> Level {
	let mut tables = Vec::new();

	for (id, (smallest, largest)) in table_ranges.iter().enumerate() {
		// Create table with just the boundary keys
		let table =
			create_test_table((id + 1) as u64, &[smallest, largest], Arc::clone(&opts)).await;
		tables.push(table);
	}

	Level {
		tables,
	}
}

// ============================================================================
// TESTS FOR is_before_range
// ============================================================================

mod is_before_range_tests {
	use super::*;

	#[tokio::test]
	async fn table_clearly_before_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "c"], opts).await;

		// Table [a,c], Range [e,g] - clearly before
		let range = make_range(Some((b"e", true)), Some((b"g", true)));
		assert!(table.is_before_range(&range));
	}

	#[tokio::test]
	async fn table_clearly_not_before_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["e", "g"], opts).await;

		// Table [e,g], Range [a,c] - table is after, not before
		let range = make_range(Some((b"a", true)), Some((b"c", true)));
		assert!(!table.is_before_range(&range));
	}

	#[tokio::test]
	async fn table_touches_range_at_boundary_included() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "c"], opts).await;

		// Table [a,c], Range [c,e] (inclusive) - they touch at 'c'
		// Table should NOT be before range (they overlap at boundary)
		let range = make_range(Some((b"c", true)), Some((b"e", true)));
		assert!(
			!table.is_before_range(&range),
			"Table [a,c] touches [c,e] at 'c' (inclusive), should NOT be before"
		);
	}

	#[tokio::test]
	async fn table_at_boundary_excluded() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "c"], opts).await;

		// Table [a,c], Range (c,e] (exclusive lower) - 'c' is excluded from range
		// Table's largest is 'c', but range excludes 'c'
		// So table IS completely before the range
		let range = make_range(Some((b"c", false)), Some((b"e", true)));

		assert!(
			table.is_before_range(&range),
			"Table [a,c] should be before (c,e] since 'c' is excluded from range"
		);
	}

	#[tokio::test]
	async fn table_overlaps_with_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "e"], opts).await;

		// Table [a,e], Range [c,g] - overlapping
		let range = make_range(Some((b"c", true)), Some((b"g", true)));
		assert!(!table.is_before_range(&range));
	}

	#[tokio::test]
	async fn unbounded_lower_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "c"], opts).await;

		// Range [*, e] - unbounded lower means table is never "before"
		let range = make_range(None, Some((b"e", true)));
		assert!(!table.is_before_range(&range));
	}
}

// ============================================================================
// TESTS FOR is_after_range
// ============================================================================

mod is_after_range_tests {
	use super::*;

	#[tokio::test]
	async fn table_clearly_after_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["e", "g"], opts).await;

		// Table [e,g], Range [a,c] - clearly after
		let range = make_range(Some((b"a", true)), Some((b"c", true)));
		assert!(table.is_after_range(&range));
	}

	#[tokio::test]
	async fn table_clearly_not_after_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "c"], opts).await;

		// Table [a,c], Range [e,g] - table is before, not after
		let range = make_range(Some((b"e", true)), Some((b"g", true)));
		assert!(!table.is_after_range(&range));
	}

	#[tokio::test]
	async fn table_touches_range_at_boundary_included() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["c", "e"], opts).await;

		// Table [c,e], Range [a,c] (inclusive) - they touch at 'c'
		// Table should NOT be after range (they overlap at boundary)
		let range = make_range(Some((b"a", true)), Some((b"c", true)));
		assert!(
			!table.is_after_range(&range),
			"Table [c,e] touches [a,c] at 'c' (inclusive), should NOT be after"
		);
	}

	#[tokio::test]
	async fn table_at_boundary_excluded() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["c", "e"], opts).await;

		// Table [c,e], Range [a,c) (exclusive upper) - 'c' is excluded from range
		// Table's smallest is 'c', but range excludes 'c'
		// So table IS completely after the range
		let range = make_range(Some((b"a", true)), Some((b"c", false)));

		assert!(
			table.is_after_range(&range),
			"Table [c,e] should be after [a,c) since 'c' is excluded from range"
		);
	}

	#[tokio::test]
	async fn unbounded_upper_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["e", "g"], opts).await;

		// Range [a, *] - unbounded upper means table is never "after"
		let range = make_range(Some((b"a", true)), None);
		assert!(!table.is_after_range(&range));
	}
}

// ============================================================================
// TESTS FOR find_first_overlapping_table and find_last_overlapping_table
// ============================================================================

#[tokio::test]
async fn select_middle_tables() {
	let opts = Arc::new(Options::default());

	// Create 5 non-overlapping tables:
	// T1: [aa, ac], T2: [ba, bc], T3: [ca, cc], T4: [da, dc], T5: [ea, ec]
	let level = create_test_level(
		&[("aa", "ac"), ("ba", "bc"), ("ca", "cc"), ("da", "dc"), ("ea", "ec")],
		opts,
	)
	.await;

	// Query [bb, cb] - should select T2 and T3
	let range = make_range(Some((b"bb", true)), Some((b"cb", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(
		selected_ids,
		vec![test_sst_id(2), test_sst_id(3)],
		"Query [bb,cb] should select T2 and T3"
	);
}

#[tokio::test]
async fn select_first_table_only() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts).await;

	// Query [aa, ab] - should select only T1
	let range = make_range(Some((b"aa", true)), Some((b"ab", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![test_sst_id(1)]);
}

#[tokio::test]
async fn select_last_table_only() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts).await;

	// Query [cb, cd] - should select only T3
	let range = make_range(Some((b"cb", true)), Some((b"cd", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![test_sst_id(3)]);
}

#[tokio::test]
async fn select_all_tables() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts).await;

	// Query [aa, cc] - should select all tables
	let range = make_range(Some((b"aa", true)), Some((b"cc", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![test_sst_id(1), test_sst_id(2), test_sst_id(3)]);
}

#[tokio::test]
async fn select_no_tables_before_all() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("ba", "bc"), ("ca", "cc"), ("da", "dc")], opts).await;

	// Query [aa, az] - before all tables
	let range = make_range(Some((b"aa", true)), Some((b"az", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	assert_eq!(start, end, "No tables should be selected");
	assert_eq!(start, 0, "Should point to beginning");
}

#[tokio::test]
async fn select_no_tables_after_all() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts).await;

	// Query [da, dz] - after all tables
	let range = make_range(Some((b"da", true)), Some((b"dz", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	assert_eq!(start, end);
	assert_eq!(start, 3, "Should point past end");
}

#[tokio::test]
async fn select_no_tables_in_gap() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(
		&[("aa", "ac"), ("da", "dc")], // Gap between ac and da
		opts,
	)
	.await;

	// Query [ba, ca] - in the gap
	let range = make_range(Some((b"ba", true)), Some((b"ca", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	assert_eq!(start, end, "No tables in the gap");
}

#[tokio::test]
async fn select_with_boundary_touching_included() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc")], opts).await;

	// Query [ac, ba] - touches both table boundaries
	let range = make_range(Some((b"ac", true)), Some((b"ba", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();

	// Both tables should be selected (T1 has 'ac', T2 has 'ba')
	assert_eq!(
		selected_ids,
		vec![test_sst_id(1), test_sst_id(2)],
		"Both tables touch the range boundaries"
	);
}

#[tokio::test]
async fn select_with_exclusive_bounds_at_boundaries() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc")], opts).await;

	// Query (ac, ba) - exclusive at both boundaries
	// This is keys STRICTLY BETWEEN 'ac' and 'ba'
	// T1's largest is 'ac' (excluded), T2's smallest is 'ba' (excluded)
	// Neither table should be selected!
	let range = make_range(Some((b"ac", false)), Some((b"ba", false)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	assert_eq!(start, end, "No tables should be selected for exclusive range (ac, ba)");
}

#[tokio::test]
async fn select_with_exclusive_lower_bound() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts).await;

	// Query (ac, cc] - exclusive lower at T1's largest
	// T1 should NOT be selected, T2 and T3 should be selected
	let range = make_range(Some((b"ac", false)), Some((b"cc", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(
		selected_ids,
		vec![test_sst_id(2), test_sst_id(3)],
		"T1 should be excluded for range (ac, cc]"
	);
}

#[tokio::test]
async fn select_with_exclusive_upper_bound() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts).await;

	// Query [aa, ca) - exclusive upper at T3's smallest
	// T3 should NOT be selected, T1 and T2 should be selected
	let range = make_range(Some((b"aa", true)), Some((b"ca", false)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(
		selected_ids,
		vec![test_sst_id(1), test_sst_id(2)],
		"T3 should be excluded for range [aa, ca)"
	);
}

#[tokio::test]
async fn unbounded_ranges() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts).await;

	// Query [*, bc] - unbounded lower
	let range = make_range(None, Some((b"bc", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	let selected: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();
	assert_eq!(selected, vec![test_sst_id(1), test_sst_id(2)]);

	// Query [bc, *] - unbounded upper
	let range = make_range(Some((b"bc", true)), None);
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	let selected: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();
	assert_eq!(selected, vec![test_sst_id(2), test_sst_id(3)]);

	// Query [*, *] - fully unbounded
	let range = make_range(None, None);
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	let selected: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();
	assert_eq!(selected, vec![test_sst_id(1), test_sst_id(2), test_sst_id(3)]);
}

#[tokio::test]
async fn single_point_query() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts).await;

	// Query exactly for key 'bb' → [bb, bb]
	let range = make_range(Some((b"bb", true)), Some((b"bb", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<SstId> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![test_sst_id(2)], "Only T2 contains 'bb'");
}

#[tokio::test]
async fn empty_level() {
	let level = Level {
		tables: vec![],
	};

	let range = make_range(Some((b"aa", true)), Some((b"zz", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	assert_eq!(start, 0);
	assert_eq!(end, 0);
}

#[tokio::test]
async fn single_table_level() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("ba", "bc")], opts).await;

	// Query overlapping
	let range = make_range(Some((b"bb", true)), Some((b"cc", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	assert_eq!(level.tables[start..end].len(), 1);

	// Query before
	let range = make_range(Some((b"aa", true)), Some((b"az", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	assert_eq!(start, end);

	// Query after
	let range = make_range(Some((b"ca", true)), Some((b"cz", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	assert_eq!(start, end);
}

// =============================================================================
// Level Visibility Tests
// =============================================================================

/// Helper: create a test table with specific key range and sequence numbers for manifest tests
async fn create_manifest_test_table(
	id: u64,
	key_prefix: &str,
	seq_start: u64,
	seq_end: u64,
	opts: Arc<Options>,
	table_store: Arc<CloudStore>,
) -> Arc<crate::sstable::table::Table> {
	let sst_id = test_sst_id(id);
	let mut buf = Vec::new();
	let mut writer = TableWriter::new(&mut buf, sst_id, Arc::clone(&opts), 0);

	for seq in seq_start..=seq_end {
		let key = format!("{key_prefix}_{seq:05}");
		let ikey = InternalKey::new(key.as_bytes().to_vec(), seq, InternalKeyKind::Set, 0);
		writer.add(ikey, b"value").unwrap();
	}

	writer.finish().unwrap();
	Arc::new(new_test_table_with_store(sst_id, opts, buf, table_store).await.unwrap())
}

/// Helper: create a fresh LevelManifest for tests
fn new_level_test_manifest(opts: &Arc<Options>) -> LevelManifest {
	let levels = LevelManifest::initialize_levels_for_test(opts.level_count);
	LevelManifest {
		manifest_dir: opts.manifest_dir(),
		manifest_id: 0,
		levels,
		hidden_set: HashSet::with_capacity(10),
		manifest_format_version: MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
		writer_epoch: 0,
		compactor_epoch: 0,
		leader_id: 0,
	}
}

/// Fuzz test: random hide/unhide operations with model comparison.
/// Verifies hidden_set exactly matches a model after each operation.
#[tokio::test]
async fn test_level_hide_unhide_fuzz() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	opts.path = temp_dir.path().to_path_buf();
	opts.level_count = 4;
	let opts = Arc::new(opts);
	std::fs::create_dir_all(opts.manifest_dir()).unwrap();

	let table_store = test_table_store();
	let mut manifest = new_level_test_manifest(&opts);

	// Pre-populate with 20 tables across 4 levels
	let mut all_ids: Vec<SstId> = Vec::new();
	let mut next_id = 1u64;
	let mut next_seq = 1u64;
	for level in 0..4u8 {
		for _ in 0..5 {
			let table = create_manifest_test_table(
				next_id,
				&format!("L{level}t{next_id}"),
				next_seq,
				next_seq + 2,
				Arc::clone(&opts),
				Arc::clone(&table_store),
			)
			.await;
			all_ids.push(table.id);
			let changeset = ManifestChangeSet {
				new_tables: vec![(level, table)],
				..Default::default()
			};
			manifest.apply_changeset(&changeset).unwrap();
			next_id += 1;
			next_seq += 10;
		}
	}

	// Model: expected hidden set
	let mut model_hidden: HashSet<SstId> = HashSet::new();
	let mut rng = fastrand::Rng::with_seed(42);

	for _iteration in 0..100u32 {
		let op = rng.u8(0..100);

		if op < 30 {
			// HIDE: pick 1-3 random non-hidden table IDs
			let visible: Vec<_> =
				all_ids.iter().filter(|id| !model_hidden.contains(id)).copied().collect();
			if visible.is_empty() {
				continue;
			}
			let count = rng.usize(1..=visible.len().min(3));
			let to_hide: Vec<_> =
				(0..count).map(|_| visible[rng.usize(0..visible.len())]).collect();
			manifest.hide_tables(&to_hide);
			for id in &to_hide {
				model_hidden.insert(*id);
			}
		} else if op < 60 {
			// UNHIDE: pick 1-3 random hidden table IDs
			let hidden: Vec<_> = model_hidden.iter().copied().collect();
			if hidden.is_empty() {
				continue;
			}
			let count = rng.usize(1..=hidden.len().min(3));
			let to_unhide: Vec<_> =
				(0..count).map(|_| hidden[rng.usize(0..hidden.len())]).collect();
			manifest.unhide_tables(&to_unhide);
			for id in &to_unhide {
				model_hidden.remove(id);
			}
		} else if op < 80 {
			// INSERT: add new table (always visible)
			let level = rng.u8(0..4);
			let table = create_manifest_test_table(
				next_id,
				&format!("new{next_id}"),
				next_seq,
				next_seq + 2,
				Arc::clone(&opts),
				Arc::clone(&table_store),
			)
			.await;
			all_ids.push(table.id);
			let changeset = ManifestChangeSet {
				new_tables: vec![(level, table)],
				..Default::default()
			};
			manifest.apply_changeset(&changeset).unwrap();
			next_id += 1;
			next_seq += 10;
		} else {
			// DELETE: remove a visible (non-hidden) table + add replacement to maintain
			// last_sequence
			let visible: Vec<(u8, SstId)> = manifest
				.levels
				.get_levels()
				.iter()
				.enumerate()
				.flat_map(|(lvl, level)| {
					level
						.tables
						.iter()
						.filter(|t| !model_hidden.contains(&t.id))
						.map(move |t| (lvl as u8, t.id))
				})
				.collect();
			if visible.is_empty() {
				continue;
			}
			let idx = rng.usize(0..visible.len());
			let (del_level, del_id) = visible[idx];

			// Create replacement to maintain last_sequence consistency
			let replacement = create_manifest_test_table(
				next_id,
				&format!("rep{next_id}"),
				next_seq,
				next_seq + 2,
				Arc::clone(&opts),
				Arc::clone(&table_store),
			)
			.await;
			let rep_id = replacement.id;
			all_ids.push(rep_id);

			let mut deleted = HashSet::new();
			deleted.insert((del_level, del_id));
			let changeset = ManifestChangeSet {
				deleted_tables: deleted,
				new_tables: vec![(del_level, replacement)],
				..Default::default()
			};
			manifest.apply_changeset(&changeset).unwrap();
			all_ids.retain(|id| *id != del_id);
			model_hidden.remove(&del_id);
			next_id += 1;
			next_seq += 10;
		}

		// INVARIANT: hidden_set matches model
		assert_eq!(
			manifest.hidden_set, model_hidden,
			"hidden_set mismatch at iteration {_iteration}"
		);

		// INVARIANT: get_all_tables returns ALL tables (including hidden)
		let all_tables = manifest.get_all_tables();
		assert_eq!(
			all_tables.len(),
			all_ids.len(),
			"get_all_tables count mismatch: got {}, expected {}",
			all_tables.len(),
			all_ids.len()
		);

		// INVARIANT: no hidden ID that doesn't exist in any level
		for hidden_id in &manifest.hidden_set {
			assert!(all_tables.contains_key(hidden_id), "hidden ID {hidden_id:?} not in any level");
		}
	}
}

/// RAII test: verify HiddenTablesGuard correctly restores visibility on drop vs commit.
#[tokio::test]
async fn test_hidden_tables_guard_raii() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	opts.path = temp_dir.path().to_path_buf();
	opts.level_count = 4;
	let opts = Arc::new(opts);
	std::fs::create_dir_all(opts.manifest_dir()).unwrap();

	let table_store = test_table_store();
	let manifest = Arc::new(RwLock::new(new_level_test_manifest(&opts)));

	// Add 4 tables
	let mut table_ids = Vec::new();
	for i in 1..=4u64 {
		let table = create_manifest_test_table(
			i,
			&format!("guard{i}"),
			i * 10,
			i * 10 + 2,
			Arc::clone(&opts),
			Arc::clone(&table_store),
		)
		.await;
		table_ids.push(table.id);
		let changeset = ManifestChangeSet {
			new_tables: vec![(0, table)],
			..Default::default()
		};
		manifest.write().unwrap().apply_changeset(&changeset).unwrap();
	}

	// Test 1: Drop without commit — guard should unhide tables
	{
		let ids = vec![table_ids[0], table_ids[1]];
		manifest.write().unwrap().hide_tables(&ids);
		assert!(manifest.read().unwrap().hidden_set.contains(&table_ids[0]));
		assert!(manifest.read().unwrap().hidden_set.contains(&table_ids[1]));

		let guard = HiddenTablesGuard::new(Arc::clone(&manifest), &ids);
		// Guard drops without commit()
		drop(guard);
	}
	assert!(
		!manifest.read().unwrap().hidden_set.contains(&table_ids[0]),
		"table 0 should be unhidden after guard drop"
	);
	assert!(
		!manifest.read().unwrap().hidden_set.contains(&table_ids[1]),
		"table 1 should be unhidden after guard drop"
	);

	// Test 2: Commit then drop — guard should NOT unhide tables
	{
		let ids = vec![table_ids[2], table_ids[3]];
		manifest.write().unwrap().hide_tables(&ids);
		assert!(manifest.read().unwrap().hidden_set.contains(&table_ids[2]));
		assert!(manifest.read().unwrap().hidden_set.contains(&table_ids[3]));

		let mut guard = HiddenTablesGuard::new(Arc::clone(&manifest), &ids);
		guard.commit();
		drop(guard);
	}
	assert!(
		manifest.read().unwrap().hidden_set.contains(&table_ids[2]),
		"table 2 should remain hidden after committed guard drop"
	);
	assert!(
		manifest.read().unwrap().hidden_set.contains(&table_ids[3]),
		"table 3 should remain hidden after committed guard drop"
	);
}

/// Move table test: verify move_table correctly relocates a table between levels.
#[tokio::test]
async fn test_move_table_between_levels() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	opts.path = temp_dir.path().to_path_buf();
	opts.level_count = 4;
	let opts = Arc::new(opts);
	std::fs::create_dir_all(opts.manifest_dir()).unwrap();

	let table_store = test_table_store();
	let mut manifest = new_level_test_manifest(&opts);

	// Add a table to L1
	let table = create_manifest_test_table(
		1,
		"move_me",
		10,
		15,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await;
	let table_id = table.id;

	let changeset = ManifestChangeSet {
		new_tables: vec![(1, table)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset).unwrap();

	// Verify table is in L1
	assert_eq!(manifest.levels.get_levels()[1].tables.len(), 1);
	assert_eq!(manifest.levels.get_levels()[1].tables[0].id, table_id);
	assert_eq!(manifest.levels.get_levels()[2].tables.len(), 0);

	// Move from L1 to L2
	manifest.move_table(1, 2, table_id).unwrap();

	// Verify table moved
	assert_eq!(manifest.levels.get_levels()[1].tables.len(), 0, "L1 should be empty after move");
	assert_eq!(
		manifest.levels.get_levels()[2].tables.len(),
		1,
		"L2 should have 1 table after move"
	);
	assert_eq!(
		manifest.levels.get_levels()[2].tables[0].id,
		table_id,
		"Moved table should have same ID"
	);

	// Move with multiple tables: add more to L1 and L2, move one
	let table_a =
		create_manifest_test_table(2, "alpha", 20, 25, Arc::clone(&opts), Arc::clone(&table_store))
			.await;
	let table_b =
		create_manifest_test_table(3, "zeta", 30, 35, Arc::clone(&opts), Arc::clone(&table_store))
			.await;
	let id_a = table_a.id;
	let id_b = table_b.id;

	let changeset = ManifestChangeSet {
		new_tables: vec![(1, table_a), (1, table_b)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset).unwrap();

	assert_eq!(manifest.levels.get_levels()[1].tables.len(), 2);

	// Move table_a from L1 to L2
	manifest.move_table(1, 2, id_a).unwrap();

	assert_eq!(manifest.levels.get_levels()[1].tables.len(), 1);
	assert_eq!(manifest.levels.get_levels()[1].tables[0].id, id_b);
	assert_eq!(manifest.levels.get_levels()[2].tables.len(), 2); // original + moved

	// Verify L2 contains both tables
	let l2_ids: HashSet<_> = manifest.levels.get_levels()[2].tables.iter().map(|t| t.id).collect();
	assert!(l2_ids.contains(&table_id), "Original table should still be in L2");
	assert!(l2_ids.contains(&id_a), "Moved table should be in L2");
}
