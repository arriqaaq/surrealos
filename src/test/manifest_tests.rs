use std::collections::HashSet;
use std::sync::Arc;

use crate::cloud_store::CloudStore;
use crate::levels::LevelManifest;
use crate::manifest::{
	load_from_file,
	write_manifest_to_disk,
	ManifestChangeSet,
	SnapshotInfo,
	MANIFEST_FORMAT_VERSION_V1,
};
use crate::sstable::table::TableWriter;
use crate::sstable::SstId;
use crate::test::{new_test_table_with_store, test_sst_id, test_table_store};
use crate::{InternalKey, InternalKeyKind, Options, Result};

// Helper function to create a test table with the async cloud-native approach
async fn create_test_table(
	table_id: SstId,
	num_items: u64,
	opts: Arc<Options>,
	table_store: Arc<CloudStore>,
) -> Result<Arc<crate::sstable::table::Table>> {
	let mut buf = Vec::new();

	// Create TableWriter that writes to an in-memory buffer
	let mut writer = TableWriter::new(&mut buf, table_id, Arc::clone(&opts), 0); // L0 for test

	// Generate and add items
	for i in 0..num_items {
		let key = format!("key_{i:05}");
		let value = format!("value_{i:05}");

		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), i + 1, InternalKeyKind::Set, 0);

		writer.add(internal_key, value.as_bytes())?;
	}

	// Finish writing the table
	writer.finish()?;

	// Create the table via the async test helper with shared store
	let table = new_test_table_with_store(table_id, opts, buf, table_store).await?;

	Ok(Arc::new(table))
}

#[tokio::test]
async fn test_level_manifest_persistence() {
	let mut opts = Options::default();
	// Set up temporary directory for test
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3; // Set level count for the manifest
	let opts = Arc::new(opts);

	// Create manifest directory
	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest directory");

	let table_store = test_table_store();

	// Create a new manifest with 3 levels (using struct literal for tests)
	let levels = LevelManifest::initialize_levels_for_test(opts.level_count);
	let mut manifest = LevelManifest {
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
	};

	// Create tables and add them to the manifest
	// Create 2 tables for level 0
	let table_id1 = test_sst_id(1);
	let table1 = create_test_table(table_id1, 100, Arc::clone(&opts), Arc::clone(&table_store))
		.await
		.expect("Failed to create table 1");

	let table_id2 = test_sst_id(2);
	let table2 = create_test_table(table_id2, 200, Arc::clone(&opts), Arc::clone(&table_store))
		.await
		.expect("Failed to create table 2");

	// Create a table for level 1
	let table_id3 = test_sst_id(3);
	let table3 = create_test_table(table_id3, 300, Arc::clone(&opts), Arc::clone(&table_store))
		.await
		.expect("Failed to create table 3");

	let changeset = ManifestChangeSet {
		new_tables: vec![
			(0, table1), // Level 0
			(0, table2), // Level 0
			(1, table3), // Level 1
		],
		new_snapshots: vec![
			SnapshotInfo {
				seq_num: 10,
				created_at: std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.map(|d| d.as_nanos())
					.unwrap_or(0),
			},
			SnapshotInfo {
				seq_num: 20,
				created_at: std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.map(|d| d.as_nanos())
					.unwrap_or(0),
			},
		],
		..Default::default()
	};

	// Apply changeset to manifest
	manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	// Verify last_sequence was updated correctly (table3 has largest_seq_num = 300)
	assert_eq!(manifest.get_last_sequence(), 300, "last_sequence should be 300");

	// Persist the manifest
	write_manifest_to_disk(&mut manifest).expect("Failed to write to disk");

	// Load the manifest directly to verify persistence
	let manifest_path = opts.manifest_file_path(1);
	let loaded_manifest = load_from_file(
		&manifest_path,
		opts.manifest_dir(),
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to load manifest");

	assert_eq!(
		loaded_manifest.manifest_format_version, MANIFEST_FORMAT_VERSION_V1,
		"Manifest version not persisted correctly"
	);
	assert_eq!(loaded_manifest.snapshots.len(), 2, "Snapshots not persisted correctly");
	assert_eq!(
		loaded_manifest.snapshots[0].seq_num, 10,
		"First snapshot seq_num not persisted correctly"
	);
	assert_eq!(
		loaded_manifest.snapshots[1].seq_num, 20,
		"Second snapshot seq_num not persisted correctly"
	);

	// Verify level count matches what we created
	assert_eq!(
		loaded_manifest.levels.as_ref().len(),
		opts.level_count as usize,
		"Incorrect number of levels loaded"
	);

	// Verify table IDs were persisted correctly
	let loaded_level0 = &loaded_manifest.levels.as_ref()[0];
	let loaded_level1 = &loaded_manifest.levels.as_ref()[1];

	assert_eq!(loaded_level0.tables.len(), 2, "Level 0 should have 2 tables");
	assert!(
		loaded_level0.tables.iter().any(|t| t.id == table_id1),
		"Level 0 should contain table_id1"
	);
	assert!(
		loaded_level0.tables.iter().any(|t| t.id == table_id2),
		"Level 0 should contain table_id2"
	);

	assert_eq!(loaded_level1.tables.len(), 1, "Level 1 should have 1 table");
	assert!(
		loaded_level1.tables.iter().any(|t| t.id == table_id3),
		"Level 1 should contain table_id3"
	);

	// Reload manifest via load_from_file (simulating restart/recovery)
	let new_manifest = load_from_file(
		&manifest_path,
		opts.manifest_dir(),
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create manifest from existing file");

	// Verify all manifest fields were loaded correctly in the new manifest
	assert_eq!(
		new_manifest.manifest_format_version, MANIFEST_FORMAT_VERSION_V1,
		"Manifest version not loaded correctly"
	);
	assert_eq!(new_manifest.snapshots.len(), 2, "Snapshots not loaded correctly");
	assert_eq!(new_manifest.snapshots[0].seq_num, 10, "First snapshot not loaded correctly");
	assert_eq!(new_manifest.snapshots[1].seq_num, 20, "Second snapshot not loaded correctly");
	assert_eq!(new_manifest.manifest_dir, opts.manifest_dir(), "manifest_dir not set correctly");

	// Verify the number of levels in the new manifest
	assert_eq!(
		new_manifest.levels.as_ref().len(),
		opts.level_count as usize,
		"Incorrect number of levels in new manifest"
	);

	// Verify tables were loaded correctly
	let level0 = &new_manifest.levels.as_ref()[0];
	assert_eq!(level0.tables.len(), 2, "Level 0 should have 2 tables");
	assert!(
		level0.tables.iter().any(|t| t.id == table_id1),
		"Level 0 should contain table with ID {table_id1}"
	);
	assert!(
		level0.tables.iter().any(|t| t.id == table_id2),
		"Level 0 should contain table with ID {table_id2}"
	);

	let level1 = &new_manifest.levels.as_ref()[1];
	assert_eq!(level1.tables.len(), 1, "Level 1 should have 1 table");
	assert!(
		level1.tables.iter().any(|t| t.id == table_id3),
		"Level 1 should contain table with ID {table_id3}"
	);

	// Verify table data was loaded correctly by checking all properties
	let table1_reloaded =
		level0.tables.iter().find(|t| t.id == table_id1).expect("Table 1 not found");

	// Check Table1 basic properties
	assert_eq!(table1_reloaded.id, table_id1, "Table 1 ID mismatch");

	// Check Table1 metadata properties
	let props1 = &table1_reloaded.meta.properties;
	assert_eq!(props1.id, table_id1, "Table 1 properties ID mismatch");
	assert_eq!(props1.num_entries, 100, "Table 1 should have 100 entries");
	assert!(props1.data_size > 0, "Table 1 data size should be greater than 0");
	assert!(props1.created_at > 0, "Table 1 created_at should be set");
	assert_eq!(props1.item_count, 100, "Table 1 item count should match entries");
	assert_eq!(props1.key_count, 100, "Table 1 key count should match entries");
	assert!(props1.block_count > 0, "Table 1 should have at least one block");
	assert_eq!(props1.seqnos, (1, 100), "Table 1 sequence numbers should be (1, 100)");

	// Check Table1 metadata fields
	assert_eq!(
		table1_reloaded.meta.smallest_seq_num,
		Some(1),
		"Table 1 smallest seq num should be 1"
	);
	assert_eq!(
		table1_reloaded.meta.largest_seq_num,
		Some(100),
		"Table 1 largest seq num should be 100"
	);
	assert!(table1_reloaded.meta.has_point_keys.unwrap_or(false), "Table 1 should have point keys");
	assert!(
		table1_reloaded.meta.smallest_point.is_some(),
		"Table 1 should have smallest point key"
	);
	assert!(table1_reloaded.meta.largest_point.is_some(), "Table 1 should have largest point key");

	let table2_reloaded =
		level0.tables.iter().find(|t| t.id == table_id2).expect("Table 2 not found");

	// Check Table2 basic properties
	assert_eq!(table2_reloaded.id, table_id2, "Table 2 ID mismatch");

	// Check Table2 metadata properties
	let props2 = &table2_reloaded.meta.properties;
	assert_eq!(props2.id, table_id2, "Table 2 properties ID mismatch");
	assert_eq!(props2.num_entries, 200, "Table 2 should have 200 entries");
	assert!(props2.data_size > 0, "Table 2 data size should be greater than 0");
	assert!(props2.created_at > 0, "Table 2 created_at should be set");
	assert_eq!(props2.item_count, 200, "Table 2 item count should match entries");
	assert_eq!(props2.key_count, 200, "Table 2 key count should match entries");
	assert!(props2.block_count > 0, "Table 2 should have at least one block");
	assert_eq!(props2.seqnos, (1, 200), "Table 2 sequence numbers should be (1, 200)");

	// Check Table2 metadata fields
	assert_eq!(
		table2_reloaded.meta.smallest_seq_num,
		Some(1),
		"Table 2 smallest seq num should be 1"
	);
	assert_eq!(
		table2_reloaded.meta.largest_seq_num,
		Some(200),
		"Table 2 largest seq num should be 200"
	);
	assert!(table2_reloaded.meta.has_point_keys.unwrap_or(false), "Table 2 should have point keys");
	assert!(
		table2_reloaded.meta.smallest_point.is_some(),
		"Table 2 should have smallest point key"
	);
	assert!(table2_reloaded.meta.largest_point.is_some(), "Table 2 should have largest point key");

	let table3_reloaded =
		level1.tables.iter().find(|t| t.id == table_id3).expect("Table 3 not found");

	// Check Table3 basic properties
	assert_eq!(table3_reloaded.id, table_id3, "Table 3 ID mismatch");

	// Check Table3 metadata properties
	let props3 = &table3_reloaded.meta.properties;
	assert_eq!(props3.id, table_id3, "Table 3 properties ID mismatch");
	assert_eq!(props3.num_entries, 300, "Table 3 should have 300 entries");
	assert!(props3.data_size > 0, "Table 3 data size should be greater than 0");
	assert!(props3.created_at > 0, "Table 3 created_at should be set");
	assert_eq!(props3.item_count, 300, "Table 3 item count should match entries");
	assert_eq!(props3.key_count, 300, "Table 3 key count should match entries");
	assert!(props3.block_count > 0, "Table 3 should have at least one block");
	assert_eq!(props3.seqnos, (1, 300), "Table 3 sequence numbers should be (1, 300)");

	// Check Table3 metadata fields
	assert_eq!(
		table3_reloaded.meta.smallest_seq_num,
		Some(1),
		"Table 3 smallest seq num should be 1"
	);
	assert_eq!(
		table3_reloaded.meta.largest_seq_num,
		Some(300),
		"Table 3 largest seq num should be 300"
	);
	assert!(table3_reloaded.meta.has_point_keys.unwrap_or(false), "Table 3 should have point keys");
	assert!(
		table3_reloaded.meta.smallest_point.is_some(),
		"Table 3 should have smallest point key"
	);
	assert!(table3_reloaded.meta.largest_point.is_some(), "Table 3 should have largest point key");

	// Verify table format and compression are set correctly
	assert_eq!(
		props1.table_format,
		crate::sstable::table::TableFormat::LSMV1,
		"Table 1 format should be LSMV1"
	);
	assert_eq!(
		props2.table_format,
		crate::sstable::table::TableFormat::LSMV1,
		"Table 2 format should be LSMV1"
	);
	assert_eq!(
		props3.table_format,
		crate::sstable::table::TableFormat::LSMV1,
		"Table 3 format should be LSMV1"
	);

	// Verify no deletions in test tables
	assert_eq!(props1.num_deletions, 0, "Table 1 should have no deletions");
	assert_eq!(props2.num_deletions, 0, "Table 2 should have no deletions");
	assert_eq!(props3.num_deletions, 0, "Table 3 should have no deletions");

	// Verify tombstone counts
	assert_eq!(props1.tombstone_count, 0, "Table 1 should have no tombstones");
	assert_eq!(props2.tombstone_count, 0, "Table 2 should have no tombstones");
	assert_eq!(props3.tombstone_count, 0, "Table 3 should have no tombstones");
}

// Helper function to create a test table with specific sequence numbers
async fn create_test_table_with_seq_nums(
	table_id: SstId,
	seq_start: u64,
	seq_end: u64,
	opts: Arc<Options>,
	table_store: Arc<CloudStore>,
) -> Result<Arc<crate::sstable::table::Table>> {
	let mut buf = Vec::new();

	// Create TableWriter that writes to an in-memory buffer
	let mut writer = TableWriter::new(&mut buf, table_id, Arc::clone(&opts), 0); // L0 for test

	// Generate and add items with specific sequence numbers
	for seq_num in seq_start..=seq_end {
		let key = format!("key_{seq_num:05}");
		let value = format!("value_{seq_num:05}");

		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), seq_num, InternalKeyKind::Set, 0);

		writer.add(internal_key, value.as_bytes())?;
	}

	// Finish writing the table
	writer.finish()?;

	// Create the table via the async test helper with shared store
	let table = new_test_table_with_store(table_id, opts, buf, table_store).await?;

	Ok(Arc::new(table))
}

/// Helper to create a fresh empty LevelManifest for tests (struct literal).
fn new_test_manifest(opts: &Arc<Options>) -> LevelManifest {
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

#[tokio::test]
async fn test_lsn_with_multiple_l0_tables() {
	let mut opts = Options::default();
	// Set up temporary directory for test
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	// Create manifest directory
	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest directory");

	// Create a new manifest
	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	// Test 1: Empty manifest should have last_sequence of 0
	assert_eq!(manifest.get_last_sequence(), 0, "Empty manifest should return last_sequence of 0");

	// Test 2: Add single table via changeset
	// Create table with sequence numbers 1-10 (largest_seq_num = 10)
	let table1 = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 1");

	let changeset1 = ManifestChangeSet {
		new_tables: vec![(0, table1)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset1).expect("Failed to apply changeset");

	assert_eq!(manifest.get_last_sequence(), 10, "Single table should update last_sequence");

	// Test 3: Add table with higher sequence numbers
	// Create table with sequence numbers 11-20 (largest_seq_num = 20)
	let table2 = create_test_table_with_seq_nums(
		test_sst_id(2),
		11,
		20,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 2");

	let changeset2 = ManifestChangeSet {
		new_tables: vec![(0, table2)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset2).expect("Failed to apply changeset");

	assert_eq!(
		manifest.get_last_sequence(),
		20,
		"Should update to highest sequence from new table"
	);

	// Test 4: Add table with even higher sequence numbers
	// Create table with sequence numbers 21-30 (largest_seq_num = 30)
	let table3 = create_test_table_with_seq_nums(
		test_sst_id(3),
		21,
		30,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 3");

	let changeset3 = ManifestChangeSet {
		new_tables: vec![(0, table3)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset3).expect("Failed to apply changeset");

	assert_eq!(
		manifest.get_last_sequence(),
		30,
		"Should update to highest sequence after adding new table"
	);

	// Test 5: Verify table ordering - tables should be sorted by largest_seq_num
	// descending
	{
		let level0 = &manifest.levels.get_levels()[0];
		assert_eq!(level0.tables.len(), 3, "Should have 3 tables in L0");

		// Tables should be in descending order of their largest sequence number
		assert_eq!(
			level0.tables[0].meta.largest_seq_num,
			Some(30),
			"First table should have highest seq num"
		);
		assert_eq!(
			level0.tables[1].meta.largest_seq_num,
			Some(20),
			"Second table should have middle seq num"
		);
		assert_eq!(
			level0.tables[2].meta.largest_seq_num,
			Some(10),
			"Third table should have lowest seq num"
		);
	}

	// Test 6: Add table with lower sequence numbers (simulating out-of-order
	// insertion) Create table with sequence numbers 5-8 (largest_seq_num = 8)
	let table4 = create_test_table_with_seq_nums(
		test_sst_id(4),
		5,
		8,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 4");

	let changeset4 = ManifestChangeSet {
		new_tables: vec![(0, table4)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset4).expect("Failed to apply changeset");

	// last_sequence should still be 30 (highest among all tables)
	assert_eq!(
		manifest.get_last_sequence(),
		30,
		"last_sequence should remain highest even after adding table with lower seq nums"
	);

	// Verify correct ordering after out-of-order insertion
	{
		let level0 = &manifest.levels.get_levels()[0];
		assert_eq!(level0.tables.len(), 4, "Should have 4 tables in L0");

		// Tables should still be in descending order of their largest sequence number
		assert_eq!(
			level0.tables[0].meta.largest_seq_num,
			Some(30),
			"First table should have seq num 30"
		);
		assert_eq!(
			level0.tables[1].meta.largest_seq_num,
			Some(20),
			"Second table should have seq num 20"
		);
		assert_eq!(
			level0.tables[2].meta.largest_seq_num,
			Some(10),
			"Third table should have seq num 10"
		);
		assert_eq!(
			level0.tables[3].meta.largest_seq_num,
			Some(8),
			"Fourth table should have seq num 8"
		);
	}

	// Test 7: Test with overlapping sequence ranges
	// Create table with sequence numbers 25-35 (largest_seq_num = 35, overlaps with
	// table3)
	let table5 =
		create_test_table_with_seq_nums(test_sst_id(5), 25, 35, opts, Arc::clone(&table_store))
			.await
			.expect("Failed to create table 5");

	let changeset5 = ManifestChangeSet {
		new_tables: vec![(0, table5)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset5).expect("Failed to apply changeset");

	assert_eq!(
		manifest.get_last_sequence(),
		35,
		"Should return new highest last_sequence from overlapping ranges"
	);

	// Verify final ordering
	{
		let level0 = &manifest.levels.get_levels()[0];
		assert_eq!(level0.tables.len(), 5, "Should have 5 tables in L0");

		// First table should have the highest sequence number
		assert_eq!(
			level0.tables[0].meta.largest_seq_num,
			Some(35),
			"First table should have highest seq num 35"
		);
	}
}

#[tokio::test]
async fn test_last_sequence_persistence_across_manifest_reload() {
	let mut opts = Options::default();
	// Set up temporary directory for test
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	// Create manifest directory
	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest directory");

	let table_store = test_table_store();
	let expected_last_sequence = 50;

	// Create manifest with tables via changeset and verify last_sequence
	{
		let mut manifest = new_test_manifest(&opts);

		// Create tables with different sequence ranges
		let table1 = create_test_table_with_seq_nums(
			test_sst_id(1),
			1,
			20,
			Arc::clone(&opts),
			Arc::clone(&table_store),
		)
		.await
		.expect("Failed to create table 1");
		let table2 = create_test_table_with_seq_nums(
			test_sst_id(2),
			21,
			expected_last_sequence,
			Arc::clone(&opts),
			Arc::clone(&table_store),
		)
		.await
		.expect("Failed to create table 2");
		let table3 = create_test_table_with_seq_nums(
			test_sst_id(3),
			10,
			30,
			Arc::clone(&opts),
			Arc::clone(&table_store),
		)
		.await
		.expect("Failed to create table 3");

		// Add all tables via a single changeset
		let changeset = ManifestChangeSet {
			new_tables: vec![(0, table1), (0, table2), (0, table3)],
			..Default::default()
		};
		manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

		// Verify last_sequence before persistence
		assert_eq!(
			manifest.get_last_sequence(),
			expected_last_sequence,
			"last_sequence should be {} before persistence",
			expected_last_sequence
		);

		// Persist the manifest
		write_manifest_to_disk(&mut manifest).expect("Failed to write manifest to disk");
	}

	// Reload manifest and verify last_sequence is preserved
	{
		let manifest_path = opts.manifest_file_path(1);
		let reloaded_manifest = load_from_file(
			&manifest_path,
			opts.manifest_dir(),
			Arc::clone(&opts),
			Arc::clone(&table_store),
		)
		.await
		.expect("Failed to reload manifest");

		// Verify last_sequence after reload
		assert_eq!(
			reloaded_manifest.get_last_sequence(),
			expected_last_sequence,
			"last_sequence should be {} after reload",
			expected_last_sequence
		);

		// Verify table count and ordering
		let level0 = &reloaded_manifest.levels.get_levels()[0];
		assert_eq!(level0.tables.len(), 3, "Should have 3 tables after reload");

		// Verify tables are still properly ordered
		assert_eq!(
			level0.tables[0].meta.largest_seq_num,
			Some(50),
			"First table should have highest seq num"
		);
		assert_eq!(
			level0.tables[1].meta.largest_seq_num,
			Some(30),
			"Second table should have middle seq num"
		);
		assert_eq!(
			level0.tables[2].meta.largest_seq_num,
			Some(20),
			"Third table should have lowest seq num"
		);
	}
}

#[tokio::test]
async fn test_manifest_v1_with_log_number_and_last_sequence() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	// Create required directories
	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let table_store = test_table_store();

	// Create a manifest with log_number and last_sequence set
	let mut manifest = new_test_manifest(&opts);

	// Add a table to ensure non-trivial state
	// Table with sequence numbers 100-200, so last_sequence should be 200
	let table = create_test_table_with_seq_nums(
		test_sst_id(1),
		100,
		200,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table");

	// Use changeset to atomically set log_number and add table
	// The changeset will automatically update last_sequence from the table's
	// largest_seq_num
	let changeset = ManifestChangeSet {
		log_number: Some(42),
		new_tables: vec![(0, table)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	// Verify last_sequence was updated by the changeset
	assert_eq!(manifest.get_last_sequence(), 200, "last_sequence should be updated by changeset");

	// Persist to disk
	write_manifest_to_disk(&mut manifest).expect("Failed to write manifest");

	// Reload and verify
	let manifest_path = opts.manifest_file_path(1);
	let loaded_manifest = load_from_file(
		&manifest_path,
		opts.manifest_dir(),
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to reload manifest");

	// Verify format version is still V3
	assert_eq!(
		loaded_manifest.manifest_format_version, MANIFEST_FORMAT_VERSION_V1,
		"Should be V3 format"
	);

	// Verify new fields persisted correctly
	assert_eq!(loaded_manifest.get_log_number(), 42, "log_number should persist");
	assert_eq!(loaded_manifest.get_last_sequence(), 200, "last_sequence should persist");

	// Verify table loaded correctly
	assert_eq!(loaded_manifest.levels.get_levels()[0].tables.len(), 1);
}

#[tokio::test]
async fn test_revert_empty_changeset() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);

	let initial_last_sequence = manifest.get_last_sequence();
	let initial_log_number = manifest.get_log_number();
	let initial_version = manifest.manifest_format_version;

	let changeset = ManifestChangeSet::default();
	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.get_last_sequence(),
		initial_last_sequence,
		"last_sequence should be unchanged"
	);
	assert_eq!(manifest.get_log_number(), initial_log_number, "log_number should be unchanged");
	assert_eq!(
		manifest.manifest_format_version, initial_version,
		"manifest_format_version should be unchanged"
	);
}

#[tokio::test]
async fn test_revert_added_tables_only() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	let table1 = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(
		test_sst_id(2),
		11,
		20,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 2");
	let table3 = create_test_table_with_seq_nums(
		test_sst_id(3),
		21,
		30,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 3");

	let initial_table_count_l0 = manifest.levels.get_levels()[0].tables.len();
	let initial_table_count_l1 = manifest.levels.get_levels()[1].tables.len();

	let changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (1, table2), (1, table3)],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.levels.get_levels()[0].tables.len(), initial_table_count_l0 + 1);
	assert_eq!(manifest.levels.get_levels()[1].tables.len(), initial_table_count_l1 + 2);

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.levels.get_levels()[0].tables.len(),
		initial_table_count_l0,
		"L0 table count should be restored"
	);
	assert_eq!(
		manifest.levels.get_levels()[1].tables.len(),
		initial_table_count_l1,
		"L1 table count should be restored"
	);
}

#[tokio::test]
async fn test_revert_deleted_tables_only() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	// Add some tables first
	let table1 = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(
		test_sst_id(2),
		11,
		20,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 2");
	let table3 = create_test_table_with_seq_nums(
		test_sst_id(3),
		21,
		30,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 3");

	let add_changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (0, table2), (1, table3)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	let initial_table_count_l0 = manifest.levels.get_levels()[0].tables.len();
	let initial_table_count_l1 = manifest.levels.get_levels()[1].tables.len();

	// Now delete them
	let delete_changeset = ManifestChangeSet {
		deleted_tables: HashSet::from([
			(0, test_sst_id(1)),
			(0, test_sst_id(2)),
			(1, test_sst_id(3)),
		]),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&delete_changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.levels.get_levels()[0].tables.len(), initial_table_count_l0 - 2);
	assert_eq!(manifest.levels.get_levels()[1].tables.len(), initial_table_count_l1 - 1);

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.levels.get_levels()[0].tables.len(),
		initial_table_count_l0,
		"L0 table count should be restored"
	);
	assert_eq!(
		manifest.levels.get_levels()[1].tables.len(),
		initial_table_count_l1,
		"L1 table count should be restored"
	);

	// Verify table IDs are present
	assert!(manifest.levels.get_levels()[0].tables.iter().any(|t| t.id == test_sst_id(1)));
	assert!(manifest.levels.get_levels()[0].tables.iter().any(|t| t.id == test_sst_id(2)));
	assert!(manifest.levels.get_levels()[1].tables.iter().any(|t| t.id == test_sst_id(3)));
}

#[tokio::test]
async fn test_revert_mixed_add_delete() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	// Add initial tables
	let table1 = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(
		test_sst_id(2),
		11,
		20,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 2");

	let add_changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (0, table2)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	let initial_table_count_l0 = manifest.levels.get_levels()[0].tables.len();

	// Mixed: delete table1, add table3
	let table3 = create_test_table_with_seq_nums(
		test_sst_id(3),
		21,
		30,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 3");

	let mixed_changeset = ManifestChangeSet {
		deleted_tables: HashSet::from([(0, test_sst_id(1))]),
		new_tables: vec![(0, table3)],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&mixed_changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.levels.get_levels()[0].tables.len(), initial_table_count_l0);
	assert!(manifest.levels.get_levels()[0].tables.iter().any(|t| t.id == test_sst_id(3)));
	assert!(!manifest.levels.get_levels()[0].tables.iter().any(|t| t.id == test_sst_id(1)));

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.levels.get_levels()[0].tables.len(),
		initial_table_count_l0,
		"L0 table count should be restored"
	);
	assert!(manifest.levels.get_levels()[0].tables.iter().any(|t| t.id == test_sst_id(1)));
	assert!(!manifest.levels.get_levels()[0].tables.iter().any(|t| t.id == test_sst_id(3)));
}

#[tokio::test]
async fn test_revert_preserves_table_ordering_l0() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	// Add tables with different sequence numbers
	let table1 = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(
		test_sst_id(2),
		11,
		20,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 2");
	let table3 = create_test_table_with_seq_nums(
		test_sst_id(3),
		21,
		30,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 3");

	let add_changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (0, table2), (0, table3)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	// Capture initial ordering
	let initial_order: Vec<SstId> =
		manifest.levels.get_levels()[0].tables.iter().map(|t| t.id).collect();

	// Delete and re-add to test ordering preservation
	let delete_changeset = ManifestChangeSet {
		deleted_tables: HashSet::from([(0, test_sst_id(2))]),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&delete_changeset).expect("Failed to apply changeset");

	manifest.revert_changeset(rollback);

	// Verify ordering is preserved
	let restored_order: Vec<SstId> =
		manifest.levels.get_levels()[0].tables.iter().map(|t| t.id).collect();

	assert_eq!(initial_order, restored_order, "L0 table ordering should be preserved");
}

#[tokio::test]
async fn test_revert_preserves_table_ordering_l1() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	// Add tables to L1 (sorted by key)
	let table1 = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(
		test_sst_id(2),
		11,
		20,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 2");
	let table3 = create_test_table_with_seq_nums(
		test_sst_id(3),
		21,
		30,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 3");

	let add_changeset = ManifestChangeSet {
		new_tables: vec![(1, table1), (1, table2), (1, table3)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	// Capture initial ordering
	let initial_order: Vec<SstId> =
		manifest.levels.get_levels()[1].tables.iter().map(|t| t.id).collect();

	// Delete and re-add to test ordering preservation
	let delete_changeset = ManifestChangeSet {
		deleted_tables: HashSet::from([(1, test_sst_id(2))]),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&delete_changeset).expect("Failed to apply changeset");

	manifest.revert_changeset(rollback);

	// Verify ordering is preserved
	let restored_order: Vec<SstId> =
		manifest.levels.get_levels()[1].tables.iter().map(|t| t.id).collect();

	assert_eq!(initial_order, restored_order, "L1+ table ordering should be preserved");
}

#[tokio::test]
async fn test_revert_log_number_change() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);

	let initial_log_number = manifest.get_log_number();
	let new_log_number = 42;

	let changeset = ManifestChangeSet {
		log_number: Some(new_log_number),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.get_log_number(), new_log_number, "log_number should be updated");

	manifest.revert_changeset(rollback);

	assert_eq!(manifest.get_log_number(), initial_log_number, "log_number should be reverted");
}

#[tokio::test]
async fn test_revert_log_number_not_changed() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);

	// Set log_number to a higher value first
	let higher_log_number = 50;
	let changeset1 = ManifestChangeSet {
		log_number: Some(higher_log_number),
		..Default::default()
	};
	let _ = manifest.apply_changeset(&changeset1).expect("Failed to apply changeset");

	let current_log_number = manifest.get_log_number();

	// Try to set it to a lower value (should not change)
	let lower_log_number = 30;
	let changeset2 = ManifestChangeSet {
		log_number: Some(lower_log_number),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset2).expect("Failed to apply changeset");

	assert_eq!(manifest.get_log_number(), current_log_number, "log_number should not decrease");

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.get_log_number(),
		current_log_number,
		"log_number should remain unchanged after revert"
	);
}

#[tokio::test]
async fn test_revert_last_sequence() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	let initial_last_sequence = manifest.get_last_sequence();

	let table = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		100,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table");

	let changeset = ManifestChangeSet {
		new_tables: vec![(0, table)],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.get_last_sequence(), 100, "last_sequence should be updated");

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.get_last_sequence(),
		initial_last_sequence,
		"last_sequence should be reverted"
	);
}

#[tokio::test]
async fn test_revert_manifest_version() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);

	let initial_version = manifest.manifest_format_version;
	let new_version = 2;

	let changeset = ManifestChangeSet {
		manifest_format_version: Some(new_version),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(
		manifest.manifest_format_version, new_version,
		"manifest_format_version should be updated"
	);

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.manifest_format_version, initial_version,
		"manifest_format_version should be reverted"
	);
}

#[tokio::test]
async fn test_revert_snapshots_added() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);

	let initial_snapshot_count = manifest.snapshots.len();

	let snapshot1 = SnapshotInfo {
		seq_num: 10,
		created_at: std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|d| d.as_nanos())
			.unwrap_or(0),
	};
	let snapshot2 = SnapshotInfo {
		seq_num: 20,
		created_at: std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|d| d.as_nanos())
			.unwrap_or(0),
	};

	let changeset = ManifestChangeSet {
		new_snapshots: vec![snapshot1, snapshot2],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.snapshots.len(), initial_snapshot_count + 2, "Snapshots should be added");

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.snapshots.len(),
		initial_snapshot_count,
		"Snapshots should be removed on revert"
	);
}

#[tokio::test]
async fn test_revert_snapshots_deleted() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);

	// Add snapshots first
	let snapshot1 = SnapshotInfo {
		seq_num: 10,
		created_at: std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|d| d.as_nanos())
			.unwrap_or(0),
	};
	let snapshot2 = SnapshotInfo {
		seq_num: 20,
		created_at: std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.map(|d| d.as_nanos())
			.unwrap_or(0),
	};

	let add_changeset = ManifestChangeSet {
		new_snapshots: vec![snapshot1, snapshot2],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	let initial_snapshot_count = manifest.snapshots.len();

	// Now delete one
	let delete_changeset = ManifestChangeSet {
		deleted_snapshots: HashSet::from([10]),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&delete_changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.snapshots.len(), initial_snapshot_count - 1, "Snapshot should be deleted");

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.snapshots.len(),
		initial_snapshot_count,
		"Snapshot should be restored on revert"
	);
	assert!(manifest.snapshots.iter().any(|s| s.seq_num == 10));
}

#[tokio::test]
async fn test_revert_multiple_tables_same_level() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	let table1 = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(
		test_sst_id(2),
		11,
		20,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 2");
	let table3 = create_test_table_with_seq_nums(
		test_sst_id(3),
		21,
		30,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 3");
	let table4 = create_test_table_with_seq_nums(
		test_sst_id(4),
		31,
		40,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 4");

	let changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (0, table2), (0, table3), (0, table4)],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.levels.get_levels()[0].tables.len(), 4);

	manifest.revert_changeset(rollback);

	assert_eq!(manifest.levels.get_levels()[0].tables.len(), 0, "All tables should be removed");
}

#[tokio::test]
async fn test_revert_table_only_one_in_level() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	let table = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table");

	let add_changeset = ManifestChangeSet {
		new_tables: vec![(1, table)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.levels.get_levels()[1].tables.len(), 1);

	let delete_changeset = ManifestChangeSet {
		deleted_tables: HashSet::from([(1, test_sst_id(1))]),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&delete_changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.levels.get_levels()[1].tables.len(), 0);

	manifest.revert_changeset(rollback);

	assert_eq!(manifest.levels.get_levels()[1].tables.len(), 1, "Single table should be restored");
	assert_eq!(manifest.levels.get_levels()[1].tables[0].id, test_sst_id(1));
}

#[tokio::test]
async fn test_revert_idempotent() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	let table = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table");

	let changeset = ManifestChangeSet {
		new_tables: vec![(0, table)],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.levels.get_levels()[0].tables.len(), 1, "Table should be added");

	// Revert once
	manifest.revert_changeset(rollback);

	let state_after_revert = manifest.levels.get_levels()[0].tables.len();

	assert_eq!(state_after_revert, 0, "Table should be removed after revert");
}

#[tokio::test]
async fn test_apply_revert_apply_cycle() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	let table1 = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(
		test_sst_id(2),
		11,
		20,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 2");

	// First apply
	let changeset1 = ManifestChangeSet {
		new_tables: vec![(0, table1)],
		..Default::default()
	};
	let rollback1 = manifest.apply_changeset(&changeset1).expect("Failed to apply changeset");

	assert_eq!(manifest.levels.get_levels()[0].tables.len(), 1);

	// Revert
	manifest.revert_changeset(rollback1);

	assert_eq!(manifest.levels.get_levels()[0].tables.len(), 0);

	// Apply again
	let table1_again = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 1");
	let changeset2 = ManifestChangeSet {
		new_tables: vec![(0, table1_again), (0, table2)],
		..Default::default()
	};
	let _rollback2 = manifest.apply_changeset(&changeset2).expect("Failed to apply changeset");

	assert_eq!(manifest.levels.get_levels()[0].tables.len(), 2);
}

#[tokio::test]
async fn test_revert_after_disk_write_failure_simulation() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let mut manifest = new_test_manifest(&opts);
	let table_store = test_table_store();

	// Add some initial tables
	let table1 = create_test_table_with_seq_nums(
		test_sst_id(1),
		1,
		10,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(
		test_sst_id(2),
		11,
		20,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 2");

	let initial_changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (0, table2)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&initial_changeset).expect("Failed to apply changeset");

	// Capture state before the operation that will "fail"
	let state_before = (
		manifest.levels.get_levels()[0].tables.len(),
		manifest.levels.get_levels()[1].tables.len(),
		manifest.get_last_sequence(),
		manifest.get_log_number(),
		manifest.manifest_format_version,
	);

	// Simulate a compaction-like operation: delete old tables, add new table
	let table3 = create_test_table_with_seq_nums(
		test_sst_id(3),
		21,
		100,
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to create table 3");

	let compaction_changeset = ManifestChangeSet {
		deleted_tables: HashSet::from([(0, test_sst_id(1)), (0, test_sst_id(2))]),
		new_tables: vec![(1, table3)],
		log_number: Some(50),
		..Default::default()
	};

	// Apply changeset (simulating in-memory update)
	let rollback =
		manifest.apply_changeset(&compaction_changeset).expect("Failed to apply changeset");

	// Verify in-memory state changed
	assert_eq!(manifest.levels.get_levels()[0].tables.len(), 0);
	assert_eq!(manifest.levels.get_levels()[1].tables.len(), 1);
	assert_eq!(manifest.get_last_sequence(), 100);
	assert_eq!(manifest.get_log_number(), 50);

	// Simulate disk write failure - revert the changeset
	manifest.revert_changeset(rollback);

	// Verify state is restored to before the operation
	assert_eq!(
		manifest.levels.get_levels()[0].tables.len(),
		state_before.0,
		"L0 table count should be restored"
	);
	assert_eq!(
		manifest.levels.get_levels()[1].tables.len(),
		state_before.1,
		"L1 table count should be restored"
	);
	assert_eq!(manifest.get_last_sequence(), state_before.2, "last_sequence should be restored");
	assert_eq!(manifest.get_log_number(), state_before.3, "log_number should be restored");
	assert_eq!(
		manifest.manifest_format_version, state_before.4,
		"manifest_format_version should be restored"
	);

	// Verify original tables are still present
	assert!(manifest.levels.get_levels()[0].tables.iter().any(|t| t.id == test_sst_id(1)));
	assert!(manifest.levels.get_levels()[0].tables.iter().any(|t| t.id == test_sst_id(2)));
	assert!(!manifest.levels.get_levels()[1].tables.iter().any(|t| t.id == test_sst_id(3)));
}

// =============================================================================
// Manifest Fuzz Tests
// =============================================================================

/// Fuzz test: random changeset sequences with model comparison.
/// Applies random inserts, deletes, checkpoints, and reverts, verifying
/// manifest state matches a BTreeMap model after each operation.
#[tokio::test]
async fn test_manifest_changeset_fuzz() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	opts.path = temp_dir.path().to_path_buf();
	opts.level_count = 4;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let table_store = test_table_store();
	let mut manifest = new_test_manifest(&opts);

	// Model: (level, SstId) → exists
	let mut model: std::collections::BTreeMap<(u8, SstId), bool> =
		std::collections::BTreeMap::new();
	let mut rng = fastrand::Rng::with_seed(42);
	let mut next_table_id = 100u64;

	for iteration in 0..200u32 {
		let op = rng.u8(0..100);

		if op < 40 {
			// INSERT: add a table to a random level
			let level = rng.u8(0..4);
			let table_id = test_sst_id(next_table_id);
			next_table_id += 1;
			let seq_start = next_table_id * 10;
			let seq_end = seq_start + 5;

			let table = create_test_table_with_seq_nums(
				table_id,
				seq_start,
				seq_end,
				Arc::clone(&opts),
				Arc::clone(&table_store),
			)
			.await
			.expect("Failed to create table");

			let changeset = ManifestChangeSet {
				new_tables: vec![(level, table)],
				..Default::default()
			};

			manifest.apply_changeset(&changeset).expect("Failed to apply insert changeset");
			model.insert((level, table_id), true);
		} else if op < 70 {
			// COMPACTION-STYLE REPLACE: delete old table + add new with higher seq.
			// Standalone deletes would cause last_sequence > max_table_seq, which the
			// manifest loader correctly rejects. In production, compaction always replaces.
			if model.is_empty() {
				continue;
			}
			let keys: Vec<_> = model.keys().copied().collect();
			let idx = rng.usize(0..keys.len());
			let (level, table_id) = keys[idx];

			// Create replacement with seq higher than current last_sequence
			let new_table_id = test_sst_id(next_table_id);
			next_table_id += 1;
			let seq_start = manifest.get_last_sequence() + 1;
			let seq_end = seq_start + 3;
			let new_table = create_test_table_with_seq_nums(
				new_table_id,
				seq_start,
				seq_end,
				Arc::clone(&opts),
				Arc::clone(&table_store),
			)
			.await
			.expect("Failed to create replacement table");

			let target_level = rng.u8(0..4);
			let mut deleted = HashSet::new();
			deleted.insert((level, table_id));
			let changeset = ManifestChangeSet {
				deleted_tables: deleted,
				new_tables: vec![(target_level, new_table)],
				..Default::default()
			};

			manifest.apply_changeset(&changeset).expect("Failed to apply replace changeset");
			model.remove(&(level, table_id));
			model.insert((target_level, new_table_id), true);
		} else if op < 90 {
			// CHECKPOINT: write to disk + reload + verify
			let _bytes = write_manifest_to_disk(&mut manifest).expect("Failed to write manifest");
			let manifest_path = opts.manifest_file_path(manifest.manifest_id);

			let loaded = load_from_file(
				&manifest_path,
				opts.manifest_dir(),
				Arc::clone(&opts),
				Arc::clone(&table_store),
			)
			.await
			.expect("Failed to reload manifest");

			// Verify all model entries present in loaded manifest
			for &(level, table_id) in model.keys() {
				let found = loaded.levels.get_levels()[level as usize]
					.tables
					.iter()
					.any(|t| t.id == table_id);
				assert!(
					found,
					"iteration {iteration}: table {table_id:?} at L{level} missing after reload"
				);
			}

			// Verify no extra tables in loaded manifest
			let mut loaded_count = 0;
			for level in loaded.levels.get_levels() {
				loaded_count += level.tables.len();
			}
			assert_eq!(
				loaded_count,
				model.len(),
				"iteration {iteration}: loaded manifest has {loaded_count} tables, model has {}",
				model.len()
			);

			assert_eq!(
				loaded.manifest_id, manifest.manifest_id,
				"iteration {iteration}: manifest_id mismatch after reload"
			);
		} else {
			// REVERT: apply changeset then immediately revert
			if next_table_id > 10000 {
				continue; // Avoid creating too many tables
			}
			let level = rng.u8(0..4);
			let table_id = test_sst_id(next_table_id);
			next_table_id += 1;
			let seq_start = next_table_id * 10;
			let seq_end = seq_start + 5;

			let table = create_test_table_with_seq_nums(
				table_id,
				seq_start,
				seq_end,
				Arc::clone(&opts),
				Arc::clone(&table_store),
			)
			.await
			.expect("Failed to create table");

			// Snapshot pre-revert state
			let pre_table_count: usize =
				manifest.levels.get_levels().iter().map(|l| l.tables.len()).sum();
			let pre_last_seq = manifest.get_last_sequence();

			let changeset = ManifestChangeSet {
				new_tables: vec![(level, table)],
				..Default::default()
			};

			let rollback =
				manifest.apply_changeset(&changeset).expect("Failed to apply revert changeset");
			manifest.revert_changeset(rollback);

			// Verify state unchanged
			let post_table_count: usize =
				manifest.levels.get_levels().iter().map(|l| l.tables.len()).sum();
			assert_eq!(
				pre_table_count, post_table_count,
				"iteration {iteration}: table count changed after revert"
			);
			assert_eq!(
				pre_last_seq,
				manifest.get_last_sequence(),
				"iteration {iteration}: last_sequence changed after revert"
			);
		}
	}

	// Final checkpoint + full comparison
	let _bytes = write_manifest_to_disk(&mut manifest).expect("Failed to write final manifest");
	let manifest_path = opts.manifest_file_path(manifest.manifest_id);
	let loaded = load_from_file(
		&manifest_path,
		opts.manifest_dir(),
		Arc::clone(&opts),
		Arc::clone(&table_store),
	)
	.await
	.expect("Failed to reload final manifest");

	let mut loaded_tables: std::collections::BTreeSet<(u8, SstId)> =
		std::collections::BTreeSet::new();
	for (level_idx, level) in loaded.levels.get_levels().iter().enumerate() {
		for table in &level.tables {
			loaded_tables.insert((level_idx as u8, table.id));
		}
	}

	let model_keys: std::collections::BTreeSet<_> = model.keys().copied().collect();
	assert_eq!(loaded_tables, model_keys, "Final manifest does not match model");
}

/// Manifest invariant test: verify critical invariants hold after every changeset.
#[tokio::test]
async fn test_manifest_invariants_under_mutation() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	opts.path = temp_dir.path().to_path_buf();
	opts.level_count = 4;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let table_store = test_table_store();
	let mut manifest = new_test_manifest(&opts);
	let mut rng = fastrand::Rng::with_seed(77);
	let mut next_table_id = 200u64;
	let mut prev_manifest_id = manifest.manifest_id;

	for iteration in 0..50u32 {
		// Random changeset: insert or delete
		let level = rng.u8(0..4);

		if rng.bool() || manifest.levels.total_tables() == 0 {
			// Insert
			let table_id = test_sst_id(next_table_id);
			next_table_id += 1;
			let seq_start = next_table_id * 10;
			let seq_end = seq_start + rng.u64(1..10);

			let table = create_test_table_with_seq_nums(
				table_id,
				seq_start,
				seq_end,
				Arc::clone(&opts),
				Arc::clone(&table_store),
			)
			.await
			.expect("Failed to create table");

			let changeset = ManifestChangeSet {
				new_tables: vec![(level, table)],
				..Default::default()
			};
			manifest.apply_changeset(&changeset).expect("Failed to apply changeset");
		} else {
			// Delete random table
			let all_tables: Vec<_> = manifest
				.levels
				.get_levels()
				.iter()
				.enumerate()
				.flat_map(|(lvl, level)| level.tables.iter().map(move |t| (lvl as u8, t.id)))
				.collect();

			if all_tables.is_empty() {
				continue;
			}
			let idx = rng.usize(0..all_tables.len());
			let (del_level, del_id) = all_tables[idx];

			let mut deleted = HashSet::new();
			deleted.insert((del_level, del_id));
			let changeset = ManifestChangeSet {
				deleted_tables: deleted,
				..Default::default()
			};
			manifest.apply_changeset(&changeset).expect("Failed to apply delete");
		}

		// INVARIANT 1: manifest_id increases on each write
		let _bytes = write_manifest_to_disk(&mut manifest).expect("Failed to write manifest");
		assert!(
			manifest.manifest_id > prev_manifest_id,
			"iteration {iteration}: manifest_id did not increase: {} <= {}",
			manifest.manifest_id,
			prev_manifest_id
		);
		prev_manifest_id = manifest.manifest_id;

		// INVARIANT 2: log_number never decreases (we don't modify it, so it stays 0)
		assert_eq!(
			manifest.get_log_number(),
			0,
			"iteration {iteration}: log_number should remain 0 in this test"
		);

		// INVARIANT 3: last_sequence >= max sequence of all tables
		let max_seq_in_tables: u64 = manifest
			.levels
			.get_levels()
			.iter()
			.flat_map(|level| level.tables.iter())
			.filter_map(|t| t.meta.largest_seq_num)
			.max()
			.unwrap_or(0);
		assert!(
			manifest.get_last_sequence() >= max_seq_in_tables,
			"iteration {iteration}: last_sequence {} < max table seq {}",
			manifest.get_last_sequence(),
			max_seq_in_tables
		);

		// INVARIANT 4: no table ID in two different levels
		let mut seen_ids: std::collections::HashMap<SstId, u8> = std::collections::HashMap::new();
		for (level_idx, level) in manifest.levels.get_levels().iter().enumerate() {
			for table in &level.tables {
				if let Some(prev_level) = seen_ids.insert(table.id, level_idx as u8) {
					panic!(
						"iteration {iteration}: table {:?} in L{prev_level} and L{level_idx}",
						table.id
					);
				}
			}
		}
	}
}

/// Multi-cycle checkpoint recovery test: write + reload across 10 rounds,
/// verifying state is preserved each time.
#[tokio::test]
async fn test_manifest_checkpoint_recovery_cycles() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	opts.path = temp_dir.path().to_path_buf();
	opts.level_count = 4;
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

	let table_store = test_table_store();
	let mut manifest = new_test_manifest(&opts);
	let mut rng = fastrand::Rng::with_seed(123);
	let mut next_table_id = 300u64;

	for round in 0..10u32 {
		// Apply 5-10 random changesets (inserts only, or compaction-style replace)
		// NOTE: standalone deletes cause last_sequence mismatch on reload because
		// last_sequence is a high-water mark that doesn't decrease. This matches
		// production behavior where compaction always replaces (delete + add).
		let num_changes = rng.usize(5..=10);
		for _ in 0..num_changes {
			let level = rng.u8(0..4);

			if rng.u8(0..10) < 7 || manifest.levels.total_tables() == 0 {
				// INSERT: add a new table
				let table_id = test_sst_id(next_table_id);
				next_table_id += 1;
				let seq_start = next_table_id * 10;
				let seq_end = seq_start + 3;

				let table = create_test_table_with_seq_nums(
					table_id,
					seq_start,
					seq_end,
					Arc::clone(&opts),
					Arc::clone(&table_store),
				)
				.await
				.expect("Failed to create table");

				let changeset = ManifestChangeSet {
					new_tables: vec![(level, table)],
					..Default::default()
				};
				manifest.apply_changeset(&changeset).expect("Failed to apply insert");
			} else {
				// COMPACTION-STYLE REPLACE: delete old + add new with higher seq
				let all_tables: Vec<_> = manifest
					.levels
					.get_levels()
					.iter()
					.enumerate()
					.flat_map(|(lvl, level)| level.tables.iter().map(move |t| (lvl as u8, t.id)))
					.collect();

				if !all_tables.is_empty() {
					let idx = rng.usize(0..all_tables.len());
					let (del_level, del_id) = all_tables[idx];

					// Create replacement table with seq higher than current last_sequence
					let new_table_id = test_sst_id(next_table_id);
					next_table_id += 1;
					let seq_start = manifest.get_last_sequence() + 1;
					let seq_end = seq_start + 3;

					let new_table = create_test_table_with_seq_nums(
						new_table_id,
						seq_start,
						seq_end,
						Arc::clone(&opts),
						Arc::clone(&table_store),
					)
					.await
					.expect("Failed to create replacement table");

					let mut deleted = HashSet::new();
					deleted.insert((del_level, del_id));
					let changeset = ManifestChangeSet {
						deleted_tables: deleted,
						new_tables: vec![(level, new_table)],
						..Default::default()
					};
					manifest.apply_changeset(&changeset).expect("Failed to apply replace");
				}
			}
		}

		// Checkpoint: write + reload
		let _bytes = write_manifest_to_disk(&mut manifest).expect("Failed to write manifest");
		let manifest_path = opts.manifest_file_path(manifest.manifest_id);

		let loaded = load_from_file(
			&manifest_path,
			opts.manifest_dir(),
			Arc::clone(&opts),
			Arc::clone(&table_store),
		)
		.await
		.unwrap_or_else(|e| panic!("round {round}: Failed to reload manifest: {e}"));

		// Verify loaded state matches
		assert_eq!(loaded.manifest_id, manifest.manifest_id, "round {round}: manifest_id mismatch");
		assert_eq!(
			loaded.get_log_number(),
			manifest.get_log_number(),
			"round {round}: log_number mismatch"
		);
		assert_eq!(
			loaded.get_last_sequence(),
			manifest.get_last_sequence(),
			"round {round}: last_sequence mismatch"
		);

		// Verify same tables
		let orig_tables: std::collections::BTreeSet<_> = manifest
			.levels
			.get_levels()
			.iter()
			.enumerate()
			.flat_map(|(lvl, level)| level.tables.iter().map(move |t| (lvl as u8, t.id)))
			.collect();
		let loaded_tables: std::collections::BTreeSet<_> = loaded
			.levels
			.get_levels()
			.iter()
			.enumerate()
			.flat_map(|(lvl, level)| level.tables.iter().map(move |t| (lvl as u8, t.id)))
			.collect();
		assert_eq!(orig_tables, loaded_tables, "round {round}: table sets differ after reload");

		// Continue using loaded manifest for next round (simulates restart)
		manifest = loaded;
	}
}

/// Verifies that `filter_handle` (offset + size) survives `Levels::encode()/decode()`.
///
/// Analogous to SlateDB's `test_should_encode_decode_ssts_with_visible_ranges` —
/// directly validates the new V1 binary format for the filter_handle field.
#[tokio::test]
async fn test_levels_filter_handle_roundtrip() {
	use std::io::Cursor;

	use crate::levels::level::{Level, Levels};

	let store = test_table_store();

	// --- Case 1: table written WITH bloom filter (Options::default() enables it) ---
	let opts_with = Arc::new(Options::default());
	let table_with =
		create_test_table(test_sst_id(1), 20, Arc::clone(&opts_with), Arc::clone(&store))
			.await
			.unwrap();

	let original_handle = table_with.filter_handle.clone();
	assert!(original_handle.is_some(), "bloom filter should have been written");

	let levels_with = Levels(vec![Arc::new(Level {
		tables: vec![Arc::clone(&table_with)],
	})]);
	let mut encoded = Vec::new();
	levels_with.encode(&mut encoded).unwrap();

	let decoded_with = Levels::decode(&mut Cursor::new(&encoded)).unwrap();
	let entry_with = &decoded_with[0][0];
	assert_eq!(
		entry_with.filter_handle, original_handle,
		"filter_handle must survive Levels encode/decode round-trip"
	);

	// --- Case 2: table written WITHOUT bloom filter ---
	let opts_none = Arc::new(Options::default().with_filter_policy(None));
	let table_none =
		create_test_table(test_sst_id(2), 20, Arc::clone(&opts_none), Arc::clone(&store))
			.await
			.unwrap();

	assert!(table_none.filter_handle.is_none(), "no filter handle expected");

	let levels_none = Levels(vec![Arc::new(Level {
		tables: vec![Arc::clone(&table_none)],
	})]);
	let mut encoded_none = Vec::new();
	levels_none.encode(&mut encoded_none).unwrap();

	let decoded_none = Levels::decode(&mut Cursor::new(&encoded_none)).unwrap();
	assert!(
		decoded_none[0][0].filter_handle.is_none(),
		"filter_handle should remain None when no filter policy"
	);
}

/// Full manifest write → reload → `Table::get()` with bloom filter active.
///
/// Analogous to SlateDB's `test_put_get_reopen_delete_with_separate_wal_store`.
/// Verifies that `filter_handle` is preserved across the manifest round-trip and
/// that the filter correctly functions after reload (both hit and miss paths).
#[tokio::test]
async fn test_manifest_reload_filter_works() {
	use std::collections::HashSet;
	use std::path::PathBuf;

	use crate::levels::LevelManifest;
	use crate::manifest::{load_from_bytes, serialize_manifest};

	let store = test_table_store();
	let opts = Arc::new(Options::default()); // bloom filter on by default

	// Write an SST with known keys
	let table_id = test_sst_id(50);
	let num_keys: u64 = 20;
	let table =
		create_test_table(table_id, num_keys, Arc::clone(&opts), Arc::clone(&store)).await.unwrap();

	assert!(table.filter_handle.is_some(), "bloom filter should have been written");
	let original_handle = table.filter_handle.clone();

	// Build a manifest with the table in level 0
	let mut manifest = LevelManifest {
		manifest_dir: PathBuf::from("/tmp"),
		manifest_id: 0,
		levels: LevelManifest::initialize_levels_for_test(opts.level_count),
		hidden_set: HashSet::with_capacity(10),
		manifest_format_version: crate::manifest::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
		writer_epoch: 0,
		compactor_epoch: 0,
		leader_id: 0,
	};
	manifest
		.apply_changeset(&ManifestChangeSet {
			new_tables: vec![(0, Arc::clone(&table))],
			..Default::default()
		})
		.unwrap();

	// Serialize without writing to disk
	let bytes = serialize_manifest(&manifest).unwrap();

	// Reload with fresh opts (fresh block_cache) — simulates a cold restart
	let opts_reload = Arc::new(Options::default());
	let loaded = load_from_bytes(
		&bytes,
		PathBuf::from("/tmp"),
		Arc::clone(&opts_reload),
		Arc::clone(&store),
	)
	.await
	.unwrap();

	// filter_handle must be preserved
	let reloaded_table = &loaded.levels.as_ref()[0].tables[0];
	assert_eq!(
		reloaded_table.filter_handle, original_handle,
		"filter_handle must survive manifest round-trip"
	);

	// get() for a key that EXISTS — filter should pass it through
	let existing_key = InternalKey::new(
		b"key_00000".to_vec(),
		u64::MAX, // latest version
		InternalKeyKind::Set,
		0,
	);
	let result = reloaded_table.get(&existing_key).await.unwrap();
	assert!(result.is_some(), "get() should find an existing key after manifest reload");

	// get() for a key that DOES NOT EXIST — filter should reject it early
	let missing_key =
		InternalKey::new(b"zzz_no_such_key".to_vec(), u64::MAX, InternalKeyKind::Set, 0);
	let result = reloaded_table.get(&missing_key).await.unwrap();
	assert!(result.is_none(), "get() should return None for a missing key");
}
