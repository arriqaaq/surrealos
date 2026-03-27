use std::fs::File as SysFile;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::cloud_store::CloudStore;
use crate::compaction::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::error::{BackgroundErrorHandler, Result};
use crate::iter::{BoxedLSMIterator, CompactionIterator};
use crate::levels::LevelManifest;
use crate::manifest::{write_manifest_to_disk, ManifestChangeSet, ManifestUploader};
use crate::memtable::ImmutableMemtables;
use crate::snapshot::SnapshotTracker;
use crate::sstable::table::{Table, TableWriter};
use crate::sstable::SstId;
use crate::{Comparator, Options as LSMOptions};

/// RAII guard to ensure tables are unhidden if compaction fails
pub(crate) struct HiddenTablesGuard {
	level_manifest: Arc<RwLock<LevelManifest>>,
	table_ids: Vec<SstId>,
	committed: bool,
}

impl HiddenTablesGuard {
	pub(crate) fn new(level_manifest: Arc<RwLock<LevelManifest>>, table_ids: &[SstId]) -> Self {
		Self {
			level_manifest,
			table_ids: table_ids.to_vec(),
			committed: false,
		}
	}

	pub(crate) fn commit(&mut self) {
		self.committed = true;
	}
}

impl Drop for HiddenTablesGuard {
	fn drop(&mut self) {
		if !self.committed {
			if let Ok(mut levels) = self.level_manifest.write() {
				levels.unhide_tables(&self.table_ids);
			}
		}
	}
}

/// Compaction options
pub(crate) struct CompactionOptions {
	pub(crate) lopts: Arc<LSMOptions>,
	pub(crate) level_manifest: Arc<RwLock<LevelManifest>>,
	pub(crate) immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
	pub(crate) error_handler: Arc<BackgroundErrorHandler>,
	/// Snapshot tracker for snapshot-aware compaction.
	///
	/// During compaction, we query this to get the list of active snapshot
	/// sequence numbers. Versions visible to any active snapshot must be
	/// preserved (unless hidden by a newer version in the same visibility boundary).
	pub(crate) snapshot_tracker: SnapshotTracker,
	/// Table store for reading/writing SSTs via object store.
	pub(crate) table_store: Arc<CloudStore>,
	/// Manifest uploader for async object store uploads.
	pub(crate) manifest_uploader: Arc<ManifestUploader>,
	/// Local compactor epoch for fencing checks.
	/// 0 means no fencing (single-node mode).
	pub(crate) local_compactor_epoch: u64,
	/// When true, SST deletion from object store is deferred to the
	/// background purger. Only local cache copies are removed.
	pub(crate) branching_enabled: bool,
}

// ============================================================================
// Shared helpers
// ============================================================================

/// Upload a locally-written SST file to the object store, then open it.
pub(crate) async fn upload_and_open_table(
	lopts: &Arc<LSMOptions>,
	table_id: SstId,
	local_path: &Path,
	table_store: &Arc<CloudStore>,
) -> Result<Arc<Table>> {
	let data = std::fs::read(local_path)?;
	let file_size = data.len() as u64;
	table_store.write_sst(&table_id, bytes::Bytes::from(data)).await?;
	// Remove local file after successful upload (unless keeping local SSTs)
	if !lopts.local_sst_cache {
		let _ = std::fs::remove_file(local_path);
	}
	Ok(Arc::new(Table::new(table_id, Arc::clone(lopts), Arc::clone(table_store), file_size).await?))
}

/// Update the manifest with a completed compaction: delete old tables, add new table.
/// Commits the hidden tables guard on success.
pub(crate) fn update_manifest(
	opts: &CompactionOptions,
	input: &CompactionInput,
	new_table: Option<Arc<Table>>,
	guard: &mut HiddenTablesGuard,
) -> Result<()> {
	let mut manifest = opts.level_manifest.write()?;
	let _imm_guard = opts.immutable_memtables.write();

	// Check if compactor has been fenced
	if opts.local_compactor_epoch > 0 && manifest.compactor_epoch > opts.local_compactor_epoch {
		return Err(crate::error::Error::Fenced);
	}

	// Check for table ID collision if adding a new table
	if let Some(ref table) = new_table {
		if input.tables_to_merge.contains(&table.id) {
			return Err(crate::error::Error::TableIDCollision(table.id));
		}
	}

	let mut changeset = ManifestChangeSet::default();

	// Delete old tables
	for (level_idx, level) in manifest.levels.get_levels().iter().enumerate() {
		for &table_id in &input.tables_to_merge {
			if level.tables.iter().any(|t| t.id == table_id) {
				changeset.deleted_tables.insert((level_idx as u8, table_id));
			}
		}
	}

	// Add new table if present
	if let Some(table) = new_table {
		changeset.new_tables.push((input.target_level, table));
	}

	let rollback = manifest.apply_changeset(&changeset)?;

	// Write manifest to disk - if this fails, revert in-memory state
	match write_manifest_to_disk(&mut manifest) {
		Ok(bytes) => {
			opts.manifest_uploader.queue_upload(manifest.manifest_id, bytes);
		}
		Err(e) => {
			manifest.revert_changeset(rollback);
			opts.error_handler
				.set_error(e.clone(), crate::error::BackgroundErrorReason::ManifestWrite);
			return Err(e);
		}
	}

	// Unhide tables before committing guard (they'll be removed from manifest anyway)
	manifest.unhide_tables(&input.tables_to_merge);

	// Commit guard - tables are now properly handled in manifest
	guard.commit();

	Ok(())
}

/// Remove old table files after successful manifest update.
pub(crate) async fn cleanup_old_tables(
	table_store: &Arc<CloudStore>,
	input: &CompactionInput,
	branching_enabled: bool,
) {
	if branching_enabled {
		// Only clean local cache; object store deletion handled by purger
		for &table_id in &input.tables_to_merge {
			table_store.delete_local_sst(&table_id);
		}
		return;
	}
	for &table_id in &input.tables_to_merge {
		if let Err(e) = table_store.delete_sst(&table_id).await {
			log::warn!("Failed to remove old table file: {e}");
		}
	}
}

// ============================================================================
// Compactor: run-to-completion compaction
// ============================================================================

/// Handles the compaction state and operations
pub(crate) struct Compactor {
	pub(crate) options: CompactionOptions,
	pub(crate) strategy: Arc<dyn CompactionStrategy>,
}

impl Compactor {
	pub(crate) fn new(options: CompactionOptions, strategy: Arc<dyn CompactionStrategy>) -> Self {
		Self {
			options,
			strategy,
		}
	}

	pub(crate) async fn compact(&self) -> Result<()> {
		let choice = {
			let levels_guard = self.options.level_manifest.write()?;
			self.strategy.pick_levels(&levels_guard)?
		};

		match choice {
			CompactionChoice::Merge(input) => self.merge_tables(&input).await,
			CompactionChoice::Skip => Ok(()),
		}
	}

	async fn merge_tables(&self, input: &CompactionInput) -> Result<()> {
		// Acquire lock, hide tables, collect Arc<Table> refs, then drop lock before async work
		let (to_merge, mut guard) = {
			let mut levels = self.options.level_manifest.write()?;

			// Hide tables that are being merged
			levels.hide_tables(&input.tables_to_merge);

			// Create guard to ensure tables are unhidden on error
			let guard = HiddenTablesGuard::new(
				Arc::clone(&self.options.level_manifest),
				&input.tables_to_merge,
			);

			let tables = levels.get_all_tables();
			let to_merge: Vec<_> =
				input.tables_to_merge.iter().filter_map(|&id| tables.get(&id).cloned()).collect();

			(to_merge, guard)
		};

		// Keep tables alive while iterators borrow from them
		let iterators: Vec<BoxedLSMIterator<'_>> = to_merge
			.iter()
			.filter_map(|table| table.iter(None).ok())
			.map(|iter| Box::new(iter) as BoxedLSMIterator<'_>)
			.collect();

		// Create new table
		let new_table_id = crate::levels::next_sst_id();
		let new_table_path = self.options.lopts.sstable_file_path(new_table_id);

		// Write merged data
		let table_created =
			match self.write_merged_table(&new_table_path, new_table_id, iterators, input).await {
				Ok(result) => result,
				Err(e) => {
					// Guard will unhide tables on drop
					return Err(e);
				}
			};

		// Upload and open table only if one was created
		let new_table = if table_created {
			match upload_and_open_table(
				&self.options.lopts,
				new_table_id,
				&new_table_path,
				&self.options.table_store,
			)
			.await
			{
				Ok(table) => Some(table),
				Err(e) => {
					// Guard will unhide tables on drop
					return Err(e);
				}
			}
		} else {
			None
		};

		// Update manifest - this will commit the guard on success
		update_manifest(&self.options, input, new_table, &mut guard)?;

		cleanup_old_tables(&self.options.table_store, input, self.options.branching_enabled).await;

		Ok(())
	}

	/// Returns true if a table file was created and finished, false otherwise
	async fn write_merged_table(
		&self,
		path: &Path,
		table_id: SstId,
		merge_iter: Vec<BoxedLSMIterator<'_>>,
		input: &CompactionInput,
	) -> Result<bool> {
		let file = SysFile::create(path)?;
		let mut writer =
			TableWriter::new(file, table_id, Arc::clone(&self.options.lopts), input.target_level);

		// Get active snapshots for snapshot-aware compaction
		let snapshots = self.options.snapshot_tracker.get_all_snapshots();

		// Create a compaction iterator that filters tombstones and respects snapshots
		let max_level = self.options.lopts.level_count - 1;
		let is_bottom_level = input.target_level >= max_level;
		let mut comp_iter = CompactionIterator::new(
			merge_iter,
			Arc::clone(&self.options.lopts.internal_comparator) as Arc<dyn Comparator>,
			is_bottom_level,
			self.options.lopts.enable_versioning,
			self.options.lopts.versioned_history_retention_ns,
			Arc::clone(&self.options.lopts.clock),
			snapshots,
		);

		let mut entries = 0;
		while let Some((key, value)) = comp_iter.advance().await? {
			writer.add(key, &value)?;
			entries += 1;
		}

		if entries == 0 {
			// No entries - drop writer and remove empty file
			drop(writer);
			let _ = std::fs::remove_file(path);
			return Ok(false);
		}

		writer.finish()?;
		Ok(true)
	}
}
