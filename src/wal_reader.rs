use std::collections::VecDeque;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::time::Duration;

use crate::batch::Batch;
use crate::wal::reader::Reader as WalSegmentReader;
use crate::wal::{list_segment_ids, segment_name};
use crate::{InternalKeyKind, Result};

/// A decoded WAL entry for CDC consumers.
///
/// Contains the raw entry data including tombstones and merge operands.
/// CDC consumers receive all operations without filtering.
pub struct WalEntry {
	/// The user key bytes.
	pub key: Vec<u8>,
	/// The value bytes, or None for delete operations.
	pub value: Option<Vec<u8>>,
	/// The operation kind (Set, Delete, Merge, etc.).
	pub kind: InternalKeyKind,
	/// The sequence number assigned to this entry.
	pub seq_num: u64,
	/// The timestamp assigned to this entry.
	pub timestamp: u64,
}

/// Position in the WAL stream, used for resuming reads.
#[derive(Clone, Debug)]
pub struct WalPosition {
	/// The WAL segment ID.
	pub segment_id: u64,
	/// The byte offset within the segment file.
	pub offset: u64,
}

/// Reads WAL entries for change data capture.
///
/// `WalReader` provides a streaming interface over WAL segment files,
/// decoding batch records into individual `WalEntry` values. It automatically
/// advances to the next segment when the current one is exhausted.
///
/// CDC consumers should periodically call `Store::set_cdc_watermark()` with
/// the segment ID of the last fully processed position to prevent WAL cleanup
/// from deleting segments that are still needed.
pub struct WalReader {
	wal_dir: PathBuf,
	current_segment: u64,
	current_offset: u64,
	current_reader: Option<WalSegmentReader>,
	pending_entries: VecDeque<WalEntry>,
}

impl WalReader {
	/// Opens a WAL reader starting from the given position.
	///
	/// The reader will begin reading from the specified segment at the
	/// specified byte offset.
	pub fn new(wal_dir: PathBuf, from: WalPosition) -> Result<Self> {
		let mut reader = Self {
			wal_dir,
			current_segment: from.segment_id,
			current_offset: from.offset,
			current_reader: None,
			pending_entries: VecDeque::new(),
		};
		reader.open_segment(from.segment_id, from.offset)?;
		Ok(reader)
	}

	/// Opens a WAL reader starting from the earliest available segment.
	///
	/// Lists all WAL segments in the directory and opens the first one.
	/// Returns an error if no segments are available.
	pub fn from_earliest(wal_dir: PathBuf) -> Result<Self> {
		let segment_ids = list_segment_ids(&wal_dir, Some("wal"))
			.map_err(|e| crate::Error::Other(format!("failed to list WAL segments: {}", e)))?;

		if segment_ids.is_empty() {
			return Ok(Self {
				wal_dir,
				current_segment: 0,
				current_offset: 0,
				current_reader: None,
				pending_entries: VecDeque::new(),
			});
		}

		let first_segment = segment_ids[0];
		let mut reader = Self {
			wal_dir,
			current_segment: first_segment,
			current_offset: 0,
			current_reader: None,
			pending_entries: VecDeque::new(),
		};
		reader.open_segment(first_segment, 0)?;
		Ok(reader)
	}

	/// Reads up to `max_entries` WAL entries.
	///
	/// Returns decoded entries including tombstones and merge operands.
	/// Returns an empty vec when no more entries are available (caller
	/// can retry later or use `poll`).
	pub fn read_batch(&mut self, max_entries: usize) -> Result<Vec<WalEntry>> {
		let mut result = Vec::new();

		while result.len() < max_entries {
			// Drain pending entries first
			if let Some(entry) = self.pending_entries.pop_front() {
				result.push(entry);
				continue;
			}

			// Try to read next record from current WAL segment
			if let Some(ref mut reader) = self.current_reader {
				match reader.read() {
					Ok((record_data, offset)) => {
						self.current_offset = offset;
						// Decode the batch from the WAL record
						let batch = Batch::decode(record_data)?;
						// Convert batch entries to WalEntry
						for (i, entry) in batch.entries.into_iter().enumerate() {
							self.pending_entries.push_back(WalEntry {
								key: entry.key,
								value: entry.value,
								kind: entry.kind,
								seq_num: batch.starting_seq_num + i as u64,
								timestamp: entry.timestamp,
							});
						}
						continue;
					}
					Err(_) => {
						// End of segment or error — try advancing
						if !self.advance_to_next_segment()? {
							break; // No more segments
						}
						continue;
					}
				}
			} else {
				// No current reader — nothing to read
				break;
			}
		}

		Ok(result)
	}

	/// Returns the current read position in the WAL stream.
	pub fn position(&self) -> WalPosition {
		WalPosition {
			segment_id: self.current_segment,
			offset: self.current_offset,
		}
	}

	/// Polls for new WAL entries with a timeout.
	///
	/// Repeatedly attempts to read entries until either `max_entries` are
	/// available or the timeout expires. Returns whatever entries were read
	/// (possibly empty if the timeout was reached with no new data).
	pub async fn poll(&mut self, max_entries: usize, timeout: Duration) -> Result<Vec<WalEntry>> {
		let deadline = tokio::time::Instant::now() + timeout;
		loop {
			let entries = self.read_batch(max_entries)?;
			if !entries.is_empty() {
				return Ok(entries);
			}
			if tokio::time::Instant::now() >= deadline {
				return Ok(vec![]);
			}
			// Re-check for new segments that may have appeared
			self.try_reopen_or_advance()?;
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
	}

	/// Opens a WAL segment file and optionally seeks to an offset.
	fn open_segment(&mut self, segment_id: u64, offset: u64) -> Result<()> {
		let file_name = segment_name(segment_id, "wal");
		let file_path = self.wal_dir.join(file_name);

		match File::open(&file_path) {
			Ok(mut file) => {
				if offset > 0 {
					file.seek(SeekFrom::Start(offset)).map_err(|e| {
						crate::Error::Other(format!(
							"failed to seek WAL segment {} to offset {}: {}",
							segment_id, offset, e
						))
					})?;
				}
				self.current_reader = Some(WalSegmentReader::new(file));
				self.current_segment = segment_id;
				self.current_offset = offset;
				Ok(())
			}
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
				// Segment doesn't exist — no reader
				self.current_reader = None;
				Ok(())
			}
			Err(e) => Err(crate::Error::Other(format!(
				"failed to open WAL segment {}: {}",
				segment_id, e
			))),
		}
	}

	/// Advances to the next WAL segment file.
	///
	/// Lists available segments and opens the one immediately after the
	/// current segment. Returns `false` if no next segment is available.
	fn advance_to_next_segment(&mut self) -> Result<bool> {
		let segment_ids = list_segment_ids(&self.wal_dir, Some("wal"))
			.map_err(|e| crate::Error::Other(format!("failed to list WAL segments: {}", e)))?;

		// Find the next segment after the current one
		for &id in &segment_ids {
			if id > self.current_segment {
				self.open_segment(id, 0)?;
				return Ok(self.current_reader.is_some());
			}
		}

		// No next segment available
		self.current_reader = None;
		Ok(false)
	}

	/// Tries to reopen the current segment or advance to a new one.
	///
	/// Used by `poll()` to detect newly written WAL segments.
	fn try_reopen_or_advance(&mut self) -> Result<()> {
		if self.current_reader.is_none() {
			// Try to find any segment at or after the current position
			let segment_ids = list_segment_ids(&self.wal_dir, Some("wal"))
				.map_err(|e| crate::Error::Other(format!("failed to list WAL segments: {}", e)))?;

			for &id in &segment_ids {
				if id >= self.current_segment {
					let offset = if id == self.current_segment {
						self.current_offset
					} else {
						0
					};
					self.open_segment(id, offset)?;
					if self.current_reader.is_some() {
						break;
					}
				}
			}
		}
		Ok(())
	}
}
