//! Contains compaction strategies

pub(crate) mod compactor;
pub(crate) mod leveled;

use crate::levels::LevelManifest;
use crate::sstable::SstId;
use crate::Result;

/// Represents the input for a compaction operation
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct CompactionInput {
	pub tables_to_merge: Vec<SstId>,
	pub target_level: u8,
	pub source_level: u8,
}

/// Represents the possible compaction decisions
#[derive(Debug, Eq, PartialEq)]
pub enum CompactionChoice {
	Merge(CompactionInput),
	Skip,
}

/// Defines the strategy interface for compaction
pub trait CompactionStrategy: Send + Sync {
	/// Determines which levels should be compacted
	fn pick_levels(&self, manifest: &LevelManifest) -> Result<CompactionChoice>;
}
