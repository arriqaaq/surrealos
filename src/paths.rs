use object_store::path::Path;

use crate::sstable::SstId;

/// Resolves object store paths for SSTs and manifests.
///
/// When `branch_name` is set, manifests are stored under a branch-scoped
/// prefix (`branches/{name}/manifest/`), while SSTs remain in the shared
/// pool (`sst/`) since ULIDs are globally unique.
pub(crate) struct StorePaths {
	root: Path,
	branch_name: Option<String>,
}

impl StorePaths {
	pub(crate) fn new(root: impl Into<Path>) -> Self {
		Self {
			root: root.into(),
			branch_name: None,
		}
	}

	pub(crate) fn new_with_branch(root: impl Into<Path>, branch_name: Option<String>) -> Self {
		Self {
			root: root.into(),
			branch_name,
		}
	}

	/// Returns the object store path for an SST.
	/// SSTs always go to the shared pool (no branch prefix).
	pub(crate) fn table_path(&self, id: &SstId) -> Path {
		self.root.child("sst").child(format!("{id}.sst"))
	}

	/// Returns the object store path for a manifest file.
	/// When branching, manifests are scoped to `branches/{name}/manifest/`.
	pub(crate) fn manifest_path(&self, id: u64) -> Path {
		match &self.branch_name {
			Some(branch) => self
				.root
				.child("branches")
				.child(branch.as_str())
				.child("manifest")
				.child(format!("{id:020}.manifest")),
			None => self.root.child("manifest").child(format!("{id:020}.manifest")),
		}
	}

	/// Returns the SST directory prefix.
	pub(crate) fn sst_path(&self) -> Path {
		self.root.child("sst")
	}

	/// Returns the manifest directory prefix.
	pub(crate) fn manifest_prefix(&self) -> Path {
		match &self.branch_name {
			Some(branch) => self.root.child("branches").child(branch.as_str()).child("manifest"),
			None => self.root.child("manifest"),
		}
	}
}
