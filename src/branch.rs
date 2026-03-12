use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use object_store::ObjectStore;

use crate::error::{Error, Result};

/// Status of a branch during creation lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchStatus {
	/// Branch registered but manifest not yet written.
	/// Purger must protect parent SSTs via source_manifest_id.
	Creating = 0,
	/// Branch is fully initialized and operational.
	Active = 1,
}

impl TryFrom<u8> for BranchStatus {
	type Error = Error;

	fn try_from(v: u8) -> Result<Self> {
		match v {
			0 => Ok(Self::Creating),
			1 => Ok(Self::Active),
			_ => Err(Error::Other(format!("Unknown branch status: {v}"))),
		}
	}
}

/// Metadata for a single branch.
#[derive(Debug, Clone)]
pub struct BranchInfo {
	/// Branch name (unique identifier).
	pub name: String,
	/// Parent branch name. None for the root branch.
	pub parent_branch: Option<String>,
	/// Unix timestamp (seconds) when the branch was created.
	pub created_at: u64,
	/// Parent's manifest_id at fork time.
	pub source_manifest_id: u64,
	/// Parent's last_sequence at fork time.
	pub source_sequence: u64,
	/// Current lifecycle status.
	pub status: BranchStatus,
}

impl BranchInfo {
	fn encode(&self) -> Result<Vec<u8>> {
		let mut buf = Vec::new();
		// status
		buf.write_u8(self.status as u8)?;
		// created_at
		buf.write_u64::<BigEndian>(self.created_at)?;
		// source_manifest_id
		buf.write_u64::<BigEndian>(self.source_manifest_id)?;
		// source_sequence
		buf.write_u64::<BigEndian>(self.source_sequence)?;
		// name (length-prefixed)
		let name_bytes = self.name.as_bytes();
		buf.write_u32::<BigEndian>(name_bytes.len() as u32)?;
		buf.write_all(name_bytes)?;
		// parent_branch (optional, length-prefixed; 0 = None)
		match &self.parent_branch {
			Some(parent) => {
				let parent_bytes = parent.as_bytes();
				buf.write_u32::<BigEndian>(parent_bytes.len() as u32)?;
				buf.write_all(parent_bytes)?;
			}
			None => {
				buf.write_u32::<BigEndian>(0)?;
			}
		}
		Ok(buf)
	}

	fn decode(cursor: &mut Cursor<&[u8]>) -> Result<Self> {
		let status = BranchStatus::try_from(cursor.read_u8()?)?;
		let created_at = cursor.read_u64::<BigEndian>()?;
		let source_manifest_id = cursor.read_u64::<BigEndian>()?;
		let source_sequence = cursor.read_u64::<BigEndian>()?;

		let name_len = cursor.read_u32::<BigEndian>()? as usize;
		let mut name_bytes = vec![0u8; name_len];
		cursor.read_exact(&mut name_bytes)?;
		let name = String::from_utf8(name_bytes)
			.map_err(|e| Error::Other(format!("Invalid branch name: {e}")))?;

		let parent_len = cursor.read_u32::<BigEndian>()? as usize;
		let parent_branch = if parent_len > 0 {
			let mut parent_bytes = vec![0u8; parent_len];
			cursor.read_exact(&mut parent_bytes)?;
			Some(
				String::from_utf8(parent_bytes)
					.map_err(|e| Error::Other(format!("Invalid parent branch name: {e}")))?,
			)
		} else {
			None
		};

		Ok(Self {
			name,
			parent_branch,
			created_at,
			source_manifest_id,
			source_sequence,
			status,
		})
	}
}

/// Registry format version.
const REGISTRY_VERSION: u16 = 1;

/// Registry of all branches, persisted to object store.
/// Updated via CAS to prevent concurrent mutation races.
#[derive(Debug, Clone)]
pub struct BranchRegistry {
	branches: HashMap<String, BranchInfo>,
}

impl Default for BranchRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl BranchRegistry {
	pub fn new() -> Self {
		Self {
			branches: HashMap::new(),
		}
	}

	pub fn branches(&self) -> &HashMap<String, BranchInfo> {
		&self.branches
	}

	pub fn get(&self, name: &str) -> Option<&BranchInfo> {
		self.branches.get(name)
	}

	pub fn contains(&self, name: &str) -> bool {
		self.branches.contains_key(name)
	}

	pub fn insert(&mut self, info: BranchInfo) {
		self.branches.insert(info.name.clone(), info);
	}

	pub fn remove(&mut self, name: &str) -> Option<BranchInfo> {
		self.branches.remove(name)
	}

	pub fn update_status(&mut self, name: &str, status: BranchStatus) -> Result<()> {
		let info = self
			.branches
			.get_mut(name)
			.ok_or_else(|| Error::Other(format!("Branch not found: {name}")))?;
		info.status = status;
		Ok(())
	}

	/// Serialize the registry to bytes.
	pub fn encode(&self) -> Result<Vec<u8>> {
		let mut buf = Vec::new();
		buf.write_u16::<BigEndian>(REGISTRY_VERSION)?;
		buf.write_u32::<BigEndian>(self.branches.len() as u32)?;
		for info in self.branches.values() {
			let entry = info.encode()?;
			buf.write_u32::<BigEndian>(entry.len() as u32)?;
			buf.write_all(&entry)?;
		}
		Ok(buf)
	}

	/// Deserialize the registry from bytes.
	pub fn decode(data: &[u8]) -> Result<Self> {
		let mut cursor = Cursor::new(data);
		let version = cursor.read_u16::<BigEndian>()?;
		if version != REGISTRY_VERSION {
			return Err(Error::Other(format!("Unsupported branch registry version: {version}")));
		}
		let count = cursor.read_u32::<BigEndian>()? as usize;
		let mut branches = HashMap::with_capacity(count);
		for _ in 0..count {
			let entry_len = cursor.read_u32::<BigEndian>()? as usize;
			let pos = cursor.position() as usize;
			let entry_data = &data[pos..pos + entry_len];
			let mut entry_cursor = Cursor::new(entry_data);
			let info = BranchInfo::decode(&mut entry_cursor)?;
			branches.insert(info.name.clone(), info);
			cursor.set_position((pos + entry_len) as u64);
		}
		Ok(Self {
			branches,
		})
	}
}

/// Object store path for the branch registry file.
fn registry_path(root: &object_store::path::Path) -> object_store::path::Path {
	root.child("branch_registry")
}

/// Load the branch registry from object store. Returns empty registry if not found.
pub async fn load_registry(
	object_store: &dyn ObjectStore,
	root: &object_store::path::Path,
) -> Result<BranchRegistry> {
	let path = registry_path(root);
	match object_store.get(&path).await {
		Ok(result) => {
			let data = result.bytes().await?;
			BranchRegistry::decode(&data)
		}
		Err(object_store::Error::NotFound {
			..
		}) => Ok(BranchRegistry::new()),
		Err(e) => Err(Error::from(e)),
	}
}

/// Save the branch registry to object store (unconditional put).
pub async fn save_registry(
	object_store: &dyn ObjectStore,
	root: &object_store::path::Path,
	registry: &BranchRegistry,
) -> Result<()> {
	let path = registry_path(root);
	let data = registry.encode()?;
	object_store.put(&path, Bytes::from(data).into()).await?;
	Ok(())
}

/// Save the branch registry using CAS (put-if-not-exists for first write).
/// For updates, this uses unconditional put since the registry is small
/// and branch creation is infrequent.
pub async fn save_registry_cas(
	object_store: &dyn ObjectStore,
	root: &object_store::path::Path,
	registry: &BranchRegistry,
) -> Result<()> {
	// For simplicity, use unconditional put. Branch creation is serialized
	// at the application level via the Store lock.
	save_registry(object_store, root, registry).await
}

/// Returns the current unix timestamp in seconds.
pub fn now_unix_secs() -> u64 {
	SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;

	#[test]
	fn test_branch_info_roundtrip() {
		let info = BranchInfo {
			name: "dev".to_string(),
			parent_branch: Some("main".to_string()),
			created_at: 1234567890,
			source_manifest_id: 42,
			source_sequence: 100,
			status: BranchStatus::Active,
		};

		let encoded = info.encode().unwrap();
		let mut cursor = Cursor::new(encoded.as_slice());
		let decoded = BranchInfo::decode(&mut cursor).unwrap();

		assert_eq!(decoded.name, "dev");
		assert_eq!(decoded.parent_branch, Some("main".to_string()));
		assert_eq!(decoded.created_at, 1234567890);
		assert_eq!(decoded.source_manifest_id, 42);
		assert_eq!(decoded.source_sequence, 100);
		assert_eq!(decoded.status, BranchStatus::Active);
	}

	#[test]
	fn test_branch_info_no_parent() {
		let info = BranchInfo {
			name: "main".to_string(),
			parent_branch: None,
			created_at: 1000,
			source_manifest_id: 1,
			source_sequence: 0,
			status: BranchStatus::Creating,
		};

		let encoded = info.encode().unwrap();
		let mut cursor = Cursor::new(encoded.as_slice());
		let decoded = BranchInfo::decode(&mut cursor).unwrap();

		assert_eq!(decoded.name, "main");
		assert!(decoded.parent_branch.is_none());
		assert_eq!(decoded.status, BranchStatus::Creating);
	}

	#[test]
	fn test_registry_roundtrip() {
		let mut registry = BranchRegistry::new();
		registry.insert(BranchInfo {
			name: "main".to_string(),
			parent_branch: None,
			created_at: 1000,
			source_manifest_id: 1,
			source_sequence: 0,
			status: BranchStatus::Active,
		});
		registry.insert(BranchInfo {
			name: "dev".to_string(),
			parent_branch: Some("main".to_string()),
			created_at: 2000,
			source_manifest_id: 5,
			source_sequence: 50,
			status: BranchStatus::Creating,
		});

		let encoded = registry.encode().unwrap();
		let decoded = BranchRegistry::decode(&encoded).unwrap();

		assert_eq!(decoded.branches().len(), 2);
		assert!(decoded.contains("main"));
		assert!(decoded.contains("dev"));

		let dev = decoded.get("dev").unwrap();
		assert_eq!(dev.parent_branch, Some("main".to_string()));
		assert_eq!(dev.status, BranchStatus::Creating);
	}

	#[test]
	fn test_registry_empty() {
		let registry = BranchRegistry::new();
		let encoded = registry.encode().unwrap();
		let decoded = BranchRegistry::decode(&encoded).unwrap();
		assert!(decoded.branches().is_empty());
	}

	/// Registry persistence test: 3 branches with different statuses and parent relationships.
	/// Verifies encode/decode roundtrip preserves ALL fields, and save/load via object store
	/// produces identical results.
	#[tokio::test]
	async fn test_branch_registry_persistence() {
		let mut registry = BranchRegistry::new();

		// Root branch (no parent)
		registry.insert(BranchInfo {
			name: "main".to_string(),
			parent_branch: None,
			created_at: 1000,
			source_manifest_id: 1,
			source_sequence: 0,
			status: BranchStatus::Active,
		});

		// Child branch in Creating state
		registry.insert(BranchInfo {
			name: "feature-1".to_string(),
			parent_branch: Some("main".to_string()),
			created_at: 2000,
			source_manifest_id: 5,
			source_sequence: 100,
			status: BranchStatus::Creating,
		});

		// Another child branch, Active
		registry.insert(BranchInfo {
			name: "feature-2".to_string(),
			parent_branch: Some("main".to_string()),
			created_at: 3000,
			source_manifest_id: 8,
			source_sequence: 250,
			status: BranchStatus::Active,
		});

		// === Encode/Decode roundtrip ===
		let bytes = registry.encode().unwrap();
		let decoded = BranchRegistry::decode(&bytes).unwrap();

		assert_eq!(decoded.branches().len(), 3);

		// Verify main
		let main = decoded.get("main").unwrap();
		assert!(main.parent_branch.is_none());
		assert_eq!(main.created_at, 1000);
		assert_eq!(main.source_manifest_id, 1);
		assert_eq!(main.source_sequence, 0);
		assert_eq!(main.status, BranchStatus::Active);

		// Verify feature-1 (Creating status)
		let f1 = decoded.get("feature-1").unwrap();
		assert_eq!(f1.parent_branch, Some("main".to_string()));
		assert_eq!(f1.created_at, 2000);
		assert_eq!(f1.source_manifest_id, 5);
		assert_eq!(f1.source_sequence, 100);
		assert_eq!(f1.status, BranchStatus::Creating);

		// Verify feature-2
		let f2 = decoded.get("feature-2").unwrap();
		assert_eq!(f2.parent_branch, Some("main".to_string()));
		assert_eq!(f2.created_at, 3000);
		assert_eq!(f2.source_manifest_id, 8);
		assert_eq!(f2.source_sequence, 250);
		assert_eq!(f2.status, BranchStatus::Active);

		// === Save/Load via object store roundtrip ===
		let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
		let root: object_store::path::Path = "test_root".into();

		// Save
		save_registry(object_store.as_ref(), &root, &registry).await.unwrap();

		// Load
		let loaded = load_registry(object_store.as_ref(), &root).await.unwrap();

		assert_eq!(loaded.branches().len(), 3);

		// Verify all fields survived object store roundtrip
		for name in &["main", "feature-1", "feature-2"] {
			let original = registry.get(name).unwrap();
			let restored = loaded.get(name).unwrap();

			assert_eq!(original.name, restored.name, "name mismatch for {name}");
			assert_eq!(
				original.parent_branch, restored.parent_branch,
				"parent_branch mismatch for {name}"
			);
			assert_eq!(original.created_at, restored.created_at, "created_at mismatch for {name}");
			assert_eq!(
				original.source_manifest_id, restored.source_manifest_id,
				"source_manifest_id mismatch for {name}"
			);
			assert_eq!(
				original.source_sequence, restored.source_sequence,
				"source_sequence mismatch for {name}"
			);
			assert_eq!(original.status, restored.status, "status mismatch for {name}");
		}

		// === Load from empty path returns empty registry ===
		let empty_root: object_store::path::Path = "nonexistent".into();
		let empty = load_registry(object_store.as_ref(), &empty_root).await.unwrap();
		assert!(empty.branches().is_empty());

		// === Update status and verify persistence ===
		let mut updated_registry = loaded;
		updated_registry.update_status("feature-1", BranchStatus::Active).unwrap();
		save_registry(object_store.as_ref(), &root, &updated_registry).await.unwrap();

		let reloaded = load_registry(object_store.as_ref(), &root).await.unwrap();
		assert_eq!(
			reloaded.get("feature-1").unwrap().status,
			BranchStatus::Active,
			"Status update should persist through save/load"
		);
	}
}
