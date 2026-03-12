use std::sync::Arc;
use std::time::Duration;

use crate::cloud_store::CloudStore;
use crate::error::Error;
use crate::levels::LevelManifest;
use crate::manifest::{load_from_bytes, write_manifest_to_disk};
use crate::{Options, Result};

/// Tracks the local epoch claimed during fencing init.
///
/// After `init_writer()` or `init_compactor()`, this struct holds the epoch
/// that was successfully claimed via CAS. Before every manifest write,
/// call `check_fenced()` to verify no newer instance has taken over.
pub(crate) struct EpochManifest {
	local_writer_epoch: u64,
	local_compactor_epoch: u64,
}

impl EpochManifest {
	/// Creates a new unfenced instance (epoch 0 = no fencing active).
	/// Used when opening in single-node mode or before init is called.
	pub(crate) fn new_unfenced() -> Self {
		Self {
			local_writer_epoch: 0,
			local_compactor_epoch: 0,
		}
	}

	/// Initialize writer fencing by incrementing the writer epoch via CAS.
	///
	/// This claims exclusive write access by:
	/// 1. Reading the current manifest (local or remote)
	/// 2. Incrementing writer_epoch and setting leader_id
	/// 3. Writing the new manifest via CAS (put-if-not-exists)
	/// 4. On conflict (another writer won), refreshing and retrying
	///
	/// Returns the updated EpochManifest with the claimed epoch.
	pub(crate) async fn init_writer(
		manifest: &mut LevelManifest,
		leader_id: u128,
		table_store: &Arc<CloudStore>,
		opts: &Arc<Options>,
		timeout: Duration,
	) -> Result<Self> {
		let deadline = std::time::Instant::now() + timeout;

		loop {
			if std::time::Instant::now() > deadline {
				return Err(Error::Other(format!(
					"Manifest fencing init timed out after {:?}",
					timeout
				)));
			}

			// Increment epoch and set leader
			manifest.writer_epoch += 1;
			manifest.leader_id = leader_id;

			// Write to local disk and get serialized bytes
			let bytes = write_manifest_to_disk(manifest)?;

			// Attempt CAS write to object store
			match table_store
				.write_manifest_if_absent(manifest.manifest_id, bytes::Bytes::from(bytes))
				.await
			{
				Ok(()) => {
					log::info!(
						"Writer fencing established: epoch={}, leader_id={}, manifest_id={}",
						manifest.writer_epoch,
						manifest.leader_id,
						manifest.manifest_id
					);
					return Ok(Self {
						local_writer_epoch: manifest.writer_epoch,
						local_compactor_epoch: manifest.compactor_epoch,
					});
				}
				Err(Error::ManifestVersionExists) => {
					log::info!(
						"Manifest version {} already exists, refreshing from remote",
						manifest.manifest_id
					);
					// Another writer claimed this version — refresh from remote
					// Revert the local manifest_id increment (write_manifest_to_disk incremented
					// it)
					if let Some((remote_id, remote_bytes)) =
						table_store.read_latest_manifest().await?
					{
						let refreshed = load_from_bytes(
							&remote_bytes,
							manifest.manifest_dir.clone(),
							Arc::clone(opts),
							Arc::clone(table_store),
						)
						.await?;
						// Take the remote state but keep trying to claim
						manifest.manifest_id = refreshed.manifest_id;
						manifest.levels = refreshed.levels;
						manifest.hidden_set = refreshed.hidden_set;
						manifest.manifest_format_version = refreshed.manifest_format_version;
						manifest.snapshots = refreshed.snapshots;
						manifest.log_number = refreshed.log_number;
						manifest.last_sequence = refreshed.last_sequence;
						manifest.writer_epoch = refreshed.writer_epoch;
						manifest.compactor_epoch = refreshed.compactor_epoch;
						manifest.leader_id = refreshed.leader_id;

						log::info!(
							"Refreshed manifest from remote: id={}, writer_epoch={}",
							remote_id,
							manifest.writer_epoch
						);
					} else {
						// No remote manifest — this shouldn't happen if CAS failed,
						// but handle gracefully by retrying
						log::warn!("CAS failed but no remote manifest found, retrying");
					}
					continue;
				}
				Err(e) => return Err(e),
			}
		}
	}

	/// Claim the compactor epoch by incrementing it via CAS.
	/// Updates `self.local_compactor_epoch` on success.
	/// Called after `init_writer()` during startup when distributed fencing is active.
	pub(crate) async fn claim_compactor_epoch(
		&mut self,
		manifest: &mut LevelManifest,
		table_store: &Arc<CloudStore>,
		opts: &Arc<Options>,
		timeout: Duration,
	) -> Result<()> {
		let deadline = std::time::Instant::now() + timeout;

		loop {
			if std::time::Instant::now() > deadline {
				return Err(Error::Other(format!(
					"Compactor fencing init timed out after {:?}",
					timeout
				)));
			}

			manifest.compactor_epoch += 1;

			let bytes = write_manifest_to_disk(manifest)?;

			match table_store
				.write_manifest_if_absent(manifest.manifest_id, bytes::Bytes::from(bytes))
				.await
			{
				Ok(()) => {
					log::info!(
						"Compactor fencing established: epoch={}, manifest_id={}",
						manifest.compactor_epoch,
						manifest.manifest_id
					);
					self.local_compactor_epoch = manifest.compactor_epoch;
					return Ok(());
				}
				Err(Error::ManifestVersionExists) => {
					log::info!(
						"Manifest version {} already exists during compactor init, refreshing",
						manifest.manifest_id
					);
					if let Some((_remote_id, remote_bytes)) =
						table_store.read_latest_manifest().await?
					{
						let refreshed = load_from_bytes(
							&remote_bytes,
							manifest.manifest_dir.clone(),
							Arc::clone(opts),
							Arc::clone(table_store),
						)
						.await?;
						manifest.manifest_id = refreshed.manifest_id;
						manifest.levels = refreshed.levels;
						manifest.hidden_set = refreshed.hidden_set;
						manifest.manifest_format_version = refreshed.manifest_format_version;
						manifest.snapshots = refreshed.snapshots;
						manifest.log_number = refreshed.log_number;
						manifest.last_sequence = refreshed.last_sequence;
						manifest.writer_epoch = refreshed.writer_epoch;
						manifest.compactor_epoch = refreshed.compactor_epoch;
						manifest.leader_id = refreshed.leader_id;
					}
					continue;
				}
				Err(e) => return Err(e),
			}
		}
	}

	/// Check if the writer has been fenced by a newer instance.
	/// Call this before every manifest write under the manifest write lock.
	pub(crate) fn check_writer_fenced(&self, manifest: &LevelManifest) -> Result<()> {
		if manifest.writer_epoch > self.local_writer_epoch {
			return Err(Error::Fenced);
		}
		Ok(())
	}

	/// Returns the local compactor epoch claimed during init.
	pub(crate) fn compactor_epoch(&self) -> u64 {
		self.local_compactor_epoch
	}
}
