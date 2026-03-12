use std::fs::File as SysFile;

/// Opens a file with the access mode required for `sync_all()` to work
/// on all platforms.
///
/// On Windows, `FlushFileBuffers` (called by `sync_all`) requires
/// `GENERIC_WRITE` — a read-only handle returns `ERROR_ACCESS_DENIED`.
/// On Unix, read-only access is sufficient for fsync.
pub fn open_for_sync<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<SysFile> {
	#[cfg(target_os = "windows")]
	{
		std::fs::OpenOptions::new().read(true).write(true).open(path)
	}
	#[cfg(not(target_os = "windows"))]
	{
		SysFile::open(path)
	}
}
