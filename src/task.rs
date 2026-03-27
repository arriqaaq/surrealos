use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::compaction::leveled::Strategy;
use crate::compaction::CompactionStrategy;
use crate::error::BackgroundErrorReason;
use crate::lsm::CompactionOperations;
use crate::stall::WriteStallController;
use crate::Options;

/// Manages background tasks for the LSM tree
pub(crate) struct TaskManager {
	/// Flag to signal tasks to stop
	stop_flag: Arc<AtomicBool>,

	/// Notification for memtable compaction task
	memtable_notify: Arc<Notify>,

	/// Notification for level compaction task
	level_notify: Arc<Notify>,

	/// Flag indicating if memtable compaction is running
	memtable_running: Arc<AtomicBool>,

	/// Flag indicating if level compaction is running
	level_running: Arc<AtomicBool>,

	/// Task handles for cleanup
	task_handles: Mutex<Option<Vec<tokio::task::JoinHandle<()>>>>,
}

impl fmt::Debug for TaskManager {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("TaskManager")
			.field("memtable_running", &self.memtable_running.load(Ordering::Acquire))
			.field("level_running", &self.level_running.load(Ordering::Acquire))
			.finish()
	}
}

impl TaskManager {
	pub(crate) fn new(
		core: Arc<dyn CompactionOperations>,
		opts: Arc<Options>,
		write_stall: Arc<WriteStallController>,
	) -> Self {
		let stop_flag = Arc::new(AtomicBool::new(false));
		let memtable_notify = Arc::new(Notify::new());
		let level_notify = Arc::new(Notify::new());
		let memtable_running = Arc::new(AtomicBool::new(false));
		let level_running = Arc::new(AtomicBool::new(false));
		let task_handles = Mutex::new(Some(Vec::new()));

		// Spawn memtable compaction task
		{
			let core = Arc::clone(&core);
			let stop_flag = Arc::clone(&stop_flag);
			let notify = Arc::clone(&memtable_notify);
			let running = Arc::clone(&memtable_running);
			let level_notify = Arc::clone(&level_notify);
			let write_stall = Arc::clone(&write_stall);

			let handle = tokio::spawn(async move {
				loop {
					// Wait for notification
					notify.notified().await;

					if stop_flag.load(Ordering::SeqCst) {
						break;
					}

					running.store(true, Ordering::SeqCst);
					log::debug!("Memtable flush task starting");

					// Flush ALL pending immutable memtables in a loop
					let mut flush_count = 0;
					loop {
						match core.compact_memtable().await {
							Ok(true) => {
								flush_count += 1;
								write_stall.signal_work_done();
								// Check if there are more immutables to flush
								if !core.has_pending_immutables() {
									break;
								}
							}
							Ok(false) => {
								// No more immutables to flush
								break;
							}
							Err(e) => {
								log::error!("Memtable compaction task error: {e:?}");
								core.error_handler()
									.set_error(e, BackgroundErrorReason::MemtableFlush);
								write_stall.signal_shutdown();
								break;
							}
						}
					}

					if flush_count > 0 {
						log::debug!(
							"Memtable flush task completed: flushed {} memtables",
							flush_count
						);
						// Trigger level compaction after successful flushes
						level_notify.notify_one();
					} else {
						log::debug!("Memtable flush task: no immutables to flush");
					}

					running.store(false, Ordering::SeqCst);
				}
			});
			task_handles.lock().unwrap().as_mut().unwrap().push(handle);
		}

		// Spawn level compaction task
		{
			let core = Arc::clone(&core);
			let stop_flag = Arc::clone(&stop_flag);
			let notify = Arc::clone(&level_notify);
			let running = Arc::clone(&level_running);
			let write_stall = Arc::clone(&write_stall);

			let handle = tokio::spawn(async move {
				loop {
					// Wait for notification
					notify.notified().await;

					if stop_flag.load(Ordering::SeqCst) {
						break;
					}

					running.store(true, Ordering::SeqCst);
					log::debug!("Level compaction task starting");

					// Use leveled compaction strategy
					let strategy: Arc<dyn CompactionStrategy> =
						Arc::new(Strategy::from_options(Arc::clone(&opts)));
					if let Err(e) = core.compact(strategy).await {
						log::error!("Level compaction task error: {e:?}");
						core.error_handler().set_error(e, BackgroundErrorReason::Compaction);
						write_stall.signal_shutdown();
					} else {
						log::debug!("Level compaction completed successfully");
						write_stall.signal_work_done();
					}
					running.store(false, Ordering::SeqCst);
				}
			});
			task_handles.lock().unwrap().as_mut().unwrap().push(handle);
		}

		Self {
			stop_flag,
			memtable_notify,
			level_notify,
			memtable_running,
			level_running,
			task_handles,
		}
	}

	pub(crate) fn wake_up_memtable(&self) {
		// Only notify if not already running
		if !self.memtable_running.load(Ordering::Acquire) {
			self.memtable_notify.notify_one();
		}
	}

	pub(crate) fn wake_up_level(&self) {
		// Only notify if not already running
		if !self.level_running.load(Ordering::Acquire) {
			self.level_notify.notify_one();
		}
	}

	pub async fn stop(&self) {
		// Set the stop flag to prevent new operations from starting
		self.stop_flag.store(true, Ordering::SeqCst);

		// Wake up any waiting tasks so they can check the stop flag and exit
		self.memtable_notify.notify_one();
		self.level_notify.notify_one();

		// Wait for any in-progress compactions to complete (no timeout - wait
		// indefinitely)
		while self.memtable_running.load(Ordering::Acquire)
			|| self.level_running.load(Ordering::Acquire)
		{
			// Yield to other tasks and wait a short time before checking again
			tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
		}

		// Now it's safe to wait for all tasks to complete
		let task_handles = self.task_handles.lock().unwrap().take().unwrap();
		for handle in task_handles {
			if let Err(e) = handle.await {
				log::error!("Error shutting down task: {e:?}");
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
	use std::sync::Arc;
	use std::time::Duration;

	use test_log::test;
	use tokio::time;

	use crate::compaction::CompactionStrategy;
	use crate::error::{BackgroundErrorHandler, Result};
	use crate::lsm::CompactionOperations;
	use crate::stall::{
		StallCounts,
		StallThresholds,
		WriteStallController,
		WriteStallCountProvider,
	};
	use crate::task::TaskManager;
	use crate::{Error, Options};

	struct NoopStallProvider;

	impl WriteStallCountProvider for NoopStallProvider {
		fn get_stall_counts(&self) -> StallCounts {
			StallCounts {
				immutable_memtables: 0,
				l0_files: 0,
			}
		}
	}

	fn test_write_stall() -> Arc<WriteStallController> {
		let provider: Arc<dyn WriteStallCountProvider> = Arc::new(NoopStallProvider);
		let thresholds = StallThresholds {
			memtable_limit: 2,
			l0_file_limit: 12,
		};
		Arc::new(WriteStallController::new(provider, thresholds))
	}

	// Mock CoreInner for testing
	struct MockCoreInner {
		memtable_compactions: Arc<AtomicUsize>,
		level_compactions: Arc<AtomicUsize>,
		// Add delay configuration to test different scenarios
		memtable_delay_ms: u64,
		level_delay_ms: u64,
		fail_memtable: Arc<AtomicBool>,
		fail_level: Arc<AtomicBool>,
	}

	impl MockCoreInner {
		fn new() -> Self {
			Self {
				memtable_compactions: Arc::new(AtomicUsize::new(0)),
				level_compactions: Arc::new(AtomicUsize::new(0)),
				memtable_delay_ms: 20,
				level_delay_ms: 20,
				fail_memtable: Arc::new(AtomicBool::new(false)),
				fail_level: Arc::new(AtomicBool::new(false)),
			}
		}

		fn with_delays(memtable_delay_ms: u64, level_delay_ms: u64) -> Self {
			Self {
				memtable_compactions: Arc::new(AtomicUsize::new(0)),
				level_compactions: Arc::new(AtomicUsize::new(0)),
				memtable_delay_ms,
				level_delay_ms,
				fail_memtable: Arc::new(AtomicBool::new(false)),
				fail_level: Arc::new(AtomicBool::new(false)),
			}
		}
	}

	#[async_trait::async_trait]
	impl CompactionOperations for MockCoreInner {
		async fn compact_memtable(&self) -> Result<bool> {
			let start = std::time::Instant::now();
			while start.elapsed() < Duration::from_millis(self.memtable_delay_ms) {
				std::hint::spin_loop();
			}

			// Check if should fail
			if self.fail_memtable.load(Ordering::SeqCst) {
				return Err(Error::Other("memtable error".into()));
			}

			// Only increment counter on success
			self.memtable_compactions.fetch_add(1, Ordering::SeqCst);
			Ok(true)
		}

		async fn compact(&self, _strategy: Arc<dyn CompactionStrategy>) -> Result<()> {
			let start = std::time::Instant::now();
			while start.elapsed() < Duration::from_millis(self.level_delay_ms) {
				std::hint::spin_loop();
			}

			// Check if should fail
			if self.fail_level.load(Ordering::SeqCst) {
				return Err(Error::Other("level error".into()));
			}

			// Only increment counter on success
			self.level_compactions.fetch_add(1, Ordering::SeqCst);
			Ok(())
		}

		fn error_handler(&self) -> Arc<BackgroundErrorHandler> {
			Arc::new(BackgroundErrorHandler::new())
		}

		fn has_pending_immutables(&self) -> bool {
			false // Mock always returns false (no immutables)
		}
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_wake_up_memtable() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await; // Allow time for task to complete

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 1);
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 1); // Level compaction should follow

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_multiple_wake_up_memtable() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		for _ in 0..3 {
			task_manager.wake_up_memtable();
			time::sleep(Duration::from_millis(100)).await;
		}

		// Wait for all operations to complete
		time::sleep(Duration::from_millis(300)).await;

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 3);
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 3);

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_wake_up_level() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		task_manager.wake_up_level();
		time::sleep(Duration::from_millis(100)).await;

		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 1);
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 0);

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_multiple_wake_up_level() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		for _ in 0..3 {
			task_manager.wake_up_level();
			time::sleep(Duration::from_millis(100)).await;
		}

		time::sleep(Duration::from_millis(300)).await;

		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 3);
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 0);

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_already_running_tasks() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::with_delays(100, 100));
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		// Wake up memtable and immediately try again while it's still running
		task_manager.wake_up_memtable();
		task_manager.wake_up_memtable(); // This should be ignored as task is already running

		time::sleep(Duration::from_millis(150)).await;

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 1);

		// Do the same test with level compaction
		time::sleep(Duration::from_millis(150)).await;

		task_manager.wake_up_level();
		task_manager.wake_up_level(); // This should be ignored

		time::sleep(Duration::from_millis(150)).await;

		// 1 from memtable + 1 directly triggered = 2
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 2);

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_error_handling() {
		let core = Arc::new(MockCoreInner::new());

		// First, check how the mock behaves with a direct call
		core.fail_memtable.store(true, Ordering::SeqCst);
		let direct_result = core.compact_memtable().await;
		assert!(direct_result.is_err(), "Should return an error when fail_memtable is true");

		// Reset counters
		core.memtable_compactions.store(0, Ordering::SeqCst);
		core.level_compactions.store(0, Ordering::SeqCst);

		let opts = Arc::new(Options::default());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		// Trigger memtable compaction that will fail
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		let memtable_count = core.memtable_compactions.load(Ordering::SeqCst);
		let level_count = core.level_compactions.load(Ordering::SeqCst);

		assert_eq!(memtable_count, 0);
		assert!(
			level_count == 0,
			"Level compaction was triggered after memtable failure. Expected 0, got {level_count}"
		);

		// Reset counters
		core.memtable_compactions.store(0, Ordering::SeqCst);
		core.level_compactions.store(0, Ordering::SeqCst);

		// Set memtable to succeed but level to fail
		core.fail_memtable.store(false, Ordering::SeqCst);
		core.fail_level.store(true, Ordering::SeqCst);

		// Trigger memtable compaction (which should succeed)
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		// Reset counters
		core.memtable_compactions.store(0, Ordering::SeqCst);
		core.level_compactions.store(0, Ordering::SeqCst);

		// Fix level compaction and try direct level compaction
		core.fail_level.store(false, Ordering::SeqCst);
		task_manager.wake_up_level();
		time::sleep(Duration::from_millis(100)).await;

		let level_count = core.level_compactions.load(Ordering::SeqCst);
		assert_eq!(level_count, 1);

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_task_from_fail() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		// Make first memtable compaction fail
		core.fail_memtable.store(true, Ordering::SeqCst);

		// Trigger a failing compaction
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 0);

		// Make next memtable compaction succeed
		core.fail_memtable.store(false, Ordering::SeqCst);

		// Trigger another compaction
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		// This should succeed
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 1);

		// Task should still be responsive after error
		task_manager.stop().await;
	}

	struct AboveThresholdProvider {
		l0_files: usize,
	}

	impl WriteStallCountProvider for AboveThresholdProvider {
		fn get_stall_counts(&self) -> StallCounts {
			StallCounts {
				immutable_memtables: 0,
				l0_files: self.l0_files,
			}
		}
	}

	fn stalled_write_stall(l0_files: usize) -> Arc<WriteStallController> {
		let provider: Arc<dyn WriteStallCountProvider> = Arc::new(AboveThresholdProvider {
			l0_files,
		});
		let thresholds = StallThresholds {
			memtable_limit: 2,
			l0_file_limit: 12,
		};
		Arc::new(WriteStallController::new(provider, thresholds))
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_stalled_writer_unblocked_on_level_compaction_failure() {
		let write_stall = stalled_write_stall(20);
		let core = Arc::new(MockCoreInner::new());
		core.fail_level.store(true, Ordering::SeqCst);

		let opts = Arc::new(Options::default());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			Arc::clone(&write_stall),
		);

		let stall_clone = Arc::clone(&write_stall);
		let writer_handle = tokio::spawn(async move { stall_clone.check().await });

		// Let the writer enter the stall loop
		time::sleep(Duration::from_millis(50)).await;

		// Trigger level compaction that will fail, which should call signal_shutdown()
		task_manager.wake_up_level();

		let result = time::timeout(Duration::from_secs(2), writer_handle).await;
		let writer_result = result
			.expect("Writer should not timeout (was it unblocked?)")
			.expect("Writer task should not panic");

		assert!(
			writer_result.is_err(),
			"Stalled writer should return Err(PipelineStall) after compaction failure"
		);

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_stalled_writer_unblocked_on_memtable_flush_failure() {
		let write_stall = stalled_write_stall(20);
		let core = Arc::new(MockCoreInner::new());
		core.fail_memtable.store(true, Ordering::SeqCst);

		let opts = Arc::new(Options::default());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			Arc::clone(&write_stall),
		);

		let stall_clone = Arc::clone(&write_stall);
		let writer_handle = tokio::spawn(async move { stall_clone.check().await });

		// Let the writer enter the stall loop
		time::sleep(Duration::from_millis(50)).await;

		// Trigger memtable flush that will fail, which should call signal_shutdown()
		task_manager.wake_up_memtable();

		let result = time::timeout(Duration::from_secs(2), writer_handle).await;
		let writer_result = result
			.expect("Writer should not timeout (was it unblocked?)")
			.expect("Writer task should not panic");

		assert!(
			writer_result.is_err(),
			"Stalled writer should return Err(PipelineStall) after flush failure"
		);

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_multiple_stalled_writers_unblocked_on_failure() {
		let write_stall = stalled_write_stall(20);
		let core = Arc::new(MockCoreInner::new());
		core.fail_level.store(true, Ordering::SeqCst);

		let opts = Arc::new(Options::default());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			Arc::clone(&write_stall),
		);

		let mut writer_handles = Vec::new();
		for _ in 0..5 {
			let stall_clone = Arc::clone(&write_stall);
			writer_handles.push(tokio::spawn(async move { stall_clone.check().await }));
		}

		// Let all writers enter the stall loop
		time::sleep(Duration::from_millis(50)).await;

		// Trigger level compaction failure — signal_shutdown uses notify_waiters
		// which must wake ALL 5 writers, not just one
		task_manager.wake_up_level();

		for (i, handle) in writer_handles.into_iter().enumerate() {
			let result = time::timeout(Duration::from_secs(2), handle).await;
			let writer_result = result
				.unwrap_or_else(|_| panic!("Writer {i} timed out (not unblocked)"))
				.unwrap_or_else(|e| panic!("Writer {i} panicked: {e}"));
			assert!(writer_result.is_err(), "Writer {i} should return Err(PipelineStall)");
		}

		task_manager.stop().await;
	}
}
