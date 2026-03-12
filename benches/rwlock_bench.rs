//! Benchmark: std::sync::RwLock vs parking_lot::RwLock vs tokio::sync::RwLock
//!
//! Pure lock acquire/release overhead. No domain logic.
//! Run: cargo bench --bench rwlock_bench

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

// ─── Single-thread: read lock acquire + release ───

fn bench_read_lock(c: &mut Criterion) {
	let mut group = c.benchmark_group("read_lock");

	group.bench_function("std", |b| {
		let lock = std::sync::RwLock::new(42u64);
		b.iter(|| {
			let guard = lock.read().unwrap();
			std::hint::black_box(*guard);
		});
	});

	group.bench_function("parking_lot", |b| {
		let lock = parking_lot::RwLock::new(42u64);
		b.iter(|| {
			let guard = lock.read();
			std::hint::black_box(*guard);
		});
	});

	group.bench_function("tokio", |b| {
		let rt = tokio::runtime::Runtime::new().unwrap();
		let lock = tokio::sync::RwLock::new(42u64);
		b.iter(|| {
			rt.block_on(async {
				let guard = lock.read().await;
				std::hint::black_box(*guard);
			});
		});
	});

	group.finish();
}

// ─── Single-thread: write lock acquire + release ───

fn bench_write_lock(c: &mut Criterion) {
	let mut group = c.benchmark_group("write_lock");

	group.bench_function("std", |b| {
		let lock = std::sync::RwLock::new(0u64);
		b.iter(|| {
			let mut guard = lock.write().unwrap();
			*guard += 1;
			std::hint::black_box(&*guard);
		});
	});

	group.bench_function("parking_lot", |b| {
		let lock = parking_lot::RwLock::new(0u64);
		b.iter(|| {
			let mut guard = lock.write();
			*guard += 1;
			std::hint::black_box(&*guard);
		});
	});

	group.bench_function("tokio", |b| {
		let rt = tokio::runtime::Runtime::new().unwrap();
		let lock = tokio::sync::RwLock::new(0u64);
		b.iter(|| {
			rt.block_on(async {
				let mut guard = lock.write().await;
				*guard += 1;
				std::hint::black_box(&*guard);
			});
		});
	});

	group.finish();
}

// ─── Concurrent: N readers ───

fn bench_concurrent_reads(c: &mut Criterion) {
	let mut group = c.benchmark_group("concurrent_reads");

	for n in [4, 8] {
		group.bench_with_input(BenchmarkId::new("std", n), &n, |b, &n| {
			let rt = tokio::runtime::Runtime::new().unwrap();
			let lock = Arc::new(std::sync::RwLock::new(42u64));
			b.iter(|| {
				rt.block_on(async {
					let mut handles = Vec::new();
					for _ in 0..n {
						let lock = Arc::clone(&lock);
						handles.push(tokio::spawn(async move {
							for _ in 0..1000 {
								let g = lock.read().unwrap();
								std::hint::black_box(*g);
							}
						}));
					}
					for h in handles {
						h.await.unwrap();
					}
				});
			});
		});

		group.bench_with_input(BenchmarkId::new("parking_lot", n), &n, |b, &n| {
			let rt = tokio::runtime::Runtime::new().unwrap();
			let lock = Arc::new(parking_lot::RwLock::new(42u64));
			b.iter(|| {
				rt.block_on(async {
					let mut handles = Vec::new();
					for _ in 0..n {
						let lock = Arc::clone(&lock);
						handles.push(tokio::spawn(async move {
							for _ in 0..1000 {
								let g = lock.read();
								std::hint::black_box(*g);
							}
						}));
					}
					for h in handles {
						h.await.unwrap();
					}
				});
			});
		});

		group.bench_with_input(BenchmarkId::new("tokio", n), &n, |b, &n| {
			let rt = tokio::runtime::Runtime::new().unwrap();
			let lock = Arc::new(tokio::sync::RwLock::new(42u64));
			b.iter(|| {
				rt.block_on(async {
					let mut handles = Vec::new();
					for _ in 0..n {
						let lock = Arc::clone(&lock);
						handles.push(tokio::spawn(async move {
							for _ in 0..1000 {
								let g = lock.read().await;
								std::hint::black_box(*g);
							}
						}));
					}
					for h in handles {
						h.await.unwrap();
					}
				});
			});
		});
	}

	group.finish();
}

// ─── Concurrent: N readers + 1 writer ───

fn bench_mixed(c: &mut Criterion) {
	let mut group = c.benchmark_group("mixed_rw");

	for n in [4, 8] {
		group.bench_with_input(BenchmarkId::new("std", n), &n, |b, &n| {
			let rt = tokio::runtime::Runtime::new().unwrap();
			let lock = Arc::new(std::sync::RwLock::new(0u64));
			b.iter(|| {
				rt.block_on(async {
					let mut handles = Vec::new();
					for _ in 0..n {
						let lock = Arc::clone(&lock);
						handles.push(tokio::spawn(async move {
							for _ in 0..1000 {
								let g = lock.read().unwrap();
								std::hint::black_box(*g);
							}
						}));
					}
					let lock = Arc::clone(&lock);
					handles.push(tokio::spawn(async move {
						for i in 0..1000u64 {
							let mut g = lock.write().unwrap();
							*g = i;
						}
					}));
					for h in handles {
						h.await.unwrap();
					}
				});
			});
		});

		group.bench_with_input(BenchmarkId::new("parking_lot", n), &n, |b, &n| {
			let rt = tokio::runtime::Runtime::new().unwrap();
			let lock = Arc::new(parking_lot::RwLock::new(0u64));
			b.iter(|| {
				rt.block_on(async {
					let mut handles = Vec::new();
					for _ in 0..n {
						let lock = Arc::clone(&lock);
						handles.push(tokio::spawn(async move {
							for _ in 0..1000 {
								let g = lock.read();
								std::hint::black_box(*g);
							}
						}));
					}
					let lock = Arc::clone(&lock);
					handles.push(tokio::spawn(async move {
						for i in 0..1000u64 {
							let mut g = lock.write();
							*g = i;
						}
					}));
					for h in handles {
						h.await.unwrap();
					}
				});
			});
		});

		group.bench_with_input(BenchmarkId::new("tokio", n), &n, |b, &n| {
			let rt = tokio::runtime::Runtime::new().unwrap();
			let lock = Arc::new(tokio::sync::RwLock::new(0u64));
			b.iter(|| {
				rt.block_on(async {
					let mut handles = Vec::new();
					for _ in 0..n {
						let lock = Arc::clone(&lock);
						handles.push(tokio::spawn(async move {
							for _ in 0..1000 {
								let g = lock.read().await;
								std::hint::black_box(*g);
							}
						}));
					}
					let lock = Arc::clone(&lock);
					handles.push(tokio::spawn(async move {
						for i in 0..1000u64 {
							let mut g = lock.write().await;
							*g = i;
						}
					}));
					for h in handles {
						h.await.unwrap();
					}
				});
			});
		});
	}

	group.finish();
}

criterion_group!(benches, bench_read_lock, bench_write_lock, bench_concurrent_reads, bench_mixed,);
criterion_main!(benches);
