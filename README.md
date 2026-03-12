# SurrealOS

[![License](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/surrealos)

SurrealOS is a versioned, embedded key-value store built on an LSM (Log-Structured Merge) tree architecture, with support for time-travel queries.

It is designed specifically for use within SurrealDB, with the goal of reducing dependency on external storage engines (RocksDB). This approach allows the storage layer to evolve in alignment with SurrealDB's requirements and access patterns.

## Features

- **Snapshot Isolation**: MVCC support with non-blocking concurrent reads and writes
- **Durability Levels**: Immediate and Eventual durability modes
- **Time-Travel Queries**: Built-in versioning with point-in-time reads and historical queries
- **Checkpoint and Restore**: Create consistent snapshots for backup and recovery, including cloud checkpoints
- **Object Store Support**: Cloud-native storage via S3, GCS, Azure, or local filesystem
- **Distributed Fencing**: Writer/compactor epoch-based fencing for multi-node deployments
- **Beat/Bar Compaction**: TigerBeetle-inspired synchronous compaction pacing for predictable latency

## Quick Start

```rust
use surrealos::StoreBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new store using StoreBuilder
    let store = StoreBuilder::new()
        .with_path("path/to/db".into())
        .build()
        .await?;

    // Write a key-value pair
    store.set(b"hello", b"world").await?;

    // Read a key
    if let Some(value) = store.get(b"hello").await? {
        println!("Value: {:?}", value);
    }

    // Close the store
    store.close().await?;

    Ok(())
}
```

## Configuration

SurrealOS can be configured through various options when creating a new store:

### Basic Configuration

```rust
use surrealos::StoreBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = StoreBuilder::new()
        .with_path("path/to/db".into())           // Database directory path
        .with_max_memtable_size(100 * 1024 * 1024) // 100MB memtable size (default)
        .with_block_size(64 * 1024)                // 64KB block size (default)
        .with_level_count(6)                       // Number of levels in LSM tree (default)
        .with_block_cache_capacity(1 << 20)        // 1MB block cache (default)
        .build()
        .await?;

    Ok(())
}
```

**Options:**
- `with_path()` - Database directory where SSTables and WAL files are stored
- `with_max_memtable_size()` - Size threshold for memtable before flushing to SSTable (default: 100MB)
- `with_block_size()` - Size of data blocks in SSTables (default: 64KB)
- `with_level_count()` - Number of levels in the LSM tree structure (default: 6)
- `with_block_cache_capacity()` - Unified block cache capacity in bytes (default: 1MB)
- `with_index_partition_size()` - Partitioned index block size (default: 16KB)

### Compression Configuration

SurrealOS supports per-level compression for SSTable data blocks, allowing different compression algorithms for different LSM levels. By default, no compression is used.

```rust
use surrealos::{CompressionType, Options, StoreBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Default: No compression (for maximum write performance)
    let store = StoreBuilder::new()
        .with_path("path/to/db".into())
        .build()
        .await?;

    // Explicitly disable compression (same as default)
    let opts = Options::new()
        .with_path("path/to/db".into())
        .without_compression();

    let store = StoreBuilder::with_options(opts).build().await?;

    // Per-level compression configuration
    let opts = Options::new()
        .with_path("path/to/db".into())
        .with_compression_per_level(vec![
            CompressionType::None,               // L0: No compression for speed
            CompressionType::SnappyCompression,  // L1+: Snappy compression
        ]);

    let store = StoreBuilder::with_options(opts).build().await?;

    // Convenience: No compression on L0, Snappy on other levels
    let opts = Options::new()
        .with_path("path/to/db".into())
        .with_l0_no_compression();

    let store = StoreBuilder::with_options(opts).build().await?;

    Ok(())
}
```

**Options:**
- `without_compression()` - Disable compression for all levels (default behavior)
- `with_compression_per_level()` - Set compression type per level (vector index = level number)
- `with_l0_no_compression()` - Convenience method for no compression on L0, Snappy compression on other levels

**Compression Types:**
- `CompressionType::None` - No compression (fastest writes, largest files)
- `CompressionType::SnappyCompression` - Snappy compression (good balance of speed and compression ratio)

### Versioning Configuration

Enable time-travel queries to read historical versions of your data:

```rust
use surrealos::{Options, StoreBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Options::new()
        .with_path("path/to/db".into())
        .with_versioning(true, 0);  // Enable versioning, retention_ns = 0 means no limit

    let store = StoreBuilder::with_options(opts).build().await?;

    Ok(())
}
```

### Object Store Configuration

SurrealOS can store SSTables and manifests in cloud object stores:

```rust
use surrealos::{Options, StoreBuilder};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Options::new()
        .with_path("path/to/local/cache".into())
        .with_object_store(Arc::new(
            object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name("my-bucket")
                .build()?
        ))
        .with_object_store_root("my-db".into());

    let store = StoreBuilder::with_options(opts).build().await?;

    Ok(())
}
```

**Options:**
- `with_object_store()` - Set the object store backend (default: in-memory)
- `with_object_store_root()` - Root path prefix in the object store
- `with_local_sst_cache()` - Cache SSTs locally after upload (default: true)
- `with_max_sst_size()` - Max SST size for compaction output splits (default: 64MB)

### Distributed Fencing Configuration

For multi-node deployments, enable writer fencing to prevent split-brain:

```rust
use surrealos::{Options, StoreBuilder};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Options::new()
        .with_path("path/to/db".into())
        .with_leader_id(1)  // Non-zero enables fencing
        .with_manifest_update_timeout(Duration::from_secs(30));

    let store = StoreBuilder::with_options(opts).build().await?;

    Ok(())
}
```

## Store Operations

### Basic Operations

```rust
use surrealos::StoreBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = StoreBuilder::new()
        .with_path("path/to/db".into())
        .build()
        .await?;

    // Write operations
    store.set(b"foo1", b"bar1").await?;
    store.set(b"foo2", b"bar2").await?;

    // Read a key
    if let Some(value) = store.get(b"foo1").await? {
        println!("Value: {:?}", value);
    }

    // Delete a key
    store.delete(b"foo1").await?;

    Ok(())
}
```

### Batch Operations

Use batches to apply multiple writes atomically:

```rust
use surrealos::StoreBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = StoreBuilder::new()
        .with_path("path/to/db".into())
        .build()
        .await?;

    // Create and populate a batch
    let mut batch = store.new_batch();
    batch.set(b"key1", b"value1")?;
    batch.set(b"key2", b"value2")?;
    batch.delete(b"key3")?;

    // Apply atomically (sync = false for eventual durability)
    store.apply(batch, false).await?;

    // Apply with immediate durability (fsync before returning)
    let mut batch = store.new_batch();
    batch.set(b"key4", b"value4")?;
    store.apply(batch, true).await?;

    Ok(())
}
```

### Range Operations

Range operations use a cursor-based iterator API via snapshots:

```rust
use surrealos::{StoreBuilder, LSMIterator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = StoreBuilder::new()
        .with_path("path/to/db".into())
        .build()
        .await?;

    let snapshot = store.new_snapshot();

    // Range scan (inclusive lower, exclusive upper)
    let mut iter = snapshot.range(Some(b"key1".as_ref()), Some(b"key5".as_ref()))?;
    iter.seek_first().await?;
    while iter.valid() {
        let key_ref = iter.key();
        let user_key = key_ref.user_key();
        let value = iter.value()?;
        println!("{:?} = {:?}", user_key, value);
        iter.next().await?;
    }

    // Backward iteration
    let mut iter = snapshot.range(Some(b"key1".as_ref()), Some(b"key5".as_ref()))?;
    iter.seek_last().await?;
    while iter.valid() {
        let key_ref = iter.key();
        let value = iter.value()?;
        println!("{:?} = {:?}", key_ref.user_key(), value);
        iter.prev().await?;
    }

    Ok(())
}
```

### Durability Levels

Control durability guarantees via the `sync` parameter on `Store::apply()`:

```rust
use surrealos::StoreBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = StoreBuilder::new()
        .with_path("path/to/db".into())
        .build()
        .await?;

    let mut batch = store.new_batch();
    batch.set(b"key", b"value")?;

    // Eventual durability (sync = false) - faster, data written to OS buffer
    store.apply(batch, false).await?;

    let mut batch = store.new_batch();
    batch.set(b"key2", b"value2")?;

    // Immediate durability (sync = true) - slower, fsync before returning
    store.apply(batch, true).await?;

    Ok(())
}
```

**Durability Levels:**
- `sync = false` (Eventual): Commits are guaranteed to be persistent eventually. Data is written to the kernel buffer but not fsynced. This provides the best performance.
- `sync = true` (Immediate): Commits are guaranteed to be persistent as soon as `apply()` returns. Data is fsynced to disk. This is slower but provides the strongest durability guarantees.

## Time-Travel Queries

Time-travel queries allow you to read historical versions of your data at specific points in time.

### Enabling Versioning

```rust
use surrealos::{Options, StoreBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Options::new()
        .with_path("path/to/db".into())
        .with_versioning(true, 0);  // retention_ns = 0 means no retention limit

    let store = StoreBuilder::with_options(opts).build().await?;

    Ok(())
}
```

### Writing Versioned Data

```rust
// Write data with explicit timestamps
let mut batch = store.new_batch();
batch.set_at(b"key1", b"value_v1", 100)?;
store.apply(batch, false).await?;

// Update with a new version at a later timestamp
let mut batch = store.new_batch();
batch.set_at(b"key1", b"value_v2", 200)?;
store.apply(batch, false).await?;
```

### Point-in-Time Reads

Query data as it existed at a specific timestamp:

```rust
let snapshot = store.new_snapshot();

// Get value at specific timestamp
let value = snapshot.get_at(b"key1", 100).await?;
assert_eq!(value.unwrap().as_ref(), b"value_v1");

// Get value at later timestamp
let value = snapshot.get_at(b"key1", 200).await?;
assert_eq!(value.unwrap().as_ref(), b"value_v2");
```

### Retrieving All Versions

Use the `history_iter()` API to iterate over all historical versions of keys in a range:

```rust
use surrealos::LSMIterator;

let snapshot = store.new_snapshot();
let mut iter = snapshot.history_iter(
    Some(b"key1".as_ref()),  // lower bound (inclusive)
    Some(b"key2".as_ref()),  // upper bound (exclusive)
    false,                    // include_tombstones
    None,                     // ts_range filter
    None,                     // limit
)?;

iter.seek_first().await?;
while iter.valid() {
    let key_ref = iter.key();
    let user_key = key_ref.user_key();
    let timestamp = key_ref.timestamp();

    if key_ref.is_tombstone() {
        println!("Key {:?} deleted at timestamp {}", user_key, timestamp);
    } else {
        let value = iter.value()?;
        println!("Key {:?} = {:?} at timestamp {}", user_key, value, timestamp);
    }
    iter.next().await?;
}
```

With filtering options:

```rust
let snapshot = store.new_snapshot();
let mut iter = snapshot.history_iter(
    Some(b"key1".as_ref()),
    Some(b"key2".as_ref()),
    true,                       // include tombstones
    Some((100, 200)),           // only versions with timestamp in [100, 200]
    Some(100),                  // limit to 100 entries
)?;
```

## Checkpoint and Restore

Create consistent point-in-time snapshots of your database for backup and recovery.

### Creating Checkpoints

```rust
let store = StoreBuilder::new()
    .with_path("path/to/db".into())
    .build()
    .await?;

// Insert some data
store.set(b"key1", b"value1").await?;
store.set(b"key2", b"value2").await?;

// Create checkpoint
let checkpoint_dir = "path/to/checkpoint";
let metadata = store.create_checkpoint(&checkpoint_dir).await?;

println!("Checkpoint created at timestamp: {}", metadata.timestamp);
println!("Sequence number: {}", metadata.sequence_number);
println!("SSTable count: {}", metadata.sstable_count);
println!("Total size: {} bytes", metadata.total_size);
```

### Restoring from Checkpoint

```rust
// Restore database to checkpoint state
store.restore_from_checkpoint(&checkpoint_dir).await?;

// Data is now restored to the checkpoint state
// Any data written after checkpoint creation is discarded
```

### Cloud Checkpoints

SurrealOS supports cloud-native checkpoint operations when using an object store:

```rust
// Create a named checkpoint in the object store
let metadata = store.create_cloud_checkpoint("backup-2024-01-01").await?;

// Restore from a cloud checkpoint
let metadata = store.restore_cloud_checkpoint("backup-2024-01-01").await?;
```

**What's included in a checkpoint:**
- All SSTables from all levels
- Current WAL segments
- Level manifest
- Checkpoint metadata (binary format)

**Note:** Restoring from a checkpoint discards any pending writes in the active memtable and returns the database to the exact state when the checkpoint was created.

## Platform Compatibility

### Supported Platforms
- **Linux** (x86_64, aarch64): Full support including all features and tests
- **macOS** (x86_64, aarch64): Full support including all features and tests

### Not Supported
- **WebAssembly (WASM)**: Not supported due to fundamental incompatibilities:
  - Requires file system access not available in WASM environments
  - Write-Ahead Log (WAL) operations are not compatible
  - System-level I/O operations are not available

- **Windows** (x86_64): Basic functionality supported, but some features are limited:
  - File operations are not thread safe (TODO)
  - Some advanced file system operations may have reduced functionality
  - Performance may be lower compared to Unix-like systems

## History

SurrealOS has undergone a significant architectural evolution to address scalability challenges:

### Previous Design (VART-based)
The original implementation used a **versioned adaptive radix trie (VART)** architecture with the following components:

- **In-Memory Index**: Versioned adaptive radix trie using [vart](https://github.com/surrealdb/vart) for key-to-offset mappings
- **Sequential Log Storage**: Append-only storage divided into segments with binary record format
- **Memory Limitations**: The entire index had to reside in memory, limiting scalability for large datasets

**Why the Change?**
The VART-based design had fundamental scalability limitations:
- **Memory Constraint**: The entire index must fit in memory, making it unsuitable for datasets larger than available RAM
- **Recovery Overhead**: Startup required scanning all log segments to rebuild the in-memory index
- **Write Amplification**: Each update created new versions, leading to memory pressure

### Current Design (LSM Tree)
The new LSM (Log-Structured Merge) tree architecture provides:

- **Better Scalability**: Supports datasets much larger than available memory
- **Leveled Compaction**: Beat/bar pacing model for predictable, incremental compaction
- **Cloud-Native Storage**: Object store abstraction for S3, GCS, Azure backends
- **Distributed Coordination**: Writer/compactor fencing for multi-node deployments

This architectural change enables SurrealOS to handle larger than memory datasets.

For detailed architecture documentation, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## References

SurrealOS draws inspiration from design ideas used in [RocksDB](https://github.com/facebook/rocksdb), [Pebble](https://github.com/cockroachdb/pebble), and [TigerBeetle](https://tigerbeetle.com/).

## License

Licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.
