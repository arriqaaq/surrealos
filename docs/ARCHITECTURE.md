# SurrealOS Architecture

SurrealOS is a versioned, embedded key-value database built on an LSM (Log-Structured Merge) tree architecture. It is designed specifically for use within SurrealDB, with the goal of reducing dependency on external storage engines (RocksDB). This approach allows the storage layer to evolve in alignment with SurrealDB's requirements and access patterns.

This document provides a comprehensive overview of the internal design, data flow, and key subsystems.

## Table of Contents

1. [Overall Architecture](#overall-architecture)
2. [Write Path](#write-path)
3. [Read Path](#read-path)
4. [Versioning and MVCC](#versioning-and-mvcc)
5. [Compaction](#compaction)
6. [Object Store and Cloud Storage](#object-store-and-cloud-storage)
7. [Fencing (Distributed Coordination)](#fencing-distributed-coordination)
8. [Recovery](#recovery)
9. [Checkpoint and Restore](#checkpoint-and-restore)

---

## Overall Architecture

SurrealOS is organized into four main layers: the Client API, Core Components, Storage Layer, and Background Tasks.

### Data Flow

Data flows through the layers as follows:

1. **Writes**: Client -> Store.set()/apply(Batch) -> CommitPipeline -> WAL + MemTable -> Compaction Beat -> Flush -> SSTables -> Object Store
2. **Reads**: Client -> Snapshot -> MemTable(s) -> SSTables (via TableStore + BlockCache)
3. **Compaction**: Synchronous beat/bar pacing from commit path, merges SSTables, uploads to object store

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT LAYER                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │    Store     │  │    Batch     │  │   Snapshot   │  │ StoreBuilder │    │
│  │  (Tree)     │  │  (buffered   │  │ (consistent  │  │  (builder    │    │
│  │             │  │   writes)    │  │   reads)     │  │   pattern)   │    │
│  │ get()       │  │ set()        │  │ get()        │  │              │    │
│  │ set()       │  │ set_at()     │  │ range()      │  │              │    │
│  │ delete()    │  │ delete()     │  │ history_iter()│  │              │    │
│  │ apply()     │  │              │  │ get_at()     │  │              │    │
│  │ new_batch() │  │              │  │              │  │              │    │
│  │ new_snapshot│  │              │  │              │  │              │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                              CORE COMPONENTS                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │   Commit     │  │   Active     │  │  Immutable   │  │  Compaction  │    │
│  │   Pipeline   │  │  MemTable    │  │  MemTables   │  │  Scheduler   │    │
│  │ (lock-free)  │  │  (skiplist)  │  │   (queue)    │  │  (beat/bar)  │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
│         │                 │                 │                 │            │
│         └─────────────────┼─────────────────┼─────────────────┘             │
│                           ▼                 ▼                               │
│                    ┌──────────────────────────────────────┐                │
│                    │           Snapshot                   │                │
│                    │   (point-in-time consistent view)    │                │
│                    └──────────────────────────────────────┘                │
└────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                              STORAGE LAYER                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │     WAL      │  │   SSTables   │  │  TableStore  │  │   Manifest   │    │
│  │  (durability)│  │   (L0..Ln)   │  │ (object store│  │   (levels)   │    │
│  │  32KB blocks │  │  sorted runs │  │  abstraction)│  │  table meta  │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
│                                      ┌──────────────┐  ┌──────────────┐    │
│                                      │  Block Cache │  │   Fencing    │    │
│                                      │  (data+index)│  │  (epochs)    │    │
│                                      └──────────────┘  └──────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                            BACKGROUND TASKS                                │
│  ┌──────────────────┐  ┌───────────────────┐  ┌──────────────────┐         │
│  │  MemTable Flush  │  │ Manifest Upload   │  │   Orphan SST    │         │
│  │  (imm -> L0)     │  │ (async to object  │  │   GC Cleanup    │         │
│  │                  │  │  store)            │  │                 │         │
│  └──────────────────┘  └───────────────────┘  └──────────────────┘         │
└────────────────────────────────────────────────────────────────────────────┘
```

### Layer Details

**Client Layer**: The `Store` struct (aliased as `Tree`) is the main entry point, providing methods for direct key-value operations (`get`, `set`, `delete`), batch writes (`new_batch`, `apply`), snapshots (`new_snapshot`), and database lifecycle management (`close`, `create_checkpoint`). The `Batch` struct buffers multiple write operations for atomic application. `Snapshot` provides a consistent point-in-time view for reads.

**Core Components**: These in-memory structures coordinate between the client API and persistent storage:
- The **CommitPipeline** queues pending commits and ensures WAL writes are serialized while allowing concurrent memtable updates. Sequence numbers are assigned atomically within the pipeline.
- The **MemTable** is a lock-free skip list that holds recent writes before they are flushed to disk
- **Snapshots** capture the visible sequence number at creation time, providing isolation from concurrent commits
- The **CompactionScheduler** drives synchronous compaction from the commit path using a beat/bar pacing model

**Storage Layer**: All durable state lives here:
- The **WAL** ensures committed data survives crashes by persisting entries before they become visible
- **SSTables** are immutable sorted files organized into levels, with Level 0 containing recently flushed data
- The **TableStore** is the central abstraction for SST lifecycle over an object store (local, S3, GCS, Azure)
- The **Manifest** tracks which SSTables exist at each level and their key ranges
- The **Block Cache** is a unified LRU cache for data blocks and index blocks
- **Fencing** tracks writer/compactor epochs for distributed coordination

**Background Tasks**: These run asynchronously to maintain system health:
- **Flush** converts immutable memtables into Level 0 SSTables
- **Manifest Upload** asynchronously uploads manifest snapshots to the object store
- **Orphan SST GC** removes local SST files not referenced by the manifest

### Component Overview

| Component | Purpose | Key Files |
|-----------|---------|-----------|
| **Store / Tree** | Public API for database operations | `src/lsm.rs` |
| **StoreBuilder** | Builder pattern for Store configuration | `src/lsm.rs` |
| **Batch** | Atomic batch of write operations | `src/batch.rs` |
| **Snapshot** | Point-in-time consistent view | `src/snapshot.rs` |
| **CommitPipeline** | Lock-free commit queue (inspired by Pebble) | `src/commit.rs` |
| **MemTable** | In-memory skip list for recent writes | `src/memtable/` |
| **WAL** | Write-ahead log for durability | `src/wal/` |
| **SSTable** | Sorted string table on disk | `src/sstable/` |
| **TableStore** | SST lifecycle via object store | `src/tablestore.rs` |
| **LevelManifest** | Level metadata and SSTable tracking | `src/levels/` |
| **Manifest I/O** | Manifest serialization and disk persistence | `src/manifest.rs` |
| **CompactionScheduler** | Beat/bar compaction pacing | `src/compaction/` |
| **Block Cache** | Unified data + index block cache | `src/cache.rs` |
| **FenceableManifest** | Distributed writer/compactor fencing | `src/fencing.rs` |
| **Compression** | Per-level Snappy compression | `src/compression.rs` |
| **GC** | Local orphan SST cleanup | `src/gc.rs` |
| **VFS** | Platform-abstracted file sync | `src/vfs.rs` |
| **PathResolver** | Object store path resolution | `src/paths.rs` |
| **Clock** | Logical clock for timestamp assignment | `src/clock.rs` |

### Directory Structure

```
database_path/
├── wal/                    # Write-ahead log segments
│   ├── 00000000000000000001.wal
│   └── 00000000000000000002.wal
├── sst/                    # SSTable files (ULID-based names)
│   ├── 01ARZ3NDEKTSV4RRFFQ.sst
│   └── 01ARZ3NDEKTSV4RRFFR.sst
├── manifest/               # Level manifest files (sequential IDs)
│   └── 00000000000000000001.manifest
└── LOCK                    # Lock file
```

---

## Write Path

This section covers how data flows from a `set()` or `apply()` call through to durable storage.

### Overview

A write operation proceeds through three main phases:

1. **Batch Phase**: The application creates a `Batch` (via `Store::new_batch()`) and buffers writes with `set()`, `set_at()`, or `delete()`. Alternatively, convenience methods like `Store::set()` create single-entry batches internally. No locks are held, no disk I/O occurs.

2. **Commit Phase**: When the application calls `Store::apply(batch, sync)`, the batch enters the commit pipeline. The pipeline assigns sequence numbers, writes to WAL, applies to the memtable, publishes visibility, and advances one compaction beat.

3. **Flush Phase**: Asynchronously, full memtables are rotated and flushed to Level 0 SSTables, which are uploaded to the object store.

The design minimizes contention during concurrent writes. The WAL write is kept as short as possible, while memtable insertion and visibility publishing can proceed in parallel.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                               BATCH PHASE                                  │
│                                                                            │
│  store.set(key, value)  ──►  Creates single-entry Batch internally         │
│        - OR -                                                              │
│  let mut batch = store.new_batch();                                        │
│  batch.set(key, value);      Buffer in Batch.entries (Vec<BatchEntry>)     │
│  batch.delete(key2);                                                       │
│  store.apply(batch, sync);                                                 │
└────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                         COMMIT PIPELINE (Serialized)                       │
│                                                                            │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │ Acquire Write   │───►│  Assign SeqNums │───►│   Write to WAL  │         │
│  │     Mutex       │    │  (atomic inc)   │    │  (durability)   │         │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘         │
│                                                        │                   │
│                                                        ▼                   │
│                    ┌─────────────────────────────┐                         │
│                    │   Apply to Active MemTable  │                         │
│                    │       (skip list insert)    │                         │
│                    └─────────────────────────────┘                         │
│                                  │                                         │
│                                  ▼                                         │
│                    ┌─────────────────────────────┐                         │
│                    │  Publish Visibility         │                         │
│                    │  (update visible_seq_num)   │                         │
│                    └─────────────────────────────┘                         │
│                                  │                                         │
│                                  ▼                                         │
│                    ┌─────────────────────────────┐                         │
│                    │  Advance Compaction Beat    │                         │
│                    │  (synchronous compaction)   │                         │
│                    └─────────────────────────────┘                         │
└────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                                FLUSH PHASE                                 │
│                                                                            │
│  ┌─────────────────────────┐    ┌─────────────────────────────────────┐    │
│  │  MemTable Full?         │───►│  Rotate: Active → Immutable Queue   │    │
│  │  (size > 100MB default) │Yes │  Create new empty Active MemTable   │    │
│  └─────────────────────────┘    └─────────────────────────────────────┘    │
│                                              │                             │
│                                              ▼                             │
│                                 ┌─────────────────────────────────────┐    │
│                                 │  Flush Immutable to L0 SSTable      │    │
│                                 │  Upload SST to Object Store         │    │
│                                 └─────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘
```

### Key Implementation Details

**Batch API**

All writes are expressed as `Batch` operations. A `Batch` contains a `Vec<BatchEntry>`, where each entry has a `kind` (Set or Delete), `key`, optional `value`, and `timestamp`. Batches are created via `Store::new_batch()` and applied atomically via `Store::apply(batch, sync)`.

Convenience methods `Store::set()`, `Store::set_at()`, and `Store::delete()` create single-entry batches internally and call `apply()`.

**Commit Pipeline**

The commit pipeline is inspired by [Pebble's commit design](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md#commit-pipeline). The key insight is that while WAL writes must be serialized (to maintain a consistent log order), other commit steps can proceed concurrently.

The pipeline operates in four stages:
1. **Write**: Acquire the write mutex, assign sequence numbers, append entries to WAL, release mutex
2. **Apply**: Insert entries into the active memtable (lock-free skip list)
3. **Publish**: Update `visible_seq_num` to make entries visible to readers
4. **Compact**: Call `CompactionScheduler::compact_beat()` to advance one beat of compaction work

A semaphore limits concurrent commits (default: 8) to prevent memory exhaustion from too many in-flight batches.

**Durability Modes**

SurrealOS offers two durability levels, controlled by the `sync` parameter on `Store::apply(batch, sync)`:
- **Eventual** (`sync = false`): Data is written to the OS buffer cache but not fsynced. This is fast but data may be lost if the system crashes before the OS flushes to disk.
- **Immediate** (`sync = true`): Each commit calls fsync before returning. This guarantees durability but reduces throughput.

**Sequence Numbers**

Every entry receives a monotonically increasing 56-bit sequence number (the upper 56 bits of the 64-bit trailer). Sequence numbers are assigned atomically within the commit pipeline. They serve two purposes:
- **MVCC visibility**: Readers with snapshot seq=N see only entries with seq <= N
- **Ordering**: During compaction, newer entries supersede older ones with the same key

**MemTable Rotation**

When the active memtable exceeds `max_memtable_size` (default: 100MB), it is rotated:
1. The current memtable is marked immutable and added to a flush queue
2. A new empty memtable becomes the active one, with a new WAL segment
3. The immutable memtable is flushed to a Level 0 SSTable

Lock ordering during rotation: `active_memtable` → `level_manifest` → `immutable_memtables`.

---

## Read Path

The read path describes how data is retrieved from a snapshot, traversing all storage layers.

### Overview

In an LSM tree, reads must check multiple locations because the same key may exist in several places simultaneously:

- The active memtable (most recent committed writes)
- One or more immutable memtables (pending flush)
- Level 0 SSTables (recently flushed, may overlap)
- Level 1+ SSTables (compacted, non-overlapping)

The search proceeds from newest to oldest, stopping at the first match. This ensures that the most recent version of a key is always returned.

### Read Amplification

**Read amplification** measures how many locations must be checked to find a key. In the worst case (key doesn't exist), a read may check all levels. LSM trees trade write performance for read performance: writes are fast (sequential appends) but reads may require multiple lookups.

SurrealOS minimizes read amplification through:
- **Bloom filters**: Quickly eliminate SSTables that definitely don't contain a key
- **Block cache**: Keep frequently accessed data blocks and index blocks in memory
- **Non-overlapping levels**: L1+ SSTables don't overlap, enabling binary search

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SNAPSHOT READ                                     │
│                                                                             │
│  snapshot = store.new_snapshot()                                            │
│  snapshot.get(key)                                                          │
│                            │                                                │
│                            ▼                                                │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ Search Order (stop on first match with seq_num <= snapshot.seq_num): │   │
│  │                                                                      │   │
│  │   1. Active MemTable (newest writes, in-memory skiplist)             │   │
│  │              │                                                       │   │
│  │              ▼                                                       │   │
│  │   2. Immutable MemTables (newest first, pending flush)               │   │
│  │              │                                                       │   │
│  │              ▼                                                       │   │
│  │   3. Level 0 SSTables (ALL tables, may overlap)                      │   │
│  │              │                                                       │   │
│  │              ▼                                                       │   │
│  │   4. Level 1...N SSTables (binary search, non-overlapping)           │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SSTABLE LOOKUP DETAIL                               │
│                                                                             │
│  For each SSTable candidate:                                                │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐          │
│  │  Bloom Filter   │───►│  Index Block    │───►│  Data Block     │          │
│  │  (may_contain?) │ Yes│  (binary search)│    │  (with cache)   │          │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘          │
│         │ No                                                                │
│         ▼                                                                   │
│    Skip to next SSTable                                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Implementation Details

**Snapshot Isolation**

When a snapshot is created via `Store::new_snapshot()`, it captures the current `visible_seq_num`. All reads within that snapshot see a consistent view of the database as of that moment:
- Entries with `seq_num <= snapshot.seq_num` are visible
- Entries with `seq_num > snapshot.seq_num` are invisible (committed after the snapshot was created)

This provides **repeatable reads**: the same query will return the same results for the lifetime of the snapshot, regardless of concurrent commits.

**Level 0 vs Level 1+**

Level 0 is special because it contains recently flushed memtables whose key ranges may overlap. When searching L0, **all** SSTables must be checked because any of them might contain the target key.

Levels 1 and above maintain a **sorted, non-overlapping** invariant: each SSTable covers a distinct key range. This enables binary search to find the single SSTable that might contain a key, reducing read amplification for deeper levels.

**Bloom Filters**

Each SSTable includes a Bloom filter, a probabilistic data structure that can definitively say "key is NOT in this table" or "key MIGHT be in this table." With a 1% false positive rate (tunable via bits-per-key, default 10), Bloom filters eliminate ~99% of unnecessary SSTable reads.

The filter is loaded into memory when the SSTable is opened, so checking it is extremely fast compared to reading from disk.

**Block Cache**

SSTable data is organized into blocks (default: 64KB). When a block is read from the object store, it is cached in a unified LRU cache that holds both data blocks and index blocks. Subsequent reads for keys in the same block are served from memory.

The cache uses a weighted LRU policy with a default capacity of 1MB (configurable via `with_block_cache_capacity`).

---

## Versioning and MVCC

SurrealOS implements Multi-Version Concurrency Control (MVCC) with snapshot isolation for concurrent reads and writes. MVCC stores multiple versions of each key, allowing readers and writers to operate concurrently without blocking each other.

### Why MVCC?

MVCC provides the following:

1. **Non-blocking reads**: Readers never block writers, and writers never block readers. Each snapshot sees a consistent view.

2. **Time-travel queries**: Applications can query historical data at specific timestamps, enabling auditing, debugging, and undo functionality.

3. **Isolation guarantees**: Each snapshot sees a consistent view of the database, unaffected by concurrent modifications.

### InternalKey Structure

Every key stored in SurrealOS is an `InternalKey` that combines the user-provided key with version metadata. This encoding is central to how MVCC works:

```
┌──────────────────┬───────────────────┬──────────────────┐
│    user_key      │    trailer (8B)   │  timestamp (8B)  │
│   (variable)     │  seq_num | kind   │   nanoseconds    │
└──────────────────┴───────────────────┴──────────────────┘

trailer = (seq_num << 8) | kind

kind values:
  0 = Delete (tombstone)
  1 = Set (normal write)
  2 = Merge
  3 = LogData
  4 = RangeDelete
  5 = Separator
  24 = Max (internal sentinel)
```

The **trailer** packs the sequence number and operation kind into 8 bytes. The sequence number occupies the upper 56 bits, and the kind occupies the lower 8 bits. This encoding allows efficient comparison: keys are sorted first by user key, then by sequence number (descending), so the newest version appears first.

The **timestamp** (optional, enabled with versioning) stores the system-assigned or application-provided timestamp in nanoseconds. This enables point-in-time queries using actual wall-clock times rather than internal sequence numbers.

**Operation Kinds:**

- **Delete (0)**: A tombstone that marks the key as deleted. During compaction at the bottom level, this tombstone and all older versions can be dropped.
- **Set (1)**: A normal write that creates a new version of the key.
- **Merge (2)**: Reserved for merge operations.
- **RangeDelete (4)**: Deletes a range of keys.

### Snapshot Isolation

Snapshot isolation ensures that each snapshot sees a consistent view of the database. When `Store::new_snapshot()` is called, it captures the current `visible_seq_num`. All operations within that snapshot see exactly the state as of that moment.

```
Timeline:
─────────────────────────────────────────────────────────────►
     │           │           │           │
  seq=1       seq=2       seq=3       seq=4
  Set(A,1)    Set(A,2)    Set(A,3)    Delete(A)
     │           │           │           │
     │           │           │           │
     ▼           ▼           ▼           ▼
┌─────────────────────────────────────────────────────────┐
│ Snapshot at seq=2 sees: A=2                             │
│ Snapshot at seq=3 sees: A=3                             │
│ Snapshot at seq=4 sees: A deleted (tombstone)           │
└─────────────────────────────────────────────────────────┘
```

**How visibility works:**

When reading key A with snapshot seq=2:
1. Find all versions of A: (seq=4, Delete), (seq=3, Set), (seq=2, Set), (seq=1, Set)
2. Filter to versions with seq <= 2: (seq=2, Set), (seq=1, Set)
3. Return the newest visible version: seq=2, value=2

### Version Retention

By default, compaction drops old versions as soon as they are no longer visible to any active snapshot. When versioning is enabled (`with_versioning(true, retention_ns)`), the behavior changes:

- **All versions are preserved** during compaction, not just the latest
- **Retention period**: Versions older than `retention_ns` nanoseconds may be garbage collected
- **Unlimited retention**: Setting `retention_ns = 0` keeps all versions forever

This enables time-travel queries where applications can read the database state at any historical timestamp within the retention window.

### Versioned Query API

When versioning is enabled, the `Snapshot` provides additional query methods:

- **`history_iter(lower, upper, include_tombstones, ts_range, limit)`**: Creates a `HistoryIterator` that scans all versions of keys in the given range. Uses a k-way merge across all LSM components (active memtable, immutable memtables, all SSTable levels). Supports filtering by timestamp range and limiting results.

- **`get_at(key, timestamp)`**: Point-in-time query that returns the value of a key at a specific timestamp. Finds the latest version at or before the requested timestamp.

- **`HistoryOptions`**: Configuration struct for history iteration with `include_tombstones`, `ts_range`, and `limit` fields.

---

## Compaction

Compaction merges SSTables to reduce read amplification, reclaim space from obsolete versions, and maintain the leveled structure of the LSM tree.

### Why Compaction is Necessary

Without compaction, an LSM tree would degrade over time:
- **Read amplification grows**: More SSTables means more files to check for each read
- **Space amplification grows**: Obsolete versions and tombstones accumulate
- **Level 0 bloats**: Overlapping SSTables in L0 hurt read performance

Compaction addresses these issues by:
1. Merging overlapping SSTables into non-overlapping ones
2. Dropping obsolete versions that are no longer visible
3. Removing tombstones at the bottom level
4. Maintaining size ratios between levels

### Beat/Bar Pacing Model

SurrealOS uses a compaction model inspired by [TigerBeetle](https://tigerbeetle.com/). Rather than running compaction as a background task, compaction is driven **synchronously from the commit path**. Each call to `Store::apply()` triggers one "beat" of compaction work. A "bar" (default 32 beats) represents one full compaction cycle.

This approach provides predictable, incremental compaction that avoids the latency spikes associated with background compaction bursts.

**Half-Bar Alternation:**

Levels are split into two groups that alternate per half-bar:
- **First half-bar** (beats 0-15): ODD target levels active (immutable→0, 1→2, 3→4, 5→6)
- **Second half-bar** (beats 16-31): EVEN target levels active (0→1, 2→3, 4→5)

This alternation ensures that data flows through levels progressively and that freshly compacted outputs are not immediately re-compacted.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BEAT/BAR LIFECYCLE                                   │
│                                                                             │
│  Half-bar start (beat 0 or beat 16):                                        │
│    bar_commence → select tables for all active levels, calculate quotas     │
│                                                                             │
│  Every beat (each Store::apply() call):                                     │
│    beat_commence → set beat_quota = ceil(remaining / beats_remaining)       │
│    merge work    → process beat_quota entries per compaction                │
│                                                                             │
│  Half-bar end (beat 15 or beat 31):                                         │
│    bar_complete → apply ALL manifest changes at once, cleanup old tables    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### LiveCompaction State Machine

Each active compaction for a level follows this lifecycle:

```
  Commenced ──► Paused ──► Completed
      │           ▲  │         ▲
      │           └──┘         │
      └────────────────────────┘
```

- **Commenced**: Tables selected, iterator and writer created, no entries processed yet
- **Paused**: Beat quota consumed, entries remain, waiting for next beat
- **Completed**: Iterator exhausted, table file finalized, waiting for commit at half-bar end

### Compaction Quotas

At the start of a half-bar, the `bar` quota is set to the total estimated entries across all selected tables. Each beat, `beat_quota = ceil(remaining / beats_remaining)` distributes remaining work evenly. This progressive pacing prevents compaction from monopolizing any single commit.

### Compaction Choices

For each level that needs compaction, the strategy selects one of:
- **Merge**: Full merge via `CompactionIterator` — reads entries from source tables, applies snapshot-aware filtering, writes to new SSTable(s)
- **Move**: Zero-overlap level promotion — moves an SSTable to the next level without rewriting (when no overlapping keys exist in the target level)
- **Skip**: No compaction needed for this level

### Leveled Compaction Strategy

SurrealOS uses **leveled compaction**, similar to RocksDB's default strategy. Each level has a target size, with each level being approximately 10x larger than the previous:

- **Level 0**: 4 files trigger compaction (`level0_max_files`)
- **Level 1**: 256MB base size (`max_bytes_for_level`)
- **Level 2**: ~2.5GB
- **Level N**: 10x Level N-1 (`level_multiplier = 10.0`)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         LSM TREE STRUCTURE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  LEVEL 0 (Overlapping - recently flushed memtables)                         │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐                                       │
│  │ SST A   │ │ SST B   │ │ SST C   │  ← May have overlapping key ranges    │
│  │ a───z   │ │ d───m   │ │ b───k   │                                       │
│  └─────────┘ └─────────┘ └─────────┘                                       │
│       │           │           │                                             │
│       └───────────┼───────────┘                                             │
│                   ▼                                                         │
│  LEVEL 1 (Non-overlapping, sorted)                                          │
│  ┌─────────┬─────────┬─────────┐                                            │
│  │ SST 1   │ SST 2   │ SST 3   │  ← Non-overlapping, can binary search     │
│  │ a───f   │ g───m   │ n───z   │                                            │
│  └─────────┴─────────┴─────────┘                                            │
│                   │                                                         │
│                   ▼                                                         │
│  LEVEL 2 (Non-overlapping, 10x larger)                                      │
│  ┌──────┬──────┬──────┬──────┐                                              │
│  │SST 1 │SST 2 │SST 3 │SST 4 │                                             │
│  │ a─c  │ d─h  │ i─o  │ p─z  │                                             │
│  └──────┴──────┴──────┴──────┘                                              │
│                   │                                                         │
│                   ▼                                                         │
│  LEVEL N (Bottom level)                                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Compaction Scoring

Each level receives a compaction score that determines priority:

**Level 0 Score:**
```
L0_score = max(file_count / L0_compaction_trigger, total_bytes / max_bytes)
```
L0 is special because overlapping files hurt read performance. The score considers both file count and total size.

**Level N Score (N > 0):**
```
Ln_score = level_bytes / target_bytes_for_level
```
Higher levels are compacted when they exceed their target size.

The level with the highest score >= 1.0 is selected for compaction.

### Snapshot-Aware Compaction

During compaction, the iterator applies snapshot-aware filtering to determine which versions to keep or drop:

```
┌───────────────────────────────────────────────────────────────────────┐
│                   SNAPSHOT-AWARE DECISION                             │
│                                                                       │
│  1. Find earliest snapshot that can see this version                  │
│                                                                       │
│  2. If same visibility boundary as newer version → DROP (superseded)  │
│                                                                       │
│  3. If visible to any active snapshot → KEEP                          │
│                                                                       │
│  4. If versioning enabled and within retention → KEEP                 │
│                                                                       │
│  5. Tombstone at bottom level with no snapshots → DROP                │
│                                                                       │
│  6. Otherwise → DROP                                                  │
└───────────────────────────────────────────────────────────────────────┘
```

### Object Store Integration

After compaction completes at a half-bar boundary:
1. New SSTables are uploaded to the object store via `upload_and_open_table()`
2. If local caching is enabled (`local_sst_cache`), the SST is also written to local disk
3. The manifest is updated atomically with new and removed table entries
4. Old SSTables are deleted from the object store via `cleanup_old_tables()`

### Write Amplification

**Write amplification** measures how many times data is written to disk over its lifetime. In leveled compaction:
- Each entry is written once per level it passes through
- With a 10x size ratio and N levels, write amplification is approximately N
- Typical values: 10-30x for write-heavy workloads

---

## Object Store and Cloud Storage

SurrealOS supports cloud-native storage through the `TableStore` abstraction, which manages all SSTable and manifest I/O via the Apache Arrow `object_store` crate.

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     TableStore                           │
│                                                         │
│  write_sst()      read_range()      delete_sst()        │
│  read_table_block()   read_footer()                     │
│  write_manifest()    write_manifest_cas()               │
│  read_manifest()     read_latest_manifest()             │
│  list_sst_ids()      list_manifest_ids()                │
│       │              │              │                    │
│       ▼              ▼              ▼                    │
│  ┌──────────────────────────────────────────────────┐   │
│  │              ObjectStore trait                     │   │
│  │  (InMemory | LocalFS | S3 | GCS | Azure)         │   │
│  └──────────────────────────────────────────────────┘   │
│       │                                                 │
│       ▼                                                 │
│  ┌──────────────────────────────────────────────────┐   │
│  │         Optional Local SST Cache                  │   │
│  │  (local_sst_cache = true)                   │   │
│  │  Reads: local first, fallback to object store    │   │
│  │  Writes: atomic tmp+rename to local disk         │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Key Operations

| Operation | Purpose |
|-----------|---------|
| `write_sst()` | Atomic upload of SSTable data |
| `read_range()` | Byte-range read for blocks (local-first with fallback) |
| `read_table_block()` | Read, verify checksum, and decompress a data block |
| `delete_sst()` | Delete SSTable from object store and local cache |
| `write_manifest_cas()` | CAS (put-if-not-exists) write for fencing safety |
| `write_manifest()` | Unconditional put for background manifest sync |
| `read_latest_manifest()` | List and read the highest-ID manifest |

### Path Resolution

The `PathResolver` maps logical IDs to object store paths using the configured `object_store_root`:
- SST files: `{root}/sst/{ulid}.sst`
- Manifest files: `{root}/manifest/{id:020}.manifest`

### Local SST Caching

When `local_sst_cache = true` (the default), SSTables are cached on local disk after upload to the object store:
- **Writes**: After uploading to object store, data is written to local disk via atomic tmp+rename
- **Reads**: Local disk is checked first; on miss, falls back to object store
- **Cleanup**: `gc::cleanup_local_orphans()` removes local files not referenced by the manifest

### Manifest Uploader

The `ManifestUploader` runs as a background task that asynchronously uploads manifest snapshots to the object store via an mpsc channel. This provides best-effort remote sync without blocking the commit path.

### Cloud Checkpoints

SurrealOS supports cloud-native backup and restore:
- `Store::create_cloud_checkpoint(name)` — creates a checkpoint in the object store
- `Store::restore_cloud_checkpoint(name)` — restores from an object store checkpoint

---

## Fencing (Distributed Coordination)

When SurrealOS is used in a distributed setting (e.g., with Raft-based replication), fencing prevents split-brain scenarios where multiple nodes attempt to write to the same database concurrently.

### Overview

The `FenceableManifest` tracks two epochs:
- **`writer_epoch`**: Claimed by `init_writer()` during startup
- **`compactor_epoch`**: Claimed by `claim_compactor_epoch()` after writer init

The `leader_id` (a u128) is set by the external coordination layer (e.g., Raft term + replica ID). A `leader_id` of 0 means single-node mode with no fencing.

### Fencing Protocol

```
Node A (old leader)          Object Store           Node B (new leader)
       │                         │                         │
       │   init_writer()         │                         │
       │   epoch=1, CAS ───────►│                         │
       │                         │◄──────── init_writer()  │
       │                         │          epoch=2, CAS   │
       │                         │          ✓ (wins)       │
       │                         │                         │
       │   manifest write        │                         │
       │   check_writer_fenced() │                         │
       │   epoch=2 > local=1     │                         │
       │   → Error::Fenced       │                         │
```

### How It Works

1. **Startup**: When `leader_id > 0`, `init_writer()` increments `writer_epoch` in the manifest and attempts a CAS write to the object store. On conflict (another writer already claimed this manifest version), the manifest is refreshed from remote and the process retries until success or timeout.

2. **Compactor claim**: After writer init, `claim_compactor_epoch()` similarly increments and claims the compactor epoch via CAS.

3. **Fence check**: Before every manifest write during normal operation, `check_writer_fenced()` verifies that `manifest.writer_epoch` has not exceeded the locally claimed epoch. If a newer writer has taken over, `Error::Fenced` is returned.

4. **CAS mechanism**: `write_manifest_cas()` uses `object_store::PutMode::Create` (put-if-not-exists) keyed by manifest version ID. This ensures only one writer can claim each manifest version.

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `leader_id` | 0 | Leader identity (0 = single-node, no fencing) |
| `manifest_update_timeout` | 30s | Timeout for CAS retry loop during init |

---

## Recovery

When SurrealOS starts, it recovers the database state from disk by loading the manifest and replaying the WAL. This process ensures that all committed data is restored, even after crashes or power failures.

### Crash Recovery Guarantees

SurrealOS provides the following guarantees:

- **Committed data is durable**: Any batch that received a successful `apply()` response will be recovered
- **Uncommitted data may be lost**: Batches that crashed before `apply()` completed are not recovered
- **Atomic recovery**: Either all of a batch's writes are recovered, or none are

The strength of the durability guarantee depends on the `sync` parameter used at apply time:
- **Immediate** (`sync = true`): Data was fsynced before apply returned; it will survive any crash
- **Eventual** (`sync = false`): Data was written to OS buffers; it may be lost if the OS crashes before flushing

### Recovery Process Overview

Recovery proceeds in three phases:

1. **Initialization**: Load metadata and acquire exclusive access
2. **WAL Replay**: Reconstruct in-memory state from the write-ahead log
3. **Finalization**: Clean up orphaned files and start background tasks

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                INITIALIZATION                               │
│                                                                             │
│  1. Acquire LOCK file (prevents concurrent access)                          │
│                                                                             │
│  2. Initialize TableStore with object store                                 │
│                                                                             │
│  3. Load Level Manifest                                                     │
│     - SSTable locations per level                                           │
│     - min_wal_number (WALs older than this are flushed)                     │
│     - last_sequence number                                                  │
│                                                                             │
│  4. Clean up local orphaned SST files (gc::cleanup_local_orphans)           │
│                                                                             │
│  5. If distributed mode (leader_id > 0):                                    │
│     - init_writer() — claim writer epoch via CAS                            │
│     - claim_compactor_epoch() — claim compactor epoch via CAS               │
│                                                                             │
│  6. Open/create required directories                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                  WAL REPLAY                                 │
│                                                                             │
│  For each WAL segment with id >= min_wal_number:                            │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  Read records sequentially:                                           │  │
│  │                                                                       │  │
│  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │  │
│  │  │ Read Header │ ──► │ Verify CRC  │ ──► │ Decompress  │              │  │
│  │  │ (7 bytes)   │     │             │     │ (if needed) │              │  │
│  │  └─────────────┘     └─────────────┘     └─────────────┘              │  │
│  │         │                   │                   │                     │  │
│  │         ▼                   ▼                   ▼                     │  │
│  │  On corruption:       CRC mismatch:       Apply to MemTable           │  │
│  │  - TolerateWithRepair: truncate & continue                            │  │
│  │  - AbsoluteConsistency: return error                                  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  If MemTable fills during replay → Flush to L0 SSTable                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 FINALIZATION                                │
│                                                                             │
│  1. Set sequence numbers:                                                   │
│     visible_seq_num = max(manifest_last_seq, wal_max_seq)                   │
│     log_seq_num = visible_seq_num + 1                                       │
│                                                                             │
│  2. Cleanup orphaned files:                                                 │
│     - SST files not in manifest → delete                                    │
│     - WAL files < min_wal_number → delete                                   │
│                                                                             │
│  3. Start background tasks:                                                 │
│     - Manifest uploader (async object store sync)                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### WAL Record Format

The WAL uses a block-based format inspired by LevelDB/RocksDB. Each block is 32KB, and records that don't fit in a single block are fragmented across multiple blocks.

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         WAL BLOCK (32KB)                                  │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌───────┬───────┬───────┬─────────────────────────────────────────────┐  │
│  │  CRC  │ Size  │ Type  │                 Payload                     │  │
│  │ (4B)  │ (2B)  │ (1B)  │              (variable)                     │  │
│  └───────┴───────┴───────┴─────────────────────────────────────────────┘  │
│                                                                           │
│  Record Types:                                                            │
│    0 = Empty (padding)                                                    │
│    1 = Full (complete record in one block)                                │
│    2 = First (start of fragmented record)                                 │
│    3 = Middle (continuation of fragmented record)                         │
│    4 = Last (end of fragmented record)                                    │
│                                                                           │
│  CRC covers: Type byte + Payload                                          │
└───────────────────────────────────────────────────────────────────────────┘
```

**Record Fragmentation:**

Large records that don't fit in the remaining space of a block are split:
- **First**: Contains the beginning of the record
- **Middle**: Contains continuation data (may be multiple)
- **Last**: Contains the final portion

During replay, fragments are reassembled before processing.

### Recovery Modes

SurrealOS supports two recovery modes that control behavior when WAL corruption is detected:

| Mode | Behavior |
|------|----------|
| **AbsoluteConsistency** | Return error on any corruption; do not start |
| **TolerateCorruptedWithRepair** | Truncate corrupted tail and continue recovery |

`TolerateCorruptedWithRepair` is useful for recovering from crashes that left partial writes. The corrupted data represents uncommitted batches (they crashed before completing), so truncating them maintains consistency.

### Manifest's Role in Recovery

The manifest tracks the durable state of the LSM tree:
- Which SSTables exist at each level
- Their key ranges and file sizes
- The `min_wal_number`: WALs older than this have been fully flushed to SSTables

During recovery, SurrealOS only replays WALs with `id >= min_wal_number`. Older WALs contain data that is already in SSTables and can be safely deleted.

### Orphan File Cleanup

After recovery, the database may contain orphaned files:
- **SST files not in manifest**: Created but crashed before manifest update; safe to delete
- **WAL files < min_wal_number**: Already flushed to SSTables; safe to delete
- **Temporary files**: Incomplete compaction outputs; safe to delete

This cleanup ensures disk space is reclaimed after crashes.

---

## Checkpoint and Restore

Checkpoints create consistent point-in-time snapshots of the database for backup and recovery.

### Use Cases

**Backup and Disaster Recovery:**
- Create periodic checkpoints to external storage (S3, NFS, etc.)
- Restore from checkpoint after catastrophic failure
- Geographic distribution of backup copies

**Testing and Development:**
- Checkpoint production data for test environments
- Restore to known state between test runs
- Debug issues with production data snapshots

**Migration:**
- Checkpoint before major upgrades
- Transfer database between machines
- Clone database for read replicas

### Why Flush Before Checkpoint?

A checkpoint must capture a consistent state. The active memtable contains in-flight data that may be partially applied. By flushing to SSTables first:

1. All committed data is durably stored in files
2. The checkpoint captures exactly what was committed at that moment
3. Recovery from checkpoint is deterministic

### Local Checkpoint Process

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CHECKPOINT CREATION                             │
│                                                                         │
│  1. PREPARATION                                                         │
│     - Flush active memtable to SSTable                                  │
│     - Wait for all immutable memtables to flush                         │
│     - Capture current sequence number                                   │
│                                                                         │
│  2. COPY FILES TO CHECKPOINT DIRECTORY                                  │
│     checkpoint_path/                                                    │
│     ├── sst/              ← Copy all SSTable files                      │
│     ├── wal/              ← Copy WAL segments                           │
│     ├── manifest/         ← Copy manifest file                          │
│     └── CHECKPOINT_METADATA  ← Binary metadata file                     │
│                                                                         │
│  3. METADATA (binary format)                                            │
│     - version: u32                                                      │
│     - timestamp: u64                                                    │
│     - sequence_number: u64                                              │
│     - sstable_count: u64                                                │
│     - total_size: u64                                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Restore Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            RESTORE PROCESS                              │
│                                                                         │
│  1. VALIDATION                                                          │
│     - Verify checkpoint metadata exists                                 │
│     - Verify all required files present                                 │
│                                                                         │
│  2. REPLACE DATABASE FILES                                              │
│     - Clear current sst/, wal/, manifest/ directories                   │
│     - Copy files from checkpoint                                        │
│                                                                         │
│  3. RELOAD STATE                                                        │
│     - Load manifest from checkpoint                                     │
│     - Clear in-memory memtables (discards uncommitted data)             │
│     - Replay WAL segments from checkpoint                               │
│     - Set sequence numbers from checkpoint                              │
│                                                                         │
│  4. RESUME OPERATIONS                                                   │
│     - Database returns to checkpoint state                              │
│     - Any data written after checkpoint is discarded                    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Cloud Checkpoints

SurrealOS also supports cloud-native checkpoint operations:

- **`Store::create_cloud_checkpoint(name)`**: Flushes memtable, then creates a named checkpoint in the object store containing all SSTable and manifest data.

- **`Store::restore_cloud_checkpoint(name)`**: Restores the database state from a named cloud checkpoint, downloading SSTables and manifest from the object store.

Both methods return `CheckpointMetadata` with version, timestamp, sequence number, SSTable count, and total size.
