use crate::Result;

/// User-defined merge operator for associative operations.
///
/// Merge operators enable efficient read-modify-write patterns (counters,
/// append-only lists) without requiring a full read-modify-write cycle.
/// Instead of reading the current value, modifying it, and writing it back,
/// callers simply issue a `Merge` operation with the operand. The merge
/// operator is invoked lazily during reads and compaction to combine the
/// operands with any existing base value.
///
/// # Contract
///
/// - `full_merge` is called when a base `Set` value exists followed by one or more `Merge`
///   operands.
/// - `partial_merge` is called when only `Merge` operands exist (no base `Set` value was found).
/// - Operands are always provided in oldest-to-newest order.
/// - Implementations must be deterministic: the same inputs must always produce the same output.
pub trait MergeOperator: Send + Sync {
	/// Merge operands with an existing base value (Put + Merge sequence).
	///
	/// `existing` is the base Put value. `operands` are in oldest-to-newest
	/// order.
	fn full_merge(&self, key: &[u8], existing: &[u8], operands: &[&[u8]]) -> Result<Vec<u8>>;

	/// Merge operands together when no base Put exists.
	///
	/// `operands` are in oldest-to-newest order.
	fn partial_merge(&self, key: &[u8], operands: &[&[u8]]) -> Result<Vec<u8>>;
}
