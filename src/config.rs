//! This module contains constants and configuration parameters for configuring avro writers and readers.

/// Synchronization marker bytes length, defaults to  16 bytes.
pub const SYNC_MARKER_SIZE: usize = 16;
/// The magic header for recognizing a file as an avro data file.
pub const MAGIC_BYTES: &[u8] = b"Obj\x01";
/// Checksum length for snappy compressed data.
#[cfg(feature = "snappy")]
pub const CRC_CHECKSUM_LEN: usize = 4;
/// Minimum flush interval that a block can have.
pub const BLOCK_SIZE: usize = 4096;
/// This value defines the threshold post which the scratch buffer is
/// is flushed/synced to the main buffer. Suggested values are between 2K (bytes) and 2M
// TODO make this configurable
pub const DEFAULT_FLUSH_INTERVAL: usize = 16 * BLOCK_SIZE;
