//! Shared opener for reading existing Iceberg Parquet data files.
//!
//! Both the query crate's metadata scan and the compaction merge source open
//! data files the same way: resolve the object-store path through the table's
//! [`FileIO`], wrap the reader in an [`ArrowFileReader`], and feed it to the
//! parquet reader. This module owns that opener so the sequence lives in exactly
//! one place rather than being copy-pasted per caller.
//!
//! The caller supplies the file's already-known size (from the manifest /
//! `DataFile::file_size_in_bytes` or a planned `MergeInput`), so the opener
//! constructs [`FileMetadata`] directly instead of issuing an extra object-store
//! stat round-trip per file.

use iceberg::arrow::ArrowFileReader;
use iceberg::io::{FileIO, FileMetadata};

/// Open an Iceberg data file by path into an [`ArrowFileReader`].
///
/// Uses the caller-supplied `file_size_in_bytes` to build [`FileMetadata`],
/// avoiding a separate `metadata()` stat call. The returned reader still needs
/// its parquet footer decoded (`get_metadata` /
/// `ParquetRecordBatchStreamBuilder::new`) by the caller, which keeps each
/// caller's own parquet-error handling at its call site.
///
/// # Errors
///
/// Returns the [`iceberg::Error`] from opening the input (`new_input`) or
/// obtaining its reader (`reader`). Callers map it into their own error type.
pub async fn open_arrow_file_reader(
    file_io: &FileIO,
    path: &str,
    file_size_in_bytes: u64,
) -> std::result::Result<ArrowFileReader, iceberg::Error> {
    let input = file_io.new_input(path)?;
    let reader = input.reader().await?;
    Ok(ArrowFileReader::new(
        FileMetadata {
            size: file_size_in_bytes,
        },
        reader,
    ))
}
