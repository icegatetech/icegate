//! Iceberg data-file [`MergeSource`] adapter for Parquet compaction.
//!
//! The k-way [`RowGroupsMerger`](icegate_common::merge::RowGroupsMerger) merges a
//! set of already-sorted inputs that it opens lazily through a
//! [`MergeSource`](icegate_common::merge::MergeSource).
//!
//! The Shifter plugs in a WAL-backed source; compaction plugs in
//! [`IcebergMergeSource`], which resolves a merge input's opaque
//! [`position`](icegate_common::merge::MergeInput::position) back to an existing
//! Iceberg Parquet data-file path and streams that file's rows in sort order.
//!
//! The compaction rewrite task (Task 4.2) assigns each input file a `position`
//! equal to its index in sorted input order and builds the position→path map
//! from the same [`DataFileStats`](icegate_common::manifest_scan::DataFileStats)
//! it enumerated for planning. Because every input file is already internally
//! sorted by the table's Iceberg sort order, the merger only has to interleave
//! them.
//!
//! Files are opened by path through the table's
//! [`FileIO`](iceberg::io::FileIO) — the same object-store handle the writer
//! used — so the source never assumes a local filesystem. Opening goes through
//! the shared [`open_arrow_file_reader`] helper (the same opener the query
//! crate's metadata scan uses): `new_input(path)` → `reader()` →
//! [`ArrowFileReader`](iceberg::arrow::ArrowFileReader) built from the file's
//! already-known size → [`ParquetRecordBatchStreamBuilder`]. All read failures
//! map to [`Error::CompactRead`], the compaction read-path error variant.

use std::collections::HashMap;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use iceberg::io::FileIO;
use icegate_common::Error;
use icegate_common::error::Result;
use icegate_common::iceberg_write::CommonRecordBatchStream;
use icegate_common::merge::{MergeInput, MergePosition, MergeSource};
use icegate_common::parquet_source::open_arrow_file_reader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use tokio_util::sync::CancellationToken;

/// A [`MergeSource`] that opens existing Iceberg Parquet data files as Arrow
/// record-batch streams for the compaction k-way merger.
///
/// Each [`MergeInput::position`] the merger asks for is looked up in
/// [`paths_by_position`](Self::paths_by_position) to recover the data file's
/// object-store path, which is then opened through the table's
/// [`FileIO`]. The map is built by the planner / rewrite task in sorted input
/// order, so positions double as the merger's stable equal-key tie-break.
pub struct IcebergMergeSource {
    /// Object-store handle (cloned from `table.file_io()`) used to open data
    /// files by path. Carries the same credentials/endpoint the writer used.
    file_io: FileIO,
    /// Map from a merge input's opaque `position` to the data-file path the
    /// rewrite task assigned it, in sorted input order.
    paths_by_position: HashMap<MergePosition, String>,
}

impl IcebergMergeSource {
    /// Build a source over a table's [`FileIO`] and a position→path map.
    ///
    /// `paths_by_position` must contain an entry for every
    /// [`MergeInput::position`] the merger will open (the rewrite task builds it
    /// alongside the [`MergeInput`]s it hands to the merger); an `open` call for
    /// an absent position fails with [`Error::CompactRead`].
    #[must_use]
    pub const fn new(file_io: FileIO, paths_by_position: HashMap<MergePosition, String>) -> Self {
        Self {
            file_io,
            paths_by_position,
        }
    }

    /// Resolve a merge input's `position` to its data-file path.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CompactRead`] if the position is not present in the map,
    /// which indicates the rewrite task built the [`MergeInput`]s and the
    /// position→path map inconsistently.
    fn path_for(&self, position: MergePosition) -> Result<&str> {
        self.paths_by_position.get(&position).map(String::as_str).ok_or_else(|| {
            Error::CompactRead(format!(
                "no data-file path registered for merge input position {position}"
            ))
        })
    }
}

#[async_trait]
impl MergeSource for IcebergMergeSource {
    /// Open the data file mapped to `input.position` as a stream of its Parquet
    /// record batches, already in the table's sort order.
    ///
    /// Cancellation is observed both at construction (before any object-store
    /// I/O is issued) and between batches: the returned stream short-circuits
    /// with [`Error::CompactRead`] if `cancel` fires while the merger is pulling
    /// from it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CompactRead`] if `input.position` has no registered
    /// path, if `cancel` is already triggered, if the file cannot be opened /
    /// read through [`FileIO`], if the Parquet footer cannot be decoded, or
    /// (lazily, while streaming) if any record batch fails to decode.
    async fn open(&self, input: &MergeInput, cancel: &CancellationToken) -> Result<CommonRecordBatchStream> {
        // Fail fast before issuing any object-store I/O if the merge was already
        // cancelled.
        if cancel.is_cancelled() {
            return Err(Error::CompactRead(
                "compaction merge cancelled before opening data file".to_string(),
            ));
        }

        let path = self.path_for(input.position)?;

        // Open the data file through the table's FileIO (same object-store handle
        // the writer used) via the shared opener. The file size is already known
        // from the planned `MergeInput`, so the opener builds the footer metadata
        // from it rather than issuing a separate stat round-trip per file.
        let arrow_reader = open_arrow_file_reader(&self.file_io, path, input.bytes)
            .await
            .map_err(|err| Error::CompactRead(format!("failed to open data file '{path}': {err}")))?;
        let parquet_stream = ParquetRecordBatchStreamBuilder::new(arrow_reader)
            .await
            .map_err(|err| Error::CompactRead(format!("failed to read parquet metadata for '{path}': {err}")))?
            .build()
            .map_err(|err| Error::CompactRead(format!("failed to build parquet stream for '{path}': {err}")))?;

        // Bridge the parquet stream into a `CommonRecordBatchStream`:
        // - map each item's `ParquetError` into `Error::CompactRead` (the
        //   merger's stream item type is `icegate_common::error::Result`);
        // - inject a cancellation check before each yielded batch so a cancelled
        //   merge stops pulling even mid-file. `path` is moved into the closures
        //   so error messages stay localized to the file being read.
        let cancel = cancel.clone();
        let read_path = path.to_string();
        let stream = parquet_stream
            .map_err(move |err| Error::CompactRead(format!("failed to read record batch from '{read_path}': {err}")))
            .map(move |batch| {
                if cancel.is_cancelled() {
                    return Err(Error::CompactRead(
                        "compaction merge cancelled while reading data file".to_string(),
                    ));
                }
                batch
            });

        Ok(stream.boxed())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use iceberg::io::FileIO;
    use icegate_common::Error;
    use icegate_common::merge::sort_key::{
        RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, RowGroupBoundaryValue,
    };
    use icegate_common::merge::{MergeInput, MergePosition, MergeSource};
    use tokio_util::sync::CancellationToken;

    use super::IcebergMergeSource;

    /// A boundary range over a single `service_name` ASC column, enough to
    /// satisfy [`MergeInput`] construction in the unit tests below (these tests
    /// never reach the merger's overlap logic — they exercise `open` directly).
    fn dummy_boundary_range() -> RowGroupBoundaryRange {
        let component = RowGroupBoundaryComponent {
            value: Some(RowGroupBoundaryValue::String("svc".to_string())),
            descending: false,
            nulls_first: true,
        };
        RowGroupBoundaryRange {
            names: Arc::from(["service_name".to_string()]),
            min_key: RowGroupBoundaryKey::new(vec![component.clone()]),
            max_key: RowGroupBoundaryKey::new(vec![component]),
        }
    }

    fn merge_input(position: u128) -> MergeInput {
        MergeInput::new(MergePosition::new(position), 1, dummy_boundary_range())
    }

    #[tokio::test]
    async fn open_errors_when_position_not_in_map() {
        // A `position` with no registered path is a planner/rewrite-task bug;
        // `open` must surface it as a compaction read error rather than panic.
        let file_io = FileIO::new_with_memory();
        let source = IcebergMergeSource::new(file_io, HashMap::new());
        let cancel = CancellationToken::new();

        let result = source.open(&merge_input(7), &cancel).await;
        assert!(
            matches!(result, Err(Error::CompactRead(_))),
            "unregistered position must yield a CompactRead error"
        );
    }

    #[tokio::test]
    async fn open_errors_when_already_cancelled() {
        // A merge cancelled before `open` must not issue any object-store I/O;
        // it short-circuits with a compaction read error.
        let file_io = FileIO::new_with_memory();
        let mut paths = HashMap::new();
        paths.insert(MergePosition::new(0), "memory://does-not-matter.parquet".to_string());
        let source = IcebergMergeSource::new(file_io, paths);

        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = source.open(&merge_input(0), &cancel).await;
        assert!(
            matches!(result, Err(Error::CompactRead(_))),
            "pre-cancelled merge must yield a CompactRead error before any I/O"
        );
    }

    #[tokio::test]
    async fn open_errors_on_missing_file() {
        // A registered path that does not exist in the backing store must fail
        // as a compaction read error (open/stat failure), not a panic.
        let file_io = FileIO::new_with_memory();
        let mut paths = HashMap::new();
        paths.insert(
            MergePosition::new(0),
            "memory://nonexistent/icegate-compact-missing.parquet".to_string(),
        );
        let source = IcebergMergeSource::new(file_io, paths);
        let cancel = CancellationToken::new();

        let result = source.open(&merge_input(0), &cancel).await;
        // The failure may surface either when opening/statting the input or when
        // the stream is first polled; either way it must be a CompactRead error.
        match result {
            Err(Error::CompactRead(_)) => {}
            Err(other) => panic!("expected CompactRead error, got {other:?}"),
            Ok(stream) => {
                let collected: std::result::Result<Vec<_>, _> = stream.try_collect().await;
                assert!(
                    matches!(collected, Err(Error::CompactRead(_))),
                    "missing file must surface as a CompactRead error while streaming"
                );
            }
        }
    }
}
