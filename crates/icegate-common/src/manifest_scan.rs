//! Snapshot data-file enumeration with decoded sort-key bounds.
//!
//! Parquet compaction needs, for every data file in a table's current snapshot,
//! the file's inclusive sort-key envelope (its min and max sort key). The
//! Iceberg manifest already stores per-column lower/upper bounds, so this module
//! walks the manifest tree directly rather than going through
//! [`table.scan().plan_files()`](iceberg::table::Table::scan): a planned
//! [`FileScanTask`](iceberg::scan::FileScanTask) exposes no per-column bounds.
//!
//! [`list_data_files_with_stats`] loads the current snapshot's manifest list,
//! reads each manifest, and for every alive data-file entry decodes the
//! sort-key columns named by a [`SortColumnsDescriptor`] from the manifest
//! `lower_bounds`/`upper_bounds` maps (keyed by Iceberg field id, already
//! decoded into [`Datum`](iceberg::spec::Datum)s) into a pair of
//! [`RowGroupBoundaryKey`]s ordered exactly like the descriptor.
//!
//! The manifest bounds are *literal* per-column min/max. Because a
//! [`RowGroupBoundaryKey`] is a *sort* key compared with the direction-aware
//! [`RowGroupBoundaryKey::compare`], each key is assembled per column direction
//! so the produced `min_key` is the sort-order-first key and `max_key` the
//! sort-order-last key (for a descending column the literal maximum sorts
//! first). This guarantees `min_key <= max_key` for every file.

use std::collections::HashMap;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use iceberg::io::FileIO;
use iceberg::spec::{DataContentType, Datum, Literal, ManifestFile, PrimitiveLiteral, PrimitiveType, Struct};
use iceberg::{spec::DataFile, table::Table};

use crate::Error;
use crate::error::Result;
use crate::merge::sort_key::{
    RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, RowGroupBoundaryValue, SortColumnDescriptor,
    SortColumnsDescriptor,
};

/// One data file with its `(tenant, day)` partition key and decoded sort-key
/// envelope.
///
/// The envelope's min/max keys carry one [`RowGroupBoundaryComponent`] per sort
/// column in the descriptor's order, so two `DataFileStats` produced from the
/// same descriptor have compatible key structures and can be compared directly
/// with [`RowGroupBoundaryKey::compare`]. The compaction planner reasons about
/// a file through its [`partition_key`](Self::partition_key),
/// [`size_bytes`](Self::size_bytes), and [`boundary_range`](Self::boundary_range).
#[derive(Debug, Clone)]
pub struct DataFileStats {
    /// The underlying Iceberg data file (path, size, record count, partition).
    pub data_file: DataFile,
    /// Stable textual key for the file's `(tenant_id, day)` partition, derived
    /// from [`DataFile::partition`] by [`partition_key_string`]. Files sharing
    /// this string belong to the same partition and may be compacted together.
    pub partition_key: String,
    /// Inclusive sort-order envelope (`names` + min/max keys) for this file,
    /// the single source of truth for [`Self::min_key`] and [`Self::max_key`].
    /// `min_key` is the sort-order-first key and `max_key` the
    /// sort-order-last, each drawn per column from the manifest
    /// `lower_bounds`/`upper_bounds` according to the column's direction.
    pub boundary_range: RowGroupBoundaryRange,
}

impl DataFileStats {
    /// Stable `(tenant_id, day)` partition key for this file.
    #[must_use]
    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    /// On-disk size of the underlying data file in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.data_file.file_size_in_bytes()
    }

    /// Inclusive sort-order envelope of this file.
    #[must_use]
    pub const fn boundary_range(&self) -> &RowGroupBoundaryRange {
        &self.boundary_range
    }

    /// Decoded inclusive sort-order minimum key (the sort-order-first key).
    #[must_use]
    pub const fn min_key(&self) -> &RowGroupBoundaryKey {
        &self.boundary_range.min_key
    }

    /// Decoded inclusive sort-order maximum key (the sort-order-last key).
    #[must_use]
    pub const fn max_key(&self) -> &RowGroupBoundaryKey {
        &self.boundary_range.max_key
    }
}

/// A sort column paired with the schema field id its bounds are keyed by.
///
/// The field id is resolved once from the table's current schema so the
/// per-file loop only does `HashMap` lookups, never name resolution.
struct ResolvedSortColumn<'descriptor> {
    /// Iceberg field id used to index the `lower_bounds`/`upper_bounds` maps.
    field_id: i32,
    /// The descriptor column carrying primitive type, direction, and null order.
    column: &'descriptor SortColumnDescriptor,
}

/// Upper bound on how many manifests are loaded and decoded concurrently.
///
/// Manifest loads are independent object-store reads, so fanning them out hides
/// per-manifest latency; the cap keeps a wide manifest tree from issuing an
/// unbounded number of reads at once. Results are still assembled in
/// manifest-list order (see [`collect_data_file_stats`]) so enumeration stays
/// deterministic.
const MANIFEST_READ_CONCURRENCY: usize = 8;

/// Enumerate all data files in the table's current snapshot with sort-key bounds.
///
/// For each data file, decodes the sort-key columns' min/max from the manifest
/// `lower_bounds`/`upper_bounds` into [`RowGroupBoundaryKey`]s ordered per the
/// `descriptor`. Returns an empty vector if the table has no current snapshot (a
/// freshly created table that has never been committed to).
///
/// # Errors
///
/// Returns an error if:
/// - a sort column named by `descriptor` is absent from the table's current
///   schema (the bounds maps could not be keyed),
/// - loading the manifest list or any manifest fails ([`Error::Iceberg`]), or
/// - a bound `Datum` decodes to a [`PrimitiveLiteral`] variant that disagrees
///   with its column's declared Iceberg primitive type ([`Error::CompactRead`]).
pub async fn list_data_files_with_stats(
    table: &Table,
    descriptor: &SortColumnsDescriptor,
) -> Result<Vec<DataFileStats>> {
    collect_data_file_stats(table, descriptor, None).await
}

/// Enumerate only the data files belonging to one `(tenant, day)` partition.
///
/// Identical to [`list_data_files_with_stats`] but skips — without decoding
/// their sort-key bounds — every alive data file whose partition key differs
/// from `partition_key`. The compaction REWRITE task uses this to re-check the
/// liveness of one planner group's files without re-decoding the bounds of every
/// file in the whole table on every fanned-out task.
///
/// # Errors
///
/// Same as [`list_data_files_with_stats`].
pub async fn list_data_files_in_partition(
    table: &Table,
    descriptor: &SortColumnsDescriptor,
    partition_key: &str,
) -> Result<Vec<DataFileStats>> {
    collect_data_file_stats(table, descriptor, Some(partition_key)).await
}

/// Walk the current snapshot's manifests and collect [`DataFileStats`], loading
/// the manifests concurrently while preserving manifest-list order.
///
/// `partition_filter`, when set, keeps only files in that partition; matching is
/// done on the cheap partition-key string before the (more expensive) bound
/// decode, so a filtered scan never decodes files it will discard. Only live
/// [`DataContentType::Data`] entries participate — delete-file manifests are
/// skipped so they are never opened as data by the merger.
async fn collect_data_file_stats(
    table: &Table,
    descriptor: &SortColumnsDescriptor,
    partition_filter: Option<&str>,
) -> Result<Vec<DataFileStats>> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        // No committed snapshot yet: nothing to enumerate.
        return Ok(Vec::new());
    };

    // Resolve every sort column's field id once against the current schema. The
    // bounds maps are keyed by field id, so this is the only name resolution we
    // need before walking the (potentially many) manifest entries.
    let resolved = resolve_sort_columns(metadata.current_schema(), descriptor)?;
    // Resolve the sort-column names once; every file's envelope shares the same
    // `Arc<[String]>`, so the per-file work is an Arc refcount bump rather than a
    // fresh allocation of the name vector.
    let names = descriptor.column_names();

    let file_io = table.file_io();
    let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;

    // Build one load+decode future per manifest eagerly, then drive them with a
    // bounded-concurrency, ORDER-PRESERVING `buffered` stream so the flattened
    // output stays deterministic for a fixed snapshot (downstream grouping and
    // clustering rely on that stable order). Collecting the futures first avoids
    // a stream-`map` closure whose borrowed-future signature the borrow checker
    // cannot prove general over the manifest reference's lifetime.
    let manifest_futures: Vec<_> = manifest_list
        .entries()
        .iter()
        .map(|manifest_file| process_manifest(manifest_file, file_io, &resolved, &names, partition_filter))
        .collect();
    let per_manifest: Vec<Vec<DataFileStats>> = futures::stream::iter(manifest_futures)
        .buffered(MANIFEST_READ_CONCURRENCY)
        .try_collect()
        .await?;

    Ok(per_manifest.into_iter().flatten().collect())
}

/// Load one manifest and decode its live DATA-file entries into
/// [`DataFileStats`], applying the optional partition filter.
///
/// Factored out of [`collect_data_file_stats`] so the concurrent
/// [`futures::stream::StreamExt::buffered`] driver maps over a named future with
/// concrete lifetimes (an inline async closure trips the borrow checker's
/// higher-ranked lifetime inference here).
async fn process_manifest(
    manifest_file: &ManifestFile,
    file_io: &FileIO,
    resolved: &[ResolvedSortColumn<'_>],
    names: &Arc<[String]>,
    partition_filter: Option<&str>,
) -> Result<Vec<DataFileStats>> {
    let manifest = manifest_file.load_manifest(file_io).await?;
    let mut stats = Vec::new();
    for entry in manifest.entries() {
        // Only files alive at the current snapshot participate.
        if !entry.is_alive() {
            continue;
        }
        let data_file = entry.data_file();
        // Compaction merges DATA files only; positional/equality delete files
        // must never be opened as data by the merger.
        if data_file.content_type() != DataContentType::Data {
            continue;
        }
        let partition_key = partition_key_string(data_file.partition());
        if let Some(target) = partition_filter {
            if partition_key != target {
                continue;
            }
        }
        let (min_key, max_key) = decode_boundary_keys(resolved, data_file.lower_bounds(), data_file.upper_bounds())?;
        stats.push(DataFileStats {
            data_file: data_file.clone(),
            partition_key,
            boundary_range: RowGroupBoundaryRange {
                names: names.clone(),
                min_key,
                max_key,
            },
        });
    }
    Ok(stats)
}

/// Decode one data file's inclusive sort-order envelope, using the table's
/// current schema to resolve the sort columns' field ids.
///
/// This is the single-file counterpart of [`list_data_files_with_stats`]: it
/// runs the exact same direction-aware bound decode against one
/// [`DataFile`]'s `lower_bounds`/`upper_bounds` maps, so an envelope produced
/// here is directly comparable (via [`RowGroupBoundaryKey::compare`]) with the
/// envelopes that enumeration produced for the same descriptor.
///
/// The compaction rewrite executor uses it to read the bounds of the freshly
/// written output files (which are not yet enumerable through any snapshot)
/// and check them against the union of the input files' envelopes, without
/// duplicating the descending-column inversion logic.
///
/// # Errors
///
/// Returns [`Error::CompactRead`] if a sort column named by `descriptor` is
/// absent from the table's current schema, or if a bound `Datum` decodes to a
/// [`PrimitiveLiteral`] variant that disagrees with its column's declared
/// Iceberg primitive type.
pub fn decode_data_file_envelope(
    table: &Table,
    descriptor: &SortColumnsDescriptor,
    data_file: &DataFile,
) -> Result<RowGroupBoundaryRange> {
    let resolved = resolve_sort_columns(table.metadata().current_schema(), descriptor)?;
    let (min_key, max_key) = decode_boundary_keys(&resolved, data_file.lower_bounds(), data_file.upper_bounds())?;
    Ok(RowGroupBoundaryRange {
        names: descriptor.column_names(),
        min_key,
        max_key,
    })
}

/// Resolve every sort column named by `descriptor` to its schema field id.
///
/// The manifest `lower_bounds`/`upper_bounds` maps are keyed by field id, so
/// this resolution is the only name lookup the per-file decode needs. Shared by
/// [`list_data_files_with_stats`] and [`decode_data_file_envelope`] so both use
/// identical field-id resolution.
fn resolve_sort_columns<'descriptor>(
    schema: &iceberg::spec::Schema,
    descriptor: &'descriptor SortColumnsDescriptor,
) -> Result<Vec<ResolvedSortColumn<'descriptor>>> {
    descriptor
        .columns()
        .iter()
        .map(|column| {
            let field = schema.field_by_name(column.column_name()).ok_or_else(|| {
                Error::CompactRead(format!(
                    "sort column '{}' not found in current schema",
                    column.column_name()
                ))
            })?;
            Ok(ResolvedSortColumn {
                field_id: field.id,
                column,
            })
        })
        .collect()
}

/// Build the inclusive sort-order min and max boundary keys for one data file
/// from its manifest `lower_bounds` and `upper_bounds` maps.
///
/// The manifest stores the *literal* per-column minimum (`lower_bounds`) and
/// maximum (`upper_bounds`), which only coincide with the *sort-order* min/max
/// for ascending columns. For a descending sort column the sort-order-first
/// value is the literal maximum, so the min key must draw that column from
/// `upper_bounds` and the max key from `lower_bounds`. Selecting per direction
/// here is what keeps `min_key.compare(max_key) != Greater` under the
/// sort-aware [`RowGroupBoundaryKey::compare`] semantics, regardless of each
/// column's direction.
///
/// Components are assembled in `resolved` (descriptor) order. A column whose
/// field id is absent from a bounds map (for example an all-null optional
/// column, which Iceberg omits from the bounds map) yields a component with
/// `value: None`.
///
/// # Known limitation (clustering quality, not correctness)
///
/// Iceberg manifest bounds describe only the NON-NULL values in a file. For a
/// file that mixes nulls with values in a `nulls-first` sort column (e.g.
/// `logs.service_name`), the decoded `min_key` therefore reports the smallest
/// non-null value even though the file's true sort-order range begins at NULL
/// (which sorts first). The planner's overlap clustering can then judge two
/// files that overlap only in their NULL region as disjoint and place them in
/// separate rewrite groups. This only weakens compaction/pruning — each group is
/// still rewritten correctly and in place, so no row is lost, duplicated, or
/// reordered. A precise fix would need a distinct "null present" marker in the
/// boundary component (one that does not collide with the `None`-is-absent-bound
/// identity the rewrite envelope invariant depends on), which is deferred.
fn decode_boundary_keys(
    resolved: &[ResolvedSortColumn<'_>],
    lower_bounds: &HashMap<i32, Datum>,
    upper_bounds: &HashMap<i32, Datum>,
) -> Result<(RowGroupBoundaryKey, RowGroupBoundaryKey)> {
    let mut min_components = Vec::with_capacity(resolved.len());
    let mut max_components = Vec::with_capacity(resolved.len());

    for resolved_column in resolved {
        let column = resolved_column.column;
        let lower = decode_optional_bound(lower_bounds, resolved_column)?;
        let upper = decode_optional_bound(upper_bounds, resolved_column)?;

        // Pick the sort-order extremes per direction: ascending columns sort
        // from the literal minimum, descending columns from the literal maximum.
        let (min_value, max_value) = if column.descending() {
            (upper, lower)
        } else {
            (lower, upper)
        };

        min_components.push(RowGroupBoundaryComponent {
            value: min_value,
            descending: column.descending(),
            nulls_first: column.nulls_first(),
        });
        max_components.push(RowGroupBoundaryComponent {
            value: max_value,
            descending: column.descending(),
            nulls_first: column.nulls_first(),
        });
    }

    Ok((
        RowGroupBoundaryKey::new(min_components),
        RowGroupBoundaryKey::new(max_components),
    ))
}

/// Decode the bound for one sort column out of a bounds map, returning `None`
/// when the column's field id is absent (an all-null optional column).
fn decode_optional_bound(
    bounds: &HashMap<i32, Datum>,
    resolved_column: &ResolvedSortColumn<'_>,
) -> Result<Option<RowGroupBoundaryValue>> {
    let column = resolved_column.column;
    match bounds.get(&resolved_column.field_id) {
        Some(datum) => Ok(Some(datum_to_boundary_value(
            datum,
            column.primitive_type(),
            column.column_name(),
        )?)),
        None => Ok(None),
    }
}

/// Convert a bound [`Datum`] into a [`RowGroupBoundaryValue`] according to the
/// sort column's declared Iceberg primitive type.
///
/// The three sort-key column shapes IceGate uses decode as:
/// - [`PrimitiveType::String`] → [`PrimitiveLiteral::String`] → [`RowGroupBoundaryValue::String`],
/// - [`PrimitiveType::Timestamp`] → [`PrimitiveLiteral::Long`] (micros) → [`RowGroupBoundaryValue::TimestampMicros`],
/// - [`PrimitiveType::Fixed`] → [`PrimitiveLiteral::Binary`] → [`RowGroupBoundaryValue::FixedBytes`].
///
/// `column_name` is only used to make error messages identify the offending
/// column.
///
/// # Errors
///
/// Returns [`Error::CompactRead`] when the column's primitive type is not one
/// of the three supported sort-key shapes, or when the `Datum`'s
/// [`PrimitiveLiteral`] variant does not match the expected shape (for example
/// a `Timestamp` column whose bound is not a `Long`).
fn datum_to_boundary_value(
    datum: &Datum,
    primitive_type: &PrimitiveType,
    column_name: &str,
) -> Result<RowGroupBoundaryValue> {
    match (primitive_type, datum.literal()) {
        (PrimitiveType::String, PrimitiveLiteral::String(value)) => Ok(RowGroupBoundaryValue::String(value.clone())),
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(micros)) => {
            Ok(RowGroupBoundaryValue::TimestampMicros(*micros))
        }
        (PrimitiveType::Fixed(_), PrimitiveLiteral::Binary(bytes)) => {
            Ok(RowGroupBoundaryValue::FixedBytes(bytes.clone()))
        }
        (expected, actual) => Err(Error::CompactRead(format!(
            "sort column '{column_name}' bound type mismatch: expected primitive {expected}, \
             manifest bound decoded as {actual:?}"
        ))),
    }
}

/// ASCII unit-separator used between partition-field tokens. It is a
/// non-printable control byte that never occurs in a tenant id or a day
/// ordinal, so distinct partition tuples always map to distinct strings.
const PARTITION_FIELD_SEPARATOR: char = '\u{1f}';

/// Build a stable textual key for a data file's partition tuple.
///
/// The key is only ever compared for equality and used as a grouping token by
/// the compaction planner; it is never parsed back. Each partition field is
/// rendered to a type-tagged token by [`partition_field_token`] and the tokens
/// are joined by [`PARTITION_FIELD_SEPARATOR`]. Tagging by type keeps tuples
/// such as `("1", 2)` and `(1, "2")` distinct, and the control-byte separator
/// keeps field boundaries unambiguous regardless of string contents.
///
/// For every IceGate table the partition spec is
/// `(tenant_id: identity String, timestamp_day: day Int)`, so the typical key
/// is `s:tenant-a␟i:20250611`.
fn partition_key_string(partition: &Struct) -> String {
    let mut key = String::new();
    for (index, literal) in partition.iter().enumerate() {
        if index > 0 {
            key.push(PARTITION_FIELD_SEPARATOR);
        }
        partition_field_token(literal, &mut key);
    }
    key
}

/// Append one partition field's type-tagged token to `key`.
///
/// A missing field (`None`) renders as the single tag `n`. A present field is
/// tagged by primitive kind (`b`/`i`/`l`/`f`/`d`/`s`/`x`/`q`/`u`) followed by
/// its value; binary/fixed bytes are hex-encoded so the token stays printable
/// and separator-free. The `AboveMax`/`BelowMin` sentinels (which do not occur
/// in materialized partition values) render as fixed tags so the function is
/// total over [`PrimitiveLiteral`].
fn partition_field_token(literal: Option<&Literal>, key: &mut String) {
    use std::fmt::Write as _;

    let Some(Literal::Primitive(primitive)) = literal else {
        // `None` (struct slot empty) or a non-primitive partition value: either
        // way there is no scalar to render. Identity/day/bucket/truncate
        // partition transforms all yield primitives, so the non-primitive arm
        // is defensive rather than expected.
        key.push('n');
        return;
    };

    // `write!` into a String is infallible; the `let _ =` documents that we
    // intentionally ignore the always-`Ok` result instead of unwrapping.
    match primitive {
        PrimitiveLiteral::Boolean(value) => {
            let _ = write!(key, "b:{value}");
        }
        PrimitiveLiteral::Int(value) => {
            let _ = write!(key, "i:{value}");
        }
        PrimitiveLiteral::Long(value) => {
            let _ = write!(key, "l:{value}");
        }
        PrimitiveLiteral::Float(value) => {
            let _ = write!(key, "f:{}", value.into_inner());
        }
        PrimitiveLiteral::Double(value) => {
            let _ = write!(key, "d:{}", value.into_inner());
        }
        PrimitiveLiteral::String(value) => {
            let _ = write!(key, "s:{value}");
        }
        PrimitiveLiteral::Binary(bytes) => {
            key.push_str("x:");
            push_hex(key, bytes);
        }
        PrimitiveLiteral::Int128(value) => {
            let _ = write!(key, "q:{value}");
        }
        PrimitiveLiteral::UInt128(value) => {
            let _ = write!(key, "u:{value}");
        }
        PrimitiveLiteral::AboveMax => key.push_str("+inf"),
        PrimitiveLiteral::BelowMin => key.push_str("-inf"),
    }
}

/// Append the lowercase hex encoding of `bytes` to `key`.
fn push_hex(key: &mut String, bytes: &[u8]) {
    use std::fmt::Write as _;

    for byte in bytes {
        // Two-digit, zero-padded hex per byte keeps the encoding fixed-width
        // and reversible-free of the separator character.
        let _ = write!(key, "{byte:02x}");
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::HashMap;

    use iceberg::spec::{Datum, PrimitiveType};

    use super::{ResolvedSortColumn, datum_to_boundary_value, decode_boundary_keys};
    use crate::Error;
    use crate::merge::sort_key::{RowGroupBoundaryValue, SortColumnsDescriptor};

    #[test]
    fn datum_to_boundary_value_decodes_string() {
        let datum = Datum::string("svc-a");
        let value = datum_to_boundary_value(&datum, &PrimitiveType::String, "service_name").expect("decode string");
        assert_eq!(value, RowGroupBoundaryValue::String("svc-a".to_string()));
    }

    #[test]
    fn datum_to_boundary_value_decodes_timestamp_micros() {
        let datum = Datum::timestamp_micros(1_749_600_000_000_000);
        let value = datum_to_boundary_value(&datum, &PrimitiveType::Timestamp, "timestamp").expect("decode timestamp");
        assert_eq!(value, RowGroupBoundaryValue::TimestampMicros(1_749_600_000_000_000));
    }

    #[test]
    fn datum_to_boundary_value_decodes_fixed_bytes() {
        let bytes: Vec<u8> = (0..16_u8).collect();
        let datum = Datum::fixed(bytes.clone());
        let value = datum_to_boundary_value(&datum, &PrimitiveType::Fixed(16), "trace_id").expect("decode fixed bytes");
        assert_eq!(value, RowGroupBoundaryValue::FixedBytes(bytes));
    }

    #[test]
    fn datum_to_boundary_value_rejects_type_mismatch() {
        // A Timestamp column whose bound decoded as a String must be rejected
        // rather than silently mis-typed.
        let datum = Datum::string("not-a-timestamp");
        match datum_to_boundary_value(&datum, &PrimitiveType::Timestamp, "timestamp") {
            Ok(_) => panic!("type mismatch must be rejected"),
            Err(err) => assert!(matches!(err, Error::CompactRead(_))),
        }
    }

    #[test]
    fn datum_to_boundary_value_rejects_unsupported_primitive() {
        // Boolean is not a sort-key shape IceGate decodes.
        let datum = Datum::bool(true);
        match datum_to_boundary_value(&datum, &PrimitiveType::Boolean, "flag") {
            Ok(_) => panic!("unsupported primitive must be rejected"),
            Err(err) => assert!(matches!(err, Error::CompactRead(_))),
        }
    }

    /// Resolve the logs descriptor's columns to their schema field ids
    /// (`service_name` = 2, `timestamp` = 3) so the boundary decode helper can
    /// be exercised without a live table.
    fn resolved_logs_columns(descriptor: &SortColumnsDescriptor) -> Vec<ResolvedSortColumn<'_>> {
        descriptor
            .columns()
            .iter()
            .map(|column| {
                let field_id = match column.column_name() {
                    "service_name" => 2,
                    "timestamp" => 3,
                    other => panic!("unexpected logs sort column: {other}"),
                };
                ResolvedSortColumn { field_id, column }
            })
            .collect()
    }

    #[test]
    fn decode_boundary_keys_inverts_descending_timestamp_so_min_le_max() {
        // The logs sort key is (service_name ASC, timestamp DESC). With a single
        // service_name value per file and a timestamp range whose literal lower
        // bound is smaller than its literal upper bound, a naive
        // lower->min/upper->max mapping would make min_key sort AFTER max_key on
        // the descending timestamp component. Direction-aware assembly must draw
        // the descending column's min from the UPPER bound, keeping min <= max.
        let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");
        let resolved = resolved_logs_columns(descriptor);

        let lower: HashMap<i32, Datum> =
            HashMap::from([(2, Datum::string("svc-a")), (3, Datum::timestamp_micros(100))]);
        let upper: HashMap<i32, Datum> =
            HashMap::from([(2, Datum::string("svc-a")), (3, Datum::timestamp_micros(900))]);

        let (min_key, max_key) = decode_boundary_keys(&resolved, &lower, &upper).expect("decode boundary keys");

        // Both keys carry (service_name, timestamp).
        assert_eq!(min_key.components().len(), 2);
        assert_eq!(max_key.components().len(), 2);

        // The descending timestamp component (index 1) of min_key must hold the
        // literal MAXIMUM (900), and max_key the literal minimum (100).
        assert_eq!(
            min_key.components()[1].value,
            Some(RowGroupBoundaryValue::TimestampMicros(900))
        );
        assert_eq!(
            max_key.components()[1].value,
            Some(RowGroupBoundaryValue::TimestampMicros(100))
        );

        // Under the sort-aware comparison, min_key must not sort after max_key.
        assert_ne!(min_key.compare(&max_key), Ordering::Greater);
    }

    #[test]
    fn decode_boundary_keys_maps_missing_bound_to_none() {
        // An optional sort column absent from the bounds map (an all-null
        // column Iceberg omits) yields a None-valued component on both keys.
        let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");
        let resolved = resolved_logs_columns(descriptor);

        // Only timestamp present; service_name (field id 2) omitted entirely.
        let lower: HashMap<i32, Datum> = HashMap::from([(3, Datum::timestamp_micros(100))]);
        let upper: HashMap<i32, Datum> = HashMap::from([(3, Datum::timestamp_micros(900))]);

        let (min_key, max_key) = decode_boundary_keys(&resolved, &lower, &upper).expect("decode boundary keys");

        assert_eq!(min_key.components()[0].value, None);
        assert_eq!(max_key.components()[0].value, None);
        // Timestamp is still present and inverted for the descending direction.
        assert_eq!(
            min_key.components()[1].value,
            Some(RowGroupBoundaryValue::TimestampMicros(900))
        );
    }
}
