//! Filters the committed Iceberg scan cannot bind, on a table that has a WAL topic.
//!
//! The engine answers such a table from two readers: the WAL segments the Shifter
//! has not committed yet, and the committed Iceberg snapshot. A null check of a
//! list, struct or MAP column, and a filter holding a cast to a floating type, do
//! not translate into an Iceberg predicate that binds, so they must stay above
//! both readers. Each case runs the same query while the rows sit in the WAL,
//! while they are split across both readers, and once all of them are committed,
//! and expects the same rows every time.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "support/wal_fixture.rs"]
mod wal_fixture;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray, StringArray, TimestampMicrosecondArray,
    new_empty_array, new_null_array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use icegate_common::{ICEGATE_NAMESPACE, SPANS_TABLE, SPANS_TOPIC, icegate_table_ident, schema};
use icegate_query::engine::{QueryEngine, QueryEngineConfig};
use icegate_queue::ParquetQueueReader;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use wal_fixture::{
    build_catalog, build_empty_attribute_map, commit_shifted_rows, derive_arrow_schema, write_wal_segment,
};

/// Segments seeded into the WAL: offsets `0..SEGMENT_COUNT`, one span each.
const SEGMENT_COUNT: u64 = 4;

/// Tenant every seeded span belongs to; nothing here tests tenant scoping.
const TENANT_ID: &str = "acme";

/// Fixed start, end and ingestion time of every seeded span, in microseconds since the epoch.
const TIMESTAMP_MICROS: i64 = 1_700_000_000_000_000;

/// Where the seeded spans sit when a query runs.
#[derive(Debug, Clone, Copy)]
enum RowPlacement {
    /// No snapshot: every span is read from the WAL.
    AllInWal,
    /// Segments `0..=1` committed: spans 0 and 1 come from Iceberg, 2 and 3 from the WAL.
    SplitAcrossReaders,
    /// Every segment committed: every span is read from Iceberg.
    AllInIceberg,
}

impl RowPlacement {
    /// The offset the Shifter commit records, if there is one.
    const fn committed_offset(self) -> Option<u64> {
        match self {
            Self::AllInWal => None,
            Self::SplitAcrossReaders => Some(1),
            Self::AllInIceberg => Some(SEGMENT_COUNT - 1),
        }
    }
}

/// Every placement a case runs its query under.
const ROW_PLACEMENTS: [RowPlacement; 3] = [
    RowPlacement::AllInWal,
    RowPlacement::SplitAcrossReaders,
    RowPlacement::AllInIceberg,
];

/// The `events` column: no list for an even offset, an empty list for an odd one.
fn build_events(arrow_schema: &ArrowSchemaRef, offsets: &[u64]) -> ArrayRef {
    let field = arrow_schema.field_with_name(schema::COL_EVENTS).expect("events column");
    let DataType::List(item_field) = field.data_type() else {
        panic!("events must be a list in the canonical spans schema");
    };
    let validity = NullBuffer::from(offsets.iter().map(|offset| offset % 2 == 1).collect::<Vec<bool>>());
    Arc::new(
        ListArray::try_new(
            Arc::clone(item_field),
            OffsetBuffer::new_zeroed(offsets.len()),
            new_empty_array(item_field.data_type()),
            Some(validity),
        )
        .expect("events list"),
    )
}

/// One span per offset, in the canonical `spans` schema: span `k` is named `span-k`, has
/// status code `k`, and has no `events` list when `k` is even.
///
/// Only those columns and the required ones carry values; every other column is null. A
/// required column the fixture does not fill panics, so the fixture breaks loudly when the
/// schema gains one.
fn build_spans_batch(arrow_schema: &ArrowSchemaRef, offsets: &[u64]) -> RecordBatch {
    let row_count = offsets.len();
    let timestamps: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![TIMESTAMP_MICROS; row_count]));
    let span_number = |offset: &u64| u8::try_from(*offset).expect("offset fits a byte");
    let mut columns: HashMap<&str, ArrayRef> = HashMap::from([
        (
            schema::COL_TENANT_ID,
            Arc::new(StringArray::from(vec![TENANT_ID; row_count])) as ArrayRef,
        ),
        (
            schema::COL_TRACE_ID,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(offsets.iter().map(|offset| {
                    let mut id = [0_u8; 16];
                    id[0] = span_number(offset);
                    id
                }))
                .expect("trace ids"),
            ),
        ),
        (
            schema::COL_SPAN_ID,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(offsets.iter().map(|offset| {
                    let mut id = [0_u8; 8];
                    id[0] = span_number(offset);
                    id
                }))
                .expect("span ids"),
            ),
        ),
        (schema::COL_TIMESTAMP, Arc::clone(&timestamps)),
        (schema::COL_END_TIMESTAMP, Arc::clone(&timestamps)),
        (schema::COL_INGESTED_TIMESTAMP, timestamps),
        (
            schema::COL_DURATION_MICROS,
            Arc::new(Int64Array::from(vec![0_i64; row_count])),
        ),
        (
            schema::COL_NAME,
            Arc::new(StringArray::from(
                offsets.iter().map(|offset| format!("span-{offset}")).collect::<Vec<String>>(),
            )),
        ),
        (
            schema::COL_STATUS_CODE,
            Arc::new(Int32Array::from(
                offsets
                    .iter()
                    .map(|offset| i32::from(span_number(offset)))
                    .collect::<Vec<i32>>(),
            )),
        ),
        (schema::COL_EVENTS, build_events(arrow_schema, offsets)),
    ]);
    columns.extend(
        [
            schema::COL_RESOURCE_ATTRIBUTES,
            schema::COL_SPAN_ATTRIBUTES,
            schema::COL_SCOPE_ATTRIBUTES,
        ]
        .map(|column| (column, build_empty_attribute_map(arrow_schema, column, row_count))),
    );

    let arrays: Vec<ArrayRef> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            columns.remove(field.name().as_str()).unwrap_or_else(|| {
                assert!(
                    field.is_nullable(),
                    "the fixture must fill required column {}",
                    field.name()
                );
                new_null_array(field.data_type(), row_count)
            })
        })
        .collect();
    assert!(
        columns.is_empty(),
        "every filled column must exist in the schema: {:?}",
        columns.keys()
    );

    RecordBatch::try_new(Arc::clone(arrow_schema), arrays).expect("spans batch")
}

/// The sorted names of the spans `where_clause` selects from the `spans` table, with the
/// seeded spans placed as `placement` says.
async fn query_span_names(placement: RowPlacement, where_clause: &str) -> Vec<String> {
    let (catalog, _warehouse) = build_catalog(SPANS_TABLE, schema::spans_schema().expect("spans schema")).await;
    let table = catalog
        .load_table(&icegate_table_ident(SPANS_TABLE))
        .await
        .expect("load spans table");
    let arrow_schema = derive_arrow_schema(&table);
    let wal_store = Arc::new(InMemory::new());
    for offset in 0..SEGMENT_COUNT {
        write_wal_segment(
            &wal_store,
            SPANS_TOPIC,
            offset,
            &build_spans_batch(&arrow_schema, &[offset]),
        )
        .await;
    }
    if let Some(committed_offset) = placement.committed_offset() {
        let offsets: Vec<u64> = (0..=committed_offset).collect();
        commit_shifted_rows(
            &catalog,
            &table,
            build_spans_batch(&arrow_schema, &offsets),
            committed_offset,
        )
        .await;
    }

    let wal_reader = Arc::new(
        ParquetQueueReader::new("", Arc::clone(&wal_store) as Arc<dyn ObjectStore>, 8_192).expect("wal reader"),
    );
    let engine = QueryEngine::new(
        Arc::clone(&catalog),
        QueryEngineConfig {
            wal_query_enabled: true,
            ..QueryEngineConfig::default()
        },
        Arc::clone(&wal_store) as Arc<dyn ObjectStore>,
        wal_reader,
    );
    let session = engine.create_session().await.expect("session");
    let batches = session
        .sql(&format!(
            "SELECT {name} FROM iceberg.{ICEGATE_NAMESPACE}.{SPANS_TABLE} WHERE {where_clause}",
            name = schema::COL_NAME
        ))
        .await
        .unwrap_or_else(|error| panic!("{placement:?}: plan `{where_clause}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{placement:?}: run `{where_clause}`: {error}"));

    let mut names: Vec<String> = Vec::new();
    for batch in &batches {
        let column = batch
            .column_by_name(schema::COL_NAME)
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name is a string column");
        names.extend((0..column.len()).map(|row| column.value(row).to_string()));
    }
    names.sort();
    names
}

/// The regression: a null check of the `events` list reached the committed Iceberg scan as an
/// Iceberg predicate on a list column, which does not bind, so the query failed as soon as any
/// row was committed. Spans 0 and 2 have no list, spans 1 and 3 an empty one.
#[tokio::test]
async fn a_null_check_on_a_list_column_answers_the_same_spans_from_the_wal_as_from_iceberg() {
    for placement in ROW_PLACEMENTS {
        assert_eq!(
            query_span_names(placement, &format!("{} IS NULL", schema::COL_EVENTS)).await,
            vec!["span-0", "span-2"],
            "{placement:?}: IS NULL"
        );
        assert_eq!(
            query_span_names(placement, &format!("{} IS NOT NULL", schema::COL_EVENTS)).await,
            vec!["span-1", "span-3"],
            "{placement:?}: IS NOT NULL"
        );
    }
}

/// The same regression on a MAP column: every span carries an empty `span_attributes` map, so
/// the column is never null and only an unbound predicate could fail the query.
#[tokio::test]
async fn a_null_check_on_a_map_column_answers_the_same_spans_from_the_wal_as_from_iceberg() {
    for placement in ROW_PLACEMENTS {
        assert_eq!(
            query_span_names(placement, &format!("{} IS NOT NULL", schema::COL_SPAN_ATTRIBUTES)).await,
            vec!["span-0", "span-1", "span-2", "span-3"],
            "{placement:?}: IS NOT NULL"
        );
        assert_eq!(
            query_span_names(placement, &format!("{} IS NULL", schema::COL_SPAN_ATTRIBUTES)).await,
            Vec::<String>::new(),
            "{placement:?}: IS NULL"
        );
    }
}

/// A filter holding a cast to a floating type reaches the readers with the cast intact or not
/// at all: the Iceberg translation drops a cast and compares the bare column, so a cast of an
/// integer column against a fractional literal must not reach it. Span `k` has status code `k`.
#[tokio::test]
async fn a_cast_to_a_floating_type_answers_the_same_spans_from_the_wal_as_from_iceberg() {
    for placement in ROW_PLACEMENTS {
        assert_eq!(
            query_span_names(placement, &format!("CAST({} AS DOUBLE) > 1.5", schema::COL_STATUS_CODE)).await,
            vec!["span-2", "span-3"],
            "{placement:?}"
        );
    }
}
