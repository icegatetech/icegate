//! What a query sees after WAL retention cleanup has run underneath it.
//!
//! The scenario both cases share is the one the two services actually produce:
//! segments are written to the WAL, the Shifter commits a prefix of them into
//! Iceberg and records the offset it reached, a query is planned against the
//! catalog provider of THAT moment, and cleanup then deletes below a bound
//! derived from a later commit. Whether the query still answers depends on one
//! thing only — whether the bound stayed at or below the boundary the planned
//! query reads the WAL from, which is what `wal_cleanup.keep_segments_count`
//! buys in the maintain service.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "support/wal_fixture.rs"]
mod wal_fixture;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use iceberg::Catalog;
use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE, LOGS_TOPIC, icegate_table_ident, schema};
use icegate_query::engine::{QueryEngine, QueryEngineConfig};
use icegate_queue::{DeleteLimits, ParquetQueueReader, QueueCleaner, SegmentId};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio_util::sync::CancellationToken;
use wal_fixture::{
    build_catalog, build_empty_attribute_map, commit_shifted_rows, derive_arrow_schema, write_wal_segment,
};

/// Segments seeded into the WAL: offsets `0..SEGMENT_COUNT`, one row each.
const SEGMENT_COUNT: u64 = 6;

/// Offset the first commit records: segments `0..=2` are in Iceberg, and a query
/// planned on that commit reads the WAL from segment 3.
const FIRST_COMMITTED_OFFSET: u64 = 2;

/// Offset the second commit records: the whole seeded queue is in Iceberg, which
/// is what lets a cleanup bound reach above [`FIRST_COMMITTED_OFFSET`].
const SECOND_COMMITTED_OFFSET: u64 = SEGMENT_COUNT - 1;

/// Tenant every seeded row belongs to; nothing here tests tenant scoping.
const TENANT_ID: &str = "acme";

/// Fixed timestamp for every seeded row, in microseconds since the epoch.
const TIMESTAMP_MICROS: i64 = 1_700_000_000_000_000;

/// The body of the row that segment `offset` carries, and the value the
/// assertions compare on.
fn body_of_segment(offset: u64) -> String {
    format!("segment-{offset}")
}

/// The bodies a query must return when it can see segments `offsets`.
fn bodies_of_segments(offsets: impl IntoIterator<Item = u64>) -> Vec<String> {
    let mut bodies: Vec<String> = offsets.into_iter().map(body_of_segment).collect();
    bodies.sort();
    bodies
}

/// One log row per offset, in the canonical `logs` schema.
///
/// Only `body` distinguishes the rows; the other required columns carry fixed
/// values — the three attribute maps an empty map per row, since the schema
/// requires them — and the optional ones are left null.
fn logs_batch(arrow_schema: &ArrowSchemaRef, offsets: impl IntoIterator<Item = u64>) -> RecordBatch {
    let bodies: Vec<String> = offsets.into_iter().map(body_of_segment).collect();
    let row_count = bodies.len();

    let timestamps: ArrayRef = Arc::new(datafusion::arrow::array::TimestampMicrosecondArray::from(
        vec![TIMESTAMP_MICROS; row_count],
    ));
    let mut columns: HashMap<&str, ArrayRef> = HashMap::from([
        (
            schema::COL_TENANT_ID,
            Arc::new(StringArray::from(vec![TENANT_ID; row_count])) as ArrayRef,
        ),
        (schema::COL_TIMESTAMP, Arc::clone(&timestamps)),
        (schema::COL_OBSERVED_TIMESTAMP, Arc::clone(&timestamps)),
        (schema::COL_INGESTED_TIMESTAMP, timestamps),
        (schema::COL_BODY, Arc::new(StringArray::from(bodies)) as ArrayRef),
    ]);
    columns.extend(
        [
            schema::COL_RESOURCE_ATTRIBUTES,
            schema::COL_SCOPE_ATTRIBUTES,
            schema::COL_LOG_ATTRIBUTES,
        ]
        .map(|column| (column, build_empty_attribute_map(arrow_schema, column, row_count))),
    );

    let arrays: Vec<ArrayRef> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            columns.get(field.name().as_str()).map_or_else(
                || datafusion::arrow::array::new_null_array(field.data_type(), row_count),
                Arc::clone,
            )
        })
        .collect();

    RecordBatch::try_new(Arc::clone(arrow_schema), arrays).expect("logs batch")
}

/// The seeded world: a catalog whose `logs` table holds the first commit and a
/// WAL store holding every segment.
struct Fixture {
    catalog: Arc<dyn Catalog>,
    wal_store: Arc<InMemory>,
    _warehouse: tempfile::TempDir,
}

impl Fixture {
    /// Seed the WAL and commit segments `0..=FIRST_COMMITTED_OFFSET` into
    /// Iceberg.
    async fn seed() -> Self {
        let (catalog, warehouse) = build_catalog(LOGS_TABLE, schema::logs_schema().expect("logs schema")).await;
        let table = catalog
            .load_table(&icegate_table_ident(LOGS_TABLE))
            .await
            .expect("load logs table");
        let arrow_schema = derive_arrow_schema(&table);

        let wal_store = Arc::new(InMemory::new());
        for offset in 0..SEGMENT_COUNT {
            write_wal_segment(&wal_store, LOGS_TOPIC, offset, &logs_batch(&arrow_schema, [offset])).await;
        }
        commit_shifted_rows(
            &catalog,
            &table,
            logs_batch(&arrow_schema, 0..=FIRST_COMMITTED_OFFSET),
            FIRST_COMMITTED_OFFSET,
        )
        .await;

        Self {
            catalog,
            wal_store,
            _warehouse: warehouse,
        }
    }

    /// An engine whose provider cache is cold, so the next session it creates
    /// plans against the catalog as it stands NOW.
    ///
    /// A second session off the same engine would not: within `max_age_secs` it
    /// reuses the provider already cached, which is exactly the staleness these
    /// cases are about. Background refresh is deliberately never started — every
    /// provider here is built by the session that needs it.
    fn build_engine(&self) -> Arc<QueryEngine> {
        let engine_config = QueryEngineConfig {
            wal_query_enabled: true,
            ..QueryEngineConfig::default()
        };
        let wal_reader = Arc::new(
            ParquetQueueReader::new("", Arc::clone(&self.wal_store) as Arc<dyn ObjectStore>, 8_192)
                .expect("wal reader"),
        );
        Arc::new(QueryEngine::new(
            Arc::clone(&self.catalog),
            engine_config,
            Arc::clone(&self.wal_store) as Arc<dyn ObjectStore>,
            wal_reader,
        ))
    }

    /// Commit the segments the first commit left behind, recording
    /// [`SECOND_COMMITTED_OFFSET`]: every seeded row is then in Iceberg, and a
    /// provider built after this reads the WAL from above the whole queue.
    async fn shift_the_rest(&self) {
        let table = self
            .catalog
            .load_table(&icegate_table_ident(LOGS_TABLE))
            .await
            .expect("load logs table");
        commit_shifted_rows(
            &self.catalog,
            &table,
            logs_batch(
                &derive_arrow_schema(&table),
                FIRST_COMMITTED_OFFSET + 1..=SECOND_COMMITTED_OFFSET,
            ),
            SECOND_COMMITTED_OFFSET,
        )
        .await;
    }

    /// Commit the rest of the seeded segments, then delete ONE segment from the
    /// middle of the range an older provider still reads.
    ///
    /// That is the state a cleanup cycle leaves when the store refuses a single
    /// key and the cycle steps over it: the run keeps its floor and breaks above
    /// it. The key goes by hand because [`QueueCleaner`]'s bound is inclusive
    /// and cannot single out one offset — the same reason
    /// `icegate-queue`'s `reader_integration.rs` punches its hole directly.
    async fn shift_the_rest_and_punch_a_hole(&self, missing_offset: u64) {
        self.shift_the_rest().await;

        let missing = SegmentId {
            topic: LOGS_TOPIC.to_string(),
            offset: missing_offset,
        }
        .to_relative_path();
        self.wal_store
            .delete(&missing)
            .await
            .expect("delete the segment the case is about");

        let listing = self
            .wal_store
            .list_with_delimiter(Some(&Path::from(LOGS_TOPIC)))
            .await
            .expect("list the wal topic");
        assert_eq!(
            listing.objects.len(),
            usize::try_from(SEGMENT_COUNT - 1).expect("segment count fits usize"),
            "the fixture must leave exactly one hole"
        );
        assert!(
            !listing.objects.iter().any(|meta| meta.location == missing),
            "the hole must be at the offset the case is about"
        );
    }

    /// Commit the rest of the seeded segments, then delete every WAL segment at
    /// or below `delete_bound` — the pair of steps that lets cleanup reach above
    /// an older provider's boundary.
    async fn shift_the_rest_and_clean(&self, delete_bound: u64) {
        self.shift_the_rest().await;

        let cleaner = QueueCleaner::new("", Arc::clone(&self.wal_store) as Arc<dyn ObjectStore>);
        let summary = cleaner
            .delete_segments_upto(
                &LOGS_TOPIC.to_string(),
                delete_bound,
                &DeleteLimits {
                    max_deletes: 100,
                    concurrency: 4,
                    dry_run: false,
                },
                &CancellationToken::new(),
            )
            .await
            .expect("cleanup");
        assert_eq!(
            summary.deleted,
            delete_bound + 1,
            "the fixture must actually delete the segments the case is about"
        );
    }
}

/// The bodies a session returns for the `logs` table, sorted.
async fn query_bodies(session: &SessionContext) -> Result<Vec<String>, DataFusionError> {
    let batches = session
        .sql(&format!(
            "SELECT {body} FROM iceberg.{ICEGATE_NAMESPACE}.{LOGS_TABLE}",
            body = schema::COL_BODY
        ))
        .await?
        .collect()
        .await?;

    let mut bodies: Vec<String> = Vec::new();
    for batch in &batches {
        let column = batch
            .column_by_name(schema::COL_BODY)
            .expect("body column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body is a string column");
        for row in 0..column.len() {
            assert!(!column.is_null(row), "no seeded row has a null body");
            bodies.push(column.value(row).to_string());
        }
    }
    bodies.sort();
    Ok(bodies)
}

/// Cleanup that stays at or below the boundary a planned query reads the WAL
/// from takes nothing that query needs: the answer still carries every row,
/// three of them out of Iceberg and three out of the segments left in the WAL.
#[tokio::test]
async fn a_query_planned_before_cleanup_keeps_every_row_when_the_bound_stays_below_its_boundary() {
    let fixture = Fixture::seed().await;
    let session = fixture.build_engine().create_session().await.expect("session");

    fixture.shift_the_rest_and_clean(FIRST_COMMITTED_OFFSET).await;

    assert_eq!(
        query_bodies(&session).await.expect("the query must still answer"),
        bodies_of_segments(0..SEGMENT_COUNT)
    );
}

/// The regression: cleanup reaches ABOVE that boundary, so segment 3 — the first
/// one the planned query reads from the WAL — is gone while the snapshot that
/// provider pinned does not hold its rows either. Answering from the segments
/// left would drop those rows in silence, so the query fails; replanning on a
/// fresh provider is what recovers them, and it returns the full set.
#[tokio::test]
async fn a_query_planned_before_cleanup_fails_when_the_bound_reaches_above_its_boundary() {
    let fixture = Fixture::seed().await;
    let stale_session = fixture.build_engine().create_session().await.expect("session");

    fixture.shift_the_rest_and_clean(FIRST_COMMITTED_OFFSET + 1).await;

    let error = query_bodies(&stale_session)
        .await
        .expect_err("a query missing committed rows must fail, not answer without them");
    assert!(
        matches!(error.find_root(), DataFusionError::Execution(_)),
        "the missing segments must fail the scan, got: {error}"
    );

    let fresh_session = fixture.build_engine().create_session().await.expect("session");
    assert_eq!(
        query_bodies(&fresh_session).await.expect("a fresh provider must answer"),
        bodies_of_segments(0..SEGMENT_COUNT),
        "the rows the stale plan could not reach are committed to Iceberg"
    );
}

/// The other shape retention leaves behind: not a floor above the boundary but a
/// HOLE inside the range the planned query reads, which is what a cycle produces
/// when the store refuses one key and the cycle steps over it. The segments
/// above the hole would be read as following the one below it, so the query must
/// fail here too — through the SAME operator-facing mapping, since the fix is
/// the same `keep_segments_count`.
#[tokio::test]
async fn a_query_planned_before_cleanup_fails_when_a_segment_inside_its_range_is_gone() {
    /// The segment taken out of the stale provider's range. Deliberately not its
    /// first one ([`FIRST_COMMITTED_OFFSET`] + 1): losing that would raise
    /// `SegmentsGone`, and this case is about a break ABOVE an intact floor.
    const MISSING_OFFSET: u64 = FIRST_COMMITTED_OFFSET + 2;

    let fixture = Fixture::seed().await;
    let stale_session = fixture.build_engine().create_session().await.expect("session");

    fixture.shift_the_rest_and_punch_a_hole(MISSING_OFFSET).await;

    let error = query_bodies(&stale_session)
        .await
        .expect_err("a query spanning a hole must fail, not answer across it");
    assert!(
        matches!(error.find_root(), DataFusionError::Execution(_)),
        "the hole must fail the scan, got: {error}"
    );

    let fresh_session = fixture.build_engine().create_session().await.expect("session");
    assert_eq!(
        query_bodies(&fresh_session).await.expect("a fresh provider must answer"),
        bodies_of_segments(0..SEGMENT_COUNT),
        "a provider reading the WAL from above the queue never touches the hole"
    );
}
