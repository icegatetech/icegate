//! Encoding observations to Arrow and committing them to `icegate.prices`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use iceberg::Catalog;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use icegate_common::iceberg_write::{WriteConfig, write_record_batches_to_parquet};
use icegate_common::{PRICES_TABLE, icegate_table_ident};
use tokio_util::sync::CancellationToken;

use crate::error::{MaintainError, Result};
use crate::pricing::decimal::{RATE_PRECISION, RATE_SCALE, rate_to_unscaled};
use crate::pricing::source::RateObservation;

/// Parquet row-group size for the prices writer.
///
/// The table holds at most a few thousand rows per commit (one crawl's worth of
/// changed rates), so this only needs to be a sane default, not tuned: it never
/// approaches the row-group boundary in practice.
const ROW_GROUP_SIZE: usize = 20_000;

/// Parquet data-page size limit in bytes, matching the ingest/compaction default.
const DATA_PAGE_SIZE_LIMIT_BYTES: usize = 2 * 1024 * 1024;

/// Rolling-writer rollover budget in bytes.
///
/// A crawl's worth of rate rows is at most a few hundred KB encoded, far below
/// this; it exists only as a failover ceiling, never expected to trigger.
const MAX_FILE_SIZE_BYTES: u64 = 128 * 1024 * 1024;

/// Build the Arrow batch for a set of rate rows.
///
/// Column order is taken from [`icegate_common::schema::prices_schema`] rather
/// than written out by hand: Arrow columns are positional, so a hand-maintained
/// order would silently write rates into the wrong columns if the schema grew.
///
/// # Errors
///
/// Returns an error if the Iceberg schema cannot be converted to Arrow or the
/// column arrays do not line up with it.
pub fn build_price_batch(rates: &[RateObservation]) -> Result<RecordBatch> {
    let iceberg_schema = icegate_common::schema::prices_schema()
        .map_err(|e| MaintainError::InvariantViolation(format!("prices schema: {e}")))?;
    let arrow_schema = Arc::new(
        schema_to_arrow_schema(&iceberg_schema)
            .map_err(|e| MaintainError::InvariantViolation(format!("prices arrow schema: {e}")))?,
    );

    let columns: Vec<ArrayRef> = vec![
        required_text(rates, |r| &r.provider),
        required_text(rates, |r| &r.model),
        optional_text(rates, |r| r.canonical_id.as_deref()),
        required_text(rates, |r| &r.service_tier),
        required_text(rates, |r| &r.region),
        Arc::new(Int64Array::from_iter_values(rates.iter().map(|r| r.min_input_tokens))),
        Arc::new(rates.iter().map(|r| r.max_input_tokens).collect::<Int64Array>()),
        Arc::new(TimestampMicrosecondArray::from_iter_values(
            rates.iter().map(|r| r.valid_from.timestamp_micros()),
        )),
        required_text(rates, |r| r.valid_from_source.as_str()),
        optional_decimal(rates, |r| r.input_usd_per_1m)?,
        optional_decimal(rates, |r| r.output_usd_per_1m)?,
        optional_decimal(rates, |r| r.cache_read_usd_per_1m)?,
        optional_decimal(rates, |r| r.cache_write_usd_per_1m)?,
        optional_decimal(rates, |r| r.reasoning_usd_per_1m)?,
        optional_decimal(rates, |r| r.request_usd)?,
        optional_decimal(rates, |r| r.image_input_usd_per_unit)?,
        optional_decimal(rates, |r| r.image_output_usd_per_unit)?,
        optional_decimal(rates, |r| r.audio_input_usd_per_second)?,
        optional_decimal(rates, |r| r.audio_output_usd_per_second)?,
        required_text(rates, |r| &r.currency),
        required_text(rates, |r| &r.source),
        optional_text(rates, |r| r.source_url.as_deref()),
    ];

    RecordBatch::try_new(arrow_schema, columns)
        .map_err(|e| MaintainError::InvariantViolation(format!("prices batch: {e}")))
}

/// Build a required (non-null) `Utf8` column by applying `field` to every row.
fn required_text<'a>(rates: &'a [RateObservation], field: impl Fn(&'a RateObservation) -> &'a str) -> ArrayRef {
    Arc::new(rates.iter().map(|r| Some(field(r))).collect::<StringArray>())
}

/// Build an optional `Utf8` column by applying `field` to every row.
fn optional_text<'a>(rates: &'a [RateObservation], field: impl Fn(&'a RateObservation) -> Option<&'a str>) -> ArrayRef {
    Arc::new(rates.iter().map(field).collect::<StringArray>())
}

/// Build an optional `Decimal(RATE_PRECISION, RATE_SCALE)` column.
///
/// Each rate is encoded to its unscaled `i128` via [`rate_to_unscaled`], which
/// rounds onto the storage grid — the same rounding the crawl applies before the
/// diff, so a stored rate and its re-fetched twin compare equal.
///
/// # Errors
///
/// Returns an error if the built array's precision/scale are rejected — a
/// programmer error, since they come from the fixed schema constants.
fn optional_decimal(rates: &[RateObservation], field: impl Fn(&RateObservation) -> Option<f64>) -> Result<ArrayRef> {
    // `rate_to_unscaled` is fallible (it refuses non-finite / out-of-range rather
    // than letting the `as i128` cast saturate silently); `transpose` + `?` lifts
    // that failure out of the per-cell `Option` instead of swallowing it.
    let unscaled = rates
        .iter()
        .map(|r| field(r).map(rate_to_unscaled).transpose())
        .collect::<Result<Vec<Option<i128>>>>()?;
    let array = Decimal128Array::from(unscaled)
        .with_precision_and_scale(RATE_PRECISION, RATE_SCALE)
        .map_err(|e| MaintainError::InvariantViolation(format!("prices decimal column: {e}")))?;
    Ok(Arc::new(array))
}

/// Append rate rows to `icegate.prices` in a single commit.
///
/// A crawl that produces no rows must not call this: an empty `fast_append`
/// would create a snapshot recording nothing, polluting the table's history.
/// Unlike the ingest WAL-fed tables, this append sets no snapshot property —
/// `prices` has no offset to track, since a crawl either produces rows to
/// append or it doesn't.
///
/// # Errors
///
/// Returns an error if the table cannot be loaded, the Parquet writer fails, or
/// the transaction cannot be committed.
pub async fn append_prices(catalog: &Arc<dyn Catalog>, rates: &[RateObservation]) -> Result<()> {
    if rates.is_empty() {
        return Ok(());
    }
    let batch = build_price_batch(rates)?;
    let ident = icegate_table_ident(PRICES_TABLE);
    let table = catalog.load_table(&ident).await?;

    // Writer construction mirrors the ingest shift / compaction paths, both of
    // which funnel through this same shared pipeline (see
    // `icegate_common::iceberg_write`) rather than each hand-building the
    // ParquetWriterBuilder -> RollingFileWriterBuilder -> DataFileWriterBuilder
    // chain; prices reuses it instead of adding a third copy.
    let cfg = WriteConfig {
        row_group_size: ROW_GROUP_SIZE,
        data_page_size_limit_bytes: DATA_PAGE_SIZE_LIMIT_BYTES,
        max_file_size_bytes: MAX_FILE_SIZE_BYTES,
        bloom_filter_columns: &[],
        column_encodings: &[],
    };
    let stream = futures::stream::once(async { Ok(batch) }).boxed();
    // The Parquet write consumes the `Table` but cannot invalidate its metadata
    // (it only uploads new files), so the transaction reuses this snapshot
    // rather than paying a second catalog round trip to reload it.
    let written = write_record_batches_to_parquet(table.clone(), cfg, stream, &CancellationToken::new()).await?;

    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(written.data_files);
    let tx = append_action
        .apply(tx)
        .map_err(|e| MaintainError::Storage(format!("prices fast append: {e}")))?;
    tx.commit(catalog.as_ref())
        .await
        .map_err(|e| MaintainError::Storage(format!("prices commit: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    // `float_cmp`: rate values round-trip through the decimal grid exactly, so an
    // exact `assert_eq!` is the correct assertion, not an epsilon comparison.
    #![allow(clippy::float_cmp)]

    use chrono::DateTime;

    use super::build_price_batch;
    use crate::pricing::source::{RateObservation, ValidFromSource};

    fn rate() -> RateObservation {
        RateObservation {
            provider: "anthropic".to_string(),
            model: "claude-opus-4-8".to_string(),
            canonical_id: Some("anthropic/claude-opus-4-8".to_string()),
            service_tier: "standard".to_string(),
            region: "global".to_string(),
            min_input_tokens: 0,
            max_input_tokens: None,
            valid_from: DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp"),
            valid_from_source: ValidFromSource::Observed,
            input_usd_per_1m: Some(5.0),
            output_usd_per_1m: Some(25.0),
            cache_read_usd_per_1m: None,
            cache_write_usd_per_1m: None,
            reasoning_usd_per_1m: None,
            request_usd: None,
            image_input_usd_per_unit: None,
            image_output_usd_per_unit: None,
            audio_input_usd_per_second: None,
            audio_output_usd_per_second: None,
            currency: "USD".to_string(),
            source: "litellm".to_string(),
            source_url: Some("https://example.invalid/card.json".to_string()),
        }
    }

    /// A second, differently-valued observation. Distinct on provider, model,
    /// tier, prices, `valid_from_source`, and `source_url` from [`rate`], so a
    /// bug that overwrites every row with row 0's values is caught by a
    /// multi-row batch built from `[rate(), other_rate()]`.
    fn other_rate() -> RateObservation {
        RateObservation {
            provider: "openai".to_string(),
            model: "gpt-5".to_string(),
            canonical_id: None,
            service_tier: "batch".to_string(),
            region: "eu-west-1".to_string(),
            min_input_tokens: 128_000,
            max_input_tokens: Some(256_000),
            valid_from: DateTime::from_timestamp(1_770_000_000, 0).expect("valid timestamp"),
            valid_from_source: ValidFromSource::Vendor,
            input_usd_per_1m: Some(1.5),
            output_usd_per_1m: Some(7.5),
            cache_read_usd_per_1m: Some(0.5),
            cache_write_usd_per_1m: Some(2.0),
            reasoning_usd_per_1m: Some(9.0),
            request_usd: Some(0.01),
            image_input_usd_per_unit: Some(0.02),
            image_output_usd_per_unit: Some(0.03),
            audio_input_usd_per_second: Some(0.001),
            audio_output_usd_per_second: Some(0.002),
            currency: "USD".to_string(),
            source: "openrouter".to_string(),
            source_url: None,
        }
    }

    #[test]
    fn batch_column_order_matches_the_iceberg_schema() {
        // Arrow columns are positional. A drift here writes rates into the wrong
        // columns without any error.
        let batch = build_price_batch(&[rate()]).expect("batch builds");
        let iceberg = icegate_common::schema::prices_schema().expect("schema builds");
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let expected: Vec<String> = iceberg.as_struct().fields().iter().map(|f| f.name.clone()).collect();
        assert_eq!(names, expected.iter().map(String::as_str).collect::<Vec<_>>());
    }

    #[test]
    fn batch_round_trips_a_single_row() {
        let batch = build_price_batch(&[rate()]).expect("batch builds");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 22);
    }

    #[test]
    fn nulls_survive_encoding() {
        let batch = build_price_batch(&[rate()]).expect("batch builds");
        let schema = batch.schema();
        let idx = schema.index_of("cache_read_usd_per_1m").expect("column present");
        assert!(batch.column(idx).is_null(0));
    }

    #[test]
    fn valid_from_source_is_written_as_its_label() {
        use arrow::array::StringArray;
        let batch = build_price_batch(&[rate()]).expect("batch builds");
        let schema = batch.schema();
        let idx = schema.index_of("valid_from_source").expect("column present");
        let col = batch.column(idx).as_any().downcast_ref::<StringArray>().expect("utf8 column");
        assert_eq!(col.value(0), "observed");
    }

    #[test]
    fn a_multi_row_batch_keeps_each_rows_own_values() {
        // Every test above uses a single row, which cannot catch a bug that
        // writes row 0's value into every row of a column array (e.g. an
        // `Iterator::map` that closes over the first element instead of `r`).
        // Two rows differing on almost every field pin that each column array
        // is built by iterating `rates`, not by repeating one row's value.
        use arrow::array::{Array, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray};

        use crate::pricing::decimal::unscaled_to_rate;

        let batch = build_price_batch(&[rate(), other_rate()]).expect("batch builds");
        assert_eq!(batch.num_rows(), 2);
        let schema = batch.schema();

        let col = |name: &str| batch.column(schema.index_of(name).expect("column present"));

        let provider = col("provider").as_any().downcast_ref::<StringArray>().expect("utf8");
        assert_eq!(provider.value(0), "anthropic");
        assert_eq!(provider.value(1), "openai");

        let model = col("model").as_any().downcast_ref::<StringArray>().expect("utf8");
        assert_eq!(model.value(0), "claude-opus-4-8");
        assert_eq!(model.value(1), "gpt-5");

        let min_input_tokens = col("min_input_tokens").as_any().downcast_ref::<Int64Array>().expect("int64");
        assert_eq!(min_input_tokens.value(0), 0);
        assert_eq!(min_input_tokens.value(1), 128_000);

        let max_input_tokens = col("max_input_tokens").as_any().downcast_ref::<Int64Array>().expect("int64");
        assert!(max_input_tokens.is_null(0));
        assert_eq!(max_input_tokens.value(1), 256_000);

        let input_usd = col("input_usd_per_1m")
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal128");
        assert_eq!(unscaled_to_rate(input_usd.value(0)), 5.0);
        assert_eq!(unscaled_to_rate(input_usd.value(1)), 1.5);

        let cache_read = col("cache_read_usd_per_1m")
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal128");
        assert!(cache_read.is_null(0));
        assert_eq!(unscaled_to_rate(cache_read.value(1)), 0.5);

        let valid_from_source = col("valid_from_source").as_any().downcast_ref::<StringArray>().expect("utf8");
        assert_eq!(valid_from_source.value(0), "observed");
        assert_eq!(valid_from_source.value(1), "vendor");

        let source_url = col("source_url").as_any().downcast_ref::<StringArray>().expect("utf8");
        assert!(!source_url.is_null(0));
        assert!(source_url.is_null(1));

        let valid_from = col("valid_from")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");
        assert_eq!(valid_from.value(0), 1_760_000_000_000_000);
        assert_eq!(valid_from.value(1), 1_770_000_000_000_000);
    }

    #[test]
    fn empty_input_builds_a_zero_row_batch() {
        // `append_prices` short-circuits on an empty slice before ever calling
        // this, but `build_price_batch` itself must not panic or misbehave on
        // one — an empty crawl result should never reach the writer with a
        // botched schema.
        let batch = build_price_batch(&[]).expect("batch builds");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 22);
    }
}
