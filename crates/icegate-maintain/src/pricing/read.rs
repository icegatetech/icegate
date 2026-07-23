//! Reading `icegate.prices` back into [`RateObservation`]s.
//!
//! The inverse of [`crate::pricing::writer::build_price_batch`]: scans the
//! table's current snapshot and decodes each Arrow row into a
//! [`RateObservation`] by column name, then reduces to the newest revision per
//! key via [`LiveRates::from_rows`].

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use iceberg::Catalog;
use icegate_common::{PRICES_TABLE, icegate_table_ident};

use crate::error::{MaintainError, Result};
use crate::pricing::diff::LiveRates;
use crate::pricing::source::{RateObservation, ValidFromSource};

/// Scan `icegate.prices` and reduce it to the newest revision per rate key.
///
/// Neither an unmigrated catalog (the table does not exist) nor a migrated but
/// never-crawled one (the table exists with no committed snapshot) is an
/// error: both return an empty [`LiveRates`] so the crawler can bootstrap. A
/// missing table is checked explicitly via [`Catalog::table_exists`]; an empty
/// snapshot falls out naturally, since [`iceberg::table::Table::scan`] yields
/// no rows when the table has never been committed to.
///
/// # Errors
///
/// Returns an error if the table exists but the catalog scan fails or a row
/// fails to decode (a column is missing, has an unexpected Arrow type, or
/// carries a value the writer never produces).
pub async fn read_live_rates(catalog: &Arc<dyn Catalog>) -> Result<LiveRates> {
    let ident = icegate_table_ident(PRICES_TABLE);
    if !catalog.table_exists(&ident).await? {
        return Ok(LiveRates::from_rows(Vec::new()));
    }
    let table = catalog.load_table(&ident).await?;
    let mut stream = table.scan().build()?.to_arrow().await?;

    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        rows.extend(decode_batch(&batch)?);
    }
    Ok(LiveRates::from_rows(rows))
}

/// Every `prices` column resolved once per batch by name, then indexed per row.
///
/// Resolving each column once by name via [`RecordBatch::schema`] — rather
/// than decoding positionally — means a reordered or added column in the
/// Iceberg schema fails loudly at the missing/mistyped column it belongs to,
/// not silently at the wrong field.
struct PriceColumns<'a> {
    provider: &'a StringArray,
    model: &'a StringArray,
    canonical_id: &'a StringArray,
    service_tier: &'a StringArray,
    region: &'a StringArray,
    min_input_tokens: &'a Int64Array,
    max_input_tokens: &'a Int64Array,
    valid_from: &'a TimestampMicrosecondArray,
    valid_from_source: &'a StringArray,
    input_usd_per_1m: &'a Float64Array,
    output_usd_per_1m: &'a Float64Array,
    cache_read_usd_per_1m: &'a Float64Array,
    cache_write_usd_per_1m: &'a Float64Array,
    reasoning_usd_per_1m: &'a Float64Array,
    request_usd: &'a Float64Array,
    image_input_usd_per_unit: &'a Float64Array,
    image_output_usd_per_unit: &'a Float64Array,
    audio_input_usd_per_second: &'a Float64Array,
    audio_output_usd_per_second: &'a Float64Array,
    currency: &'a StringArray,
    source: &'a StringArray,
    source_url: &'a StringArray,
}

/// Look up `name` in `batch`'s schema and downcast its column to `T`.
///
/// # Errors
///
/// Returns [`MaintainError::InvariantViolation`] if `name` is absent from the
/// batch or its array is not of Arrow type `T`.
fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| MaintainError::InvariantViolation(format!("prices.{name}: column missing from scanned batch")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| MaintainError::InvariantViolation(format!("prices.{name}: unexpected Arrow array type")))
}

/// Resolve every `prices` column in `batch` by name.
fn resolve_columns(batch: &RecordBatch) -> Result<PriceColumns<'_>> {
    Ok(PriceColumns {
        provider: column(batch, "provider")?,
        model: column(batch, "model")?,
        canonical_id: column(batch, "canonical_id")?,
        service_tier: column(batch, "service_tier")?,
        region: column(batch, "region")?,
        min_input_tokens: column(batch, "min_input_tokens")?,
        max_input_tokens: column(batch, "max_input_tokens")?,
        valid_from: column(batch, "valid_from")?,
        valid_from_source: column(batch, "valid_from_source")?,
        input_usd_per_1m: column(batch, "input_usd_per_1m")?,
        output_usd_per_1m: column(batch, "output_usd_per_1m")?,
        cache_read_usd_per_1m: column(batch, "cache_read_usd_per_1m")?,
        cache_write_usd_per_1m: column(batch, "cache_write_usd_per_1m")?,
        reasoning_usd_per_1m: column(batch, "reasoning_usd_per_1m")?,
        request_usd: column(batch, "request_usd")?,
        image_input_usd_per_unit: column(batch, "image_input_usd_per_unit")?,
        image_output_usd_per_unit: column(batch, "image_output_usd_per_unit")?,
        audio_input_usd_per_second: column(batch, "audio_input_usd_per_second")?,
        audio_output_usd_per_second: column(batch, "audio_output_usd_per_second")?,
        currency: column(batch, "currency")?,
        source: column(batch, "source")?,
        source_url: column(batch, "source_url")?,
    })
}

/// An optional `Utf8` cell.
fn optional_str(array: &StringArray, row: usize) -> Option<String> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row).to_string())
    }
}

/// An optional `Int64` cell.
fn optional_i64(array: &Int64Array, row: usize) -> Option<i64> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

/// An optional `Float64` cell.
fn optional_f64(array: &Float64Array, row: usize) -> Option<f64> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

/// Decode the `valid_from_source` label back into its enum.
///
/// # Errors
///
/// Returns [`MaintainError::InvariantViolation`] for any label other than the
/// two the writer produces — never guessed, since a wrong provenance would mislabel
/// history rather than merely degrade it.
fn decode_valid_from_source(label: &str) -> Result<ValidFromSource> {
    match label {
        "vendor" => Ok(ValidFromSource::Vendor),
        "observed" => Ok(ValidFromSource::Observed),
        other => Err(MaintainError::InvariantViolation(format!(
            "prices.valid_from_source: unrecognised label '{other}'"
        ))),
    }
}

/// Decode one row of `columns` into a [`RateObservation`].
///
/// # Errors
///
/// Returns [`MaintainError::InvariantViolation`] if `valid_from` is not a
/// representable timestamp or `valid_from_source` carries an unrecognised
/// label.
fn decode_row(columns: &PriceColumns<'_>, row: usize) -> Result<RateObservation> {
    let valid_from_micros = columns.valid_from.value(row);
    let valid_from = chrono::DateTime::from_timestamp_micros(valid_from_micros).ok_or_else(|| {
        MaintainError::InvariantViolation(format!(
            "prices.valid_from: {valid_from_micros} is not a representable timestamp"
        ))
    })?;

    Ok(RateObservation {
        provider: columns.provider.value(row).to_string(),
        model: columns.model.value(row).to_string(),
        canonical_id: optional_str(columns.canonical_id, row),
        service_tier: columns.service_tier.value(row).to_string(),
        region: columns.region.value(row).to_string(),
        min_input_tokens: columns.min_input_tokens.value(row),
        max_input_tokens: optional_i64(columns.max_input_tokens, row),
        valid_from,
        valid_from_source: decode_valid_from_source(columns.valid_from_source.value(row))?,
        input_usd_per_1m: optional_f64(columns.input_usd_per_1m, row),
        output_usd_per_1m: optional_f64(columns.output_usd_per_1m, row),
        cache_read_usd_per_1m: optional_f64(columns.cache_read_usd_per_1m, row),
        cache_write_usd_per_1m: optional_f64(columns.cache_write_usd_per_1m, row),
        reasoning_usd_per_1m: optional_f64(columns.reasoning_usd_per_1m, row),
        request_usd: optional_f64(columns.request_usd, row),
        image_input_usd_per_unit: optional_f64(columns.image_input_usd_per_unit, row),
        image_output_usd_per_unit: optional_f64(columns.image_output_usd_per_unit, row),
        audio_input_usd_per_second: optional_f64(columns.audio_input_usd_per_second, row),
        audio_output_usd_per_second: optional_f64(columns.audio_output_usd_per_second, row),
        currency: columns.currency.value(row).to_string(),
        source: columns.source.value(row).to_string(),
        source_url: optional_str(columns.source_url, row),
    })
}

/// Decode every row of one scanned batch into [`RateObservation`]s.
fn decode_batch(batch: &RecordBatch) -> Result<Vec<RateObservation>> {
    let columns = resolve_columns(batch)?;
    (0..batch.num_rows()).map(|row| decode_row(&columns, row)).collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use chrono::DateTime;

    use super::LiveRates;
    use crate::pricing::source::{RateObservation, ValidFromSource};

    fn rate(valid_from_secs: i64, input: f64) -> RateObservation {
        RateObservation {
            provider: "anthropic".to_string(),
            model: "claude-opus-4-8".to_string(),
            canonical_id: Some("anthropic/claude-opus-4-8".to_string()),
            service_tier: "standard".to_string(),
            region: "global".to_string(),
            min_input_tokens: 0,
            max_input_tokens: None,
            valid_from: DateTime::from_timestamp(valid_from_secs, 0).expect("valid timestamp"),
            valid_from_source: ValidFromSource::Observed,
            input_usd_per_1m: Some(input),
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
            source_url: None,
        }
    }

    #[test]
    fn live_rates_keeps_only_the_latest_revision_per_key() {
        // `read_live_rates` scans the full revision history; this pins that its
        // reduction step (`LiveRates::from_rows`) keeps the newest row per key
        // without needing a catalog to exercise the read path end to end.
        let older = rate(1_760_000_000, 5.0);
        let newer = rate(1_770_000_000, 6.0);
        let live = LiveRates::from_rows(vec![newer.clone(), older]);
        assert_eq!(live.get(&newer.key()).expect("key present").input_usd_per_1m, Some(6.0));
    }
}
