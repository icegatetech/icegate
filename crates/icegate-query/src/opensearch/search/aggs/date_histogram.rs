//! `date_histogram` buckets: the rounding from an instant to the start of its bucket, the SQL
//! key that applies the same rounding to every row, and the enumeration of empty buckets.
//!
//! A bucket's key is the UTC instant of its local start. Three roundings exist. For each, the
//! SQL key expression and [`BucketRounding::floor`] compute the same start: gap filling
//! enumerates starts with `floor` and must meet the starts the rows produced.
//!
//! - A calendar interval truncates the local wall-clock time. The SQL key relabels the stored
//!   `Timestamp(µs, None)` as `"UTC"` and only then as the zone. A cast from no zone straight to
//!   the zone would read the stored instant as local time. `floor` mirrors `date_trunc`:
//!   seconds and minutes truncate on the UTC timeline, coarser units truncate the local time,
//!   an ambiguous local start takes the offset of the instant being rounded, and a skipped local
//!   start is resolved three hours earlier and moved back three hours.
//! - A fixed interval with no zone or a fixed offset bins the stored instant with `date_bin`. The
//!   origin is shifted by the zone's offset and by `offset`.
//! - A fixed interval in a named zone rounds the local wall-clock time with
//!   [`build_local_floor_udf`]. A repeated local start yields one bucket per occurrence. A skipped
//!   local start begins where the local clock resumes.
//!
//! `offset` shifts every start: an instant is rounded as `instant - offset`, and `offset` is added
//! back to the start.
//!
//! The statements above about DataFusion's casts and `date_trunc` are pinned by the test
//! `floor_meets_the_sql_key_for_generated_instants_around_both_transitions`, which compares the
//! SQL key with `floor` around both daylight saving changes of New York and the skipped local
//! midnight of Havana: it fails if DataFusion stops computing them this way.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;

use chrono::{
    DateTime, Datelike, Days, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeDelta, TimeZone,
    Timelike, Utc,
};
use chrono_tz::Tz;
use datafusion::arrow::array::{Array, Int64Array, Int64Builder};
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, TimeUnit};
use datafusion::common::{Result as DataFusionResult, ScalarValue, exec_err};
use datafusion::functions::expr_fn::{date_bin, date_trunc};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, cast, lit,
};

use super::assemble::BucketBudget;
use super::parse::{CalendarUnit, DateHistogramSpec, HistogramInterval};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::doc_id::{
    MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MILLISECOND, MICROS_PER_MINUTE, MICROS_PER_SECOND,
};
use crate::opensearch::search::hits::DateRenderFormat;
use crate::query_dsl::date_math::TimeZoneSpec;

/// Step of the probe that looks for the next bucket start: time-zone offsets change by whole
/// quarters of an hour, so no bucket is shorter.
const PROBE_STEP_MICROS: i64 = 15 * MICROS_PER_MINUTE;
/// Probe steps allowed per bucket. The longest bucket, a 31-day month with one extra daylight
/// saving hour, is reached from the shortest probe in under 300 steps.
const MAX_PROBE_STEPS: usize = 1_000;
/// Minutes searched past a skipped local start for the instant the local clock resumes at.
const MAX_GAP_MINUTES: i64 = 1_440;
/// How far `date_trunc` moves a skipped local start before resolving it; the module doc names the
/// test that pins it.
const SKIPPED_START_SHIFT_HOURS: i64 = 3;

/// The zone a calendar interval truncates local time in.
#[derive(Debug, Clone, Copy)]
pub(super) enum CalendarZone {
    /// A fixed offset from UTC (UTC itself when no zone is requested).
    Fixed(FixedOffset),
    /// An IANA zone.
    Named(Tz),
}

impl CalendarZone {
    /// The zone as an Arrow timezone string: an IANA name or `±HH:MM`.
    fn format_arrow_time_zone(self) -> String {
        match self {
            Self::Named(zone) => zone.name().to_string(),
            Self::Fixed(offset) => format_offset(offset.local_minus_utc(), false),
        }
    }
}

/// How one `date_histogram` maps an instant (UTC microseconds) to the start of its bucket.
#[derive(Debug, Clone, Copy)]
pub(super) enum BucketRounding {
    /// A calendar unit truncated in local time, shifted by `offset_micros`.
    Calendar {
        /// The unit truncated to.
        unit: CalendarUnit,
        /// The zone whose local time is truncated.
        zone: CalendarZone,
        /// The `offset` of the request.
        offset_micros: i64,
    },
    /// A fixed interval binned on the UTC timeline from `origin_micros`.
    FixedOnTimeline {
        /// The interval.
        interval_micros: i64,
        /// A bucket start: `offset` minus the zone's offset.
        origin_micros: i64,
    },
    /// A fixed interval rounded on the wall clock of a named zone.
    FixedOnLocalClock {
        /// The interval.
        interval_micros: i64,
        /// The `offset` of the request, applied on the wall clock.
        offset_micros: i64,
        /// The zone.
        zone: Tz,
    },
}

impl BucketRounding {
    /// The rounding `spec` asks for.
    ///
    /// # Errors
    ///
    /// `400 illegal_argument_exception` when the interval or the shifted origin does not fit the
    /// microsecond range.
    pub(super) fn from_spec(spec: &DateHistogramSpec) -> Result<Self, OpenSearchError> {
        let out_of_range = || {
            OpenSearchError::illegal_argument(format!(
                "the interval or offset of the date_histogram on [{}] is out of range",
                spec.field
            ))
        };
        match spec.interval {
            HistogramInterval::Calendar(unit) => Ok(Self::Calendar {
                unit,
                zone: match spec.time_zone.as_ref() {
                    None => CalendarZone::Fixed(Utc.fix()),
                    Some(TimeZoneSpec::Fixed(offset)) => CalendarZone::Fixed(*offset),
                    Some(TimeZoneSpec::Named(zone)) => CalendarZone::Named(*zone),
                },
                offset_micros: spec.offset_micros,
            }),
            HistogramInterval::Fixed(interval) => {
                let interval_micros = i64::try_from(interval.as_micros()).map_err(|_| out_of_range())?;
                match spec.time_zone.as_ref() {
                    None => Ok(Self::FixedOnTimeline {
                        interval_micros,
                        origin_micros: spec.offset_micros,
                    }),
                    Some(TimeZoneSpec::Fixed(offset)) => {
                        let zone_micros = i64::from(offset.local_minus_utc()) * MICROS_PER_SECOND;
                        Ok(Self::FixedOnTimeline {
                            interval_micros,
                            origin_micros: spec.offset_micros.checked_sub(zone_micros).ok_or_else(out_of_range)?,
                        })
                    }
                    Some(TimeZoneSpec::Named(zone)) => Ok(Self::FixedOnLocalClock {
                        interval_micros,
                        offset_micros: spec.offset_micros,
                        zone: *zone,
                    }),
                }
            }
        }
    }

    /// The start of the bucket holding `instant_micros`; `None` when the instant or its start
    /// lies outside the representable time range.
    pub(super) fn floor(&self, instant_micros: i64) -> Option<i64> {
        match *self {
            Self::Calendar {
                unit,
                zone,
                offset_micros,
            } => {
                let shifted = instant_micros.checked_sub(offset_micros)?;
                let start = match zone {
                    CalendarZone::Fixed(zone) => truncate_calendar(&zone, unit, shifted)?,
                    CalendarZone::Named(zone) => truncate_calendar(&zone, unit, shifted)?,
                };
                start.checked_add(offset_micros)
            }
            Self::FixedOnTimeline {
                interval_micros,
                origin_micros,
            } => {
                let distance = instant_micros.checked_sub(origin_micros)?;
                origin_micros.checked_add(distance.div_euclid(interval_micros).checked_mul(interval_micros)?)
            }
            Self::FixedOnLocalClock {
                interval_micros,
                offset_micros,
                zone,
            } => floor_on_local_clock(instant_micros, interval_micros, offset_micros, zone),
        }
    }

    /// The start of the bucket after the one starting at `start_micros`; `None` outside the
    /// representable time range.
    ///
    /// Buckets in a zone with daylight saving vary in length, so the next start is found by
    /// probing from the shortest possible bucket onward in quarter-hour steps, each probe
    /// rounded with [`Self::floor`].
    pub(super) fn next_start(&self, start_micros: i64) -> Option<i64> {
        let (shortest_micros, step_micros) = match *self {
            Self::FixedOnTimeline { interval_micros, .. } => return start_micros.checked_add(interval_micros),
            Self::Calendar { unit, .. } => {
                let shortest = measure_shortest_calendar_bucket(unit);
                (shortest, PROBE_STEP_MICROS.min(shortest))
            }
            Self::FixedOnLocalClock { interval_micros, .. } => (
                (interval_micros - MICROS_PER_HOUR).max(interval_micros / 2).max(1),
                PROBE_STEP_MICROS.min(interval_micros / 2).max(1),
            ),
        };
        let mut probe = start_micros.checked_add(shortest_micros)?;
        for _ in 0..MAX_PROBE_STEPS {
            let next = self.floor(probe)?;
            if next > start_micros {
                return Some(next);
            }
            probe = probe.checked_add(step_micros)?;
        }
        None
    }

    /// The bucket start, in UTC microseconds, of the instant `timestamp` (a `Timestamp(µs, None)`
    /// expression) as SQL. NULL stays NULL.
    ///
    /// # Errors
    ///
    /// `400 illegal_argument_exception` when the interval or the origin does not fit nanoseconds,
    /// the unit `date_bin` reads.
    pub(super) fn key_expression(&self, timestamp: Expr) -> Result<Expr, OpenSearchError> {
        match *self {
            Self::Calendar {
                unit,
                zone,
                offset_micros,
            } => {
                let shifted = cast(
                    cast(timestamp, DataType::Int64) - lit(offset_micros),
                    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                );
                let zoned = cast(
                    shifted,
                    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from(zone.format_arrow_time_zone()))),
                );
                Ok(cast(date_trunc(lit(unit.truncation_name()), zoned), DataType::Int64) + lit(offset_micros))
            }
            Self::FixedOnTimeline {
                interval_micros,
                origin_micros,
            } => {
                let out_of_range = || OpenSearchError::illegal_argument("a fixed interval or offset is out of range");
                let stride_nanos = interval_micros.checked_mul(1_000).ok_or_else(out_of_range)?;
                let origin_nanos = origin_micros.checked_mul(1_000).ok_or_else(out_of_range)?;
                let stride = lit(ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
                    0,
                    0,
                    stride_nanos,
                ))));
                let origin = lit(ScalarValue::TimestampNanosecond(Some(origin_nanos), None));
                Ok(cast(date_bin(stride, timestamp, origin), DataType::Int64))
            }
            Self::FixedOnLocalClock {
                interval_micros,
                offset_micros,
                zone,
            } => Ok(build_local_floor_udf().call(vec![
                cast(timestamp, DataType::Int64),
                lit(interval_micros),
                lit(offset_micros),
                lit(zone.name()),
            ])),
        }
    }
}

/// The shortest bucket a calendar unit makes in any zone: a daylight saving change removes up to
/// an hour from a day and longer units; a half-hour change halves an hour.
const fn measure_shortest_calendar_bucket(unit: CalendarUnit) -> i64 {
    match unit {
        CalendarUnit::Second => MICROS_PER_SECOND,
        CalendarUnit::Minute => MICROS_PER_MINUTE,
        CalendarUnit::Hour => 30 * MICROS_PER_MINUTE,
        CalendarUnit::Day => MICROS_PER_DAY - MICROS_PER_HOUR,
        CalendarUnit::Week => 7 * MICROS_PER_DAY - MICROS_PER_HOUR,
        CalendarUnit::Month => 28 * MICROS_PER_DAY - MICROS_PER_HOUR,
        CalendarUnit::Quarter => 90 * MICROS_PER_DAY - MICROS_PER_HOUR,
        CalendarUnit::Year => 365 * MICROS_PER_DAY - MICROS_PER_HOUR,
    }
}

/// `date_trunc(unit, instant)` in `zone`, as DataFusion computes it; the module doc names the test
/// that pins it.
fn truncate_calendar<Z: TimeZone>(zone: &Z, unit: CalendarUnit, instant_micros: i64) -> Option<i64> {
    match unit {
        CalendarUnit::Second => instant_micros.div_euclid(MICROS_PER_SECOND).checked_mul(MICROS_PER_SECOND),
        CalendarUnit::Minute => instant_micros.div_euclid(MICROS_PER_MINUTE).checked_mul(MICROS_PER_MINUTE),
        CalendarUnit::Hour
        | CalendarUnit::Day
        | CalendarUnit::Week
        | CalendarUnit::Month
        | CalendarUnit::Quarter
        | CalendarUnit::Year => {
            let local = DateTime::<Utc>::from_timestamp_micros(instant_micros)?.with_timezone(zone);
            let truncated = truncate_local_time(local.naive_local(), unit)?;
            let start = match zone.from_local_datetime(&truncated) {
                LocalResult::Single(start) => start,
                LocalResult::Ambiguous(earlier, later) => {
                    if earlier.offset().fix() == local.offset().fix() {
                        earlier
                    } else {
                        later
                    }
                }
                LocalResult::None => {
                    let shift = TimeDelta::hours(SKIPPED_START_SHIFT_HOURS);
                    zone.from_local_datetime(&truncated.checked_sub_signed(shift)?)
                        .single()?
                        .checked_add_signed(shift)?
                }
            };
            Some(start.timestamp_micros())
        }
    }
}

/// The local start of `unit` holding the wall-clock time `local`.
fn truncate_local_time(local: NaiveDateTime, unit: CalendarUnit) -> Option<NaiveDateTime> {
    let date = local.date();
    match unit {
        CalendarUnit::Second => local.with_nanosecond(0),
        CalendarUnit::Minute => local.with_nanosecond(0)?.with_second(0),
        CalendarUnit::Hour => date.and_hms_opt(local.hour(), 0, 0),
        CalendarUnit::Day => date.and_hms_opt(0, 0, 0),
        CalendarUnit::Week => date
            .checked_sub_days(Days::new(u64::from(date.weekday().num_days_from_monday())))?
            .and_hms_opt(0, 0, 0),
        CalendarUnit::Month => NaiveDate::from_ymd_opt(date.year(), date.month(), 1)?.and_hms_opt(0, 0, 0),
        CalendarUnit::Quarter => {
            NaiveDate::from_ymd_opt(date.year(), 1 + 3 * ((date.month() - 1) / 3), 1)?.and_hms_opt(0, 0, 0)
        }
        CalendarUnit::Year => NaiveDate::from_ymd_opt(date.year(), 1, 1)?.and_hms_opt(0, 0, 0),
    }
}

/// The start of the fixed-interval bucket holding `instant_micros` on the wall clock of `zone`.
///
/// Wall-clock buckets are aligned to `offset_micros` past local midnight of 1970-01-01. A local
/// start the clock shows twice resolves to the latest occurrence not after the instant, so each
/// occurrence starts its own bucket. A local start the clock skips resolves to the first instant
/// after the gap.
fn floor_on_local_clock(instant_micros: i64, interval_micros: i64, offset_micros: i64, zone: Tz) -> Option<i64> {
    let instant = DateTime::<Utc>::from_timestamp_micros(instant_micros)?;
    let zone_micros =
        i64::from(zone.offset_from_utc_datetime(&instant.naive_utc()).fix().local_minus_utc()) * MICROS_PER_SECOND;
    let wall_clock_micros = instant_micros.checked_add(zone_micros)?;
    let distance = wall_clock_micros.checked_sub(offset_micros)?;
    let local_start_micros =
        offset_micros.checked_add(distance.div_euclid(interval_micros).checked_mul(interval_micros)?)?;
    let local_start = DateTime::<Utc>::from_timestamp_micros(local_start_micros)?.naive_utc();
    match zone.from_local_datetime(&local_start) {
        LocalResult::Single(start) => Some(start.timestamp_micros()),
        LocalResult::Ambiguous(earlier, later) => {
            let later_micros = later.timestamp_micros();
            Some(if later_micros <= instant_micros {
                later_micros
            } else {
                earlier.timestamp_micros()
            })
        }
        LocalResult::None => find_first_instant_after_gap(zone, local_start),
    }
}

/// The first instant whose wall-clock time follows the skipped local time `skipped`.
fn find_first_instant_after_gap(zone: Tz, skipped: NaiveDateTime) -> Option<i64> {
    (1..=MAX_GAP_MINUTES).find_map(|minutes| {
        let probe = skipped.checked_add_signed(TimeDelta::minutes(minutes))?;
        zone.from_local_datetime(&probe)
            .earliest()
            .map(|start| start.timestamp_micros())
    })
}

/// `opensearch_local_floor(instant, interval, offset, zone)`: the UTC-microsecond start of the
/// fixed-interval bucket holding `instant` (UTC microseconds) on the wall clock of the IANA
/// `zone`, as `floor_on_local_clock` defines it. `interval`, `offset` (microseconds) and `zone`
/// are constants; a NULL instant stays NULL.
fn build_local_floor_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(LocalFloor::new())
}

/// The scalar function behind [`build_local_floor_udf`].
#[derive(Debug, PartialEq, Eq, Hash)]
struct LocalFloor {
    signature: Signature,
}

impl LocalFloor {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64, DataType::Int64, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LocalFloor {
    fn name(&self) -> &'static str {
        "opensearch_local_floor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _argument_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let [instants, interval, offset, zone] = args.args.as_slice() else {
            return exec_err!("opensearch_local_floor takes four arguments");
        };
        let (
            ColumnarValue::Scalar(ScalarValue::Int64(Some(interval_micros))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(offset_micros))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(zone_name))),
        ) = (interval, offset, zone)
        else {
            return exec_err!("opensearch_local_floor takes a constant interval, offset and zone");
        };
        if *interval_micros <= 0 {
            return exec_err!("opensearch_local_floor needs a positive interval");
        }
        let Ok(zone) = zone_name.parse::<Tz>() else {
            return exec_err!("opensearch_local_floor: unknown time zone [{zone_name}]");
        };
        let instants = instants.to_array(args.number_rows)?;
        let Some(instants) = instants.as_any().downcast_ref::<Int64Array>() else {
            return exec_err!("opensearch_local_floor takes Int64 instants");
        };
        let mut starts = Int64Builder::with_capacity(instants.len());
        for instant in instants {
            match instant {
                None => starts.append_null(),
                Some(instant) => match floor_on_local_clock(instant, *interval_micros, *offset_micros, zone) {
                    Some(start) => starts.append_value(start),
                    None => return exec_err!("instant [{instant}] has no bucket start in [{zone_name}]"),
                },
            }
        }
        Ok(ColumnarValue::Array(Arc::new(starts.finish())))
    }
}

/// `key_as_string` of the bucket starting at `start_micros`: the epoch milliseconds for
/// `epoch_millis`; otherwise the local start in `time_zone` (UTC when absent) with its offset
/// (`Z` for a zero offset), six fraction digits for `strict_date_optional_time_nanos` and three
/// for every other format.
///
/// # Errors
///
/// `500` for a start outside the representable time range, which no bucket key reaches.
pub(super) fn render_key_as_string(
    start_micros: i64,
    time_zone: Option<&TimeZoneSpec>,
    format: Option<&DateRenderFormat>,
) -> Result<String, OpenSearchError> {
    let pattern = match format {
        Some(DateRenderFormat::EpochMillis) => return Ok(start_micros.div_euclid(MICROS_PER_MILLISECOND).to_string()),
        Some(DateRenderFormat::StrictDateOptionalTimeNanos) => "%Y-%m-%dT%H:%M:%S%.6f",
        Some(
            DateRenderFormat::DateTime | DateRenderFormat::StrictDateTime | DateRenderFormat::StrictDateOptionalTime,
        )
        | None => "%Y-%m-%dT%H:%M:%S%.3f",
    };
    let out_of_range = || OpenSearchError::internal(format!("bucket start [{start_micros}] is out of range"));
    let instant = DateTime::<Utc>::from_timestamp_micros(start_micros).ok_or_else(out_of_range)?;
    let offset_seconds = match time_zone {
        None => 0,
        Some(TimeZoneSpec::Fixed(offset)) => offset.local_minus_utc(),
        Some(TimeZoneSpec::Named(zone)) => zone.offset_from_utc_datetime(&instant.naive_utc()).fix().local_minus_utc(),
    };
    let local = instant
        .naive_utc()
        .checked_add_signed(TimeDelta::seconds(i64::from(offset_seconds)))
        .ok_or_else(out_of_range)?;
    Ok(format!(
        "{}{}",
        local.format(pattern),
        format_offset(offset_seconds, true)
    ))
}

/// `±HH:MM`, or `Z` for a zero offset when `should_render_zero_as_z`.
fn format_offset(offset_seconds: i32, should_render_zero_as_z: bool) -> String {
    if offset_seconds == 0 && should_render_zero_as_z {
        return "Z".to_string();
    }
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let magnitude = offset_seconds.unsigned_abs();
    format!("{sign}{:02}:{:02}", magnitude / 3_600, magnitude % 3_600 / 60)
}

/// Add a zero-count start to `counts` for every bucket start from the first to the last bucket,
/// widened to `extended_bounds` (epoch milliseconds, each rounded to its bucket start). Each start
/// added counts as one more bucket in `budget`, so an enumeration is cut off as soon as it
/// crosses the limit.
///
/// # Errors
///
/// `400 too_many_buckets_exception` past the budget; `400 illegal_argument_exception` for bounds
/// outside the representable time range.
pub(super) fn fill_empty_buckets(
    counts: &mut BTreeMap<i64, u64>,
    rounding: &BucketRounding,
    extended_bounds: Option<(i64, i64)>,
    budget: &mut BucketBudget,
) -> Result<(), OpenSearchError> {
    let round_bound = |millis: i64| {
        millis
            .checked_mul(MICROS_PER_MILLISECOND)
            .and_then(|micros| rounding.floor(micros))
            .ok_or_else(|| {
                OpenSearchError::illegal_argument("extended_bounds lies outside the representable time range")
            })
    };
    let bounds = match extended_bounds {
        Some((min, max)) => Some((round_bound(min)?, round_bound(max)?)),
        None => None,
    };
    let first = [counts.keys().next().copied(), bounds.map(|(min, _)| min)]
        .into_iter()
        .flatten()
        .min();
    let last = [counts.keys().next_back().copied(), bounds.map(|(_, max)| max)]
        .into_iter()
        .flatten()
        .max();
    let (Some(mut start), Some(last)) = (first, last) else {
        return Ok(());
    };
    while start <= last {
        if let Entry::Vacant(slot) = counts.entry(start) {
            budget.consume(1)?;
            slot.insert(0);
        }
        start = rounding
            .next_start(start)
            .ok_or_else(|| OpenSearchError::internal(format!("no bucket start follows [{start}]")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono_tz::Tz;
    use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::logical_expr::{LogicalPlanBuilder, col};
    use datafusion::prelude::SessionContext;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use serde_json::{Value, json};

    use super::{BucketRounding, MICROS_PER_DAY, MICROS_PER_HOUR, floor_on_local_clock, render_key_as_string};
    use crate::opensearch::search::aggs::parse::{AggregationKind, parse_aggregations};
    use crate::opensearch::search::aggs::test_support::parse_instant_micros;

    /// The rounding of a `date_histogram` on `timestamp` with `options` added.
    fn build_rounding(options: &Value) -> BucketRounding {
        let mut body = json!({"field": "timestamp"});
        if let (Some(target), Some(source)) = (body.as_object_mut(), options.as_object()) {
            target.extend(source.clone());
        }
        let tree = parse_aggregations(&json!({"h": {"date_histogram": body}})).expect("a date_histogram");
        let AggregationKind::DateHistogram(spec) = &tree.aggregations[0].kind else {
            panic!("the test wrote a date_histogram aggregation");
        };
        BucketRounding::from_spec(spec).expect("a rounding")
    }

    #[test]
    fn a_fixed_start_inside_a_skipped_local_hour_begins_where_the_local_clock_resumes() {
        let new_york = Tz::America__New_York;
        let two_hours = 2 * MICROS_PER_HOUR;

        // 03:30 EDT on 2026-03-08 floors to local 02:00, which the clock skips; the bucket begins
        // at 03:00 EDT, the first instant after the gap.
        assert_eq!(
            floor_on_local_clock(parse_instant_micros("2026-03-08T07:30:00Z"), two_hours, 0, new_york),
            Some(parse_instant_micros("2026-03-08T07:00:00Z"))
        );
        // 01:30 EST floors to local 00:00 EST.
        assert_eq!(
            floor_on_local_clock(parse_instant_micros("2026-03-08T06:30:00Z"), two_hours, 0, new_york),
            Some(parse_instant_micros("2026-03-08T05:00:00Z"))
        );
    }

    #[test]
    fn a_repeated_local_hour_starts_one_bucket_per_occurrence() {
        let new_york = Tz::America__New_York;

        assert_eq!(
            floor_on_local_clock(
                parse_instant_micros("2025-11-02T05:30:00Z"),
                MICROS_PER_HOUR,
                0,
                new_york
            ),
            Some(parse_instant_micros("2025-11-02T05:00:00Z")),
            "01:30 EDT starts at 01:00 EDT"
        );
        assert_eq!(
            floor_on_local_clock(
                parse_instant_micros("2025-11-02T06:30:00Z"),
                MICROS_PER_HOUR,
                0,
                new_york
            ),
            Some(parse_instant_micros("2025-11-02T06:00:00Z")),
            "01:30 EST starts at 01:00 EST"
        );
    }

    #[test]
    fn next_start_walks_the_existing_local_starts() {
        let hours = build_rounding(&json!({"fixed_interval": "1h", "time_zone": "America/New_York"}));
        let days = build_rounding(&json!({"calendar_interval": "1d", "time_zone": "America/New_York"}));

        assert_eq!(
            hours.next_start(parse_instant_micros("2026-03-08T06:00:00Z")),
            Some(parse_instant_micros("2026-03-08T07:00:00Z")),
            "01:00 EST is followed by 03:00 EDT"
        );
        assert_eq!(
            hours.next_start(parse_instant_micros("2025-11-02T05:00:00Z")),
            Some(parse_instant_micros("2025-11-02T06:00:00Z")),
            "01:00 EDT is followed by 01:00 EST"
        );
        assert_eq!(
            days.next_start(parse_instant_micros("2025-11-02T04:00:00Z")),
            Some(parse_instant_micros("2025-11-03T05:00:00Z")),
            "the 25-hour day"
        );
        assert_eq!(
            days.next_start(parse_instant_micros("2026-03-08T05:00:00Z")),
            Some(parse_instant_micros("2026-03-09T04:00:00Z")),
            "the 23-hour day"
        );
    }

    #[test]
    fn key_as_string_renders_the_local_start_with_its_offset() {
        let start = parse_instant_micros("2026-03-08T05:00:00Z");

        assert_eq!(
            render_key_as_string(start, None, None).expect("rendered"),
            "2026-03-08T05:00:00.000Z"
        );
        let new_york = crate::query_dsl::date_math::parse_time_zone("America/New_York").expect("zone");
        assert_eq!(
            render_key_as_string(start, Some(&new_york), None).expect("rendered"),
            "2026-03-08T00:00:00.000-05:00"
        );
    }

    #[tokio::test]
    async fn floor_meets_the_sql_key_for_generated_instants_around_both_transitions() {
        let mut random = StdRng::seed_from_u64(20_261_008);
        let centers = [
            parse_instant_micros("2025-11-02T06:00:00Z"),
            parse_instant_micros("2026-03-08T07:00:00Z"),
        ];
        let instants: Vec<i64> = (0..400)
            .map(|position| centers[position % 2] + random.random_range(-2 * MICROS_PER_DAY..2 * MICROS_PER_DAY))
            .collect();
        let values: TimestampMicrosecondArray = instants.iter().copied().map(Some).collect();
        let batch = RecordBatch::try_from_iter(vec![("timestamp", Arc::new(values) as ArrayRef)]).expect("batch");
        let table = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("table"));
        let options = [
            json!({"calendar_interval": "1h", "time_zone": "America/New_York"}),
            json!({"calendar_interval": "1d", "time_zone": "America/New_York"}),
            json!({"calendar_interval": "1w", "time_zone": "America/New_York", "offset": "+6h"}),
            json!({"calendar_interval": "1M", "time_zone": "America/New_York"}),
            // Havana's clock skips local midnight on 2026-03-08, so the day starts at 01:00.
            json!({"calendar_interval": "1d", "time_zone": "America/Havana"}),
            json!({"calendar_interval": "1d", "time_zone": "Asia/Kolkata"}),
            json!({"calendar_interval": "1m", "time_zone": "Asia/Kolkata"}),
            json!({"fixed_interval": "1h", "time_zone": "America/New_York"}),
            json!({"fixed_interval": "2h", "time_zone": "America/New_York", "offset": "-30m"}),
            json!({"fixed_interval": "90m", "time_zone": "+05:30"}),
            json!({"fixed_interval": "45m"}),
        ];
        let context = SessionContext::new();
        // The generated instants reach the Havana day that begins at 01:00 CDT (05:00Z).
        let havana_days = build_rounding(&json!({"calendar_interval": "1d", "time_zone": "America/Havana"}));
        assert!(
            instants
                .iter()
                .any(|generated| havana_days.floor(*generated) == Some(parse_instant_micros("2026-03-08T05:00:00Z")))
        );

        for option in options {
            let rounding = build_rounding(&option);
            let plan = LogicalPlanBuilder::scan("rows", provider_as_source(Arc::clone(&table) as _), None)
                .and_then(|builder| builder.project(vec![rounding.key_expression(col("timestamp")).expect("key")]))
                .and_then(LogicalPlanBuilder::build)
                .expect("plan");
            let batches = context
                .execute_logical_plan(plan)
                .await
                .expect("execute")
                .collect()
                .await
                .expect("collect");
            let keys: Vec<i64> = batches
                .iter()
                .flat_map(|batch| {
                    let keys = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("Int64 keys");
                    (0..keys.len()).map(|row| keys.value(row)).collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(keys.len(), instants.len());
            for (instant, key) in instants.iter().zip(keys) {
                assert_eq!(rounding.floor(*instant), Some(key), "{option} at {instant}");
            }
        }
    }
}
