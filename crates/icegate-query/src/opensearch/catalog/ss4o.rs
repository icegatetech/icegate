//! The ss4o profiles: the logs, traces and metrics indices of the ss4o schemas over the
//! `logs`, `spans` and `metrics` tables.
//!
//! Stored attribute keys keep their dots. A field this profile does not declare is absent from
//! field discovery, and a query on it matches nothing.

use icegate_common::schema::{
    COL_BODY, COL_BUCKET_COUNTS, COL_COUNT, COL_DESCRIPTION, COL_EXPLICIT_BOUNDS, COL_IS_MONOTONIC, COL_LOG_ATTRIBUTES,
    COL_MAX, COL_METRIC_NAME, COL_MIN, COL_NEGATIVE_BUCKET_COUNTS, COL_NEGATIVE_OFFSET, COL_OBSERVED_TIMESTAMP,
    COL_POSITIVE_BUCKET_COUNTS, COL_POSITIVE_OFFSET, COL_QUANTILE_VALUES, COL_RESOURCE_ATTRIBUTES, COL_SCALE,
    COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_START_TIMESTAMP, COL_SUM,
    COL_TIMESTAMP, COL_TRACE_ID, COL_UNIT, COL_VALUE_DOUBLE, COL_VALUE_INT, COL_ZERO_COUNT,
};
use icegate_common::{LOGS_TABLE, METRICS_TABLE, SPANS_TABLE};

use super::data_prepper::SPAN_FIELDS;
use super::encoding::{AttributeKeys, ProfileFieldSource, ProfileTable};
use super::native::METRIC_VALUE_FIELD;

/// The ss4o logs index over `logs`.
pub(super) const SS4O_LOGS_INDEX: &str = "ss4o_logs-icegate-default";
/// The ss4o traces index over `spans`.
pub(super) const SS4O_TRACES_INDEX: &str = "ss4o_traces-icegate-default";
/// The ss4o metrics index over `metrics`.
pub(super) const SS4O_METRICS_INDEX: &str = "ss4o_metrics-icegate-default";

/// The fields of the ss4o logs index, in `_mapping` order.
const SS4O_LOG_FIELDS: [(&str, ProfileFieldSource); 16] = [
    ("@timestamp", ProfileFieldSource::Time(COL_TIMESTAMP)),
    ("time", ProfileFieldSource::Alias("@timestamp")),
    ("observedTimestamp", ProfileFieldSource::Time(COL_OBSERVED_TIMESTAMP)),
    ("observerTime", ProfileFieldSource::Alias("observedTimestamp")),
    ("traceId", ProfileFieldSource::StoredHexId(COL_TRACE_ID)),
    ("spanId", ProfileFieldSource::StoredHexId(COL_SPAN_ID)),
    ("severity", ProfileFieldSource::Object),
    ("severity.text", ProfileFieldSource::Column(COL_SEVERITY_TEXT)),
    ("body", ProfileFieldSource::Column(COL_BODY)),
    ("@message", ProfileFieldSource::Alias("body")),
    ("serviceName", ProfileFieldSource::Column(COL_SERVICE_NAME)),
    ("attributes", ProfileFieldSource::Attributes(COL_LOG_ATTRIBUTES)),
    ("resource", ProfileFieldSource::Object),
    (
        "resource.attributes",
        ProfileFieldSource::Attributes(COL_RESOURCE_ATTRIBUTES),
    ),
    ("instrumentationScope", ProfileFieldSource::Object),
    (
        "instrumentationScope.attributes",
        ProfileFieldSource::Attributes(COL_SCOPE_ATTRIBUTES),
    ),
];

/// The ss4o traces fields beyond the span fields every span profile shares.
const SS4O_TRACE_EXTRA_FIELDS: [(&str, ProfileFieldSource); 1] =
    [("@timestamp", ProfileFieldSource::Alias("startTime"))];

/// The fields of the ss4o metrics index, in `_mapping` order.
const SS4O_METRIC_FIELDS: [(&str, ProfileFieldSource); 29] = [
    ("serviceName", ProfileFieldSource::Column(COL_SERVICE_NAME)),
    ("name", ProfileFieldSource::Column(COL_METRIC_NAME)),
    ("description", ProfileFieldSource::Column(COL_DESCRIPTION)),
    ("unit", ProfileFieldSource::Column(COL_UNIT)),
    ("kind", ProfileFieldSource::MetricKind),
    ("aggregationTemporality", ProfileFieldSource::Temporality),
    ("monotonic", ProfileFieldSource::Column(COL_IS_MONOTONIC)),
    ("startTime", ProfileFieldSource::Time(COL_START_TIMESTAMP)),
    ("@timestamp", ProfileFieldSource::Time(COL_TIMESTAMP)),
    ("value@int", ProfileFieldSource::Column(COL_VALUE_INT)),
    ("value@double", ProfileFieldSource::Column(COL_VALUE_DOUBLE)),
    (METRIC_VALUE_FIELD, ProfileFieldSource::MetricValue),
    ("count", ProfileFieldSource::Column(COL_COUNT)),
    ("sum", ProfileFieldSource::Column(COL_SUM)),
    ("min", ProfileFieldSource::Column(COL_MIN)),
    ("max", ProfileFieldSource::Column(COL_MAX)),
    ("scale", ProfileFieldSource::Column(COL_SCALE)),
    ("zeroCount", ProfileFieldSource::Column(COL_ZERO_COUNT)),
    ("positiveOffset", ProfileFieldSource::Column(COL_POSITIVE_OFFSET)),
    ("negativeOffset", ProfileFieldSource::Column(COL_NEGATIVE_OFFSET)),
    ("bucketCountsList", ProfileFieldSource::Column(COL_BUCKET_COUNTS)),
    ("explicitBoundsList", ProfileFieldSource::Column(COL_EXPLICIT_BOUNDS)),
    ("bucketCount", ProfileFieldSource::ArrayLength(COL_BUCKET_COUNTS)),
    (
        "explicitBoundsCount",
        ProfileFieldSource::ArrayLength(COL_EXPLICIT_BOUNDS),
    ),
    (
        "quantileValuesCount",
        ProfileFieldSource::ArrayLength(COL_QUANTILE_VALUES),
    ),
    (
        "positiveBuckets",
        ProfileFieldSource::ExponentialBuckets {
            counts: COL_POSITIVE_BUCKET_COUNTS,
            offset: COL_POSITIVE_OFFSET,
            is_negative: false,
        },
    ),
    (
        "negativeBuckets",
        ProfileFieldSource::ExponentialBuckets {
            counts: COL_NEGATIVE_BUCKET_COUNTS,
            offset: COL_NEGATIVE_OFFSET,
            is_negative: true,
        },
    ),
    ("quantiles", ProfileFieldSource::Quantiles),
    ("exemplar", ProfileFieldSource::Exemplars),
];

/// The field table of the ss4o logs index.
static SS4O_LOGS_TABLE: ProfileTable = ProfileTable {
    table: LOGS_TABLE,
    fields: &[&SS4O_LOG_FIELDS],
    attribute_keys: AttributeKeys::Stored,
};

/// The field table of the ss4o traces index.
static SS4O_TRACES_TABLE: ProfileTable = ProfileTable {
    table: SPANS_TABLE,
    fields: &[&SPAN_FIELDS, &SS4O_TRACE_EXTRA_FIELDS],
    attribute_keys: AttributeKeys::Stored,
};

/// The field table of the ss4o metrics index.
static SS4O_METRICS_TABLE: ProfileTable = ProfileTable {
    table: METRICS_TABLE,
    fields: &[&SS4O_METRIC_FIELDS],
    attribute_keys: AttributeKeys::Stored,
};

/// The ss4o field table over `table`; `None` for a table no ss4o index reads.
pub(crate) fn find_ss4o_table(table: &str) -> Option<&'static ProfileTable> {
    match table {
        LOGS_TABLE => Some(&SS4O_LOGS_TABLE),
        SPANS_TABLE => Some(&SS4O_TRACES_TABLE),
        METRICS_TABLE => Some(&SS4O_METRICS_TABLE),
        _ => None,
    }
}
