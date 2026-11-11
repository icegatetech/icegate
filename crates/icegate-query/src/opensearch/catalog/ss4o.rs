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
use icegate_common::{LOGS_TABLE, METRICS_TABLE, SPANS_TABLE, TenantId};

use super::data_prepper::SPAN_FIELDS;
use super::encoding::{AttributeKeys, ProfileFieldSource, ProfileTable};
use super::native::METRIC_VALUE_FIELD;

/// The signal an ss4o index carries: one index per signal and tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Ss4oSignal {
    /// The logs index, over `logs`.
    Logs,
    /// The traces index, over `spans`.
    Traces,
    /// The metrics index, over `metrics`.
    Metrics,
}

impl Ss4oSignal {
    /// The part of the index name before the tenant.
    const fn name_prefix(self) -> &'static str {
        match self {
            Self::Logs => "ss4o_logs-",
            Self::Traces => "ss4o_traces-",
            Self::Metrics => "ss4o_metrics-",
        }
    }
}

/// The name of the `signal` ss4o index of `tenant`: the signal's prefix and the tenant as
/// [`normalize_tenant_for_index_name`] writes it, so each tenant sees the index under its own name.
pub(super) fn build_ss4o_index_name(signal: Ss4oSignal, tenant: &TenantId) -> String {
    let prefix = signal.name_prefix();
    let tenant_text: &str = tenant.as_ref();
    let mut name = String::with_capacity(prefix.len() + tenant_text.len());
    name.push_str(prefix);
    name.extend(normalize_tenant_for_index_name(tenant_text));
    name
}

/// `tenant` as an index name may carry it: ASCII upper case lowered and every `:` replaced by
/// `_`, because an index name is lower case and a `:` in one names a remote cluster. A tenant id
/// is ASCII alphanumerics, `-`, `_` and `:` ([`TenantId::is_valid`]), so the result is always a
/// valid index-name part. Two tenants that differ only by case or by `:` against `_` share index
/// names, which mixes nothing: a request only ever sees its own tenant's indices.
///
/// Yields characters rather than a `String` so a caller writes them into the name it is building.
fn normalize_tenant_for_index_name(tenant: &str) -> impl Iterator<Item = char> + '_ {
    tenant.chars().map(|character| match character {
        ':' => '_',
        other => other.to_ascii_lowercase(),
    })
}

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
const SS4O_METRIC_FIELDS: [(&str, ProfileFieldSource); 31] = [
    ("serviceName", ProfileFieldSource::Column(COL_SERVICE_NAME)),
    ("name", ProfileFieldSource::Column(COL_METRIC_NAME)),
    ("description", ProfileFieldSource::Column(COL_DESCRIPTION)),
    ("unit", ProfileFieldSource::Column(COL_UNIT)),
    ("kind", ProfileFieldSource::MetricKind),
    ("aggregationTemporality", ProfileFieldSource::Temporality),
    ("monotonic", ProfileFieldSource::Column(COL_IS_MONOTONIC)),
    ("startTime", ProfileFieldSource::Time(COL_START_TIMESTAMP)),
    ("@timestamp", ProfileFieldSource::Time(COL_TIMESTAMP)),
    ("time", ProfileFieldSource::Alias("@timestamp")),
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
        "buckets",
        ProfileFieldSource::HistogramBuckets {
            bounds: COL_EXPLICIT_BOUNDS,
            counts: COL_BUCKET_COUNTS,
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::build_tenant;

    #[test]
    fn a_tenant_is_lower_cased_with_underscores_for_colons_in_an_index_name() {
        for (tenant, expected_part) in [("Org:Acme", "org_acme"), ("demo", "demo"), ("a-b_c", "a-b_c")] {
            assert_eq!(
                normalize_tenant_for_index_name(tenant).collect::<String>(),
                expected_part,
                "{tenant}"
            );
        }
        let tenant = build_tenant("Org:Acme");
        assert_eq!(build_ss4o_index_name(Ss4oSignal::Logs, &tenant), "ss4o_logs-org_acme");
        assert_eq!(
            build_ss4o_index_name(Ss4oSignal::Traces, &tenant),
            "ss4o_traces-org_acme"
        );
        assert_eq!(
            build_ss4o_index_name(Ss4oSignal::Metrics, &tenant),
            "ss4o_metrics-org_acme"
        );
    }
}
