//! The Data Prepper span profile: the span index `otel-v1-apm-span-000001`, aliased
//! `otel-v1-apm-span`, over the `spans` table, so an `otel-v1-apm-span-*` pattern and the
//! `otel-v1-apm-span` name both reach it.
//!
//! Field names are camel-case (`traceId`, `spanId`, ...), ids are lowercase hex, the span kind is
//! its `OTel` name, times are RFC 3339 text, and `_source` is a nested object. Attribute keys write
//! each `.` as `@`, so a dotted stored key stays one property when the document is split on dots.

use icegate_common::SPANS_TABLE;
use icegate_common::schema::{
    COL_DROPPED_ATTRIBUTES_COUNT, COL_DROPPED_EVENTS_COUNT, COL_DROPPED_LINKS_COUNT, COL_END_TIMESTAMP, COL_KIND,
    COL_NAME, COL_PARENT_SPAN_ID, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SPAN_ATTRIBUTES,
    COL_SPAN_ID, COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TIMESTAMP, COL_TRACE_ID, COL_TRACE_STATE,
};

use super::cross_row::CrossRowField;
use super::encoding::{AttributeKeys, ProfileFieldSource, ProfileTable};

/// The concrete index name of the Data Prepper span profile: the name every response gives it.
pub(crate) const DATA_PREPPER_SPAN_INDEX: &str = "otel-v1-apm-span-000001";

/// The alias of [`DATA_PREPPER_SPAN_INDEX`]: a second name that selects the same index.
pub(crate) const DATA_PREPPER_SPAN_ALIAS: &str = "otel-v1-apm-span";

/// The trace id field: the key of a trace list's buckets and of a trace-by-id lookup.
pub(crate) const TRACE_ID_FIELD: &str = "traceId";

/// The span duration field, in nanoseconds: the name the derived duration's refusals report.
pub(super) const DURATION_IN_NANOS_FIELD: &str = "durationInNanos";

/// The fields every span profile declares, in `_mapping` order. Their attribute objects follow
/// the spelling of the table that lists them.
pub(super) const SPAN_FIELDS: [(&str, ProfileFieldSource); 24] = [
    (TRACE_ID_FIELD, ProfileFieldSource::HexIdOrEmpty(COL_TRACE_ID)),
    ("spanId", ProfileFieldSource::HexIdOrEmpty(COL_SPAN_ID)),
    ("parentSpanId", ProfileFieldSource::HexIdOrEmpty(COL_PARENT_SPAN_ID)),
    ("traceState", ProfileFieldSource::EmptyWhenNull(COL_TRACE_STATE)),
    ("name", ProfileFieldSource::Column(COL_NAME)),
    ("kind", ProfileFieldSource::SpanKind(COL_KIND)),
    ("serviceName", ProfileFieldSource::Column(COL_SERVICE_NAME)),
    ("startTime", ProfileFieldSource::Time(COL_TIMESTAMP)),
    ("endTime", ProfileFieldSource::Time(COL_END_TIMESTAMP)),
    (DURATION_IN_NANOS_FIELD, ProfileFieldSource::DurationInNanos),
    ("status", ProfileFieldSource::Object),
    ("status.code", ProfileFieldSource::StatusCode(COL_STATUS_CODE)),
    ("status.message", ProfileFieldSource::Column(COL_STATUS_MESSAGE)),
    (
        "droppedAttributesCount",
        ProfileFieldSource::Column(COL_DROPPED_ATTRIBUTES_COUNT),
    ),
    (
        "droppedEventsCount",
        ProfileFieldSource::Column(COL_DROPPED_EVENTS_COUNT),
    ),
    ("droppedLinksCount", ProfileFieldSource::Column(COL_DROPPED_LINKS_COUNT)),
    ("span", ProfileFieldSource::Object),
    ("span.attributes", ProfileFieldSource::Attributes(COL_SPAN_ATTRIBUTES)),
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
    ("events", ProfileFieldSource::Events),
    ("links", ProfileFieldSource::Links),
];

/// The cross-row fields of the Data Prepper profile: every span of a trace reads them from the
/// trace's root span.
const TRACE_GROUP_FIELDS: [(&str, ProfileFieldSource); 5] = [
    (
        CrossRowField::TraceGroup.client_name(),
        ProfileFieldSource::CrossRow(CrossRowField::TraceGroup),
    ),
    ("traceGroupFields", ProfileFieldSource::Object),
    (
        CrossRowField::EndTime.client_name(),
        ProfileFieldSource::CrossRow(CrossRowField::EndTime),
    ),
    (
        CrossRowField::DurationInNanos.client_name(),
        ProfileFieldSource::CrossRow(CrossRowField::DurationInNanos),
    ),
    (
        CrossRowField::StatusCode.client_name(),
        ProfileFieldSource::CrossRow(CrossRowField::StatusCode),
    ),
];

/// The field table of the Data Prepper span index.
pub(crate) static DATA_PREPPER_TABLE: ProfileTable = ProfileTable {
    table: SPANS_TABLE,
    fields: &[&SPAN_FIELDS, &TRACE_GROUP_FIELDS],
    attribute_keys: AttributeKeys::Escaped,
};
