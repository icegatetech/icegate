//! Iceberg schema definitions for OpenTelemetry data structures.
//!
//! This module provides Iceberg table schemas for OpenTelemetry logs, spans
//! (traces), events, and metrics, based on the OpenTelemetry Protocol (OTLP)
//! protobuf definitions.

use std::sync::Arc;

use iceberg::{
    Error, ErrorKind,
    spec::{
        ListType, MapType, NestedField, PartitionSpec, PrimitiveType, Schema, SortDirection, SortField, SortOrder,
        StructType, Transform, Type,
    },
};

use crate::error::Result;

/// Creates the Iceberg schema for `OpenTelemetry` logs.
///
/// Based on the `LogRecord` message from
/// opentelemetry/proto/logs/v1/logs.proto. Body is simplified from `AnyValue`
/// variant to String type. Attributes are merged from resource, scope, and
/// log-level attributes into a single Map<String, String>.
///
/// # Partitioning
/// - `tenant_id` (identity)
/// - day(`timestamp`)
///
/// # Sorting
/// - `cloud_account_id` (ascending)
/// - `service_name` (ascending)
/// - `timestamp` (descending) - recent-first ordering
///
/// # Field IDs
/// Field IDs are assigned sequentially to match Iceberg catalog behavior.
/// Map nested fields (key=12, value=13) come after attributes (11).
pub fn logs_schema() -> Result<Schema> {
    // Create Map<String, String> for attributes
    // Nested field IDs must be sequential after parent (11 -> 12, 13)
    let attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(
            12, // Sequential after attributes field (11)
            "key",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            13, // Sequential after key (12)
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            // Multi-tenancy fields
            Arc::new(NestedField::required(
                1,
                "tenant_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "cloud_account_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                3,
                "service_name",
                Type::Primitive(PrimitiveType::String),
            )),
            // Timestamp fields (microsecond precision)
            Arc::new(NestedField::required(
                4,
                "timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                5,
                "observed_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                6,
                "ingested_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            // W3C trace context
            Arc::new(NestedField::optional(
                7,
                "trace_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                8,
                "span_id",
                Type::Primitive(PrimitiveType::String),
            )),
            // Severity information
            Arc::new(NestedField::optional(
                9,
                "severity_text",
                Type::Primitive(PrimitiveType::String),
            )),
            // Body (simplified from AnyValue variant to String)
            Arc::new(NestedField::optional(
                10,
                "body",
                Type::Primitive(PrimitiveType::String),
            )),
            // Attributes (merged from resource, scope, and log attributes)
            // Map nested fields use IDs 12, 13
            Arc::new(NestedField::required(11, "attributes", attributes_map)),
        ])
        .build()?;

    Ok(schema)
}

/// Creates partition specification for logs table.
///
/// Partitions by:
/// - `tenant_id` (identity transform)
/// - day(`timestamp`) (day transform)
pub fn logs_partition_spec(schema: &Schema) -> Result<PartitionSpec> {
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(1)
        .add_partition_field("tenant_id", "tenant_id", Transform::Identity)?
        .add_partition_field("timestamp", "timestamp_day", Transform::Day)?
        .build()?;

    Ok(spec)
}

/// Creates sort order for logs table.
///
/// Sorts by:
/// - `cloud_account_id` (ascending)
/// - `service_name` (ascending)
/// - `timestamp` (descending) - recent-first ordering
pub fn logs_sort_order(schema: &Schema) -> Result<SortOrder> {
    let cloud_account_id_field = schema.field_by_name("cloud_account_id").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "field 'cloud_account_id' not found in logs schema",
        )
    })?;

    let service_name_field = schema
        .field_by_name("service_name")
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "field 'service_name' not found in logs schema"))?;

    let timestamp_field = schema
        .field_by_name("timestamp")
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "field 'timestamp' not found in logs schema"))?;

    let sort_order = SortOrder::builder()
        .with_order_id(1)
        .with_sort_field(SortField {
            source_id: cloud_account_id_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: service_name_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: timestamp_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Descending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .build(schema)?;

    Ok(sort_order)
}

/// Creates the Iceberg schema for `OpenTelemetry` spans (traces).
///
/// Based on the Span message from opentelemetry/proto/trace/v1/trace.proto.
/// Includes nested events and links as List<Struct> types.
/// Attributes are split into two top-level maps:
/// - `resource_attributes` (Map<string, string>): OTLP resource attributes.
/// - `span_attributes` (Map<string, string>): OTLP span attributes plus
///   folded `InstrumentationScope.attributes`.
///
/// # Nested Structures
/// - events: `List<Struct>` - Span events (NO `trace_id`/`span_id`, inherits from parent)
/// - links: `List<Struct>` - Span links (HAS `trace_id`/`span_id`, references linked span)
///
/// # Partitioning
/// - `tenant_id` (identity)
/// - day(`timestamp`)
///
/// # Sorting
/// - `cloud_account_id` (ascending)
/// - `service_name` (ascending)
/// - `trace_id` (ascending) - groups spans by trace for reconstruction
/// - `timestamp` (descending)
#[allow(clippy::too_many_lines)]
pub fn spans_schema() -> Result<Schema> {
    // Field IDs are hardcoded depth-first so reading top-to-bottom gives the
    // assignment order. Parents sit on the left column, their nested children
    // on indented lines immediately below.
    let schema = Schema::builder()
        .with_schema_id(2)
        .with_fields(vec![
            // Multi-tenancy fields
            Arc::new(NestedField::required(
                1,
                "tenant_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "cloud_account_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                3,
                "service_name",
                Type::Primitive(PrimitiveType::String),
            )),
            // Trace identifiers
            Arc::new(NestedField::required(
                4,
                "trace_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                5,
                "span_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                6,
                "parent_span_id",
                Type::Primitive(PrimitiveType::String),
            )),
            // Timestamp fields
            Arc::new(NestedField::required(
                7,
                "timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                8,
                "end_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                9,
                "ingested_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                10,
                "duration_micros",
                Type::Primitive(PrimitiveType::Long),
            )),
            // Span metadata
            Arc::new(NestedField::optional(
                11,
                "trace_state",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                12,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(13, "kind", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(
                14,
                "status_code",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                15,
                "status_message",
                Type::Primitive(PrimitiveType::String),
            )),
            // Resource-level attributes (OTLP `Resource.attributes`).
            // Map nested key/value consume IDs 17, 18.
            Arc::new(NestedField::required(
                16,
                "resource_attributes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(17, "key", Type::Primitive(PrimitiveType::String))),
                    Arc::new(NestedField::required(
                        18,
                        "value",
                        Type::Primitive(PrimitiveType::String),
                    )),
                )),
            )),
            // Flags and monitoring
            Arc::new(NestedField::optional(19, "flags", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(
                20,
                "dropped_attributes_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                21,
                "dropped_events_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                22,
                "dropped_links_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            // Nested events (NO trace_id/span_id — inherits from parent span).
            // List element struct consumes ID 24; its fields consume 25–27, 30;
            // the nested attributes Map consumes 27 with key/value 28, 29.
            Arc::new(NestedField::optional(
                23,
                "events",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    24,
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::required(
                            25,
                            "timestamp",
                            Type::Primitive(PrimitiveType::Timestamp),
                        )),
                        Arc::new(NestedField::required(
                            26,
                            "name",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::required(
                            27,
                            "attributes",
                            Type::Map(MapType::new(
                                Arc::new(NestedField::required(28, "key", Type::Primitive(PrimitiveType::String))),
                                Arc::new(NestedField::required(
                                    29,
                                    "value",
                                    Type::Primitive(PrimitiveType::String),
                                )),
                            )),
                        )),
                        Arc::new(NestedField::required(
                            30,
                            "dropped_attributes_count",
                            Type::Primitive(PrimitiveType::Int),
                        )),
                    ])),
                    true,
                )))),
            )),
            // Nested links (HAS trace_id/span_id — references linked span).
            // List element struct consumes ID 32; its fields consume 33–36, 39, 40;
            // the nested attributes Map consumes 36 with key/value 37, 38.
            Arc::new(NestedField::optional(
                31,
                "links",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    32,
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::required(
                            33,
                            "trace_id",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::required(
                            34,
                            "span_id",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::optional(
                            35,
                            "trace_state",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::required(
                            36,
                            "attributes",
                            Type::Map(MapType::new(
                                Arc::new(NestedField::required(37, "key", Type::Primitive(PrimitiveType::String))),
                                Arc::new(NestedField::required(
                                    38,
                                    "value",
                                    Type::Primitive(PrimitiveType::String),
                                )),
                            )),
                        )),
                        Arc::new(NestedField::required(
                            39,
                            "dropped_attributes_count",
                            Type::Primitive(PrimitiveType::Int),
                        )),
                        Arc::new(NestedField::optional(40, "flags", Type::Primitive(PrimitiveType::Int))),
                    ])),
                    true,
                )))),
            )),
            // Span-level attributes (OTLP `Span.attributes` plus folded
            // `ScopeSpans.scope.attributes`). Parent + key + value consume
            // IDs 41, 42, 43.
            Arc::new(NestedField::required(
                41,
                "span_attributes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(42, "key", Type::Primitive(PrimitiveType::String))),
                    Arc::new(NestedField::required(
                        43,
                        "value",
                        Type::Primitive(PrimitiveType::String),
                    )),
                )),
            )),
        ])
        .build()?;

    Ok(schema)
}

/// Creates partition specification for spans table.
///
/// Partitions by:
/// - `tenant_id` (identity transform)
/// - day(`timestamp`) (day transform)
pub fn spans_partition_spec(schema: &Schema) -> Result<PartitionSpec> {
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(2)
        .add_partition_field("tenant_id", "tenant_id", Transform::Identity)?
        .add_partition_field("timestamp", "timestamp_day", Transform::Day)?
        .build()?;

    Ok(spec)
}

/// Creates sort order for spans table.
///
/// Sorts by:
/// - `cloud_account_id` (ascending)
/// - `service_name` (ascending)
/// - `trace_id` (ascending) - groups spans by trace for reconstruction
/// - `timestamp` (descending)
pub fn spans_sort_order(schema: &Schema) -> Result<SortOrder> {
    let cloud_account_id_field = schema.field_by_name("cloud_account_id").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "field 'cloud_account_id' not found in spans schema",
        )
    })?;

    let service_name_field = schema
        .field_by_name("service_name")
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "field 'service_name' not found in spans schema"))?;

    let trace_id_field = schema
        .field_by_name("trace_id")
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "field 'trace_id' not found in spans schema"))?;

    let timestamp_field = schema
        .field_by_name("timestamp")
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "field 'timestamp' not found in spans schema"))?;

    let sort_order = SortOrder::builder()
        .with_order_id(2)
        .with_sort_field(SortField {
            source_id: cloud_account_id_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: service_name_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: trace_id_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: timestamp_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Descending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .build(schema)?;

    Ok(sort_order)
}

/// Creates the Iceberg schema for `OpenTelemetry` semantic events.
///
/// Events are extracted from the logs stream based on the `event_name` field.
/// See: <https://opentelemetry.io/docs/specs/semconv/general/events/>
///
/// # Partitioning
/// - `tenant_id` (identity)
/// - day(`timestamp`)
///
/// # Sorting
/// - `cloud_account_id` (ascending)
/// - `service_name` (ascending)
/// - `timestamp` (descending) - recent-first ordering
pub fn events_schema() -> Result<Schema> {
    // Create Map<String, String> for attributes (field IDs: 12, 13)
    let attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(12, "key", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::required(
            13,
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    let schema = Schema::builder()
        .with_schema_id(3)
        .with_fields(vec![
            // Multi-tenancy fields
            Arc::new(NestedField::required(
                1,
                "tenant_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "cloud_account_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                3,
                "service_name",
                Type::Primitive(PrimitiveType::String),
            )),
            // Timestamp fields
            Arc::new(NestedField::required(
                4,
                "timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                5,
                "observed_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                6,
                "ingested_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            // Event identification
            Arc::new(NestedField::required(
                7,
                "event_domain",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                8,
                "event_name",
                Type::Primitive(PrimitiveType::String),
            )),
            // Trace context
            Arc::new(NestedField::optional(
                9,
                "trace_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                10,
                "span_id",
                Type::Primitive(PrimitiveType::String),
            )),
            // Event attributes
            Arc::new(NestedField::required(11, "attributes", attributes_map)),
        ])
        .build()?;

    Ok(schema)
}

/// Creates partition specification for events table.
///
/// Partitions by:
/// - `tenant_id` (identity transform)
/// - day(`timestamp`) (day transform)
pub fn events_partition_spec(schema: &Schema) -> Result<PartitionSpec> {
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(3)
        .add_partition_field("tenant_id", "tenant_id", Transform::Identity)?
        .add_partition_field("timestamp", "timestamp_day", Transform::Day)?
        .build()?;

    Ok(spec)
}

/// Creates sort order for events table.
///
/// Sorts by:
/// - `cloud_account_id` (ascending)
/// - `service_name` (ascending)
/// - `timestamp` (descending) - recent-first ordering
pub fn events_sort_order(schema: &Schema) -> Result<SortOrder> {
    let cloud_account_id_field = schema.field_by_name("cloud_account_id").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "field 'cloud_account_id' not found in events schema",
        )
    })?;

    let service_name_field = schema.field_by_name("service_name").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "field 'service_name' not found in events schema",
        )
    })?;

    let timestamp_field = schema
        .field_by_name("timestamp")
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "field 'timestamp' not found in events schema"))?;

    let sort_order = SortOrder::builder()
        .with_order_id(3)
        .with_sort_field(SortField {
            source_id: cloud_account_id_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: service_name_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: timestamp_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Descending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .build(schema)?;

    Ok(sort_order)
}

/// Creates the Iceberg schema for `OpenTelemetry` metrics.
///
/// Based on the `Metric` and `DataPoint` messages from
/// opentelemetry/proto/metrics/v1/metrics.proto. This schema combines all
/// metric types (gauge, sum, histogram, `exponential_histogram`, summary)
/// into a single table with optional fields for type-specific data.
///
/// # Partitioning
/// - `tenant_id` (identity)
/// - day(`timestamp`)
///
/// # Sorting
/// - `cloud_account_id` (ascending)
/// - `service_name` (ascending)
/// - `service_instance_id` (ascending)
/// - `timestamp` (descending) - recent-first ordering
#[allow(clippy::too_many_lines)]
pub fn metrics_schema() -> Result<Schema> {
    // Create Map<String, String> for main attributes (field IDs: 34, 35)
    let attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(34, "key", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::required(
            35,
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    // Create Map<String, String> for exemplar attributes (field IDs: 36, 37)
    let exemplar_attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(36, "key", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::required(
            37,
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    // Quantile value struct for summary metrics
    let quantile_struct = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::required(
            38,
            "quantile",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::required(
            39,
            "value",
            Type::Primitive(PrimitiveType::Double),
        )),
    ]));

    // Exemplar struct for histogram/gauge/sum metrics
    let exemplar_struct = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(
            40,
            "timestamp",
            Type::Primitive(PrimitiveType::Timestamp),
        )),
        Arc::new(NestedField::optional(
            41,
            "value_double",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            42,
            "value_int",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            43,
            "span_id",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            44,
            "trace_id",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(45, "attributes", exemplar_attributes_map)),
    ]));

    let schema = Schema::builder()
        .with_schema_id(4)
        .with_fields(vec![
            // Multi-tenancy fields
            Arc::new(NestedField::required(
                1,
                "tenant_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "cloud_account_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                3,
                "service_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                4,
                "service_instance_id",
                Type::Primitive(PrimitiveType::String),
            )),
            // Timestamp fields
            Arc::new(NestedField::required(
                5,
                "timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::optional(
                6,
                "start_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                7,
                "ingested_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            // Metric identification
            Arc::new(NestedField::required(
                8,
                "metric_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                9,
                "metric_type",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                10,
                "description",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                11,
                "unit",
                Type::Primitive(PrimitiveType::String),
            )),
            // Metric metadata
            Arc::new(NestedField::optional(
                12,
                "aggregation_temporality",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                13,
                "is_monotonic",
                Type::Primitive(PrimitiveType::Boolean),
            )),
            // Attributes (merged from resource, scope, and metric/data point attributes)
            Arc::new(NestedField::required(14, "attributes", attributes_map)),
            // Value fields (for gauge and sum metrics)
            Arc::new(NestedField::optional(
                15,
                "value_double",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                16,
                "value_int",
                Type::Primitive(PrimitiveType::Long),
            )),
            // Common histogram fields (for histogram, exponential_histogram, and summary)
            Arc::new(NestedField::optional(17, "count", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::optional(18, "sum", Type::Primitive(PrimitiveType::Double))),
            Arc::new(NestedField::optional(19, "min", Type::Primitive(PrimitiveType::Double))),
            Arc::new(NestedField::optional(20, "max", Type::Primitive(PrimitiveType::Double))),
            // Standard histogram fields
            Arc::new(NestedField::optional(
                21,
                "bucket_counts",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    46,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )))),
            )),
            Arc::new(NestedField::optional(
                22,
                "explicit_bounds",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    47,
                    Type::Primitive(PrimitiveType::Double),
                    true,
                )))),
            )),
            // Exponential histogram fields
            Arc::new(NestedField::optional(23, "scale", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(
                24,
                "zero_count",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                25,
                "zero_threshold",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                26,
                "positive_offset",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                27,
                "positive_bucket_counts",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    48,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )))),
            )),
            Arc::new(NestedField::optional(
                28,
                "negative_offset",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                29,
                "negative_bucket_counts",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    49,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )))),
            )),
            // Summary fields
            Arc::new(NestedField::optional(
                30,
                "quantile_values",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    50,
                    quantile_struct,
                    true,
                )))),
            )),
            // Flags and exemplars
            Arc::new(NestedField::optional(31, "flags", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(
                32,
                "exemplars",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    51,
                    exemplar_struct,
                    true,
                )))),
            )),
        ])
        .build()?;

    Ok(schema)
}

/// Creates partition specification for metrics table.
///
/// Partitions by:
/// - `tenant_id` (identity transform)
/// - day(`timestamp`) (day transform)
pub fn metrics_partition_spec(schema: &Schema) -> Result<PartitionSpec> {
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(4)
        .add_partition_field("tenant_id", "tenant_id", Transform::Identity)?
        .add_partition_field("timestamp", "timestamp_day", Transform::Day)?
        .build()?;

    Ok(spec)
}

/// Creates sort order for metrics table.
///
/// Sorts by:
/// - `cloud_account_id` (ascending)
/// - `metric_name` (ascending)
/// - `service_name` (ascending)
/// - `service_instance_id` (ascending)
/// - `timestamp` (descending) - recent-first ordering
pub fn metrics_sort_order(schema: &Schema) -> Result<SortOrder> {
    let cloud_account_id_field = schema.field_by_name("cloud_account_id").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "field 'cloud_account_id' not found in metrics schema",
        )
    })?;

    let metric_name_id_field = schema.field_by_name("metric_name").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "field 'metric_name' not found in metrics schema",
        )
    })?;

    let service_name_field = schema.field_by_name("service_name").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "field 'service_name' not found in metrics schema",
        )
    })?;

    let service_instance_id_field = schema.field_by_name("service_instance_id").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "field 'service_instance_id' not found in metrics schema",
        )
    })?;

    let timestamp_field = schema
        .field_by_name("timestamp")
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "field 'timestamp' not found in metrics schema"))?;

    let sort_order = SortOrder::builder()
        .with_order_id(4)
        .with_sort_field(SortField {
            source_id: cloud_account_id_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: metric_name_id_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: service_name_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: service_instance_id_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .with_sort_field(SortField {
            source_id: timestamp_field.id,
            transform: Transform::Identity,
            direction: SortDirection::Descending,
            null_order: iceberg::spec::NullOrder::First,
        })
        .build(schema)?;

    Ok(sort_order)
}

// ── Log table column name constants ──────────────────────────────────

/// Multi-tenancy partition column.
pub const COL_TENANT_ID: &str = "tenant_id";
/// Primary event timestamp (microsecond precision).
pub const COL_TIMESTAMP: &str = "timestamp";
/// Log message body.
pub const COL_BODY: &str = "body";
/// Merged `MAP<String, String>` of resource/scope/log attributes.
pub const COL_ATTRIBUTES: &str = "attributes";
/// Spans table — resource-attribute MAP column.
///
/// Holds OTLP `Resource.attributes` after the 2026-04-19 split.
/// Logs still use [`COL_ATTRIBUTES`] which is a single merged map.
pub const COL_RESOURCE_ATTRIBUTES: &str = "resource_attributes";

/// Spans table — span-attribute MAP column.
///
/// Holds OTLP `Span.attributes` plus folded `InstrumentationScope.attributes`
/// after the 2026-04-19 split.
pub const COL_SPAN_ATTRIBUTES: &str = "span_attributes";
/// `OpenTelemetry` severity text (e.g. `"ERROR"`, `"INFO"`).
pub const COL_SEVERITY_TEXT: &str = "severity_text";
/// Service name extracted from resource attributes.
pub const COL_SERVICE_NAME: &str = "service_name";
/// Cloud account identifier for multi-account tenancy.
pub const COL_CLOUD_ACCOUNT_ID: &str = "cloud_account_id";
/// W3C Trace Context trace identifier.
pub const COL_TRACE_ID: &str = "trace_id";
/// W3C Trace Context span identifier.
pub const COL_SPAN_ID: &str = "span_id";
/// Spans table — `parent_span_id` column (STRING).
pub const COL_PARENT_SPAN_ID: &str = "parent_span_id";
/// Spans table — `duration_micros` column (BIGINT).
pub const COL_DURATION_MICROS: &str = "duration_micros";
/// Spans table — `end_timestamp` column (TIMESTAMP).
pub const COL_END_TIMESTAMP: &str = "end_timestamp";
/// Spans table — `name` column (STRING).
pub const COL_NAME: &str = "name";
/// Spans table — `kind` column (INT, OTLP `SpanKind` enum).
pub const COL_KIND: &str = "kind";
/// Spans table — `status_code` column (INT, OTLP `Status.code` enum).
pub const COL_STATUS_CODE: &str = "status_code";
/// Spans table — `status_message` column (STRING).
pub const COL_STATUS_MESSAGE: &str = "status_message";
/// Spans table — `events` column (ARRAY of struct).
pub const COL_EVENTS: &str = "events";
/// Spans table — `links` column (ARRAY of struct).
pub const COL_LINKS: &str = "links";
/// Grafana-compatible alias for [`COL_SEVERITY_TEXT`].
pub const LEVEL_ALIAS: &str = "level";

/// Indexed attribute columns for log label extraction.
///
/// These columns are extracted as top-level fields from log query results.
/// Used by planner (for grouping) and handlers (for output).
pub const LOG_INDEXED_ATTRIBUTE_COLUMNS: &[&str] = &[
    COL_CLOUD_ACCOUNT_ID,
    COL_SERVICE_NAME,
    COL_TRACE_ID,
    COL_SPAN_ID,
    COL_SEVERITY_TEXT,
];

/// Indexed columns visible in label discovery and series identification.
///
/// Excludes high-cardinality columns (`trace_id`, `span_id`) that have
/// per-request uniqueness — including them would explode the number of
/// distinct series and make `/labels` output noisy. These columns are still
/// readable via `/label_values` when explicitly requested.
///
/// Used by `/labels`, `/label_values` discovery, and `/series` endpoints.
pub const LOG_SERIES_LABEL_COLUMNS: &[&str] = &[COL_CLOUD_ACCOUNT_ID, COL_SERVICE_NAME, COL_SEVERITY_TEXT];

// ── Span tag-discovery lists ─────────────────────────────────────────

/// Indexed top-level columns on the `spans` table whose distinct values can
/// be enumerated via the tag-values endpoint.
///
/// Combines low-cardinality columns (`cloud_account_id`, `service_name`,
/// `name`) useful for label-style discovery with high-cardinality
/// identifiers (`trace_id`, `span_id`, `parent_span_id`) that are useful
/// as explicit filter values. Numeric-only columns (`duration_micros`,
/// `kind`, `status_code`) are excluded — they are exposed separately as
/// `TraceQL` intrinsics.
pub const SPAN_INDEXED_ATTRIBUTE_COLUMNS: &[&str] = &[
    COL_CLOUD_ACCOUNT_ID,
    COL_SERVICE_NAME,
    COL_NAME,
    COL_TRACE_ID,
    COL_SPAN_ID,
    COL_PARENT_SPAN_ID,
];

/// `TraceQL` intrinsic tag names exposed on `/api/v2/search/tags` with
/// `scope=intrinsic`.
///
/// These are not literal column names — they are the user-facing tag names
/// Grafana expects. Internal mapping to underlying columns happens in the
/// tempo metadata layer.
pub const TRACEQL_INTRINSIC_TAGS: &[&str] = &[
    "name",
    "status",
    "kind",
    "duration",
    "traceDuration",
    "rootName",
    "rootServiceName",
    "traceID",
    "spanID",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logs_schema() {
        let schema = logs_schema().expect("Failed to create logs schema");
        // highest_field_id includes nested field IDs from Map
        assert!(schema.highest_field_id() >= 13);
        assert!(schema.field_by_name("tenant_id").is_some());
        assert!(schema.field_by_name("timestamp").is_some());
        assert!(schema.field_by_name("body").is_some());
        assert!(schema.field_by_name("attributes").is_some());
    }

    #[test]
    fn test_spans_schema() {
        let schema = spans_schema().expect("Failed to create spans schema");
        // Field IDs 1-22 + events(23-30) + links(31-40) + span_attributes(41-43).
        assert_eq!(schema.highest_field_id(), 43);
        assert!(schema.field_by_name("trace_id").is_some());
        assert!(schema.field_by_name("span_id").is_some());
        assert!(schema.field_by_name("events").is_some());
        assert!(schema.field_by_name("links").is_some());
        // Attributes split (spec 2026-04-19-spans-attributes-split-design.md):
        assert!(
            schema.field_by_name("attributes").is_none(),
            "legacy single `attributes` field must be gone"
        );
        assert!(schema.field_by_name("resource_attributes").is_some());
        assert!(schema.field_by_name("span_attributes").is_some());
    }

    #[test]
    fn test_spans_schema_nested_ids_sequential() {
        use iceberg::spec::Type;

        let schema = spans_schema().expect("Failed to create spans schema");

        // resource_attributes Map: parent=16, key=17, value=18
        let resource_attrs = schema.field_by_name("resource_attributes").expect("resource_attributes field");
        assert_eq!(resource_attrs.id, 16);
        let Type::Map(map) = &*resource_attrs.field_type else {
            panic!("resource_attributes must be Map");
        };
        assert_eq!(map.key_field.id, 17);
        assert_eq!(map.value_field.id, 18);

        // events List<Struct>: list=23, elem struct=24,
        // struct fields timestamp=25, name=26, attributes=27 (map key=28, value=29), dropped_attributes_count=30
        let events = schema.field_by_name("events").expect("events field");
        assert_eq!(events.id, 23);
        let Type::List(list) = &*events.field_type else {
            panic!("events must be List");
        };
        assert_eq!(list.element_field.id, 24);
        let Type::Struct(s) = &*list.element_field.field_type else {
            panic!("events element must be Struct");
        };
        let ids: Vec<i32> = s.fields().iter().map(|f| f.id).collect();
        assert_eq!(ids, vec![25, 26, 27, 30]);
        let inner_attrs = s.fields().iter().find(|f| f.name == "attributes").expect("event attrs");
        let Type::Map(m) = &*inner_attrs.field_type else {
            panic!("event attributes must be Map");
        };
        assert_eq!(m.key_field.id, 28);
        assert_eq!(m.value_field.id, 29);

        // links: unchanged by this change (still IDs 31-40).
        let links = schema.field_by_name("links").expect("links field");
        assert_eq!(links.id, 31);

        // span_attributes Map: parent=41, key=42, value=43 (new, post-links).
        let span_attrs = schema.field_by_name("span_attributes").expect("span_attributes field");
        assert_eq!(span_attrs.id, 41);
        let Type::Map(sm) = &*span_attrs.field_type else {
            panic!("span_attributes must be Map");
        };
        assert_eq!(sm.key_field.id, 42);
        assert_eq!(sm.value_field.id, 43);
    }

    #[test]
    fn test_events_schema() {
        let schema = events_schema().expect("Failed to create events schema");
        // highest_field_id includes nested field IDs from Map
        assert!(schema.highest_field_id() >= 11);
        assert!(schema.field_by_name("event_domain").is_some());
        assert!(schema.field_by_name("event_name").is_some());
    }

    #[test]
    fn test_metrics_schema() {
        let schema = metrics_schema().expect("Failed to create metrics schema");
        // highest_field_id includes nested field IDs from Maps, Lists, and Structs
        assert!(schema.highest_field_id() >= 31);
        assert!(schema.field_by_name("metric_name").is_some());
        assert!(schema.field_by_name("metric_type").is_some());
        assert!(schema.field_by_name("value_double").is_some());
        assert!(schema.field_by_name("bucket_counts").is_some());
        assert!(schema.field_by_name("exemplars").is_some());
    }
}
