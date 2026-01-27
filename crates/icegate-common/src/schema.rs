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
/// Attributes are merged from resource, scope, and span-level attributes.
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
    // Create Map<String, String> for main attributes (field IDs: 24, 25)
    let attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(24, "key", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::required(
            25,
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    // Create Map<String, String> for event attributes (field IDs: 26, 27)
    let event_attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(26, "key", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::required(
            27,
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    // Create Map<String, String> for link attributes (field IDs: 28, 29)
    let link_attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(28, "key", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::required(
            29,
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    // Event struct (nested in spans) - NO trace_id/span_id
    let event_struct = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::required(
            30,
            "timestamp",
            Type::Primitive(PrimitiveType::Timestamp),
        )),
        Arc::new(NestedField::required(
            31,
            "name",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(32, "attributes", event_attributes_map)),
        Arc::new(NestedField::required(
            33,
            "dropped_attributes_count",
            Type::Primitive(PrimitiveType::Int),
        )),
    ]));

    // Link struct (nested in spans) - HAS trace_id/span_id
    let link_struct = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::required(
            34,
            "trace_id",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            35,
            "span_id",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::optional(
            36,
            "trace_state",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(37, "attributes", link_attributes_map)),
        Arc::new(NestedField::required(
            38,
            "dropped_attributes_count",
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::optional(39, "flags", Type::Primitive(PrimitiveType::Int))),
    ]));

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
            Arc::new(NestedField::optional(
                11,
                "trace_state",
                Type::Primitive(PrimitiveType::String),
            )),
            // Span metadata
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
            // Attributes (merged from resource, scope, and span attributes)
            Arc::new(NestedField::required(16, "attributes", attributes_map)),
            // Flags and monitoring
            Arc::new(NestedField::optional(17, "flags", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(
                18,
                "dropped_attributes_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                19,
                "dropped_events_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                20,
                "dropped_links_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            // Nested events (NO trace_id/span_id - inherits from parent span)
            Arc::new(NestedField::optional(
                21,
                "events",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    40,
                    event_struct,
                    true,
                )))),
            )),
            // Nested links (HAS trace_id/span_id - references linked span)
            Arc::new(NestedField::optional(
                22,
                "links",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    41,
                    link_struct,
                    true,
                )))),
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

/// Indexed attribute columns for log label extraction.
///
/// These columns are extracted as top-level fields from log query results.
/// Used by planner (for grouping) and handlers (for output).
pub const LOG_INDEXED_ATTRIBUTE_COLUMNS: &[&str] = &[
    "cloud_account_id",
    "service_name",
    "trace_id",
    "span_id",
    "severity_text",
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
        // highest_field_id includes nested field IDs from Maps, Lists, and Structs
        assert!(schema.highest_field_id() >= 22);
        assert!(schema.field_by_name("trace_id").is_some());
        assert!(schema.field_by_name("span_id").is_some());
        assert!(schema.field_by_name("events").is_some());
        assert!(schema.field_by_name("links").is_some());
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
