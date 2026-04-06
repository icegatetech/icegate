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
    //  1  tenant_id        7  trace_id
    //  2  cloud_account_id 8  span_id
    //  3  service_name     9  severity_text
    //  4  timestamp       10  body
    //  5  observed_ts     11  attributes (Map)
    //  6  ingested_ts       12 key
    //                       13 value

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
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
            Arc::new(NestedField::optional(
                9,
                "severity_text",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                10,
                "body",
                Type::Primitive(PrimitiveType::String),
            )),
            // 11-13: Attributes MAP(String, String)
            Arc::new(NestedField::required(
                11,
                "attributes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(12, "key", Type::Primitive(PrimitiveType::String))),
                    Arc::new(NestedField::required(
                        13,
                        "value",
                        Type::Primitive(PrimitiveType::String),
                    )),
                )),
            )),
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
    // Field IDs are sequential with complex type sub-fields immediately
    // after their parent, matching catalog-assigned ordering.
    //
    //   1  tenant_id              19 flags
    //   2  cloud_account_id       20 dropped_attributes_count
    //   3  service_name           21 dropped_events_count
    //   4  trace_id               22 dropped_links_count
    //   5  span_id                23 events (List)
    //   6  parent_span_id           24 element (Struct)
    //   7  timestamp                  25 timestamp
    //   8  end_timestamp              26 name
    //   9  ingested_timestamp         27 attributes (Map)
    //  10  duration_micros              28 key
    //  11  trace_state                  29 value
    //  12  name                       30 dropped_attributes_count
    //  13  kind                   31 links (List)
    //  14  status_code              32 element (Struct)
    //  15  status_message             33 trace_id
    //  16  attributes (Map)           34 span_id
    //    17 key                       35 trace_state
    //    18 value                      36 attributes (Map)
    //                                   37 key
    //                                   38 value
    //                                 39 dropped_attributes_count
    //                                 40 flags

    let schema = Schema::builder()
        .with_schema_id(2)
        .with_fields(vec![
            // 1-3: Multi-tenancy fields
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
            // 4-6: Trace identifiers
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
            // 7-11: Timestamp fields
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
            // 12-15: Span metadata
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
            // 16-18: Attributes MAP(String, String)
            Arc::new(NestedField::required(
                16,
                "attributes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(17, "key", Type::Primitive(PrimitiveType::String))),
                    Arc::new(NestedField::required(
                        18,
                        "value",
                        Type::Primitive(PrimitiveType::String),
                    )),
                )),
            )),
            // 19-22: Flags and monitoring counters
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
            // 23-30: Events List<Struct{timestamp, name, attributes, dropped_attributes_count}>
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
            // 31-40: Links List<Struct{trace_id, span_id, trace_state, attributes, dropped_attributes_count, flags}>
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
    //  1  tenant_id        7  event_domain
    //  2  cloud_account_id 8  event_name
    //  3  service_name     9  trace_id
    //  4  timestamp       10  span_id
    //  5  observed_ts     11  attributes (Map)
    //  6  ingested_ts       12 key
    //                       13 value

    let schema = Schema::builder()
        .with_schema_id(3)
        .with_fields(vec![
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
            // 11-13: Attributes MAP(String, String)
            Arc::new(NestedField::required(
                11,
                "attributes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(12, "key", Type::Primitive(PrimitiveType::String))),
                    Arc::new(NestedField::required(
                        13,
                        "value",
                        Type::Primitive(PrimitiveType::String),
                    )),
                )),
            )),
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
    // Field IDs are sequential with complex type sub-fields immediately
    // after their parent. This matches the order assigned by Iceberg
    // catalogs (Nessie/REST) during table creation.
    //
    //   1  tenant_id              17 value_double            33 negative_offset
    //   2  cloud_account_id       18 value_int               34 negative_bucket_counts
    //   3  service_name           19 count                     35 element
    //   4  service_instance_id    20 sum                     36 quantile_values
    //   5  timestamp              21 min                       37 element (Struct)
    //   6  start_timestamp        22 max                         38 quantile
    //   7  ingested_timestamp     23 bucket_counts               39 value
    //   8  metric_name              24 element                40 flags
    //   9  metric_type            25 explicit_bounds          41 exemplars
    //  10  description              26 element                  42 element (Struct)
    //  11  unit                   27 scale                        43 timestamp
    //  12  aggregation_temp.      28 zero_count                   44 value_double
    //  13  is_monotonic           29 zero_threshold               45 value_int
    //  14  attributes             30 positive_offset              46 span_id
    //    15 key                   31 positive_bucket_counts       47 trace_id
    //    16 value                   32 element                    48 attributes
    //                                                               49 key
    //                                                               50 value

    let schema = Schema::builder()
        .with_schema_id(4)
        .with_fields(vec![
            // 1-4: Multi-tenancy fields
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
            // 5-7: Timestamp fields
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
            // 8-11: Metric identification
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
            // 12-13: Metric metadata
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
            // 14-16: Attributes MAP(String, String)
            Arc::new(NestedField::required(
                14,
                "attributes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(15, "key", Type::Primitive(PrimitiveType::String))),
                    Arc::new(NestedField::required(
                        16,
                        "value",
                        Type::Primitive(PrimitiveType::String),
                    )),
                )),
            )),
            // 17-18: Value fields (gauge/sum)
            Arc::new(NestedField::optional(
                17,
                "value_double",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                18,
                "value_int",
                Type::Primitive(PrimitiveType::Long),
            )),
            // 19-22: Common histogram fields
            Arc::new(NestedField::optional(19, "count", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::optional(20, "sum", Type::Primitive(PrimitiveType::Double))),
            Arc::new(NestedField::optional(21, "min", Type::Primitive(PrimitiveType::Double))),
            Arc::new(NestedField::optional(22, "max", Type::Primitive(PrimitiveType::Double))),
            // 23-24: Standard histogram bucket_counts
            Arc::new(NestedField::optional(
                23,
                "bucket_counts",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    24,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )))),
            )),
            // 25-26: Standard histogram explicit_bounds
            Arc::new(NestedField::optional(
                25,
                "explicit_bounds",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    26,
                    Type::Primitive(PrimitiveType::Double),
                    true,
                )))),
            )),
            // 27-35: Exponential histogram fields
            Arc::new(NestedField::optional(27, "scale", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(
                28,
                "zero_count",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                29,
                "zero_threshold",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                30,
                "positive_offset",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                31,
                "positive_bucket_counts",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    32,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )))),
            )),
            Arc::new(NestedField::optional(
                33,
                "negative_offset",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                34,
                "negative_bucket_counts",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    35,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )))),
            )),
            // 36-39: Summary quantile_values List<Struct{quantile, value}>
            Arc::new(NestedField::optional(
                36,
                "quantile_values",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    37,
                    Type::Struct(StructType::new(vec![
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
                    ])),
                    true,
                )))),
            )),
            // 40: Flags
            Arc::new(NestedField::optional(40, "flags", Type::Primitive(PrimitiveType::Int))),
            // 41-50: Exemplars List<Struct{timestamp, value_double, value_int, span_id, trace_id, attributes}>
            Arc::new(NestedField::optional(
                41,
                "exemplars",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    42,
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::optional(
                            43,
                            "timestamp",
                            Type::Primitive(PrimitiveType::Timestamp),
                        )),
                        Arc::new(NestedField::optional(
                            44,
                            "value_double",
                            Type::Primitive(PrimitiveType::Double),
                        )),
                        Arc::new(NestedField::optional(
                            45,
                            "value_int",
                            Type::Primitive(PrimitiveType::Long),
                        )),
                        Arc::new(NestedField::optional(
                            46,
                            "span_id",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::optional(
                            47,
                            "trace_id",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::required(
                            48,
                            "attributes",
                            Type::Map(MapType::new(
                                Arc::new(NestedField::required(49, "key", Type::Primitive(PrimitiveType::String))),
                                Arc::new(NestedField::required(
                                    50,
                                    "value",
                                    Type::Primitive(PrimitiveType::String),
                                )),
                            )),
                        )),
                    ])),
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

    /// Collects all field IDs from an Iceberg schema by walking the type tree.
    ///
    /// Complex types (Map, List, Struct) have sub-field IDs that must also
    /// be sequential. This helper extracts every ID in depth-first order.
    fn collect_field_ids(fields: &[Arc<NestedField>]) -> Vec<i32> {
        let mut ids = Vec::new();
        for field in fields {
            ids.push(field.id);
            collect_type_ids(&field.field_type, &mut ids);
        }
        ids
    }

    /// Recursively collects field IDs from a nested Iceberg type.
    fn collect_type_ids(ty: &Type, ids: &mut Vec<i32>) {
        match ty {
            Type::Primitive(_) => {}
            Type::Map(map) => {
                ids.push(map.key_field.id);
                collect_type_ids(&map.key_field.field_type, ids);
                ids.push(map.value_field.id);
                collect_type_ids(&map.value_field.field_type, ids);
            }
            Type::List(list) => {
                ids.push(list.element_field.id);
                collect_type_ids(&list.element_field.field_type, ids);
            }
            Type::Struct(s) => {
                for field in s.fields() {
                    ids.push(field.id);
                    collect_type_ids(&field.field_type, ids);
                }
            }
        }
    }

    /// Verifies that a schema has strictly sequential field IDs (1, 2, 3, ...).
    ///
    /// Iceberg catalogs (Nessie, REST) assign field IDs sequentially in
    /// depth-first order when creating tables. If our code-defined schemas
    /// use different IDs, the Iceberg writer fails with field-not-found
    /// errors because it matches by field ID.
    fn assert_sequential_field_ids(schema: &Schema, schema_name: &str) {
        let ids = collect_field_ids(schema.as_struct().fields());

        assert!(!ids.is_empty(), "{schema_name}: schema has no fields");

        // IDs must start at 1
        assert_eq!(ids[0], 1, "{schema_name}: first field ID should be 1, got {}", ids[0]);

        // IDs must be strictly sequential (each = previous + 1)
        for window in ids.windows(2) {
            assert_eq!(
                window[1],
                window[0] + 1,
                "{schema_name}: field IDs are not sequential — got {} after {} (expected {}). Full IDs: {ids:?}",
                window[1],
                window[0],
                window[0] + 1,
            );
        }
    }

    #[test]
    fn test_logs_schema_sequential_ids() {
        let schema = logs_schema().expect("Failed to create logs schema");
        assert_sequential_field_ids(&schema, "logs");
    }

    #[test]
    fn test_spans_schema_sequential_ids() {
        let schema = spans_schema().expect("Failed to create spans schema");
        assert_sequential_field_ids(&schema, "spans");
    }

    #[test]
    fn test_events_schema_sequential_ids() {
        let schema = events_schema().expect("Failed to create events schema");
        assert_sequential_field_ids(&schema, "events");
    }

    #[test]
    fn test_metrics_schema_sequential_ids() {
        let schema = metrics_schema().expect("Failed to create metrics schema");
        assert_sequential_field_ids(&schema, "metrics");
    }
}
