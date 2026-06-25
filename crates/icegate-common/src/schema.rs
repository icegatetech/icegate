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
/// - `service_name` (ascending)
/// - `timestamp` (descending) - recent-first ordering
///
/// # Field IDs
/// Field IDs are assigned sequentially to match Iceberg catalog behavior.
/// Map nested fields (key=11, value=12) come after attributes (10).
pub fn logs_schema() -> Result<Schema> {
    // Create Map<String, String> for attributes
    // Nested field IDs must be sequential after parent (10 -> 11, 12)
    let attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(
            11, // Sequential after attributes field (10)
            "key",
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::required(
            12, // Sequential after key (11)
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            // Multi-tenancy field
            Arc::new(NestedField::required(
                1,
                "tenant_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "service_name",
                Type::Primitive(PrimitiveType::String),
            )),
            // Timestamp fields (microsecond precision)
            Arc::new(NestedField::required(
                3,
                "timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                4,
                "observed_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                5,
                "ingested_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            // W3C trace context. Stored as raw fixed-length bytes (16 / 8) — half
            // the wire size of the previous lowercase-hex strings, and a perfect
            // fit for `FIXED_LEN_BYTE_ARRAY` in Parquet. The query layer hex-
            // encodes/decodes at API boundaries.
            Arc::new(NestedField::optional(
                6,
                "trace_id",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::optional(
                7,
                "span_id",
                Type::Primitive(PrimitiveType::Fixed(8)),
            )),
            // Severity information
            Arc::new(NestedField::optional(
                8,
                "severity_text",
                Type::Primitive(PrimitiveType::String),
            )),
            // Body (simplified from AnyValue variant to String)
            Arc::new(NestedField::optional(9, "body", Type::Primitive(PrimitiveType::String))),
            // Attributes (merged from resource, scope, and log attributes)
            // Map nested fields use IDs 11, 12
            Arc::new(NestedField::required(10, "attributes", attributes_map)),
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
/// - `service_name` (ascending)
/// - `timestamp` (descending) - recent-first ordering
pub fn logs_sort_order(schema: &Schema) -> Result<SortOrder> {
    let service_name_field = schema
        .field_by_name("service_name")
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "field 'service_name' not found in logs schema"))?;

    let timestamp_field = schema
        .field_by_name("timestamp")
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "field 'timestamp' not found in logs schema"))?;

    let sort_order = SortOrder::builder()
        .with_order_id(1)
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
            // Multi-tenancy field
            Arc::new(NestedField::required(
                1,
                "tenant_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "service_name",
                Type::Primitive(PrimitiveType::String),
            )),
            // Trace identifiers stored as raw fixed-length bytes — see logs schema
            // doc comment for rationale. Hex encode/decode at API boundaries only.
            Arc::new(NestedField::required(
                3,
                "trace_id",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::required(
                4,
                "span_id",
                Type::Primitive(PrimitiveType::Fixed(8)),
            )),
            Arc::new(NestedField::optional(
                5,
                "parent_span_id",
                Type::Primitive(PrimitiveType::Fixed(8)),
            )),
            // Timestamp fields
            Arc::new(NestedField::required(
                6,
                "timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                7,
                "end_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                8,
                "ingested_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                9,
                "duration_micros",
                Type::Primitive(PrimitiveType::Long),
            )),
            // Span metadata
            Arc::new(NestedField::optional(
                10,
                "trace_state",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                11,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(12, "kind", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(
                13,
                "status_code",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                14,
                "status_message",
                Type::Primitive(PrimitiveType::String),
            )),
            // Resource-level attributes (OTLP `Resource.attributes`).
            // Map nested key/value consume IDs 16, 17.
            Arc::new(NestedField::required(
                15,
                "resource_attributes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(16, "key", Type::Primitive(PrimitiveType::String))),
                    Arc::new(NestedField::required(
                        17,
                        "value",
                        Type::Primitive(PrimitiveType::String),
                    )),
                )),
            )),
            // Flags and monitoring
            Arc::new(NestedField::optional(18, "flags", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(
                19,
                "dropped_attributes_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                20,
                "dropped_events_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                21,
                "dropped_links_count",
                Type::Primitive(PrimitiveType::Int),
            )),
            // Nested events (NO trace_id/span_id — inherits from parent span).
            // List element struct consumes ID 23; its fields consume 24–26, 29;
            // the nested attributes Map consumes 26 with key/value 27, 28.
            Arc::new(NestedField::optional(
                22,
                "events",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    23,
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::required(
                            24,
                            "timestamp",
                            Type::Primitive(PrimitiveType::Timestamp),
                        )),
                        Arc::new(NestedField::required(
                            25,
                            "name",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::required(
                            26,
                            "attributes",
                            Type::Map(MapType::new(
                                Arc::new(NestedField::required(27, "key", Type::Primitive(PrimitiveType::String))),
                                Arc::new(NestedField::required(
                                    28,
                                    "value",
                                    Type::Primitive(PrimitiveType::String),
                                )),
                            )),
                        )),
                        Arc::new(NestedField::required(
                            29,
                            "dropped_attributes_count",
                            Type::Primitive(PrimitiveType::Int),
                        )),
                    ])),
                    true,
                )))),
            )),
            // Nested links (HAS trace_id/span_id — references linked span).
            // List element struct consumes ID 31; its fields consume 32–35, 38, 39;
            // the nested attributes Map consumes 35 with key/value 36, 37.
            Arc::new(NestedField::optional(
                30,
                "links",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    31,
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::required(
                            32,
                            "trace_id",
                            Type::Primitive(PrimitiveType::Fixed(16)),
                        )),
                        Arc::new(NestedField::required(
                            33,
                            "span_id",
                            Type::Primitive(PrimitiveType::Fixed(8)),
                        )),
                        Arc::new(NestedField::optional(
                            34,
                            "trace_state",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::required(
                            35,
                            "attributes",
                            Type::Map(MapType::new(
                                Arc::new(NestedField::required(36, "key", Type::Primitive(PrimitiveType::String))),
                                Arc::new(NestedField::required(
                                    37,
                                    "value",
                                    Type::Primitive(PrimitiveType::String),
                                )),
                            )),
                        )),
                        Arc::new(NestedField::required(
                            38,
                            "dropped_attributes_count",
                            Type::Primitive(PrimitiveType::Int),
                        )),
                        Arc::new(NestedField::optional(39, "flags", Type::Primitive(PrimitiveType::Int))),
                    ])),
                    true,
                )))),
            )),
            // Span-level attributes (OTLP `Span.attributes` plus folded
            // `ScopeSpans.scope.attributes`). Parent + key + value consume
            // IDs 40, 41, 42.
            Arc::new(NestedField::required(
                40,
                "span_attributes",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(41, "key", Type::Primitive(PrimitiveType::String))),
                    Arc::new(NestedField::required(
                        42,
                        "value",
                        Type::Primitive(PrimitiveType::String),
                    )),
                )),
            )),
        ])
        .build()?;

    Ok(schema)
}

/// Creates the Iceberg schema for the `operations` table.
///
/// `operations` is a typed columnar projection icegate maintains over the
/// LLM/GenAI-flavoured subset of trace `spans` (see TRI-72 design). Every span
/// carrying at least one LLM/GenAI marker attribute is projected into exactly
/// one `operations` row (1:1 by `span_id`), with `gen_ai.*` / `OpenInference` /
/// `Traceloop` semantic-convention attributes normalized into typed columns.
///
/// # Field IDs
/// 58 scalar fields occupy IDs 1–58 in declaration order; the three
/// `List<String>` columns are declared last so their element IDs are
/// contiguous: `stop_sequences` (59, element 60), `finish_reasons`
/// (61, element 62), `encoding_formats` (63, element 64).
/// `highest_field_id() == 64`.
///
/// # Partitioning
/// - `tenant_id` (identity)
/// - day(`timestamp`)
///
/// # Sorting
/// - `trace_id` (ascending) - clusters a trace's operations together
/// - `timestamp` (descending) - recent-first ordering
#[allow(clippy::too_many_lines)]
pub fn operations_schema() -> Result<Schema> {
    // Field IDs are hardcoded so reading top-to-bottom gives the assignment
    // order. The three `List<String>` columns are placed last so their element
    // IDs (60, 62, 64) stay contiguous after every scalar (1..=58) is assigned.
    let schema = Schema::builder()
        .with_schema_id(5)
        .with_fields(vec![
            // ── tenancy ──────────────────────────────────────────────────
            Arc::new(NestedField::required(
                1,
                "tenant_id",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── conversation ─────────────────────────────────────────────
            // Leading identity column (placed before `trace_id`): groups a
            // multi-turn conversation's operations across traces. Sourced from
            // `gen_ai.conversation.id` / session id; NULL when absent.
            Arc::new(NestedField::optional(
                2,
                "conversation_id",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── identity ─────────────────────────────────────────────────
            // Trace context stored as raw fixed-length bytes — see logs/spans
            // schema rationale. Hex encode/decode at API boundaries only.
            // `trace_id` mirrors `spans.trace_id` byte-for-byte and shares its
            // field ID (3) so operations join to spans on `trace_id`.
            Arc::new(NestedField::required(
                3,
                "trace_id",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::required(
                4,
                "span_id",
                Type::Primitive(PrimitiveType::Fixed(8)),
            )),
            Arc::new(NestedField::optional(
                5,
                "parent_span_id",
                Type::Primitive(PrimitiveType::Fixed(8)),
            )),
            Arc::new(NestedField::optional(
                6,
                "service_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                7,
                "scope_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                8,
                "scope_version",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── timing ───────────────────────────────────────────────────
            Arc::new(NestedField::required(
                9,
                "timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                10,
                "end_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                11,
                "duration_micros",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                12,
                "ingested_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            // ── discrimination ───────────────────────────────────────────
            Arc::new(NestedField::required(
                13,
                "operation_name",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── provider / model ─────────────────────────────────────────
            Arc::new(NestedField::optional(
                14,
                "provider_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                15,
                "request_model",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                16,
                "response_model",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                17,
                "response_id",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── sampling ─────────────────────────────────────────────────
            Arc::new(NestedField::optional(
                18,
                "temperature",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                19,
                "top_p",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(20, "top_k", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::optional(
                21,
                "max_tokens",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                22,
                "frequency_penalty",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                23,
                "presence_penalty",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(24, "seed", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::optional(
                25,
                "stream",
                Type::Primitive(PrimitiveType::Boolean),
            )),
            Arc::new(NestedField::optional(
                26,
                "choice_count",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                27,
                "output_type",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                28,
                "reasoning_effort",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── response ─────────────────────────────────────────────────
            Arc::new(NestedField::optional(
                29,
                "time_to_first_chunk_ms",
                Type::Primitive(PrimitiveType::Long),
            )),
            // ── tokens ───────────────────────────────────────────────────
            Arc::new(NestedField::optional(
                30,
                "input_tokens",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                31,
                "output_tokens",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                32,
                "total_tokens",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                33,
                "reasoning_tokens",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                34,
                "cache_creation_input_tokens",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                35,
                "cache_read_input_tokens",
                Type::Primitive(PrimitiveType::Long),
            )),
            // ── identity context ─────────────────────────────────────────
            Arc::new(NestedField::optional(
                36,
                "user_id",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── tool ─────────────────────────────────────────────────────
            Arc::new(NestedField::optional(
                37,
                "tool_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                38,
                "tool_call_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                39,
                "tool_type",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                40,
                "tool_description",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── retrieval ────────────────────────────────────────────────
            Arc::new(NestedField::optional(
                41,
                "data_source_id",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── embeddings ───────────────────────────────────────────────
            Arc::new(NestedField::optional(
                42,
                "embedding_dimensions",
                Type::Primitive(PrimitiveType::Int),
            )),
            // ── server / status ──────────────────────────────────────────
            Arc::new(NestedField::optional(
                43,
                "server_address",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                44,
                "server_port",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                45,
                "status_code",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                46,
                "status_message",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                47,
                "error_type",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── agent / workflow ─────────────────────────────────────────
            Arc::new(NestedField::optional(
                48,
                "agent_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                49,
                "agent_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                50,
                "agent_version",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                51,
                "agent_description",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                52,
                "workflow_name",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── content (JSON-encoded String) ────────────────────────────
            Arc::new(NestedField::optional(
                53,
                "input_messages",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                54,
                "output_messages",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                55,
                "system_instructions",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                56,
                "tool_definitions",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                57,
                "tool_call_arguments",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                58,
                "tool_call_result",
                Type::Primitive(PrimitiveType::String),
            )),
            // ── List<String> columns, placed last for contiguous element IDs ─
            // Parent 59 / element 60.
            Arc::new(NestedField::optional(
                59,
                "stop_sequences",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    60,
                    Type::Primitive(PrimitiveType::String),
                    true,
                )))),
            )),
            // Parent 61 / element 62.
            Arc::new(NestedField::optional(
                61,
                "finish_reasons",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    62,
                    Type::Primitive(PrimitiveType::String),
                    true,
                )))),
            )),
            // Parent 63 / element 64.
            Arc::new(NestedField::optional(
                63,
                "encoding_formats",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    64,
                    Type::Primitive(PrimitiveType::String),
                    true,
                )))),
            )),
        ])
        .build()?;

    Ok(schema)
}

/// Creates partition specification for the operations table.
///
/// Partitions by:
/// - `tenant_id` (identity transform)
/// - day(`timestamp`) (day transform)
///
/// Identical to `spans_partition_spec`; required by the shared shift planner
/// spec (`CURRENT_PLANNER_PARTITION_SPEC` hardcodes `tenant_id` + `timestamp`).
pub fn operations_partition_spec(schema: &Schema) -> Result<PartitionSpec> {
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(5)
        .add_partition_field("tenant_id", "tenant_id", Transform::Identity)?
        .add_partition_field("timestamp", "timestamp_day", Transform::Day)?
        .build()?;

    Ok(spec)
}

/// Creates sort order for the operations table.
///
/// Sorts by:
/// - `trace_id` (ascending) - clusters a trace's operations into adjacent
///   row groups for `findOperationsByTraceId`
/// - `timestamp` (descending) - recent-first ordering for time-range scans
///
/// Deliberately omits `service_name` (which leads `spans_sort_order`): it adds
/// no pruning value for the operations read patterns and only costs sort work
/// (D10).
pub fn operations_sort_order(schema: &Schema) -> Result<SortOrder> {
    let trace_id_field = schema.field_by_name("trace_id").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "field 'trace_id' not found in operations schema",
        )
    })?;

    let timestamp_field = schema.field_by_name("timestamp").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "field 'timestamp' not found in operations schema",
        )
    })?;

    let sort_order = SortOrder::builder()
        .with_order_id(5)
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
/// - `service_name` (ascending)
/// - `trace_id` (ascending) - groups spans by trace for reconstruction
/// - `timestamp` (descending)
pub fn spans_sort_order(schema: &Schema) -> Result<SortOrder> {
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
/// - `service_name` (ascending)
/// - `timestamp` (descending) - recent-first ordering
pub fn events_schema() -> Result<Schema> {
    // Create Map<String, String> for attributes (field IDs: 11, 12)
    let attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(11, "key", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::required(
            12,
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    let schema = Schema::builder()
        .with_schema_id(3)
        .with_fields(vec![
            // Multi-tenancy field
            Arc::new(NestedField::required(
                1,
                "tenant_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "service_name",
                Type::Primitive(PrimitiveType::String),
            )),
            // Timestamp fields
            Arc::new(NestedField::required(
                3,
                "timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                4,
                "observed_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                5,
                "ingested_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            // Event identification
            Arc::new(NestedField::required(
                6,
                "event_domain",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                7,
                "event_name",
                Type::Primitive(PrimitiveType::String),
            )),
            // Trace context — raw fixed-length bytes (see logs schema rationale).
            Arc::new(NestedField::optional(
                8,
                "trace_id",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::optional(
                9,
                "span_id",
                Type::Primitive(PrimitiveType::Fixed(8)),
            )),
            // Event attributes
            Arc::new(NestedField::required(10, "attributes", attributes_map)),
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
/// - `service_name` (ascending)
/// - `timestamp` (descending) - recent-first ordering
pub fn events_sort_order(schema: &Schema) -> Result<SortOrder> {
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
/// - `metric_name` (ascending)
/// - `service_name` (ascending)
/// - `service_instance_id` (ascending)
/// - `timestamp` (descending) - recent-first ordering
#[allow(clippy::too_many_lines)]
pub fn metrics_schema() -> Result<Schema> {
    // Create Map<String, String> for main attributes (field IDs: 32, 33)
    let attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(32, "key", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::required(
            33,
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    // Create Map<String, String> for metric metadata (field IDs: 51, 52)
    let metadata_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(51, "key", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::required(
            52,
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    // Create Map<String, String> for exemplar attributes (field IDs: 34, 35)
    let exemplar_attributes_map = Type::Map(MapType::new(
        Arc::new(NestedField::required(34, "key", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::required(
            35,
            "value",
            Type::Primitive(PrimitiveType::String),
        )),
    ));

    // Quantile value struct for summary metrics
    let quantile_struct = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::required(
            36,
            "quantile",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::required(
            37,
            "value",
            Type::Primitive(PrimitiveType::Double),
        )),
    ]));

    // Exemplar struct for histogram/gauge/sum metrics
    let exemplar_struct = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(
            38,
            "timestamp",
            Type::Primitive(PrimitiveType::Timestamp),
        )),
        Arc::new(NestedField::optional(
            39,
            "value_double",
            Type::Primitive(PrimitiveType::Double),
        )),
        Arc::new(NestedField::optional(
            40,
            "value_int",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            41,
            "span_id",
            Type::Primitive(PrimitiveType::Fixed(8)),
        )),
        Arc::new(NestedField::optional(
            42,
            "trace_id",
            Type::Primitive(PrimitiveType::Fixed(16)),
        )),
        Arc::new(NestedField::required(43, "attributes", exemplar_attributes_map)),
    ]));

    let schema = Schema::builder()
        .with_schema_id(4)
        .with_fields(vec![
            // Multi-tenancy field
            Arc::new(NestedField::required(
                1,
                "tenant_id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "service_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                3,
                "service_instance_id",
                Type::Primitive(PrimitiveType::String),
            )),
            // Timestamp fields
            Arc::new(NestedField::required(
                4,
                "timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::optional(
                5,
                "start_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                6,
                "ingested_timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            // Metric identification
            Arc::new(NestedField::required(
                7,
                "metric_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(
                8,
                "metric_type",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                9,
                "description",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                10,
                "unit",
                Type::Primitive(PrimitiveType::String),
            )),
            // Metric metadata
            Arc::new(NestedField::optional(
                11,
                "aggregation_temporality",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                12,
                "is_monotonic",
                Type::Primitive(PrimitiveType::Boolean),
            )),
            // Attributes (merged from resource, scope, and metric/data point attributes)
            Arc::new(NestedField::required(13, "attributes", attributes_map)),
            // Value fields (for gauge and sum metrics)
            Arc::new(NestedField::optional(
                14,
                "value_double",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                15,
                "value_int",
                Type::Primitive(PrimitiveType::Long),
            )),
            // Common histogram fields (for histogram, exponential_histogram, and summary)
            Arc::new(NestedField::optional(16, "count", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::optional(17, "sum", Type::Primitive(PrimitiveType::Double))),
            Arc::new(NestedField::optional(18, "min", Type::Primitive(PrimitiveType::Double))),
            Arc::new(NestedField::optional(19, "max", Type::Primitive(PrimitiveType::Double))),
            // Standard histogram fields
            Arc::new(NestedField::optional(
                20,
                "bucket_counts",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    44,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )))),
            )),
            Arc::new(NestedField::optional(
                21,
                "explicit_bounds",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    45,
                    Type::Primitive(PrimitiveType::Double),
                    true,
                )))),
            )),
            // Exponential histogram fields
            Arc::new(NestedField::optional(22, "scale", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(
                23,
                "zero_count",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                24,
                "zero_threshold",
                Type::Primitive(PrimitiveType::Double),
            )),
            Arc::new(NestedField::optional(
                25,
                "positive_offset",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                26,
                "positive_bucket_counts",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    46,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )))),
            )),
            Arc::new(NestedField::optional(
                27,
                "negative_offset",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                28,
                "negative_bucket_counts",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    47,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )))),
            )),
            // Summary fields
            Arc::new(NestedField::optional(
                29,
                "quantile_values",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    48,
                    quantile_struct,
                    true,
                )))),
            )),
            // Flags and exemplars
            Arc::new(NestedField::optional(30, "flags", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(
                31,
                "exemplars",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    49,
                    exemplar_struct,
                    true,
                )))),
            )),
            // OTLP Metric.metadata: additional KeyValue metadata describing the metric.
            Arc::new(NestedField::optional(50, "metadata", metadata_map)),
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
/// - `metric_name` (ascending)
/// - `service_name` (ascending)
/// - `service_instance_id` (ascending)
/// - `timestamp` (descending) - recent-first ordering
pub fn metrics_sort_order(schema: &Schema) -> Result<SortOrder> {
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
/// W3C Trace Context trace identifier (`FIXED_LEN_BYTE_ARRAY(16)`).
pub const COL_TRACE_ID: &str = "trace_id";
/// W3C Trace Context span identifier (`FIXED_LEN_BYTE_ARRAY(8)`).
pub const COL_SPAN_ID: &str = "span_id";
/// Spans table — `parent_span_id` column (`FIXED_LEN_BYTE_ARRAY(8)`).
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
/// Logs/events/spans — observed-by-collector timestamp (`TIMESTAMP`).
pub const COL_OBSERVED_TIMESTAMP: &str = "observed_timestamp";
/// Logs/events/spans/metrics — write-side timestamp captured by ingest (`TIMESTAMP`).
pub const COL_INGESTED_TIMESTAMP: &str = "ingested_timestamp";
/// Spans — W3C trace state (`STRING`).
pub const COL_TRACE_STATE: &str = "trace_state";
/// Metrics — histogram/summary start timestamp (`TIMESTAMP`).
pub const COL_START_TIMESTAMP: &str = "start_timestamp";
/// Metrics — histogram/summary count (`BIGINT`).
pub const COL_COUNT: &str = "count";
/// Metrics — exponential-histogram zero-bucket count (`BIGINT`).
pub const COL_ZERO_COUNT: &str = "zero_count";
/// Metrics — point value as `DOUBLE`.
pub const COL_VALUE_DOUBLE: &str = "value_double";
/// Metrics — histogram/summary sum (`DOUBLE`).
pub const COL_SUM: &str = "sum";
/// Metrics — histogram min (`DOUBLE`).
pub const COL_MIN: &str = "min";
/// Metrics — histogram max (`DOUBLE`).
pub const COL_MAX: &str = "max";
/// Metrics — exponential-histogram zero threshold (`DOUBLE`).
pub const COL_ZERO_THRESHOLD: &str = "zero_threshold";
/// Grafana-compatible alias for [`COL_SEVERITY_TEXT`].
pub const LEVEL_ALIAS: &str = "level";

/// Indexed attribute columns for log label extraction.
///
/// These columns are extracted as top-level fields from log query results.
/// Used by planner (for grouping) and handlers (for output).
pub const LOG_INDEXED_ATTRIBUTE_COLUMNS: &[&str] = &[COL_SERVICE_NAME, COL_TRACE_ID, COL_SPAN_ID, COL_SEVERITY_TEXT];

/// Indexed columns visible in label discovery and series identification.
///
/// Excludes high-cardinality columns (`trace_id`, `span_id`) that have
/// per-request uniqueness — including them would explode the number of
/// distinct series and make `/labels` output noisy. These columns are still
/// readable via `/label_values` when explicitly requested.
///
/// Used by `/labels`, `/label_values` discovery, and `/series` endpoints.
pub const LOG_SERIES_LABEL_COLUMNS: &[&str] = &[COL_SERVICE_NAME, COL_SEVERITY_TEXT];

// ── Span tag-discovery lists ─────────────────────────────────────────

/// Indexed top-level columns on the `spans` table whose distinct values can
/// be enumerated via the tag-values endpoint.
///
/// Combines low-cardinality columns (`service_name`, `name`) useful for
/// label-style discovery with high-cardinality identifiers (`trace_id`,
/// `span_id`, `parent_span_id`) that are useful as explicit filter values.
/// Numeric-only columns (`duration_micros`, `kind`, `status_code`) are
/// excluded — they are exposed separately as `TraceQL` intrinsics.
pub const SPAN_INDEXED_ATTRIBUTE_COLUMNS: &[&str] = &[
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
///
/// Both legacy bare names (`name`, `status`, `kind`, `duration`,
/// `traceDuration`, `rootName`, `rootServiceName`) and the modern
/// colon-prefixed forms (`span:name`, `trace:duration`, …) are surfaced
/// side-by-side, mirroring Tempo's own `/api/v2/search/tags` response
/// (see <https://grafana.com/docs/tempo/latest/api_docs/>). Older
/// Grafana versions and `TraceQL` queries use the bare names; newer
/// versions use the colon-prefixed forms.
///
/// `trace:id` / `span:id` (and the legacy `traceID` / `spanID`) are
/// **deliberately omitted** — Tempo itself does not surface them in tag
/// discovery because enumerating the distinct value set of a per-trace
/// or per-span identifier is both useless (every value is unique) and
/// expensive. They remain queryable in `TraceQL` via the parser; the
/// `tempo::metadata::target_column_for_tag` mapping still resolves them
/// for predicate pushdown.
pub const TRACEQL_INTRINSIC_TAGS: &[&str] = &[
    // Legacy bare names — kept for backward compat with older Grafana
    // versions that have not adopted the colon-prefixed spellings.
    "name",
    "status",
    "statusMessage",
    "kind",
    "duration",
    "traceDuration",
    "rootName",
    "rootServiceName",
    // Modern `span:*` colon-prefixed aliases per Tempo's intrinsic
    // naming convention.
    "span:duration",
    "span:kind",
    "span:name",
    "span:status",
    "span:statusMessage",
    // Modern `trace:*` colon-prefixed aliases.
    "trace:duration",
    "trace:rootName",
    "trace:rootService",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logs_schema() {
        let schema = logs_schema().expect("Failed to create logs schema");
        // highest_field_id includes nested field IDs from Map
        assert_eq!(schema.highest_field_id(), 12);
        assert!(schema.field_by_name("tenant_id").is_some());
        assert!(schema.field_by_name("timestamp").is_some());
        assert!(schema.field_by_name("body").is_some());
        assert!(schema.field_by_name("attributes").is_some());
        assert!(
            schema.field_by_name("cloud_account_id").is_none(),
            "cloud_account_id must be gone"
        );
    }

    #[test]
    fn test_spans_schema() {
        let schema = spans_schema().expect("Failed to create spans schema");
        // Field IDs 1-21 + events(22-29) + links(30-39) + span_attributes(40-42).
        assert_eq!(schema.highest_field_id(), 42);
        assert!(schema.field_by_name("trace_id").is_some());
        assert!(schema.field_by_name("span_id").is_some());
        assert!(schema.field_by_name("events").is_some());
        assert!(schema.field_by_name("links").is_some());
        assert!(
            schema.field_by_name("cloud_account_id").is_none(),
            "cloud_account_id must be gone"
        );
        // Attributes split (spec 2026-04-19-spans-attributes-split-design.md):
        assert!(
            schema.field_by_name("attributes").is_none(),
            "legacy single `attributes` field must be gone"
        );
        assert!(schema.field_by_name("resource_attributes").is_some());
        assert!(schema.field_by_name("span_attributes").is_some());
    }

    #[test]
    fn test_operations_schema() {
        let schema = operations_schema().expect("Failed to create operations schema");
        // 58 scalar fields (1..=58) + 3 List<String> columns whose parent/element
        // IDs run 59..=64. highest_field_id includes list element IDs.
        assert_eq!(schema.highest_field_id(), 64);
        assert_eq!(schema.schema_id(), 5);
        assert!(schema.field_by_name("tenant_id").is_some());
        assert!(schema.field_by_name("trace_id").is_some());
        assert!(schema.field_by_name("span_id").is_some());
        assert!(schema.field_by_name("operation_name").is_some());
        assert!(schema.field_by_name("stop_sequences").is_some());
        assert!(schema.field_by_name("finish_reasons").is_some());
        assert!(schema.field_by_name("encoding_formats").is_some());
        assert!(
            schema.field_by_name("cloud_account_id").is_none(),
            "cloud_account_id must be gone"
        );
        // operations stores typed columns only (D5) — no passthrough attribute map.
        assert!(
            schema.field_by_name("attributes").is_none(),
            "operations has no merged `attributes` map"
        );
        assert!(
            schema.field_by_name("span_attributes").is_none(),
            "operations has no `span_attributes` map"
        );
    }

    #[test]
    fn test_operations_schema_nested_ids_sequential() {
        use iceberg::spec::Type;

        let schema = operations_schema().expect("Failed to create operations schema");

        // The three List<String> columns are declared last so their parent and
        // element IDs are contiguous (the PR #146 element-id lesson):
        //   stop_sequences   parent=59 element=60
        //   finish_reasons   parent=61 element=62
        //   encoding_formats parent=63 element=64
        let stop_sequences = schema.field_by_name("stop_sequences").expect("stop_sequences field");
        assert_eq!(stop_sequences.id, 59);
        let Type::List(stop_list) = &*stop_sequences.field_type else {
            panic!("stop_sequences must be List");
        };
        assert_eq!(stop_list.element_field.id, 60);
        assert_eq!(
            *stop_list.element_field.field_type,
            Type::Primitive(iceberg::spec::PrimitiveType::String)
        );

        let finish_reasons = schema.field_by_name("finish_reasons").expect("finish_reasons field");
        assert_eq!(finish_reasons.id, 61);
        let Type::List(finish_list) = &*finish_reasons.field_type else {
            panic!("finish_reasons must be List");
        };
        assert_eq!(finish_list.element_field.id, 62);
        assert_eq!(
            *finish_list.element_field.field_type,
            Type::Primitive(iceberg::spec::PrimitiveType::String)
        );

        let encoding_formats = schema.field_by_name("encoding_formats").expect("encoding_formats field");
        assert_eq!(encoding_formats.id, 63);
        let Type::List(encoding_list) = &*encoding_formats.field_type else {
            panic!("encoding_formats must be List");
        };
        assert_eq!(encoding_list.element_field.id, 64);
        assert_eq!(
            *encoding_list.element_field.field_type,
            Type::Primitive(iceberg::spec::PrimitiveType::String)
        );

        // The three list element IDs are strictly contiguous after the scalar
        // block (58): 60, 62, 64 — no gaps that would collide with a future
        // column or break catalog round-trips.
        let mut list_element_ids = vec![
            stop_list.element_field.id,
            finish_list.element_field.id,
            encoding_list.element_field.id,
        ];
        list_element_ids.sort_unstable();
        assert_eq!(list_element_ids, vec![60, 62, 64]);

        // Fixed-width identity columns keep the canonical byte widths.
        let trace_id = schema.field_by_name("trace_id").expect("trace_id field");
        assert_eq!(
            *trace_id.field_type,
            Type::Primitive(iceberg::spec::PrimitiveType::Fixed(16))
        );
        let span_id = schema.field_by_name("span_id").expect("span_id field");
        assert_eq!(
            *span_id.field_type,
            Type::Primitive(iceberg::spec::PrimitiveType::Fixed(8))
        );
    }

    #[test]
    fn test_operations_partition_and_sort_build() {
        let schema = operations_schema().expect("Failed to create operations schema");

        // Partition spec mirrors spans: `tenant_id` identity + `timestamp` day.
        let partition = operations_partition_spec(&schema).expect("Failed to build operations partition spec");
        assert_eq!(partition.spec_id(), 5);
        assert_eq!(partition.fields().len(), 2);

        // Sort order is `trace_id` ASC -> `timestamp` DESC, with NO `service_name` leg (D10).
        let sort = operations_sort_order(&schema).expect("Failed to build operations sort order");
        assert_eq!(sort.order_id, 5);
        assert_eq!(sort.fields.len(), 2);

        let trace_id_id = schema.field_by_name("trace_id").expect("trace_id field").id;
        let timestamp_id = schema.field_by_name("timestamp").expect("timestamp field").id;
        let service_name_id = schema.field_by_name("service_name").expect("service_name field").id;

        assert_eq!(sort.fields[0].source_id, trace_id_id);
        assert_eq!(sort.fields[0].direction, SortDirection::Ascending);
        assert_eq!(sort.fields[1].source_id, timestamp_id);
        assert_eq!(sort.fields[1].direction, SortDirection::Descending);
        assert!(
            !sort.fields.iter().any(|f| f.source_id == service_name_id),
            "operations sort order must omit service_name (D10)"
        );
    }

    #[test]
    fn test_spans_schema_nested_ids_sequential() {
        use iceberg::spec::Type;

        let schema = spans_schema().expect("Failed to create spans schema");

        // resource_attributes Map: parent=15, key=16, value=17
        let resource_attrs = schema.field_by_name("resource_attributes").expect("resource_attributes field");
        assert_eq!(resource_attrs.id, 15);
        let Type::Map(map) = &*resource_attrs.field_type else {
            panic!("resource_attributes must be Map");
        };
        assert_eq!(map.key_field.id, 16);
        assert_eq!(map.value_field.id, 17);

        // events List<Struct>: list=22, elem struct=23,
        // struct fields timestamp=24, name=25, attributes=26 (map key=27, value=28), dropped_attributes_count=29
        let events = schema.field_by_name("events").expect("events field");
        assert_eq!(events.id, 22);
        let Type::List(list) = &*events.field_type else {
            panic!("events must be List");
        };
        assert_eq!(list.element_field.id, 23);
        let Type::Struct(s) = &*list.element_field.field_type else {
            panic!("events element must be Struct");
        };
        let ids: Vec<i32> = s.fields().iter().map(|f| f.id).collect();
        assert_eq!(ids, vec![24, 25, 26, 29]);
        let inner_attrs = s.fields().iter().find(|f| f.name == "attributes").expect("event attrs");
        let Type::Map(m) = &*inner_attrs.field_type else {
            panic!("event attributes must be Map");
        };
        assert_eq!(m.key_field.id, 27);
        assert_eq!(m.value_field.id, 28);

        // links List<Struct>: list=30, elem struct=31,
        // struct fields trace_id=32, span_id=33, trace_state=34,
        // attributes=35 (map key=36, value=37), dropped_attributes_count=38, flags=39
        let links = schema.field_by_name("links").expect("links field");
        assert_eq!(links.id, 30);
        let Type::List(link_list) = &*links.field_type else {
            panic!("links must be List");
        };
        assert_eq!(link_list.element_field.id, 31);
        let Type::Struct(link_struct) = &*link_list.element_field.field_type else {
            panic!("links element must be Struct");
        };
        let link_field_ids: Vec<i32> = link_struct.fields().iter().map(|f| f.id).collect();
        assert_eq!(link_field_ids, vec![32, 33, 34, 35, 38, 39]);
        let link_attrs = link_struct
            .fields()
            .iter()
            .find(|f| f.name == "attributes")
            .expect("link attrs");
        let Type::Map(link_attr_map) = &*link_attrs.field_type else {
            panic!("link attributes must be Map");
        };
        assert_eq!(link_attr_map.key_field.id, 36);
        assert_eq!(link_attr_map.value_field.id, 37);

        // span_attributes Map: parent=40, key=41, value=42.
        let span_attrs = schema.field_by_name("span_attributes").expect("span_attributes field");
        assert_eq!(span_attrs.id, 40);
        let Type::Map(sm) = &*span_attrs.field_type else {
            panic!("span_attributes must be Map");
        };
        assert_eq!(sm.key_field.id, 41);
        assert_eq!(sm.value_field.id, 42);
    }

    #[test]
    fn test_events_schema() {
        let schema = events_schema().expect("Failed to create events schema");
        assert_eq!(schema.highest_field_id(), 12);
        assert!(schema.field_by_name("event_domain").is_some());
        assert!(schema.field_by_name("event_name").is_some());
        assert!(
            schema.field_by_name("cloud_account_id").is_none(),
            "cloud_account_id must be gone"
        );
    }

    #[test]
    fn test_metrics_schema() {
        let schema = metrics_schema().expect("Failed to create metrics schema");
        // highest_field_id includes nested field IDs from Maps, Lists, and Structs.
        // Top-level fields occupy 1..=31; nested IDs run 32..=49; the appended
        // `metadata` map adds field 50 (key 51, value 52).
        assert_eq!(schema.highest_field_id(), 52);
        assert!(schema.field_by_name("metric_name").is_some());
        assert!(schema.field_by_name("metric_type").is_some());
        assert!(schema.field_by_name("value_double").is_some());
        assert!(schema.field_by_name("bucket_counts").is_some());
        assert!(schema.field_by_name("exemplars").is_some());
        assert!(schema.field_by_name("metadata").is_some());
        assert!(
            schema.field_by_name("cloud_account_id").is_none(),
            "cloud_account_id must be gone"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_metrics_schema_nested_ids_sequential() {
        use iceberg::spec::Type;

        let schema = metrics_schema().expect("Failed to create metrics schema");

        // attributes Map: parent=13, key=32, value=33.
        let attributes = schema.field_by_name("attributes").expect("attributes field");
        assert_eq!(attributes.id, 13);
        let Type::Map(attr_map) = &*attributes.field_type else {
            panic!("attributes must be Map");
        };
        assert_eq!(attr_map.key_field.id, 32);
        assert_eq!(attr_map.value_field.id, 33);

        // bucket_counts List<Long>: parent=20, element=44.
        let bucket_counts = schema.field_by_name("bucket_counts").expect("bucket_counts");
        assert_eq!(bucket_counts.id, 20);
        let Type::List(bc) = &*bucket_counts.field_type else {
            panic!("bucket_counts must be List");
        };
        assert_eq!(bc.element_field.id, 44);

        // explicit_bounds List<Double>: parent=21, element=45.
        let explicit_bounds = schema.field_by_name("explicit_bounds").expect("explicit_bounds");
        assert_eq!(explicit_bounds.id, 21);
        let Type::List(eb) = &*explicit_bounds.field_type else {
            panic!("explicit_bounds must be List");
        };
        assert_eq!(eb.element_field.id, 45);

        // positive_bucket_counts List<Long>: parent=26, element=46.
        let pos = schema.field_by_name("positive_bucket_counts").expect("positive_bucket_counts");
        assert_eq!(pos.id, 26);
        let Type::List(p) = &*pos.field_type else {
            panic!("positive_bucket_counts must be List");
        };
        assert_eq!(p.element_field.id, 46);

        // negative_bucket_counts List<Long>: parent=28, element=47.
        let neg = schema.field_by_name("negative_bucket_counts").expect("negative_bucket_counts");
        assert_eq!(neg.id, 28);
        let Type::List(n) = &*neg.field_type else {
            panic!("negative_bucket_counts must be List");
        };
        assert_eq!(n.element_field.id, 47);

        // quantile_values List<Struct{quantile=36,value=37}>: parent=29, element=48.
        let qv = schema.field_by_name("quantile_values").expect("quantile_values");
        assert_eq!(qv.id, 29);
        let Type::List(qv_list) = &*qv.field_type else {
            panic!("quantile_values must be List");
        };
        assert_eq!(qv_list.element_field.id, 48);
        let Type::Struct(qv_struct) = &*qv_list.element_field.field_type else {
            panic!("quantile_values element must be Struct");
        };
        let qv_ids: Vec<i32> = qv_struct.fields().iter().map(|f| f.id).collect();
        assert_eq!(qv_ids, vec![36, 37]);

        // exemplars List<Struct>: parent=31, element=49.
        // Struct fields: timestamp=38, value_double=39, value_int=40, span_id=41,
        // trace_id=42, attributes=43 (map key=34, value=35).
        let exemplars = schema.field_by_name("exemplars").expect("exemplars");
        assert_eq!(exemplars.id, 31);
        let Type::List(ex_list) = &*exemplars.field_type else {
            panic!("exemplars must be List");
        };
        assert_eq!(ex_list.element_field.id, 49);
        let Type::Struct(ex_struct) = &*ex_list.element_field.field_type else {
            panic!("exemplars element must be Struct");
        };
        let ex_ids: Vec<i32> = ex_struct.fields().iter().map(|f| f.id).collect();
        assert_eq!(ex_ids, vec![38, 39, 40, 41, 42, 43]);
        let ex_attrs = ex_struct
            .fields()
            .iter()
            .find(|f| f.name == "attributes")
            .expect("exemplar attributes");
        let Type::Map(ex_attr_map) = &*ex_attrs.field_type else {
            panic!("exemplar attributes must be Map");
        };
        assert_eq!(ex_attr_map.key_field.id, 34);
        assert_eq!(ex_attr_map.value_field.id, 35);

        // metadata Map<String,String>: parent=50, key=51, value=52.
        let metadata = schema.field_by_name("metadata").expect("metadata field");
        assert_eq!(metadata.id, 50);
        let Type::Map(meta_map) = &*metadata.field_type else {
            panic!("metadata must be Map");
        };
        assert_eq!(meta_map.key_field.id, 51);
        assert_eq!(meta_map.value_field.id, 52);
    }
}
