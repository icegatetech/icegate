//! OTLP logs -> Arrow transform.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, FixedSizeBinaryBuilder, MapBuilder, MapFieldNames, RecordBatch, StringBuilder,
        TimestampMicrosecondArray,
    },
    datatypes::Schema,
};
use iceberg::arrow::schema_to_arrow_schema;
use icegate_common::{DEFAULT_TENANT_ID, schema::COL_SERVICE_NAME};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

use super::attributes::{
    add_flattened_attributes, extract_map_fields_from_schema, extract_string_value, is_zero_bytes,
    serialize_any_value_to_json,
};

/// Returns the Arrow schema for logs, derived from the Iceberg schema.
///
/// Uses `icegate_common::schema::logs_schema()` as the source of truth
/// and converts it to Arrow format using
/// `iceberg::arrow::schema_to_arrow_schema()`.
///
/// # Panics
///
/// Panics if the Iceberg schema cannot be created or converted to Arrow.
/// This should never happen in practice as the schema is statically defined.
#[allow(clippy::expect_used)]
pub fn logs_arrow_schema() -> Schema {
    let iceberg_schema = icegate_common::schema::logs_schema().expect("logs_schema should always be valid");
    schema_to_arrow_schema(&iceberg_schema).expect("schema conversion should succeed")
}

/// Transforms an OTLP logs export request to an Arrow `RecordBatch`.
///
/// Extracts all log records from the request, merging resource and scope
/// attributes into each log record's attributes.
///
/// # Arguments
///
/// * `request` - The OTLP export logs request
/// * `tenant_id` - Tenant identifier (from request metadata or default)
///
/// # Returns
///
/// Arrow `RecordBatch` matching the logs schema wrapped in `Ok(Some(_))`,
/// or `Ok(None)` if no records are present.
///
/// # Errors
///
/// Returns `IngestError` if:
/// - Schema validation fails
/// - `RecordBatch` creation fails
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_possible_truncation)] // Timestamp fits in i64 for practical purposes
#[allow(clippy::expect_used)] // Byte lengths are validated before append
#[tracing::instrument(skip(request))]
pub fn logs_to_record_batch(
    request: &ExportLogsServiceRequest,
    tenant_id: Option<&str>,
) -> crate::error::Result<Option<RecordBatch>> {
    let ingested_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_micros() as i64;

    // Count total log records for capacity hints
    let total_records: usize = request
        .resource_logs
        .iter()
        .flat_map(|rl| &rl.scope_logs)
        .map(|sl| sl.log_records.len())
        .sum();

    if total_records == 0 {
        return Ok(None);
    }

    // Get the Arrow schema to extract correct field types for map builder
    let schema = logs_arrow_schema();
    let (key_field, value_field) = extract_map_fields_from_schema(&schema)?;

    // Initialize builders
    let mut tenant_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut service_name_builder = StringBuilder::with_capacity(total_records, total_records * 32);
    let mut timestamp_builder = Vec::with_capacity(total_records);
    let mut observed_timestamp_builder = Vec::with_capacity(total_records);
    let mut ingested_timestamp_builder = Vec::with_capacity(total_records);
    let mut trace_id_builder = FixedSizeBinaryBuilder::with_capacity(total_records, 16);
    let mut span_id_builder = FixedSizeBinaryBuilder::with_capacity(total_records, 8);
    let mut severity_text_builder = StringBuilder::with_capacity(total_records, total_records * 8);
    let mut body_builder = StringBuilder::with_capacity(total_records, total_records * 256);

    // Create map builder with correct field names matching Iceberg schema
    let field_names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut attributes_builder = MapBuilder::new(Some(field_names), StringBuilder::new(), StringBuilder::new())
        .with_keys_field(key_field)
        .with_values_field(value_field);

    let tenant = tenant_id.unwrap_or(DEFAULT_TENANT_ID);

    // Empty slice for default attributes
    let empty_attrs: Vec<opentelemetry_proto::tonic::common::v1::KeyValue> = Vec::new();

    // Process each resource_logs -> scope_logs -> log_record
    for resource_logs in &request.resource_logs {
        let resource_attrs = resource_logs.resource.as_ref().map_or(&empty_attrs, |r| &r.attributes);

        // Extract service.name from resource attributes
        let service_name = resource_attrs
            .iter()
            .find(|kv| kv.key == "service.name")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for scope_logs in &resource_logs.scope_logs {
            let scope_attrs = scope_logs.scope.as_ref().map_or(&empty_attrs, |s| &s.attributes);

            for log_record in &scope_logs.log_records {
                // tenant_id
                tenant_id_builder.append_value(tenant);

                // service_name
                if let Some(ref svc) = service_name {
                    service_name_builder.append_value(svc);
                } else {
                    service_name_builder.append_null();
                }

                // timestamp (nanoseconds to microseconds)
                let timestamp_micros = if log_record.time_unix_nano > 0 {
                    (log_record.time_unix_nano / 1000) as i64
                } else {
                    ingested_timestamp
                };
                timestamp_builder.push(timestamp_micros);

                // observed_timestamp
                let observed_micros = if log_record.observed_time_unix_nano > 0 {
                    (log_record.observed_time_unix_nano / 1000) as i64
                } else {
                    ingested_timestamp
                };
                observed_timestamp_builder.push(observed_micros);

                // ingested_timestamp
                ingested_timestamp_builder.push(ingested_timestamp);

                // trace_id and span_id (populated directly into top-level columns)
                process_trace_span_ids(log_record, &mut trace_id_builder, &mut span_id_builder)?;

                // severity_text
                if log_record.severity_text.is_empty() {
                    severity_text_builder.append_null();
                } else {
                    severity_text_builder.append_value(&log_record.severity_text);
                }

                // body (JSON-serialized for Loki compatibility)
                if let Some(body_str) = serialize_any_value_to_json(log_record.body.as_ref()) {
                    body_builder.append_value(body_str);
                } else {
                    body_builder.append_null();
                }

                // attributes (merged from resource, scope, and log record).
                //
                // Indexed top-level columns are deliberately NOT mirrored into this MAP —
                // the read pipeline projects them as separate Arrow columns and
                // `loki/formatters.rs::extract_labels` merges them into the final
                // labels view at row materialisation time.
                //
                // Resource flattening additionally skips the OTLP keys whose
                // normalised form collides with a promoted top-level column
                // (`service.name` → `service_name`). Without the skip these
                // would be written to the MAP a second time, polluting the
                // per-row group key
                // dictionary. Scope and log-record attributes are not filtered
                // — a user-supplied `service_name` log attribute (rare, but
                // semantically meaningful as an override) still flows through.
                add_flattened_attributes(resource_attrs, &mut attributes_builder, LOG_PROMOTED_RESOURCE_KEYS);
                add_flattened_attributes(scope_attrs, &mut attributes_builder, &[]);
                add_flattened_attributes(&log_record.attributes, &mut attributes_builder, &[]);

                attributes_builder.append(true).expect("append map entry");
            }
        }
    }

    // Build arrays
    let schema = Arc::new(schema);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_id_builder.finish()),
        Arc::new(service_name_builder.finish()),
        Arc::new(TimestampMicrosecondArray::from(timestamp_builder)),
        Arc::new(TimestampMicrosecondArray::from(observed_timestamp_builder)),
        Arc::new(TimestampMicrosecondArray::from(ingested_timestamp_builder)),
        Arc::new(trace_id_builder.finish()),
        Arc::new(span_id_builder.finish()),
        Arc::new(severity_text_builder.finish()),
        Arc::new(body_builder.finish()),
        Arc::new(attributes_builder.finish()),
    ];

    RecordBatch::try_new(schema, columns).map(Some).map_err(|e| {
        tracing::error!("Failed to create RecordBatch: {e}");
        crate::error::IngestError::Validation(format!("Failed to create RecordBatch: {e}"))
    })
}

/// Resource-attribute keys (in their normalised form) that already have a
/// dedicated top-level Iceberg column on the logs table. Skipped during
/// resource flattening to keep the attributes MAP free of redundant copies
/// — both to shrink Parquet pages and to keep the row-group key dictionary
/// uncluttered.
const LOG_PROMOTED_RESOURCE_KEYS: &[&str] = &[COL_SERVICE_NAME];

/// Validates and writes raw `trace_id` and `span_id` bytes directly into the
/// top-level fixed-size binary builders.
///
/// Invalid byte lengths or all-zero IDs are written as nulls. The values are
/// not returned: downstream consumers read them from the typed columns and
/// hex-encode at API boundaries when needed.
///
/// Length validation runs through `<&[u8; N]>::try_from` so the
/// post-validation builder appends are type-system-unreachable failures
/// (builder `value_length` matches the array's compile-time size).
/// `IngestError::Arrow` is therefore unreachable in practice; if the
/// validation chain ever regresses it surfaces as a hard error rather
/// than a silent panic.
fn process_trace_span_ids(
    log_record: &opentelemetry_proto::tonic::logs::v1::LogRecord,
    trace_id_builder: &mut FixedSizeBinaryBuilder,
    span_id_builder: &mut FixedSizeBinaryBuilder,
) -> crate::error::Result<()> {
    // trace_id (16 bytes raw)
    match <&[u8; 16]>::try_from(log_record.trace_id.as_slice()) {
        Ok(a) if !is_zero_bytes(a) => trace_id_builder.append_value(a)?,
        _ => trace_id_builder.append_null(),
    }

    // span_id (8 bytes raw)
    match <&[u8; 8]>::try_from(log_record.span_id.as_slice()) {
        Ok(a) if !is_zero_bytes(a) => span_id_builder.append_value(a)?,
        _ => span_id_builder.append_null(),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };

    use super::*;

    #[test]
    fn test_logs_to_record_batch_empty() {
        let request = ExportLogsServiceRequest { resource_logs: vec![] };

        let batch = logs_to_record_batch(&request, None).expect("should not error");
        assert!(batch.is_none());
    }

    #[test]
    fn test_logs_to_record_batch_single_record() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        observed_time_unix_nano: 1_700_000_000_000_000_000,
                        severity_number: 9, // INFO
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("Test log message".to_string())),
                        }),
                        attributes: vec![KeyValue {
                            key: "custom.attr".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("custom-value".to_string())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0; 16],
                        span_id: vec![0; 8],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let batch = logs_to_record_batch(&request, Some("test-tenant")).expect("should not error");
        assert!(batch.is_some());

        let batch = batch.expect("batch should exist");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 10);
    }

    /// The logs ingest path must NOT mirror indexed top-level columns into
    /// the `attributes` MAP. Only OTLP-supplied attributes belong there.
    /// The merged labels view is reconstructed at read time by
    /// `loki/formatters.rs::extract_labels`.
    #[test]
    fn logs_to_record_batch_does_not_mirror_indexed_columns_into_attributes_map() {
        use std::collections::BTreeMap;

        use arrow::array::{Array, MapArray, StringArray};

        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("demo".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "cloud.account.id".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("acc-1".to_string())),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        observed_time_unix_nano: 1_700_000_000_000_000_000,
                        severity_number: 17, // ERROR
                        severity_text: "ERROR".to_string(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("hello".to_string())),
                        }),
                        attributes: vec![KeyValue {
                            key: "foo".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("bar".to_string())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let batch = logs_to_record_batch(&request, Some("tenant-1")).expect("ok").expect("batch");

        // Indexed top-level columns must hold the values directly. trace_id /
        // span_id are stored as raw `FIXED_LEN_BYTE_ARRAY` bytes, not hex.
        let trace_id = batch
            .column_by_name("trace_id")
            .expect("trace_id col")
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .expect("FixedSizeBinary(16)");
        assert_eq!(trace_id.value(0), &vec![1u8; 16][..]);

        let span_id = batch
            .column_by_name("span_id")
            .expect("span_id col")
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .expect("FixedSizeBinary(8)");
        assert_eq!(span_id.value(0), &vec![2u8; 8][..]);

        let severity = batch
            .column_by_name("severity_text")
            .expect("severity_text col")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(severity.value(0), "ERROR");

        // Pull the row's attribute MAP into a BTreeMap for assertions.
        let attrs_map = batch
            .column_by_name("attributes")
            .expect("attributes col")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("MapArray");
        let entries = attrs_map.value(0);
        let entries_struct = entries
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("struct entries");
        let keys = entries_struct.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
        let values = entries_struct.column(1).as_any().downcast_ref::<StringArray>().expect("values");
        let pairs: BTreeMap<String, String> = (0..keys.len())
            .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
            .collect();

        // The user-supplied log attribute survives unchanged.
        assert_eq!(pairs.get("foo"), Some(&"bar".to_string()));

        // `cloud.account.id` is no longer a promoted top-level column —
        // it flows into the attributes MAP via the resource flattening pass
        // like any other resource attribute (normalised dots → underscores).
        assert_eq!(pairs.get("cloud_account_id"), Some(&"acc-1".to_string()));

        // Indexed-column mirrors must NOT appear in the attributes MAP:
        //
        // - `trace_id` and `span_id`: removed by deleting
        //   `add_indexed_columns_to_attributes` (these are OTLP LogRecord
        //   top-level fields, never resource attributes).
        // - `severity_text` and `level`: same — they came from the deleted
        //   helper, not from any OTLP attribute key.
        // - `service_name`: would otherwise be normalised in by
        //   `add_flattened_attributes` (`service.name` → `service_name`).
        //   Suppressed via `LOG_PROMOTED_RESOURCE_KEYS` to keep the per-row-
        //   group key dictionary free of redundant entries.
        for mirror in ["trace_id", "span_id", "severity_text", "level", "service_name"] {
            assert!(
                !pairs.contains_key(mirror),
                "mirror `{mirror}` must not appear in attributes MAP, got pairs: {pairs:?}"
            );
        }
    }
}
