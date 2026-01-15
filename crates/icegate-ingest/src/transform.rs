//! OTLP to Arrow transform utilities.
//!
//! Transforms `OpenTelemetry` Protocol log records to Arrow `RecordBatches`
//! matching the Iceberg logs schema.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, FixedSizeBinaryBuilder, Int32Builder, MapBuilder, MapFieldNames, RecordBatch, StringBuilder,
        TimestampMicrosecondArray,
    },
    datatypes::{DataType, Schema},
};
use iceberg::arrow::schema_to_arrow_schema;
use icegate_common::{DEFAULT_ACCOUNT_ID, DEFAULT_TENANT_ID};
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, any_value::Value},
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
/// * `account_id` - Optional account identifier
///
/// # Returns
///
/// Arrow `RecordBatch` matching the logs schema, or None if no records.
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_possible_truncation)] // Timestamp fits in i64 for practical purposes
#[allow(clippy::expect_used)] // Byte lengths are validated before append
#[allow(clippy::too_many_lines)] // Linear transform function
pub fn logs_to_record_batch(request: &ExportLogsServiceRequest, tenant_id: Option<&str>) -> Option<RecordBatch> {
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
        return None;
    }

    // Get the Arrow schema to extract correct field types for map builder
    let schema = logs_arrow_schema();
    let attributes_field = schema.field(11); // attributes is field index 11

    // Extract map field information from schema
    let (key_field, value_field) = match attributes_field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => (fields[0].clone(), fields[1].clone()),
            _ => panic!("Expected Struct type for map entries"),
        },
        _ => panic!("Expected Map type for attributes field"),
    };

    // Initialize builders
    let mut tenant_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut account_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut service_name_builder = StringBuilder::with_capacity(total_records, total_records * 32);
    let mut timestamp_builder = Vec::with_capacity(total_records);
    let mut observed_timestamp_builder = Vec::with_capacity(total_records);
    let mut ingested_timestamp_builder = Vec::with_capacity(total_records);
    let mut trace_id_builder = FixedSizeBinaryBuilder::with_capacity(total_records, 16);
    let mut span_id_builder = FixedSizeBinaryBuilder::with_capacity(total_records, 8);
    let mut severity_number_builder = Int32Builder::with_capacity(total_records);
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

    let mut flags_builder = Int32Builder::with_capacity(total_records);
    let mut dropped_attrs_builder = Int32Builder::with_capacity(total_records);

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
        let account_id = resource_attrs
            .iter()
            .find(|kv| kv.key == "cloud.account.id")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for scope_logs in &resource_logs.scope_logs {
            let scope_attrs = scope_logs.scope.as_ref().map_or(&empty_attrs, |s| &s.attributes);

            for log_record in &scope_logs.log_records {
                // tenant_id
                tenant_id_builder.append_value(tenant);

                // cloud_account_id (use default if not provided - required by schema)
                let acc = account_id.as_deref().unwrap_or(DEFAULT_ACCOUNT_ID);
                account_id_builder.append_value(acc);

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

                // trace_id (16 bytes)
                if log_record.trace_id.len() == 16 && !is_zero_bytes(&log_record.trace_id) {
                    trace_id_builder
                        .append_value(&log_record.trace_id)
                        .expect("trace_id is 16 bytes");
                } else {
                    trace_id_builder.append_null();
                }

                // span_id (8 bytes)
                if log_record.span_id.len() == 8 && !is_zero_bytes(&log_record.span_id) {
                    span_id_builder.append_value(&log_record.span_id).expect("span_id is 8 bytes");
                } else {
                    span_id_builder.append_null();
                }

                // severity_number
                if log_record.severity_number != 0 {
                    severity_number_builder.append_value(log_record.severity_number);
                } else {
                    severity_number_builder.append_null();
                }

                // severity_text
                if log_record.severity_text.is_empty() {
                    severity_text_builder.append_null();
                } else {
                    severity_text_builder.append_value(&log_record.severity_text);
                }

                // body
                if let Some(body_str) = extract_any_value_string(log_record.body.as_ref()) {
                    body_builder.append_value(body_str);
                } else {
                    body_builder.append_null();
                }

                // attributes (merged from resource, scope, and log record)
                let dropped_count = log_record.dropped_attributes_count;

                // Add resource attributes (normalize keys: dots -> underscores)
                for kv in resource_attrs {
                    if let Some(v) = extract_string_value(kv.value.as_ref()) {
                        attributes_builder.keys().append_value(normalize_attribute_key(&kv.key));
                        attributes_builder.values().append_value(v);
                    }
                }

                // Add scope attributes (normalize keys: dots -> underscores)
                for kv in scope_attrs {
                    if let Some(v) = extract_string_value(kv.value.as_ref()) {
                        attributes_builder.keys().append_value(normalize_attribute_key(&kv.key));
                        attributes_builder.values().append_value(v);
                    }
                }

                // Add log record attributes (normalize keys: dots -> underscores)
                for kv in &log_record.attributes {
                    if let Some(v) = extract_string_value(kv.value.as_ref()) {
                        attributes_builder.keys().append_value(normalize_attribute_key(&kv.key));
                        attributes_builder.values().append_value(v);
                    }
                }

                attributes_builder.append(true).expect("append map entry");

                // flags
                if log_record.flags != 0 {
                    flags_builder.append_value(log_record.flags as i32);
                } else {
                    flags_builder.append_null();
                }

                // dropped_attributes_count
                dropped_attrs_builder.append_value(dropped_count as i32);
            }
        }
    }

    // Build arrays
    let schema = Arc::new(schema);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_id_builder.finish()),
        Arc::new(account_id_builder.finish()),
        Arc::new(service_name_builder.finish()),
        Arc::new(TimestampMicrosecondArray::from(timestamp_builder)),
        Arc::new(TimestampMicrosecondArray::from(observed_timestamp_builder)),
        Arc::new(TimestampMicrosecondArray::from(ingested_timestamp_builder)),
        Arc::new(trace_id_builder.finish()),
        Arc::new(span_id_builder.finish()),
        Arc::new(severity_number_builder.finish()),
        Arc::new(severity_text_builder.finish()),
        Arc::new(body_builder.finish()),
        Arc::new(attributes_builder.finish()),
        Arc::new(flags_builder.finish()),
        Arc::new(dropped_attrs_builder.finish()),
    ];

    match RecordBatch::try_new(schema, columns) {
        Ok(batch) => Some(batch),
        Err(e) => {
            tracing::error!("Failed to create RecordBatch: {e}");
            None
        }
    }
}

/// Extracts a string value from an OTLP `AnyValue` reference.
///
/// Converts various OTLP value types to string representation.
fn extract_any_value_string(value: Option<&AnyValue>) -> Option<String> {
    value.and_then(|v| {
        v.value.as_ref().map(|val| match val {
            Value::StringValue(s) => s.clone(),
            Value::IntValue(i) => i.to_string(),
            Value::DoubleValue(d) => d.to_string(),
            Value::BoolValue(b) => b.to_string(),
            Value::BytesValue(b) => hex::encode(b),
            Value::ArrayValue(arr) => {
                let items: Vec<String> = arr.values.iter().filter_map(|v| extract_any_value_string(Some(v))).collect();
                format!("[{}]", items.join(", "))
            }
            Value::KvlistValue(kvs) => {
                let pairs: Vec<String> = kvs
                    .values
                    .iter()
                    .filter_map(|kv| extract_any_value_string(kv.value.as_ref()).map(|v| format!("{}={}", kv.key, v)))
                    .collect();
                format!("{{{}}}", pairs.join(", "))
            }
        })
    })
}

/// Extracts a string value from an `Option<AnyValue>`.
fn extract_string_value(value: Option<&AnyValue>) -> Option<String> {
    extract_any_value_string(value)
}

/// Checks if a byte slice is all zeros.
fn is_zero_bytes(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}

/// Normalizes an attribute key by replacing dots with underscores.
///
/// `OpenTelemetry` uses dots in attribute names (e.g., "service.name"),
/// but some systems prefer underscores for compatibility.
fn normalize_attribute_key(key: &str) -> String {
    key.replace('.', "_")
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::{
        common::v1::KeyValue,
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };

    use super::*;

    #[test]
    fn test_logs_to_record_batch_empty() {
        let request = ExportLogsServiceRequest { resource_logs: vec![] };

        let batch = logs_to_record_batch(&request, None);
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

        let batch = logs_to_record_batch(&request, Some("test-tenant"));
        assert!(batch.is_some());

        let batch = batch.expect("batch should exist");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 14);
    }

    #[test]
    fn test_normalize_attribute_key() {
        assert_eq!(normalize_attribute_key("service.name"), "service_name");
        assert_eq!(normalize_attribute_key("cloud.account.id"), "cloud_account_id");
        assert_eq!(normalize_attribute_key("no_dots"), "no_dots");
        assert_eq!(normalize_attribute_key(""), "");
    }

    #[test]
    fn test_extract_string_value_types() {
        // String
        let v = AnyValue {
            value: Some(Value::StringValue("hello".to_string())),
        };
        assert_eq!(extract_string_value(Some(&v)), Some("hello".to_string()));

        // Int
        let v = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        assert_eq!(extract_string_value(Some(&v)), Some("42".to_string()));

        // Bool
        let v = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert_eq!(extract_string_value(Some(&v)), Some("true".to_string()));

        // None
        assert_eq!(extract_string_value(None), None);
    }
}
