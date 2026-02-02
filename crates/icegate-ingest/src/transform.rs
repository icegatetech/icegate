//! OTLP to Arrow transform utilities.
//!
//! Transforms `OpenTelemetry` Protocol log records to Arrow `RecordBatches`
//! matching the Iceberg logs schema.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, MapBuilder, MapFieldNames, RecordBatch, StringBuilder, TimestampMicrosecondArray},
    datatypes::{DataType, Schema},
};
use iceberg::arrow::schema_to_arrow_schema;
use icegate_common::DEFAULT_TENANT_ID;
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
    let mut account_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut service_name_builder = StringBuilder::with_capacity(total_records, total_records * 32);
    let mut timestamp_builder = Vec::with_capacity(total_records);
    let mut observed_timestamp_builder = Vec::with_capacity(total_records);
    let mut ingested_timestamp_builder = Vec::with_capacity(total_records);
    let mut trace_id_builder = StringBuilder::with_capacity(total_records, total_records * 32);
    let mut span_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
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
        let cloud_account_id = resource_attrs
            .iter()
            .find(|kv| kv.key == "cloud.account.id")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for scope_logs in &resource_logs.scope_logs {
            let scope_attrs = scope_logs.scope.as_ref().map_or(&empty_attrs, |s| &s.attributes);

            for log_record in &scope_logs.log_records {
                // tenant_id
                tenant_id_builder.append_value(tenant);

                // account_id (optional - null if not provided)
                if let Some(ref acc) = cloud_account_id {
                    account_id_builder.append_value(acc);
                } else {
                    account_id_builder.append_null();
                }

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

                // trace_id and span_id
                let (trace_id_hex, span_id_hex) =
                    process_trace_span_ids(log_record, &mut trace_id_builder, &mut span_id_builder);

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

                // attributes (merged from resource, scope, and log record)
                // Add resource, scope, and log record attributes (flatten nested, normalize keys)
                add_flattened_attributes(resource_attrs, &mut attributes_builder);
                add_flattened_attributes(scope_attrs, &mut attributes_builder);
                add_flattened_attributes(&log_record.attributes, &mut attributes_builder);

                // Duplicate indexed columns into attributes map for query layer
                add_indexed_columns_to_attributes(
                    &mut attributes_builder,
                    trace_id_hex.as_ref(),
                    span_id_hex.as_ref(),
                    &log_record.severity_text,
                    cloud_account_id.as_ref(),
                );

                attributes_builder.append(true).expect("append map entry");
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
        Arc::new(severity_text_builder.finish()),
        Arc::new(body_builder.finish()),
        Arc::new(attributes_builder.finish()),
    ];

    RecordBatch::try_new(schema, columns).map(Some).map_err(|e| {
        tracing::error!("Failed to create RecordBatch: {e}");
        crate::error::IngestError::Validation(format!("Failed to create RecordBatch: {e}"))
    })
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

/// Serializes an OTLP `AnyValue` to JSON string format.
///
/// This is used specifically for the LogRecord.Body field, which should be
/// JSON-serialized according to Loki requirements.
///
/// # Arguments
///
/// * `value` - OTLP `AnyValue` to serialize
///
/// # Returns
///
/// JSON string representation of the value, or None if value is None
///
/// # Examples
///
/// ```
/// // StringValue("hello") -> "hello" (no quotes)
/// // IntValue(42) -> "42"
/// // ArrayValue([1, 2]) -> "[1,2]"
/// // KvlistValue({a: 1}) -> "{\"a\":1}"
/// ```
fn serialize_any_value_to_json(value: Option<&AnyValue>) -> Option<String> {
    value.and_then(|v| {
        v.value.as_ref().and_then(|val| match val {
            Value::StringValue(s) => Some(s.clone()),
            Value::IntValue(i) => Some(i.to_string()),
            Value::DoubleValue(d) => Some(d.to_string()),
            Value::BoolValue(b) => Some(b.to_string()),
            Value::BytesValue(b) => Some(hex::encode(b)),
            Value::ArrayValue(arr) => {
                let json_array: Vec<serde_json::Value> = arr.values.iter().filter_map(any_value_to_json).collect();
                serde_json::to_string(&json_array).ok()
            }
            Value::KvlistValue(kvs) => {
                let mut json_object = serde_json::Map::new();
                for kv in &kvs.values {
                    if let Some(json_val) = kv.value.as_ref().and_then(any_value_to_json) {
                        json_object.insert(kv.key.clone(), json_val);
                    }
                }
                serde_json::to_string(&json_object).ok()
            }
        })
    })
}

/// Helper to convert `AnyValue` to `serde_json::Value` for JSON serialization.
fn any_value_to_json(value: &AnyValue) -> Option<serde_json::Value> {
    value.value.as_ref().map(|val| match val {
        Value::StringValue(s) => serde_json::Value::String(s.clone()),
        Value::IntValue(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
        Value::DoubleValue(d) => {
            serde_json::Number::from_f64(*d).map_or(serde_json::Value::Null, serde_json::Value::Number)
        }
        Value::BoolValue(b) => serde_json::Value::Bool(*b),
        Value::BytesValue(b) => serde_json::Value::String(hex::encode(b)),
        Value::ArrayValue(arr) => {
            let items: Vec<serde_json::Value> = arr.values.iter().filter_map(any_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        Value::KvlistValue(kvs) => {
            let mut map = serde_json::Map::new();
            for kv in &kvs.values {
                if let Some(v) = kv.value.as_ref().and_then(any_value_to_json) {
                    map.insert(kv.key.clone(), v);
                }
            }
            serde_json::Value::Object(map)
        }
    })
}

/// Checks if a byte slice is all zeros.
fn is_zero_bytes(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}

/// Extracts map field metadata from the Arrow schema for attributes field.
///
/// Returns a tuple of (`key_field`, `value_field`) from the schema's map type definition.
///
/// # Errors
///
/// Returns `IngestError::Validation` if:
/// - Schema does not contain an 'attributes' field
/// - The 'attributes' field is not of Map type
/// - The map entries are not of Struct type
/// - The struct does not contain at least 2 fields (key and value)
fn extract_map_fields_from_schema(
    schema: &Schema,
) -> crate::error::Result<(arrow::datatypes::FieldRef, arrow::datatypes::FieldRef)> {
    let attributes_field = schema
        .field_with_name("attributes")
        .map_err(|_| crate::error::IngestError::Validation("Schema must contain an 'attributes' field".to_string()))?;

    // Extract map field information from schema
    match attributes_field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => {
                if fields.len() < 2 {
                    return Err(crate::error::IngestError::Validation(format!(
                        "Expected at least 2 fields in map entries struct, found {}",
                        fields.len()
                    )));
                }
                Ok((fields[0].clone(), fields[1].clone()))
            }
            _ => Err(crate::error::IngestError::Validation(
                "Expected Struct type for map entries in 'attributes' field".to_string(),
            )),
        },
        _ => Err(crate::error::IngestError::Validation(format!(
            "Expected Map type for 'attributes' field, found {:?}",
            attributes_field.data_type()
        ))),
    }
}

/// Adds flattened attributes to the map builder with key normalization.
///
/// Flattens nested structures and normalizes attribute keys by replacing dots with underscores.
fn add_flattened_attributes(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    attributes_builder: &mut MapBuilder<StringBuilder, StringBuilder>,
) {
    for kv in attributes {
        let flattened = flatten_any_value(&kv.key, kv.value.as_ref());
        for (key, value) in flattened {
            attributes_builder.keys().append_value(normalize_attribute_key(&key));
            attributes_builder.values().append_value(value);
        }
    }
}

/// Processes trace and span IDs, returning hex-encoded values if valid.
///
/// Returns a tuple of (`trace_id_hex`, `span_id_hex`) where each is `Some(String)` if valid,
/// or `None` if invalid or all zeros.
fn process_trace_span_ids(
    log_record: &opentelemetry_proto::tonic::logs::v1::LogRecord,
    trace_id_builder: &mut StringBuilder,
    span_id_builder: &mut StringBuilder,
) -> (Option<String>, Option<String>) {
    // trace_id (16 bytes → 32 hex chars)
    let trace_id_hex = if log_record.trace_id.len() == 16 && !is_zero_bytes(&log_record.trace_id) {
        let hex_string = hex::encode(&log_record.trace_id);
        trace_id_builder.append_value(hex_string.clone());
        Some(hex_string)
    } else {
        trace_id_builder.append_null();
        None
    };

    // span_id (8 bytes → 16 hex chars)
    let span_id_hex = if log_record.span_id.len() == 8 && !is_zero_bytes(&log_record.span_id) {
        let hex_string = hex::encode(&log_record.span_id);
        span_id_builder.append_value(hex_string.clone());
        Some(hex_string)
    } else {
        span_id_builder.append_null();
        None
    };

    (trace_id_hex, span_id_hex)
}

/// Adds indexed columns to the attributes map for query layer compatibility.
///
/// Duplicates `trace_id`, `span_id`, `severity_text`, and `account_id` into the attributes map
/// to allow queries to use indexed columns for filtering then switch to attributes for operations.
fn add_indexed_columns_to_attributes(
    attributes_builder: &mut MapBuilder<StringBuilder, StringBuilder>,
    trace_id_hex: Option<&String>,
    span_id_hex: Option<&String>,
    severity_text: &str,
    cloud_account_id: Option<&String>,
) {
    // trace_id
    if let Some(tid) = trace_id_hex {
        attributes_builder.keys().append_value("trace_id");
        attributes_builder.values().append_value(tid);
    }

    // span_id
    if let Some(sid) = span_id_hex {
        attributes_builder.keys().append_value("span_id");
        attributes_builder.values().append_value(sid);
    }

    // severity_text
    if !severity_text.is_empty() {
        attributes_builder.keys().append_value("severity_text");
        attributes_builder.values().append_value(severity_text);
    }

    // level (alias for severity_text for Grafana compatibility)
    if !severity_text.is_empty() {
        attributes_builder.keys().append_value("level"); // Loki support
        attributes_builder.values().append_value(severity_text);
    }

    // cloud_account_id
    if let Some(acc) = cloud_account_id {
        attributes_builder.keys().append_value("cloud_account_id");
        attributes_builder.values().append_value(acc);
    }
}

/// Recursively flattens an OTLP `AnyValue` into key-value pairs.
///
/// Nested `KvlistValue` structures are flattened using underscore separator,
/// matching Loki's behavior with JSON parser.
///
/// # Arguments
///
/// * `prefix` - Key prefix for nested values (use empty string for root)
/// * `value` - OTLP `AnyValue` to flatten
///
/// # Returns
///
/// Vector of (key, value) string pairs representing flattened structure
///
/// # Examples
///
/// ```
/// // Input: prefix="http", value=KvlistValue({method: "GET", details: {code: 200}})
/// // Output: [("http_method", "GET"), ("http_details_code", "200")]
/// ```
fn flatten_any_value(prefix: &str, value: Option<&AnyValue>) -> Vec<(String, String)> {
    let mut result = Vec::new();

    if let Some(v) = value {
        if let Some(val) = &v.value {
            match val {
                Value::KvlistValue(kvs) => {
                    // Recursively flatten nested key-value lists
                    for kv in &kvs.values {
                        let nested_prefix = if prefix.is_empty() {
                            kv.key.clone()
                        } else {
                            format!("{}_{}", prefix, kv.key)
                        };
                        let nested_pairs = flatten_any_value(&nested_prefix, kv.value.as_ref());
                        result.extend(nested_pairs);
                    }
                }
                Value::ArrayValue(arr) => {
                    // Arrays are stringified, not flattened (no indexable keys)
                    let items: Vec<String> =
                        arr.values.iter().filter_map(|v| extract_any_value_string(Some(v))).collect();
                    let stringified = format!("[{}]", items.join(", "));
                    result.push((prefix.to_string(), stringified));
                }
                _ => {
                    // Primitive types: stringify and return
                    if let Some(s) = extract_any_value_string(Some(v)) {
                        result.push((prefix.to_string(), s));
                    }
                }
            }
        }
    }

    result
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
        assert_eq!(batch.num_columns(), 11);
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

    #[test]
    fn test_body_json_serialization_primitives() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        // String - returned as-is
        let string_val = AnyValue {
            value: Some(Value::StringValue("hello world".to_string())),
        };
        assert_eq!(
            serialize_any_value_to_json(Some(&string_val)),
            Some("hello world".to_string())
        );

        // Int - stringified
        let int_val = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        assert_eq!(serialize_any_value_to_json(Some(&int_val)), Some("42".to_string()));

        // Bool - stringified
        let bool_val = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert_eq!(serialize_any_value_to_json(Some(&bool_val)), Some("true".to_string()));
    }

    #[test]
    fn test_body_json_serialization_array() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, any_value::Value};

        let array_val = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("tag1".to_string())),
                    },
                    AnyValue {
                        value: Some(Value::IntValue(123)),
                    },
                ],
            })),
        };

        let result = serialize_any_value_to_json(Some(&array_val));
        assert!(result.is_some());

        // Should be valid JSON array
        let parsed: serde_json::Value =
            serde_json::from_str(&result.expect("result should exist")).expect("should parse as JSON");
        assert!(parsed.is_array());
        assert_eq!(parsed[0], "tag1");
        assert_eq!(parsed[1], 123);
    }

    #[test]
    fn test_body_json_serialization_object() {
        use opentelemetry_proto::tonic::common::v1::{KeyValueList, any_value::Value};

        let object_val = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "status".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::IntValue(200)),
                        }),
                    },
                    KeyValue {
                        key: "message".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("OK".to_string())),
                        }),
                    },
                ],
            })),
        };

        let result = serialize_any_value_to_json(Some(&object_val));
        assert!(result.is_some());

        // Should be valid JSON object
        let parsed: serde_json::Value =
            serde_json::from_str(&result.expect("result should exist")).expect("should parse as JSON");
        assert!(parsed.is_object());
        assert_eq!(parsed["status"], 200);
        assert_eq!(parsed["message"], "OK");
    }

    #[test]
    fn test_flatten_nested_attributes() {
        use std::collections::HashMap;

        use opentelemetry_proto::tonic::common::v1::{KeyValueList, any_value::Value};

        // Test nested KvlistValue flattening
        let nested_kv = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "method".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("POST".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "details".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![
                                    KeyValue {
                                        key: "status".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::IntValue(200)),
                                        }),
                                    },
                                    KeyValue {
                                        key: "path".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("/api/v1".to_string())),
                                        }),
                                    },
                                ],
                            })),
                        }),
                    },
                ],
            })),
        };

        let flattened = flatten_any_value("http", Some(&nested_kv));

        // Should produce 3 flattened entries
        assert_eq!(flattened.len(), 3);

        // Check flattened keys and values
        let map: HashMap<String, String> = flattened.into_iter().collect();
        assert_eq!(map.get("http_method"), Some(&"POST".to_string()));
        assert_eq!(map.get("http_details_status"), Some(&"200".to_string()));
        assert_eq!(map.get("http_details_path"), Some(&"/api/v1".to_string()));
    }

    #[test]
    fn test_flatten_array_attribute() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, any_value::Value};

        // Arrays should be stringified, not flattened (no meaningful keys)
        let array_value = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("tag1".to_string())),
                    },
                    AnyValue {
                        value: Some(Value::StringValue("tag2".to_string())),
                    },
                ],
            })),
        };

        let flattened = flatten_any_value("tags", Some(&array_value));

        // Should produce single stringified entry
        assert_eq!(flattened.len(), 1);
        assert_eq!(flattened[0].0, "tags");
        assert_eq!(flattened[0].1, "[tag1, tag2]");
    }

    #[test]
    fn test_flatten_primitive_attribute() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        // Primitive values should work as before
        let string_value = AnyValue {
            value: Some(Value::StringValue("simple".to_string())),
        };

        let flattened = flatten_any_value("key", Some(&string_value));

        assert_eq!(flattened.len(), 1);
        assert_eq!(flattened[0].0, "key");
        assert_eq!(flattened[0].1, "simple");
    }
}
