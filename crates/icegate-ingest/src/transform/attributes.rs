//! Shared attribute-flattening, map-field, byte, and timestamp helpers for the
//! OTLP transforms.

use arrow::{
    array::{MapBuilder, MapFieldNames, StringBuilder},
    datatypes::{DataType, Fields, Schema},
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

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
pub(crate) fn extract_string_value(value: Option<&AnyValue>) -> Option<String> {
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
pub(crate) fn serialize_any_value_to_json(value: Option<&AnyValue>) -> Option<String> {
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
pub(crate) fn is_zero_bytes(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}

/// Convert an OTLP `u32` counter into the schema's signed `i32` field.
///
/// OTLP represents `flags` and `dropped_*_count` as `u32`. Iceberg stores
/// them as `Int` (i32). Realistic telemetry values sit far below `i32::MAX`,
/// but `as i32` silently wraps for anything above `2^31 - 1`, producing a
/// negative count that later readers would see as "-2 billion dropped
/// events". Fail the transform instead so the caller can surface the
/// malformed span via `partial_success`.
pub(crate) fn u32_count_to_i32(value: u32, context: &'static str) -> crate::error::Result<i32> {
    i32::try_from(value)
        .map_err(|_| crate::error::IngestError::Validation(format!("{context} exceeds i32::MAX: {value}")))
}

/// Convert an OTLP `u64` count into the schema's signed `i64` field.
///
/// OTLP counts (`count`, `zero_count`, bucket counts) are `u64`; Iceberg stores
/// them as `Long` (i64). Realistic values sit far below `i64::MAX`, but a raw
/// `as i64` would wrap above `2^63 - 1` into a negative count. Fail instead so
/// the caller surfaces the malformed point.
pub(crate) fn u64_to_i64(value: u64, context: &'static str) -> crate::error::Result<i64> {
    i64::try_from(value)
        .map_err(|_| crate::error::IngestError::Validation(format!("{context} exceeds i64::MAX: {value}")))
}

/// Name the OTLP `AnyValue` variant without echoing its payload.
///
/// Strict-parse validation errors are logged at `debug` when the operations
/// projection drops a row; embedding the raw value would spill prompt text,
/// ids, or other user data into logs, so report only the variant name.
const fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::StringValue(_) => "string",
        Value::BoolValue(_) => "bool",
        Value::IntValue(_) => "int",
        Value::DoubleValue(_) => "double",
        Value::ArrayValue(_) => "array",
        Value::KvlistValue(_) => "kvlist",
        Value::BytesValue(_) => "bytes",
    }
}

/// Strictly extract an `i64` from an OTLP `AnyValue`.
///
/// Returns `Ok(None)` when the attribute is absent. Accepts an OTLP
/// `IntValue` directly, or a `StringValue` that parses cleanly as `i64`
/// (some SDKs stringify numbers). Any other variant, or an unparseable
/// numeric string, returns `Err(IngestError::Validation)` so the operations
/// projection drops the row instead of emitting a corrupt typed column (D6).
///
/// # Errors
///
/// Returns `IngestError::Validation` if the value is present but is neither an
/// integer nor a string that parses as `i64`.
pub(crate) fn extract_i64(value: Option<&AnyValue>, context: &'static str) -> crate::error::Result<Option<i64>> {
    let Some(inner) = value.and_then(|a| a.value.as_ref()) else {
        return Ok(None);
    };
    match inner {
        Value::IntValue(i) => Ok(Some(*i)),
        Value::StringValue(s) => s
            .parse::<i64>()
            .map(Some)
            .map_err(|_| crate::error::IngestError::Validation(format!("{context} is not a valid i64"))),
        other => Err(crate::error::IngestError::Validation(format!(
            "{context} expected int, found {}",
            value_type_name(other)
        ))),
    }
}

/// Strictly extract an `f64` from an OTLP `AnyValue`.
///
/// Returns `Ok(None)` when absent. Accepts `DoubleValue`, widens an
/// `IntValue` losslessly, or parses a `StringValue`. Any other variant, or an
/// unparseable string, returns `Err(IngestError::Validation)` (D6).
///
/// # Errors
///
/// Returns `IngestError::Validation` if the value is present but is neither a
/// number nor a string that parses as `f64`.
pub(crate) fn extract_f64(value: Option<&AnyValue>, context: &'static str) -> crate::error::Result<Option<f64>> {
    let Some(inner) = value.and_then(|a| a.value.as_ref()) else {
        return Ok(None);
    };
    match inner {
        Value::DoubleValue(d) if d.is_finite() => Ok(Some(*d)),
        // Non-finite doubles (NaN, +/-inf) would saturate to garbage when later
        // scaled and cast to an integer timing column, so reject them here (D6).
        Value::DoubleValue(_) => Err(crate::error::IngestError::Validation(format!(
            "{context} must be a finite f64"
        ))),
        // `i64` -> `f64` widening is intentional and never wraps; large
        // integers lose mantissa precision but that is acceptable for sampling
        // parameters which are small.
        #[allow(clippy::cast_precision_loss)]
        Value::IntValue(i) => Ok(Some(*i as f64)),
        Value::StringValue(s) => {
            let parsed = s
                .parse::<f64>()
                .map_err(|_| crate::error::IngestError::Validation(format!("{context} is not a valid f64")))?;
            if !parsed.is_finite() {
                return Err(crate::error::IngestError::Validation(format!(
                    "{context} must be a finite f64"
                )));
            }
            Ok(Some(parsed))
        }
        other => Err(crate::error::IngestError::Validation(format!(
            "{context} expected double, found {}",
            value_type_name(other)
        ))),
    }
}

/// Strictly extract a `bool` from an OTLP `AnyValue`.
///
/// Returns `Ok(None)` when absent. Accepts only a `BoolValue`; a stringified
/// `"true"` is rejected to keep parsing unambiguous (D6).
///
/// # Errors
///
/// Returns `IngestError::Validation` if the value is present but is not a bool.
pub(crate) fn extract_bool(value: Option<&AnyValue>, context: &'static str) -> crate::error::Result<Option<bool>> {
    let Some(inner) = value.and_then(|a| a.value.as_ref()) else {
        return Ok(None);
    };
    match inner {
        Value::BoolValue(b) => Ok(Some(*b)),
        other => Err(crate::error::IngestError::Validation(format!(
            "{context} expected bool, found {}",
            value_type_name(other)
        ))),
    }
}

/// Strictly extract a `Vec<String>` from an OTLP `ArrayValue` of strings.
///
/// Returns `Ok(None)` when absent (so the caller stores a NULL list, not an
/// empty list). Requires an `ArrayValue` whose every element is a
/// `StringValue`; a scalar or any non-string element returns
/// `Err(IngestError::Validation)` (D6).
///
/// # Errors
///
/// Returns `IngestError::Validation` if the value is present but is not an
/// array of strings.
pub(crate) fn extract_string_list(
    value: Option<&AnyValue>,
    context: &'static str,
) -> crate::error::Result<Option<Vec<String>>> {
    let Some(inner) = value.and_then(|a| a.value.as_ref()) else {
        return Ok(None);
    };
    match inner {
        Value::ArrayValue(arr) => {
            let mut out = Vec::with_capacity(arr.values.len());
            for item in &arr.values {
                match item.value.as_ref() {
                    Some(Value::StringValue(s)) => out.push(s.clone()),
                    _ => {
                        return Err(crate::error::IngestError::Validation(format!(
                            "{context} array elements must be strings"
                        )));
                    }
                }
            }
            Ok(Some(out))
        }
        other => Err(crate::error::IngestError::Validation(format!(
            "{context} expected array, found {}",
            value_type_name(other)
        ))),
    }
}

/// Map field names matching the Iceberg `MAP<String,String>` Arrow layout.
pub(crate) fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

/// Current wall-clock time in microseconds since the Unix epoch.
///
/// # Errors
///
/// Returns `IngestError::Validation` if the system clock is set before the Unix
/// epoch, or if the elapsed microseconds exceed `i64::MAX`. Both are degenerate
/// states that do not occur on a correctly configured host; the transform
/// surfaces them as an error rather than panicking.
pub(crate) fn now_micros() -> crate::error::Result<i64> {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| crate::error::IngestError::Validation("system clock is set before the Unix epoch".to_string()))?
        .as_micros();
    i64::try_from(micros)
        .map_err(|_| crate::error::IngestError::Validation("current time in micros exceeds i64::MAX".to_string()))
}

/// Convert OTLP nanoseconds to microseconds (storage precision).
#[allow(clippy::cast_possible_wrap)]
pub(crate) const fn nanos_to_micros(nanos: u64) -> i64 {
    (nanos / 1000) as i64
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
pub(crate) fn extract_map_fields_from_schema(
    schema: &Schema,
) -> crate::error::Result<(arrow::datatypes::FieldRef, arrow::datatypes::FieldRef)> {
    extract_map_fields_from_schema_named(schema, "attributes")
}

/// Extracts map field metadata from the Arrow schema by field name.
///
/// Used by both `logs_to_record_batch` (via `extract_map_fields_from_schema`)
/// and `spans_to_record_batch` (directly).
pub(crate) fn extract_map_fields_from_schema_named(
    schema: &Schema,
    name: &str,
) -> crate::error::Result<(arrow::datatypes::FieldRef, arrow::datatypes::FieldRef)> {
    let field = schema
        .field_with_name(name)
        .map_err(|_| crate::error::IngestError::Validation(format!("Schema must contain a '{name}' field")))?;
    match field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => {
                if fields.len() < 2 {
                    return Err(crate::error::IngestError::Validation(format!(
                        "Expected at least 2 fields in map entries struct for '{name}', found {}",
                        fields.len()
                    )));
                }
                Ok((fields[0].clone(), fields[1].clone()))
            }
            _ => Err(crate::error::IngestError::Validation(format!(
                "Expected Struct type for map entries in '{name}' field"
            ))),
        },
        _ => Err(crate::error::IngestError::Validation(format!(
            "Expected Map type for '{name}' field, found {:?}",
            field.data_type()
        ))),
    }
}

/// Extracts map key/value field refs from a struct's inner field list.
///
/// Used to populate nested `Map` builders inside the events/links struct arrays.
pub(crate) fn extract_map_fields_from_nested_struct(
    fields: &arrow::datatypes::Fields,
    map_field_name: &str,
) -> crate::error::Result<(arrow::datatypes::FieldRef, arrow::datatypes::FieldRef)> {
    let map_field = fields.iter().find(|f| f.name() == map_field_name).ok_or_else(|| {
        crate::error::IngestError::Validation(format!("nested struct missing '{map_field_name}' field"))
    })?;
    match map_field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(inner_fields) => {
                if inner_fields.len() < 2 {
                    return Err(crate::error::IngestError::Validation(format!(
                        "map entries struct for '{map_field_name}' needs 2+ fields, got {}",
                        inner_fields.len()
                    )));
                }
                Ok((inner_fields[0].clone(), inner_fields[1].clone()))
            }
            _ => Err(crate::error::IngestError::Validation(format!(
                "map entries must be Struct for '{map_field_name}'"
            ))),
        },
        _ => Err(crate::error::IngestError::Validation(format!(
            "'{map_field_name}' must be Map"
        ))),
    }
}

/// Adds flattened attributes to the map builder with key normalization.
///
/// Flattens nested structures and normalises attribute keys by replacing
/// dots with underscores. Any normalised key present in `skip_keys` is
/// dropped (used to suppress already-promoted top-level columns from the
/// resource flattening pass).
pub(crate) fn add_flattened_attributes(
    attributes: &[KeyValue],
    attributes_builder: &mut MapBuilder<StringBuilder, StringBuilder>,
    skip_keys: &[&str],
) {
    for kv in attributes {
        let flattened = flatten_any_value(&kv.key, kv.value.as_ref());
        for (key, value) in flattened {
            let normalized = normalize_attribute_key(&key);
            if skip_keys.contains(&normalized.as_str()) {
                continue;
            }
            attributes_builder.keys().append_value(normalized);
            attributes_builder.values().append_value(value);
        }
    }
}

/// Merge dotted-flattened attributes from several precedence levels into one
/// sorted, deduplicated map. Levels apply in order, so a later (more specific)
/// level overwrites an earlier one on key collision. Any dotted key in
/// `skip_in_first` is dropped **only** from the first (most-general) level —
/// used to suppress keys already promoted to a dedicated top-level column while
/// still letting a more-specific level re-supply an override (mirrors the logs
/// `LOG_PROMOTED_RESOURCE_KEYS` rule).
pub(crate) fn merge_dotted_levels(
    levels: &[&[KeyValue]],
    skip_in_first: &[&str],
) -> std::collections::BTreeMap<String, String> {
    let mut merged = std::collections::BTreeMap::new();
    for (level_idx, attrs) in levels.iter().enumerate() {
        for kv in *attrs {
            for (key, value) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
                if level_idx == 0 && skip_in_first.contains(&key.as_str()) {
                    continue;
                }
                merged.insert(key, value);
            }
        }
    }
    merged
}

/// Flatten a single attribute list and deduplicate keys into a sorted
/// [`std::collections::BTreeMap`].
///
/// Mirrors the deduplication semantics of [`merge_dotted_attributes`] for
/// the single-input case so resource attributes get the same guarantee
/// (one entry per key, deterministic order) as scope+span attributes.
pub(crate) fn dedupe_dotted_attributes(attributes: &[KeyValue]) -> std::collections::BTreeMap<String, String> {
    let mut merged: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for kv in attributes {
        for (key, value) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
            merged.insert(key, value);
        }
    }
    merged
}

/// Merge scope and span attributes into a single deduplicated, sorted
/// key→value map.
///
/// Scope attributes are inserted first; span attributes overwrite on
/// collision so span-level metadata always wins over the (broader)
/// scope-level metadata — matching the `OTel` data model where span
/// attributes describe the operation and scope attributes describe the
/// instrumentation library that produced it.
///
/// Returning a [`std::collections::BTreeMap`] guarantees a single entry
/// per key (so downstream `MAP<K,V>` readers can't disagree on which
/// duplicate to surface) and gives a deterministic on-disk attribute
/// order for reproducible parquet output.
pub(crate) fn merge_dotted_attributes(
    scope_attrs: &[KeyValue],
    span_attrs: &[KeyValue],
) -> std::collections::BTreeMap<String, String> {
    let mut merged: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for kv in scope_attrs {
        for (key, value) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
            merged.insert(key, value);
        }
    }
    // Span attributes overwrite any scope-level entry with the same key.
    for kv in span_attrs {
        for (key, value) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
            merged.insert(key, value);
        }
    }
    merged
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

/// Flattens an OTLP `AnyValue` into dotted key-value pairs.
///
/// Mirrors [`flatten_any_value`] but joins nested `KvlistValue` keys with a
/// dot (`.`) separator instead of underscore. Use this for spans and other
/// signals where OTel-native dotted attribute names must be preserved.
///
/// # Arguments
///
/// * `prefix` - key prefix for nested values (empty string at the root).
/// * `value` - OTLP `AnyValue` to flatten.
///
/// # Returns
///
/// Vector of (key, value) string pairs representing the flattened structure.
/// Primitive values yield a single pair `(prefix, stringified_value)`.
/// Arrays are stringified (not flattened) since they have no indexable keys.
pub(crate) fn flatten_any_value_dotted(prefix: &str, value: Option<&AnyValue>) -> Vec<(String, String)> {
    // TODO(low): the primitive (general) case heap-allocates a single-element `Vec`
    // per attribute on the hot ingest path; a callback/`SmallVec` API would avoid it.
    let mut result = Vec::new();
    let Some(v) = value else {
        return result;
    };
    let Some(val) = &v.value else {
        return result;
    };

    match val {
        Value::KvlistValue(kvs) => {
            for kv in &kvs.values {
                let nested_prefix = if prefix.is_empty() {
                    kv.key.clone()
                } else {
                    format!("{prefix}.{}", kv.key)
                };
                result.extend(flatten_any_value_dotted(&nested_prefix, kv.value.as_ref()));
            }
        }
        Value::ArrayValue(arr) => {
            let items: Vec<String> = arr.values.iter().filter_map(|v| extract_any_value_string(Some(v))).collect();
            result.push((prefix.to_string(), format!("[{}]", items.join(", "))));
        }
        _ => {
            if let Some(s) = extract_any_value_string(Some(v)) {
                result.push((prefix.to_string(), s));
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

/// Extracts the `element` field of a `List` column from the Arrow schema.
///
/// The returned field carries the element name/nullability Arrow's
/// `RecordBatch::try_new` compares against, so it must be passed to
/// `ListBuilder::with_field` when building the column.
pub(crate) fn list_element_field(schema: &Schema, column: &str) -> crate::error::Result<arrow::datatypes::FieldRef> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| crate::error::IngestError::Validation(format!("Schema must contain a '{column}' field")))?;
    match field.data_type() {
        DataType::List(element) => Ok(element.clone()),
        other => Err(crate::error::IngestError::Validation(format!(
            "Expected List type for '{column}' field, found {other:?}"
        ))),
    }
}

/// Extracts the element field and inner struct fields of a `List<Struct>` column.
///
/// Returns `(element_field, struct_fields)`: the element field for
/// `ListBuilder::with_field`, and the struct's inner fields for `StructBuilder::new`.
pub(crate) fn list_struct_fields(
    schema: &Schema,
    column: &str,
) -> crate::error::Result<(arrow::datatypes::FieldRef, Fields)> {
    let element = list_element_field(schema, column)?;
    match element.data_type() {
        DataType::Struct(fields) => Ok((element.clone(), fields.clone())),
        other => Err(crate::error::IngestError::Validation(format!(
            "Expected List<Struct> for '{column}' field, found List<{other:?}>"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::AnyValue;

    use super::*;

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

    #[test]
    fn flatten_nested_attributes_with_dot_separator() {
        use std::collections::HashMap;

        use opentelemetry_proto::tonic::common::v1::{KeyValueList, any_value::Value};

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
                                values: vec![KeyValue {
                                    key: "status".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(Value::IntValue(200)),
                                    }),
                                }],
                            })),
                        }),
                    },
                ],
            })),
        };

        let flattened = flatten_any_value_dotted("http", Some(&nested_kv));
        let map: HashMap<String, String> = flattened.into_iter().collect();

        assert_eq!(map.get("http.method"), Some(&"POST".to_string()));
        assert_eq!(map.get("http.details.status"), Some(&"200".to_string()));
    }

    #[test]
    fn extract_i64_parses_int_and_numeric_string() {
        let int_val = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        assert_eq!(extract_i64(Some(&int_val), "ctx").expect("ok"), Some(42));

        let str_val = AnyValue {
            value: Some(Value::StringValue("128".to_string())),
        };
        assert_eq!(extract_i64(Some(&str_val), "ctx").expect("ok"), Some(128));

        assert_eq!(extract_i64(None, "ctx").expect("ok"), None);
    }

    #[test]
    fn extract_i64_rejects_non_numeric() {
        let bad = AnyValue {
            value: Some(Value::StringValue("hot".to_string())),
        };
        assert!(extract_i64(Some(&bad), "ctx").is_err());

        let wrong_variant = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert!(extract_i64(Some(&wrong_variant), "ctx").is_err());
    }

    #[test]
    fn extract_f64_parses_double_int_and_string() {
        let dbl = AnyValue {
            value: Some(Value::DoubleValue(0.7)),
        };
        assert!((extract_f64(Some(&dbl), "ctx").expect("ok").expect("some") - 0.7).abs() < f64::EPSILON);

        let int_val = AnyValue {
            value: Some(Value::IntValue(2)),
        };
        assert!((extract_f64(Some(&int_val), "ctx").expect("ok").expect("some") - 2.0).abs() < f64::EPSILON);

        let str_val = AnyValue {
            value: Some(Value::StringValue("1.5".to_string())),
        };
        assert!((extract_f64(Some(&str_val), "ctx").expect("ok").expect("some") - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn extract_f64_rejects_non_numeric() {
        let bad = AnyValue {
            value: Some(Value::StringValue("hot".to_string())),
        };
        assert!(extract_f64(Some(&bad), "ctx").is_err());
    }

    #[test]
    fn extract_f64_rejects_non_finite() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let double = AnyValue {
                value: Some(Value::DoubleValue(value)),
            };
            assert!(extract_f64(Some(&double), "ctx").is_err());
        }

        // Non-finite values arriving as stringified numbers must be rejected too.
        for repr in ["nan", "inf", "-inf", "infinity"] {
            let parsed = AnyValue {
                value: Some(Value::StringValue(repr.to_string())),
            };
            assert!(extract_f64(Some(&parsed), "ctx").is_err());
        }
    }

    #[test]
    fn extract_bool_parses_bool_only() {
        let b = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert_eq!(extract_bool(Some(&b), "ctx").expect("ok"), Some(true));
        assert_eq!(extract_bool(None, "ctx").expect("ok"), None);
    }

    #[test]
    fn extract_bool_rejects_non_bool() {
        let bad = AnyValue {
            value: Some(Value::StringValue("true".to_string())),
        };
        assert!(extract_bool(Some(&bad), "ctx").is_err());
    }

    #[test]
    fn extract_string_list_collects_strings() {
        use opentelemetry_proto::tonic::common::v1::ArrayValue;

        let arr = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("stop1".to_string())),
                    },
                    AnyValue {
                        value: Some(Value::StringValue("stop2".to_string())),
                    },
                ],
            })),
        };
        assert_eq!(
            extract_string_list(Some(&arr), "ctx").expect("ok"),
            Some(vec!["stop1".to_string(), "stop2".to_string()])
        );
        assert_eq!(extract_string_list(None, "ctx").expect("ok"), None);
    }

    #[test]
    fn extract_string_list_rejects_non_array_and_non_string_elements() {
        use opentelemetry_proto::tonic::common::v1::ArrayValue;

        let not_array = AnyValue {
            value: Some(Value::StringValue("single".to_string())),
        };
        assert!(extract_string_list(Some(&not_array), "ctx").is_err());

        let mixed = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![AnyValue {
                    value: Some(Value::IntValue(1)),
                }],
            })),
        };
        assert!(extract_string_list(Some(&mixed), "ctx").is_err());
    }
}
