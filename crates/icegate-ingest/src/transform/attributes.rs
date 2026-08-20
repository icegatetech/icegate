//! Shared attribute-flattening, map-field, byte, and timestamp helpers for the
//! OTLP transforms.

use arrow::{
    array::{MapBuilder, MapFieldNames, StringBuilder},
    datatypes::{DataType, Fields, Schema},
};
use icegate_common::attribute_key::{matches_wire_name, normalize_attribute_key};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

/// OTLP semantic-convention resource-attribute keys, in the dotted form they
/// arrive on the wire, that the transforms read by name.
///
/// Spelled once here rather than per signal because every transform matches the
/// same keys — to fill a promoted top-level column, and to suppress the now
/// redundant map copy through [`merge_dotted_levels`]. They belong to this
/// module and not to `icegate_common::schema`, which describes table columns:
/// the dotted key is what OTLP puts in an attribute map, and the column it is
/// promoted into is spelled differently and lives at a different layer
/// (`service.name` -> `icegate_common::schema::COL_SERVICE_NAME`).
pub(crate) const SERVICE_NAME_KEY: &str = "service.name";
/// See [`SERVICE_NAME_KEY`]. Promoted to its own column on metrics only.
pub(crate) const SERVICE_INSTANCE_ID_KEY: &str = "service.instance.id";

/// Extracts a string value from an OTLP `AnyValue` reference.
///
/// Converts various OTLP value types to string representation.
fn extract_any_value_string(value: Option<&AnyValue>) -> Option<String> {
    value.and_then(|v| {
        v.value.as_ref().and_then(|val| match val {
            Value::StringValue(s) => Some(s.clone()),
            Value::IntValue(i) => Some(i.to_string()),
            Value::DoubleValue(d) => Some(d.to_string()),
            Value::BoolValue(b) => Some(b.to_string()),
            Value::BytesValue(b) => Some(hex::encode(b)),
            Value::ArrayValue(arr) => {
                let items: Vec<String> = arr.values.iter().filter_map(|v| extract_any_value_string(Some(v))).collect();
                Some(format!("[{}]", items.join(", ")))
            }
            Value::KvlistValue(kvs) => {
                let pairs: Vec<String> = kvs
                    .values
                    .iter()
                    .filter_map(|kv| extract_any_value_string(kv.value.as_ref()).map(|v| format!("{}={}", kv.key, v)))
                    .collect();
                Some(format!("{{{}}}", pairs.join(", ")))
            }
            // TODO(otlp-strindex): DROPS DATA. OTLP 0.32 added string interning —
            // the value is an index into the request's string table rather than an
            // inline string, and that table is not plumbed through to this
            // function. Resolving it requires threading the table from the request
            // root through the transform. Until then an interned value yields no
            // attribute at all, silently. Same in `serialize_any_value_to_json` and
            // `any_value_to_json`; `KeyValue::key_strindex` is likewise ignored.
            Value::StringValueStrindex(_) => None,
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
            // See the TODO(otlp-strindex) note in `extract_any_value_string`.
            Value::StringValueStrindex(_) => None,
        })
    })
}

/// Helper to convert `AnyValue` to `serde_json::Value` for JSON serialization.
fn any_value_to_json(value: &AnyValue) -> Option<serde_json::Value> {
    value.value.as_ref().and_then(|val| match val {
        Value::StringValue(s) => Some(serde_json::Value::String(s.clone())),
        Value::IntValue(i) => Some(serde_json::Value::Number(serde_json::Number::from(*i))),
        Value::DoubleValue(d) => {
            Some(serde_json::Number::from_f64(*d).map_or(serde_json::Value::Null, serde_json::Value::Number))
        }
        Value::BoolValue(b) => Some(serde_json::Value::Bool(*b)),
        Value::BytesValue(b) => Some(serde_json::Value::String(hex::encode(b))),
        Value::ArrayValue(arr) => {
            let items: Vec<serde_json::Value> = arr.values.iter().filter_map(any_value_to_json).collect();
            Some(serde_json::Value::Array(items))
        }
        Value::KvlistValue(kvs) => {
            let mut map = serde_json::Map::new();
            for kv in &kvs.values {
                if let Some(v) = kv.value.as_ref().and_then(any_value_to_json) {
                    map.insert(kv.key.clone(), v);
                }
            }
            Some(serde_json::Value::Object(map))
        }
        // See the TODO(otlp-strindex) note in `extract_any_value_string`.
        Value::StringValueStrindex(_) => None,
    })
}

/// Serialize every attribute of a span event into a JSON object string keyed by
/// attribute name (output order follows the input).
///
/// Returns `None` when there are no serializable attributes. Used to project a
/// span event's full payload (e.g. Claude Code's `tool.output` event) into a
/// single JSON content column. On a duplicate attribute key the first value wins.
pub(crate) fn serialize_all_attrs_to_json_object(attrs: &[KeyValue]) -> Option<String> {
    let mut object = serde_json::Map::new();
    for kv in attrs {
        if object.contains_key(&kv.key) {
            continue;
        }
        if let Some(value) = kv.value.as_ref().and_then(any_value_to_json) {
            object.insert(kv.key.clone(), value);
        }
    }
    if object.is_empty() {
        return None;
    }
    serde_json::to_string(&object).ok()
}

/// Serialize the named attribute `keys` present in `attrs` into a JSON object
/// string keyed by attribute name (output order follows `keys`).
///
/// Returns `None` when none of the keys are present. Used to project a selected
/// set of flat span attributes (e.g. a tool call's `full_command` / `file_path`
/// input) into a single JSON content column. On a duplicate key the first wins.
pub(crate) fn serialize_attrs_to_json_object(attrs: &[KeyValue], keys: &[&str]) -> Option<String> {
    let mut object = serde_json::Map::new();
    for &key in keys {
        if object.contains_key(key) {
            continue;
        }
        if let Some(kv) = attrs.iter().find(|kv| kv.key == key) {
            if let Some(value) = kv.value.as_ref().and_then(any_value_to_json) {
                object.insert(key.to_string(), value);
            }
        }
    }
    if object.is_empty() {
        return None;
    }
    serde_json::to_string(&object).ok()
}

/// Serialize a single chat message into the `[{"role": role, "content": content}]`
/// JSON array shape that the message content columns (`input_messages` /
/// `output_messages`) use.
///
/// Used for SDKs (e.g. Claude Code) that emit a bare prompt/response string
/// rather than a structured messages array; consumers reconstruct a conversation
/// by parsing this column as an array of role/content messages.
pub(crate) fn serialize_message_to_json_array(role: &str, content: &str) -> Option<String> {
    serde_json::to_string(&serde_json::json!([{ "role": role, "content": content }])).ok()
}

/// One node of a rebuilt structure, before it is turned back into JSON.
///
/// Arrays are held index-keyed rather than as a `Vec` because the flattened
/// attributes arrive in whatever order the map iterated: `...10.` sorts before
/// `...2.` lexicographically, and an index may be missing entirely if the
/// producer dropped an attribute. A `BTreeMap<usize, _>` restores numeric order
/// and tolerates gaps, both of which a positional `Vec` would get wrong.
enum RebuiltNode {
    /// A terminal attribute value.
    Leaf(serde_json::Value),
    /// A nested object, keyed by field name.
    Object(std::collections::BTreeMap<String, Self>),
    /// A nested array, keyed by the index parsed out of the attribute key.
    Array(std::collections::BTreeMap<usize, Self>),
}

impl RebuiltNode {
    /// Convert the tree into JSON, collapsing index-keyed maps into arrays in
    /// ascending index order.
    fn into_json(self) -> serde_json::Value {
        match self {
            Self::Leaf(value) => value,
            Self::Object(fields) => {
                serde_json::Value::Object(fields.into_iter().map(|(key, node)| (key, node.into_json())).collect())
            }
            Self::Array(items) => serde_json::Value::Array(items.into_values().map(Self::into_json).collect()),
        }
    }

    /// Insert `value` at `path`, creating intermediate nodes as needed.
    ///
    /// A numeric segment addresses an array element and, per the `OpenInference`
    /// flattening rule, is always followed by a singular wrapper segment naming
    /// the element's type (`message`, `tool_call`, `tool`, `document`). That
    /// wrapper carries no information the position does not already give, so it
    /// is skipped, which is what turns `...messages.0.message.role` back into
    /// `[{"role": ...}]` rather than `[{"message": {"role": ...}}]`.
    ///
    /// A segment that collides with an already-built node of the other kind is
    /// dropped rather than overwriting it: a producer that emits both
    /// `x.0.a.b` and `x.0.a` has contradicted itself, and keeping the first
    /// value is preferable to letting the later one silently win.
    fn insert(&mut self, path: &[&str], value: serde_json::Value) {
        let Some((head, rest)) = path.split_first() else {
            return;
        };
        if let Ok(index) = head.parse::<usize>() {
            let Self::Array(items) = self else { return };
            // Skip the singular wrapper that always follows an index. A path
            // ending on the index itself has no wrapper to skip.
            let rest = rest.split_first().map_or(rest, |(_wrapper, tail)| tail);
            if rest.is_empty() {
                items.entry(index).or_insert(Self::Leaf(value));
                return;
            }
            items.entry(index).or_insert_with(|| Self::child_for(rest)).insert(rest, value);
            return;
        }
        let Self::Object(fields) = self else { return };
        if rest.is_empty() {
            fields.entry((*head).to_string()).or_insert(Self::Leaf(value));
            return;
        }
        fields
            .entry((*head).to_string())
            .or_insert_with(|| Self::child_for(rest))
            .insert(rest, value);
    }

    /// The empty container a path should descend into: an array when the next
    /// segment is an index, an object otherwise.
    fn child_for(path: &[&str]) -> Self {
        if path.first().is_some_and(|segment| segment.parse::<usize>().is_ok()) {
            Self::Array(std::collections::BTreeMap::new())
        } else {
            Self::Object(std::collections::BTreeMap::new())
        }
    }
}

/// Rebuild the JSON array that `OpenInference` flattened into indexed attribute
/// keys under `prefix`, and serialize it.
///
/// `OpenInference` does not emit a structured messages array the way the OTEL
/// `GenAI` convention does. It flattens one instead, as
/// `<prefix>.<index>.<singular>.<field...>` — so a two-message prompt arrives as
/// four separate span attributes:
///
/// ```text
/// llm.input_messages.0.message.role     = "system"
/// llm.input_messages.0.message.content  = "You are..."
/// llm.input_messages.1.message.role     = "user"
/// llm.input_messages.1.message.content  = "What is..."
/// ```
///
/// This is the inverse of that flattening, so the pieces land in
/// `input_messages` / `output_messages` as the same
/// `[{"role": ..., "content": ...}]` array every other convention projects,
/// rather than not landing at all. The rule is applied recursively, so a nested
/// group such as `...0.message.tool_calls.0.tool_call.function.name` rebuilds
/// into a `tool_calls` array inside its message.
///
/// Returns `None` when no attribute carries the prefix.
pub(crate) fn serialize_indexed_attrs_to_json_array(attrs: &[KeyValue], prefix: &str) -> Option<String> {
    let mut root = RebuiltNode::Array(std::collections::BTreeMap::new());
    let mut found = false;
    for kv in attrs {
        // The trailing dot matters: without it, `llm.tools_count` would be read
        // as a member of the `llm.tools` array.
        let Some(remainder) = kv.key.strip_prefix(prefix).and_then(|rest| rest.strip_prefix('.')) else {
            continue;
        };
        let Some(value) = kv.value.as_ref().and_then(any_value_to_json) else {
            continue;
        };
        let path: Vec<&str> = remainder.split('.').collect();
        // The first segment must be an index; anything else is a sibling
        // attribute that merely shares the prefix, not an array element.
        if path.first().is_none_or(|segment| segment.parse::<usize>().is_err()) {
            continue;
        }
        root.insert(&path, value);
        found = true;
    }
    if !found {
        return None;
    }
    serde_json::to_string(&root.into_json()).ok()
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
        Value::StringValueStrindex(_) => "string_strindex",
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

/// Build a `MAP<Utf8,Utf8>` builder wired to schema-derived key/value fields.
///
/// The field names match the canonical Iceberg-to-Arrow map layout used by
/// every ingest transform.
pub(crate) fn attribute_map_builder(
    key_field: arrow::datatypes::FieldRef,
    value_field: arrow::datatypes::FieldRef,
) -> MapBuilder<StringBuilder, StringBuilder> {
    MapBuilder::new(Some(map_field_names()), StringBuilder::new(), StringBuilder::new())
        .with_keys_field(key_field)
        .with_values_field(value_field)
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

/// Extracts map field metadata from the Arrow schema by field name.
///
/// Returns a tuple of (`key_field`, `value_field`) from the schema's map type
/// definition. Used by every per-level attribute MAP column (`logs`, `spans`,
/// and others as they migrate) to wire a builder to the Iceberg field metadata.
///
/// # Errors
///
/// Returns `IngestError::Validation` if:
/// - Schema does not contain a field named `name`
/// - The field is not of Map type
/// - The map entries are not of Struct type
/// - The struct does not contain at least 2 fields (key and value)
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

/// Merge dotted-flattened attributes from several precedence levels into one
/// sorted, deduplicated map. Levels apply in order, so a later (more specific)
/// level overwrites an earlier one on key collision. Any key in
/// `skip_in_first` is dropped **only** from the first (most-general) level —
/// used to suppress keys already promoted to a dedicated top-level column while
/// still letting a more-specific level re-supply an override (mirrors the logs
/// `LOG_PROMOTED_RESOURCE_KEYS` rule).
///
/// Suppression matches by **wire name**, not by the raw dotted string: a
/// promoted `service.name` also drops a resource attribute spelled
/// `service_name`. Both reach the query layer as the label `service_name`, and
/// the attribute maps are applied after the promoted columns when labels are
/// built — so letting the second spelling through would put the map's value on
/// the wire in place of the promoted column's.
pub(crate) fn merge_dotted_levels(
    levels: &[&[KeyValue]],
    skip_in_first: &[&str],
) -> std::collections::BTreeMap<String, String> {
    let skip_wire_names: Vec<_> = skip_in_first.iter().map(|promoted| normalize_attribute_key(promoted)).collect();
    let mut merged = std::collections::BTreeMap::new();
    for (level_idx, attrs) in levels.iter().enumerate() {
        for kv in *attrs {
            for (key, value) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
                if level_idx == 0 && skip_wire_names.iter().any(|wire_name| matches_wire_name(&key, wire_name)) {
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
/// Callers build each per-OTLP-level attribute column (resource, scope,
/// span, ...) with one independent call per level — there is no cross-level
/// merge, so a key present at two levels is stored twice, once per level.
/// Returning a [`std::collections::BTreeMap`] guarantees a single entry per
/// key within that level (so downstream `MAP<K,V>` readers can't disagree on
/// which duplicate to surface) and gives a deterministic on-disk attribute
/// order for reproducible parquet output.
pub(crate) fn dedupe_dotted_attributes(attributes: &[KeyValue]) -> std::collections::BTreeMap<String, String> {
    let mut merged: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for kv in attributes {
        for (key, value) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
            merged.insert(key, value);
        }
    }
    merged
}

/// Flattens an OTLP `AnyValue` into dotted key-value pairs.
///
/// Nested `KvlistValue` structures are flattened by joining keys with a dot
/// (`.`) separator, preserving the OTel-native dotted attribute name.
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
                        key_strindex: 0,
                        key: "status".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::IntValue(200)),
                        }),
                    },
                    KeyValue {
                        key_strindex: 0,
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
    fn flatten_nested_attributes_with_dot_separator() {
        use std::collections::HashMap;

        use opentelemetry_proto::tonic::common::v1::{KeyValueList, any_value::Value};

        let nested_kv = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key_strindex: 0,
                        key: "method".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("POST".to_string())),
                        }),
                    },
                    KeyValue {
                        key_strindex: 0,
                        key: "details".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![KeyValue {
                                    key_strindex: 0,
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

    #[test]
    fn serialize_all_attrs_to_json_object_includes_every_attribute() {
        let attrs = vec![
            KeyValue {
                key_strindex: 0,
                key: "output".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("done".to_string())),
                }),
            },
            KeyValue {
                key_strindex: 0,
                key: "bash_command".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("ls".to_string())),
                }),
            },
        ];

        // Every attribute is included, keyed by its name.
        let json = serialize_all_attrs_to_json_object(&attrs).expect("some");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert_eq!(parsed["output"], "done");
        assert_eq!(parsed["bash_command"], "ls");

        // No attributes -> None (a NULL content column, not "{}").
        assert!(serialize_all_attrs_to_json_object(&[]).is_none());
    }

    #[test]
    fn serialize_attrs_to_json_object_selects_named_keys() {
        let attrs = vec![
            KeyValue {
                key_strindex: 0,
                key: "full_command".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("ls -la".to_string())),
                }),
            },
            KeyValue {
                key_strindex: 0,
                key: "tool_name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("Bash".to_string())),
                }),
            },
        ];

        // Only the requested, present key is included (keyed by name); an absent
        // requested key and unrequested attributes are excluded.
        let json = serialize_attrs_to_json_object(&attrs, &["full_command", "file_path"]).expect("some");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert_eq!(parsed["full_command"], "ls -la");
        assert!(parsed.get("file_path").is_none());
        assert!(parsed.get("tool_name").is_none());

        // No requested key present -> None.
        assert!(serialize_attrs_to_json_object(&attrs, &["file_path"]).is_none());
    }

    #[test]
    fn serialize_message_to_json_array_wraps_role_and_content() {
        // Produces the [{"role","content"}] array shape conversation views parse,
        // with content correctly JSON-escaped.
        let json = serialize_message_to_json_array("user", "hello \"world\"").expect("some");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert!(parsed.is_array());
        assert_eq!(parsed[0]["role"], "user");
        assert_eq!(parsed[0]["content"], "hello \"world\"");
    }

    /// Build a string attribute, for the indexed-rebuild tests below.
    fn indexed_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key_strindex: 0,
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
        }
    }

    /// Parse a rebuilt array back out, so assertions read against structure
    /// rather than against an exact serialization.
    fn rebuild(attrs: &[KeyValue], prefix: &str) -> serde_json::Value {
        let json = serialize_indexed_attrs_to_json_array(attrs, prefix).expect("prefix is present");
        serde_json::from_str(&json).expect("valid json")
    }

    #[test]
    fn rebuilds_indexed_messages_into_the_role_content_array_shape() {
        let attrs = vec![
            indexed_kv("llm.input_messages.0.message.role", "system"),
            indexed_kv("llm.input_messages.0.message.content", "You are helpful"),
            indexed_kv("llm.input_messages.1.message.role", "user"),
            indexed_kv("llm.input_messages.1.message.content", "What is 2+2?"),
        ];
        let parsed = rebuild(&attrs, "llm.input_messages");
        assert_eq!(parsed.as_array().expect("array").len(), 2);
        assert_eq!(parsed[0]["role"], "system");
        assert_eq!(parsed[0]["content"], "You are helpful");
        assert_eq!(parsed[1]["role"], "user");
        assert_eq!(parsed[1]["content"], "What is 2+2?");
    }

    #[test]
    fn rebuilt_messages_drop_the_singular_wrapper_segment() {
        let attrs = vec![indexed_kv("llm.input_messages.0.message.role", "user")];
        let parsed = rebuild(&attrs, "llm.input_messages");
        assert!(
            parsed[0].get("message").is_none(),
            "the `message` wrapper carries nothing the position does not, and would break the shared array shape"
        );
    }

    #[test]
    fn rebuilds_indices_in_numeric_not_lexicographic_order() {
        // "10" sorts before "2" as text; a positional rebuild that trusted map
        // order would emit this conversation scrambled.
        let attrs = vec![
            indexed_kv("llm.input_messages.10.message.content", "eleventh"),
            indexed_kv("llm.input_messages.2.message.content", "third"),
            indexed_kv("llm.input_messages.0.message.content", "first"),
        ];
        let parsed = rebuild(&attrs, "llm.input_messages");
        let contents: Vec<&str> = parsed
            .as_array()
            .expect("array")
            .iter()
            .map(|item| item["content"].as_str().expect("string"))
            .collect();
        assert_eq!(contents, vec!["first", "third", "eleventh"]);
    }

    #[test]
    fn rebuilds_nested_tool_calls_inside_a_message() {
        let attrs = vec![
            indexed_kv("llm.output_messages.0.message.role", "assistant"),
            indexed_kv("llm.output_messages.0.message.tool_calls.0.tool_call.id", "call_abc"),
            indexed_kv(
                "llm.output_messages.0.message.tool_calls.0.tool_call.function.name",
                "final_answer",
            ),
            indexed_kv(
                "llm.output_messages.0.message.tool_calls.0.tool_call.function.arguments",
                r#"{"answer": 42}"#,
            ),
        ];
        let parsed = rebuild(&attrs, "llm.output_messages");
        assert_eq!(parsed[0]["role"], "assistant");
        let calls = parsed[0]["tool_calls"].as_array().expect("tool_calls is an array");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["id"], "call_abc");
        assert_eq!(calls[0]["function"]["name"], "final_answer");
        assert_eq!(calls[0]["function"]["arguments"], r#"{"answer": 42}"#);
    }

    #[test]
    fn rebuilds_a_gap_in_the_indices_without_inventing_an_element() {
        // Index 1 is missing entirely; the result must be two elements, not
        // three with a null in the middle.
        let attrs = vec![
            indexed_kv("llm.input_messages.0.message.content", "first"),
            indexed_kv("llm.input_messages.2.message.content", "third"),
        ];
        let parsed = rebuild(&attrs, "llm.input_messages");
        assert_eq!(parsed.as_array().expect("array").len(), 2);
        assert_eq!(parsed[0]["content"], "first");
        assert_eq!(parsed[1]["content"], "third");
    }

    #[test]
    fn rebuild_ignores_a_sibling_attribute_sharing_the_prefix() {
        let attrs = vec![
            indexed_kv("llm.input_messages.0.message.content", "hello"),
            indexed_kv("llm.input_messages_count", "1"),
        ];
        let parsed = rebuild(&attrs, "llm.input_messages");
        assert_eq!(parsed.as_array().expect("array").len(), 1);
        assert_eq!(parsed[0]["content"], "hello");
    }

    #[test]
    fn rebuild_ignores_a_prefixed_attribute_that_is_not_indexed() {
        // `llm.input_messages.summary` shares the prefix but names no element.
        let attrs = vec![indexed_kv("llm.input_messages.summary", "two messages")];
        assert!(serialize_indexed_attrs_to_json_array(&attrs, "llm.input_messages").is_none());
    }

    #[test]
    fn rebuild_returns_none_when_the_prefix_is_absent() {
        let attrs = vec![indexed_kv("gen_ai.input.messages", "[]")];
        assert!(serialize_indexed_attrs_to_json_array(&attrs, "llm.input_messages").is_none());
    }

    #[test]
    fn rebuilds_a_single_valued_element_such_as_a_tool_schema() {
        let attrs = vec![
            indexed_kv("llm.tools.0.tool.json_schema", r#"{"name":"web_search"}"#),
            indexed_kv("llm.tools.1.tool.json_schema", r#"{"name":"final_answer"}"#),
        ];
        let parsed = rebuild(&attrs, "llm.tools");
        assert_eq!(parsed.as_array().expect("array").len(), 2);
        assert_eq!(parsed[0]["json_schema"], r#"{"name":"web_search"}"#);
        assert_eq!(parsed[1]["json_schema"], r#"{"name":"final_answer"}"#);
    }
}
