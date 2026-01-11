//! Map filtering UDFs for `LogQL` keep/drop/by/without operations.

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, GenericListArray, ListArray, MapArray, StringArray, StringBuilder},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::DataType,
    },
    common::{DataFusionError, Result, exec_err, plan_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};

/// Helper function to create signature for map filter UDFs.
fn create_map_filter_signature() -> Signature {
    Signature::any(4, Volatility::Immutable)
}

/// Helper function to validate and return the map type for map filter UDFs.
fn map_filter_return_type(arg_types: &[DataType], udf_name: &str) -> Result<DataType> {
    if arg_types.is_empty() {
        return plan_err!("{} requires at least one argument", udf_name);
    }
    Ok(arg_types[0].clone())
}

/// UDF: `map_keep_keys(map, keys, values, ops)` - keeps only specified keys with optional matcher filtering.
///
/// # Arguments
/// - `map`: A `Map<String, String>` column
/// - `keys`: An `Array<String>` of keys to keep
/// - `values`: An `Array<String>` of values to match (NULL for simple name-based)
/// - `ops`: An `Array<String>` of operators (=, !=, =~, !~) (NULL for simple name-based)
///
/// # Returns
/// A new map containing only the key-value pairs that match the filter criteria.
///
/// # Examples
/// ```sql
/// -- Simple name-based: keep only 'level' and 'service' keys
/// SELECT map_keep_keys(attributes, ARRAY['level', 'service'], ARRAY[NULL, NULL], ARRAY[NULL, NULL])
/// -- {level: "info", service: "api", method: "GET"} → {level: "info", service: "api"}
///
/// -- With matchers: keep 'level' and 'service' if service="api"
/// SELECT map_keep_keys(attributes, ARRAY['level', 'service'], ARRAY[NULL, 'api'], ARRAY[NULL, '='])
/// -- {level: "info", service: "api"} → {level: "info", service: "api"}
/// -- {level: "info", service: "web"} → {level: "info"}
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapKeepKeys {
    signature: Signature,
}

impl Default for MapKeepKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl MapKeepKeys {
    /// Creates a new `MapKeepKeys` UDF.
    pub fn new() -> Self {
        Self {
            signature: create_map_filter_signature(),
        }
    }
}

impl ScalarUDFImpl for MapKeepKeys {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "map_keep_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        map_filter_return_type(arg_types, self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        filter_map_keys(&args.args, true) // keep=true
    }
}

/// UDF: `map_drop_keys(map, keys, values, ops)` - removes specified keys with optional matcher filtering.
///
/// # Arguments
/// - `map`: A `Map<String, String>` column
/// - `keys`: An `Array<String>` of keys to drop
/// - `values`: An `Array<String>` of values to match (NULL for simple name-based)
/// - `ops`: An `Array<String>` of operators (=, !=, =~, !~) (NULL for simple name-based)
///
/// # Returns
/// A new map with the key-value pairs removed that match the filter criteria.
///
/// # Examples
/// ```sql
/// -- Simple name-based: drop 'method' key unconditionally
/// SELECT map_drop_keys(attributes, ARRAY['method'], ARRAY[NULL], ARRAY[NULL])
/// -- {level: "info", service: "api", method: "GET"} → {level: "info", service: "api"}
///
/// -- With matchers: drop 'level' only when value is "debug"
/// SELECT map_drop_keys(attributes, ARRAY['level'], ARRAY['debug'], ARRAY['='])
/// -- {level: "debug", service: "api"} → {service: "api"}
/// -- {level: "info", service: "api"} → {level: "info", service: "api"}
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapDropKeys {
    signature: Signature,
}

impl Default for MapDropKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl MapDropKeys {
    /// Creates a new `MapDropKeys` UDF.
    pub fn new() -> Self {
        Self {
            signature: create_map_filter_signature(),
        }
    }
}

impl ScalarUDFImpl for MapDropKeys {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "map_drop_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        map_filter_return_type(arg_types, self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        filter_map_keys(&args.args, false) // keep=false (drop mode)
    }
}

/// Core implementation for map key filtering with matcher support.
///
/// # Arguments
/// - `args`: 4 `ColumnarValue`s: map, keys, values, ops
///   - `map`: Map<String, String> column
///   - `keys`: Array of key names
///   - `values`: Array of values (NULL for simple name-based matching)
///   - `ops`: Array of operators (NULL for simple name-based matching)
/// - `keep_mode`: If true, keep only keys in array; if false, drop keys in array
///
/// # Returns
/// A new `MapArray` with filtered entries.
fn filter_map_keys(args: &[ColumnarValue], keep_mode: bool) -> Result<ColumnarValue> {
    if args.len() != 4 {
        return plan_err!(
            "map_{}_keys requires exactly 4 arguments, got {}",
            if keep_mode { "keep" } else { "drop" },
            args.len()
        );
    }

    let arrays = ColumnarValue::values_to_arrays(args)?;
    let map_array = arrays[0]
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Execution("First argument must be a map".to_string()))?;

    // Extract filter keys, values, and ops
    let filter_keys = extract_filter_keys(&arrays[1])?;
    let filter_values = extract_filter_values(&arrays[2])?;
    let filter_ops = extract_filter_ops(&arrays[3])?;

    if filter_keys.len() != filter_values.len() || filter_keys.len() != filter_ops.len() {
        return plan_err!(
            "Keys, values, and ops arrays must have the same length: keys={}, values={}, ops={}",
            filter_keys.len(),
            filter_values.len(),
            filter_ops.len()
        );
    }

    // Build matcher specifications
    let matchers = build_matchers(&filter_keys, &filter_values, &filter_ops)?;

    // Process with conditional matching (handles both simple and matcher-based)
    let result = build_filtered_map_conditional(map_array, &matchers, keep_mode)?;
    Ok(ColumnarValue::from(Arc::new(result) as ArrayRef))
}

/// Extract filter keys from an array (handles both `ListArray<String>` and
/// `StringArray`).
fn extract_filter_keys(array: &ArrayRef) -> Result<Vec<String>> {
    // Try ListArray first (from make_array())
    if let Some(list_array) = array.as_any().downcast_ref::<ListArray>() {
        let values = list_array.values();
        if let Some(string_array) = values.as_any().downcast_ref::<StringArray>() {
            return Ok(string_array.iter().filter_map(|s| s.map(String::from)).collect());
        }
    }

    // Try GenericListArray<i64> (LargeList)
    if let Some(list_array) = array.as_any().downcast_ref::<GenericListArray<i64>>() {
        let values = list_array.values();
        if let Some(string_array) = values.as_any().downcast_ref::<StringArray>() {
            return Ok(string_array.iter().filter_map(|s| s.map(String::from)).collect());
        }
    }

    // Try direct StringArray
    if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(string_array.iter().filter_map(|s| s.map(String::from)).collect());
    }

    plan_err!("Second argument must be an array of strings")
}

/// Extract filter values from an array (handles NULL for simple name-based filtering).
fn extract_filter_values(array: &ArrayRef) -> Result<Vec<Option<String>>> {
    // Try ListArray first (from make_array())
    if let Some(list_array) = array.as_any().downcast_ref::<ListArray>() {
        let values = list_array.values();
        if let Some(string_array) = values.as_any().downcast_ref::<StringArray>() {
            return Ok(string_array.iter().map(|s| s.map(String::from)).collect());
        }
    }

    // Try GenericListArray<i64> (LargeList)
    if let Some(list_array) = array.as_any().downcast_ref::<GenericListArray<i64>>() {
        let values = list_array.values();
        if let Some(string_array) = values.as_any().downcast_ref::<StringArray>() {
            return Ok(string_array.iter().map(|s| s.map(String::from)).collect());
        }
    }

    plan_err!("Values argument must be an array of strings")
}

/// Extract filter operators from an array (handles NULL for simple name-based filtering).
fn extract_filter_ops(array: &ArrayRef) -> Result<Vec<Option<String>>> {
    // Try ListArray first (from make_array())
    if let Some(list_array) = array.as_any().downcast_ref::<ListArray>() {
        let values = list_array.values();
        if let Some(string_array) = values.as_any().downcast_ref::<StringArray>() {
            return Ok(string_array.iter().map(|s| s.map(String::from)).collect());
        }
    }

    // Try GenericListArray<i64> (LargeList)
    if let Some(list_array) = array.as_any().downcast_ref::<GenericListArray<i64>>() {
        let values = list_array.values();
        if let Some(string_array) = values.as_any().downcast_ref::<StringArray>() {
            return Ok(string_array.iter().map(|s| s.map(String::from)).collect());
        }
    }

    plan_err!("Ops argument must be an array of strings")
}

/// Matcher specification for conditional filtering.
#[derive(Debug, Clone)]
struct MatcherSpec {
    /// The key name.
    key: String,
    /// The value pattern (None for simple name-based matching).
    value: Option<String>,
    /// The operator (None for simple name-based matching).
    op: Option<String>,
    /// Pre-compiled regex for regex operators (None for non-regex ops).
    regex: Option<regex::Regex>,
}

/// Build matcher specifications from keys, values, and ops arrays.
///
/// Pre-compiles regex patterns once per UDF invocation for optimal performance.
///
/// # Errors
///
/// Returns an error if any matcher has inconsistent value/op state (one is `None` while the other is `Some`)
/// or if a regex pattern fails to compile.
fn build_matchers(keys: &[String], values: &[Option<String>], ops: &[Option<String>]) -> Result<Vec<MatcherSpec>> {
    let mut matchers = Vec::with_capacity(keys.len());

    for (idx, key) in keys.iter().enumerate() {
        let value = values[idx].clone();
        let op = ops[idx].clone();

        // Validate consistency: both None or both Some
        let value_is_none = value.is_none();
        let op_is_none = op.is_none();

        if value_is_none != op_is_none {
            return plan_err!(
                "Matcher validation failed for key '{}' at index {}: value and op must both be None (simple name) or both be Some (conditional). Found value={:?}, op={:?}",
                key,
                idx,
                value,
                op
            );
        }

        // Pre-compile regex patterns for regex operators
        let regex = if let (Some(pattern), Some(operator)) = (&value, &op) {
            if operator == "=~" || operator == "!~" {
                // Compile regex once here instead of on every evaluate_matcher call
                Some(regex::Regex::new(pattern).map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Invalid regex pattern '{pattern}' for key '{key}' at index {idx}: {e}"
                    ))
                })?)
            } else {
                None
            }
        } else {
            None
        };

        // Only push after validation succeeds
        matchers.push(MatcherSpec {
            key: key.clone(),
            value,
            op,
            regex,
        });
    }

    Ok(matchers)
}

/// Build a new `MapArray` with conditional matcher-based filtering.
fn build_filtered_map_conditional(map_array: &MapArray, matchers: &[MatcherSpec], keep_mode: bool) -> Result<MapArray> {
    let offsets = map_array.offsets();
    let keys = map_array
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("Map keys must be strings".to_string()))?;
    let values = map_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("Map values must be strings".to_string()))?;

    let original_entries_field = map_array.entries().fields().clone();

    // Build new arrays for the filtered map
    let mut new_keys = StringBuilder::new();
    let mut new_values = StringBuilder::new();
    let mut new_offsets: Vec<i32> = Vec::with_capacity(map_array.len() + 1);
    new_offsets.push(0);

    let mut current_offset: i32 = 0;

    for row_idx in 0..map_array.len() {
        if map_array.is_null(row_idx) {
            new_offsets.push(current_offset);
            continue;
        }

        #[allow(clippy::cast_sign_loss)]
        let start = offsets[row_idx] as usize;
        #[allow(clippy::cast_sign_loss)]
        let end = offsets[row_idx + 1] as usize;

        for entry_idx in start..end {
            if keys.is_null(entry_idx) {
                continue;
            }

            let key = keys.value(entry_idx);
            let value = if values.is_null(entry_idx) {
                None
            } else {
                Some(values.value(entry_idx))
            };

            // Find matcher for this key
            let matcher = matchers.iter().find(|m| m.key == key);

            let should_include = if let Some(matcher) = matcher {
                // Matcher found - evaluate condition
                if matcher.value.is_none() || matcher.op.is_none() {
                    // Simple name-based matching (no value/op specified)
                    keep_mode
                } else if let (Some(map_value), Some(matcher_value), Some(matcher_op)) =
                    (value, matcher.value.as_ref(), matcher.op.as_ref())
                {
                    // Conditional matching - evaluate the matcher with pre-compiled regex
                    let is_match = evaluate_matcher(map_value, matcher_value, matcher_op, matcher.regex.as_ref())?;

                    if keep_mode { is_match } else { !is_match }
                } else {
                    // Map value is NULL - skip matching
                    !keep_mode
                }
            } else {
                // Key not in matcher list
                !keep_mode
            };

            if should_include {
                new_keys.append_value(key);
                if let Some(v) = value {
                    new_values.append_value(v);
                } else {
                    new_values.append_null();
                }
                current_offset += 1;
            }
        }

        new_offsets.push(current_offset);
    }

    // Build the struct array for map entries
    let new_keys_array: ArrayRef = Arc::new(new_keys.finish());
    let new_values_array: ArrayRef = Arc::new(new_values.finish());

    let struct_array = datafusion::arrow::array::StructArray::try_new(
        original_entries_field,
        vec![new_keys_array, new_values_array],
        None,
    )?;

    let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(new_offsets));

    let null_buffer = if map_array.null_count() > 0 {
        map_array.nulls().cloned()
    } else {
        None
    };

    let original_map_field = match map_array.data_type() {
        DataType::Map(field, _) => field.clone(),
        _ => return exec_err!("Expected Map data type"),
    };

    Ok(MapArray::new(
        original_map_field,
        offset_buffer,
        struct_array,
        null_buffer,
        false,
    ))
}

/// Evaluate if a value matches a pattern using the specified operator.
///
/// Supports: `=` (equals), `!=` (not equals), `=~` (regex match), `!~` (regex not match)
///
/// # Arguments
///
/// * `value` - The value to match against
/// * `pattern` - The pattern to match
/// * `op` - The operator to use
/// * `pre_compiled_regex` - Optional pre-compiled regex for performance optimization
fn evaluate_matcher(value: &str, pattern: &str, op: &str, pre_compiled_regex: Option<&regex::Regex>) -> Result<bool> {
    match op {
        "=" => Ok(value == pattern),
        "!=" => Ok(value != pattern),
        "=~" => {
            // Regex match - use pre-compiled regex if available
            if let Some(re) = pre_compiled_regex {
                Ok(re.is_match(value))
            } else {
                // Fallback to compiling (should not happen if build_matchers works correctly)
                let re = regex::Regex::new(pattern)
                    .map_err(|e| DataFusionError::Execution(format!("Invalid regex pattern '{pattern}': {e}")))?;
                Ok(re.is_match(value))
            }
        }
        "!~" => {
            // Regex not match - use pre-compiled regex if available
            if let Some(re) = pre_compiled_regex {
                Ok(!re.is_match(value))
            } else {
                // Fallback to compiling (should not happen if build_matchers works correctly)
                let re = regex::Regex::new(pattern)
                    .map_err(|e| DataFusionError::Execution(format!("Invalid regex pattern '{pattern}': {e}")))?;
                Ok(!re.is_match(value))
            }
        }
        _ => exec_err!("Unknown matcher operator: {}", op),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Fields};

    use super::*;

    /// Helper to create a test map array for filtering tests.
    fn create_test_map_for_filter(entries: &[Vec<(&str, &str)>]) -> MapArray {
        let mut keys = StringBuilder::new();
        let mut values = StringBuilder::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut current_offset: i32 = 0;

        for row in entries {
            for (k, v) in row {
                keys.append_value(*k);
                values.append_value(*v);
                current_offset += 1;
            }
            offsets.push(current_offset);
        }

        let keys_array: ArrayRef = Arc::new(keys.finish());
        let values_array: ArrayRef = Arc::new(values.finish());

        let struct_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]);

        let entries_field = Arc::new(Field::new("entries", DataType::Struct(struct_fields.clone()), false));

        let struct_array =
            datafusion::arrow::array::StructArray::try_new(struct_fields, vec![keys_array, values_array], None)
                .unwrap();

        let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
        MapArray::new(entries_field, offset_buffer, struct_array, None, false)
    }

    /// Helper to create filter arrays for testing.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn create_filter_arrays(
        keys: &[&str],
        values: &[Option<&str>],
        ops: &[Option<&str>],
    ) -> (ListArray, ListArray, ListArray) {
        // Keys array
        let keys_string_array = StringArray::from(keys.to_vec());
        let keys_values: ArrayRef = Arc::new(keys_string_array);
        let keys_offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, keys.len() as i32]));
        let keys_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            keys_offsets,
            keys_values,
            None,
        );

        // Values array (with nulls for simple names)
        let values_string_array: StringArray = values.iter().copied().collect();
        let values_values: ArrayRef = Arc::new(values_string_array);
        let values_offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, values.len() as i32]));
        let values_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            values_offsets,
            values_values,
            None,
        );

        // Ops array (with nulls for simple names)
        let ops_string_array: StringArray = ops.iter().copied().collect();
        let ops_values: ArrayRef = Arc::new(ops_string_array);
        let ops_offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, ops.len() as i32]));
        let ops_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            ops_offsets,
            ops_values,
            None,
        );

        (keys_array, values_array, ops_array)
    }

    #[test]
    fn test_map_drop_keys_with_equals_matcher() {
        // Test: drop level="debug"
        // Input: {level: "debug", service: "api", method: "GET"}
        // Expected: {service: "api", method: "GET"}
        let map_array = create_test_map_for_filter(&[vec![("level", "debug"), ("service", "api"), ("method", "GET")]]);

        let (keys, values, ops) = create_filter_arrays(&["level"], &[Some("debug")], &[Some("=")]);

        let args = vec![
            ColumnarValue::Array(Arc::new(map_array)),
            ColumnarValue::Array(Arc::new(keys)),
            ColumnarValue::Array(Arc::new(values)),
            ColumnarValue::Array(Arc::new(ops)),
        ];

        let result = filter_map_keys(&args, false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_map = result_array.as_any().downcast_ref::<MapArray>().unwrap();
            let keys_array = result_map.keys().as_any().downcast_ref::<StringArray>().unwrap();

            // Should have 2 keys: service, method
            assert_eq!(keys_array.len(), 2);
            assert!(keys_array.iter().any(|k| k == Some("service")));
            assert!(keys_array.iter().any(|k| k == Some("method")));
            assert!(!keys_array.iter().any(|k| k == Some("level")));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_map_drop_keys_with_not_equals_matcher() {
        // Test: drop level!="debug"
        // Input: {level: "info", service: "api"}
        // Expected: {service: "api"} (level removed because it's "info" != "debug")
        let map_array = create_test_map_for_filter(&[vec![("level", "info"), ("service", "api")]]);

        let (keys, values, ops) = create_filter_arrays(&["level"], &[Some("debug")], &[Some("!=")]);

        let args = vec![
            ColumnarValue::Array(Arc::new(map_array)),
            ColumnarValue::Array(Arc::new(keys)),
            ColumnarValue::Array(Arc::new(values)),
            ColumnarValue::Array(Arc::new(ops)),
        ];

        let result = filter_map_keys(&args, false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_map = result_array.as_any().downcast_ref::<MapArray>().unwrap();
            let keys_array = result_map.keys().as_any().downcast_ref::<StringArray>().unwrap();

            // Should have 1 key: service
            assert_eq!(keys_array.len(), 1);
            assert!(keys_array.iter().any(|k| k == Some("service")));
            assert!(!keys_array.iter().any(|k| k == Some("level")));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_map_drop_keys_with_regex_matcher() {
        // Test: drop level=~"debug|info"
        // Input: {level: "debug", service: "api"}
        // Expected: {service: "api"}
        let map_array = create_test_map_for_filter(&[vec![("level", "debug"), ("service", "api")]]);

        let (keys, values, ops) = create_filter_arrays(&["level"], &[Some("debug|info")], &[Some("=~")]);

        let args = vec![
            ColumnarValue::Array(Arc::new(map_array)),
            ColumnarValue::Array(Arc::new(keys)),
            ColumnarValue::Array(Arc::new(values)),
            ColumnarValue::Array(Arc::new(ops)),
        ];

        let result = filter_map_keys(&args, false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_map = result_array.as_any().downcast_ref::<MapArray>().unwrap();
            let keys_array = result_map.keys().as_any().downcast_ref::<StringArray>().unwrap();

            assert_eq!(keys_array.len(), 1);
            assert!(keys_array.iter().any(|k| k == Some("service")));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_map_drop_keys_with_negative_regex_matcher() {
        // Test: drop level!~"debug|info"
        // Input: {level: "error", service: "api", method: "GET"}
        // Expected: {service: "api", method: "GET"} (level removed because "error" does not match "debug|info")
        let map_array = create_test_map_for_filter(&[vec![("level", "error"), ("service", "api"), ("method", "GET")]]);

        let (keys, values, ops) = create_filter_arrays(&["level"], &[Some("debug|info")], &[Some("!~")]);

        let args = vec![
            ColumnarValue::Array(Arc::new(map_array)),
            ColumnarValue::Array(Arc::new(keys)),
            ColumnarValue::Array(Arc::new(values)),
            ColumnarValue::Array(Arc::new(ops)),
        ];

        let result = filter_map_keys(&args, false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_map = result_array.as_any().downcast_ref::<MapArray>().unwrap();
            let keys_array = result_map.keys().as_any().downcast_ref::<StringArray>().unwrap();

            // Should have 2 keys: service, method (level dropped because it doesn't match the pattern)
            assert_eq!(keys_array.len(), 2);
            assert!(keys_array.iter().any(|k| k == Some("service")));
            assert!(keys_array.iter().any(|k| k == Some("method")));
            assert!(!keys_array.iter().any(|k| k == Some("level")));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_map_drop_keys_mixed_simple_and_matchers() {
        // Test: drop method, level="debug"
        // Input: {level: "debug", service: "api", method: "GET"}
        // Expected: {service: "api"}
        let map_array = create_test_map_for_filter(&[vec![("level", "debug"), ("service", "api"), ("method", "GET")]]);

        let (keys, values, ops) =
            create_filter_arrays(&["method", "level"], &[None, Some("debug")], &[None, Some("=")]);

        let args = vec![
            ColumnarValue::Array(Arc::new(map_array)),
            ColumnarValue::Array(Arc::new(keys)),
            ColumnarValue::Array(Arc::new(values)),
            ColumnarValue::Array(Arc::new(ops)),
        ];

        let result = filter_map_keys(&args, false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_map = result_array.as_any().downcast_ref::<MapArray>().unwrap();
            let keys_array = result_map.keys().as_any().downcast_ref::<StringArray>().unwrap();

            // Should have 1 key: service
            assert_eq!(keys_array.len(), 1);
            assert!(keys_array.iter().any(|k| k == Some("service")));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_map_keep_keys_with_equals_matcher() {
        // Test: keep level="info"
        // Input: {level: "info", service: "api", method: "GET"}
        // Expected: {level: "info"}
        let map_array = create_test_map_for_filter(&[vec![("level", "info"), ("service", "api"), ("method", "GET")]]);

        let (keys, values, ops) = create_filter_arrays(&["level"], &[Some("info")], &[Some("=")]);

        let args = vec![
            ColumnarValue::Array(Arc::new(map_array)),
            ColumnarValue::Array(Arc::new(keys)),
            ColumnarValue::Array(Arc::new(values)),
            ColumnarValue::Array(Arc::new(ops)),
        ];

        let result = filter_map_keys(&args, true).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_map = result_array.as_any().downcast_ref::<MapArray>().unwrap();
            let keys_array = result_map.keys().as_any().downcast_ref::<StringArray>().unwrap();

            // Should have 1 key: level
            assert_eq!(keys_array.len(), 1);
            assert!(keys_array.iter().any(|k| k == Some("level")));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_map_keep_keys_with_negative_regex_matcher() {
        // Test: keep level!~"debug|info"
        // Input: {level: "error", service: "api", method: "GET"}
        // Expected: {level: "error"} (level kept because "error" does not match "debug|info")
        let map_array = create_test_map_for_filter(&[vec![("level", "error"), ("service", "api"), ("method", "GET")]]);

        let (keys, values, ops) = create_filter_arrays(&["level"], &[Some("debug|info")], &[Some("!~")]);

        let args = vec![
            ColumnarValue::Array(Arc::new(map_array)),
            ColumnarValue::Array(Arc::new(keys)),
            ColumnarValue::Array(Arc::new(values)),
            ColumnarValue::Array(Arc::new(ops)),
        ];

        let result = filter_map_keys(&args, true).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_map = result_array.as_any().downcast_ref::<MapArray>().unwrap();
            let keys_array = result_map.keys().as_any().downcast_ref::<StringArray>().unwrap();

            // Should have 1 key: level (kept because it doesn't match the pattern)
            assert_eq!(keys_array.len(), 1);
            assert!(keys_array.iter().any(|k| k == Some("level")));
            assert!(!keys_array.iter().any(|k| k == Some("service")));
            assert!(!keys_array.iter().any(|k| k == Some("method")));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_map_keep_keys_mixed_simple_and_matchers() {
        // Test: keep level, service="api"
        // Input: {level: "info", service: "api", method: "GET"}
        // Expected: {level: "info", service: "api"}
        let map_array = create_test_map_for_filter(&[vec![("level", "info"), ("service", "api"), ("method", "GET")]]);

        let (keys, values, ops) = create_filter_arrays(&["level", "service"], &[None, Some("api")], &[None, Some("=")]);

        let args = vec![
            ColumnarValue::Array(Arc::new(map_array)),
            ColumnarValue::Array(Arc::new(keys)),
            ColumnarValue::Array(Arc::new(values)),
            ColumnarValue::Array(Arc::new(ops)),
        ];

        let result = filter_map_keys(&args, true).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_map = result_array.as_any().downcast_ref::<MapArray>().unwrap();
            let keys_array = result_map.keys().as_any().downcast_ref::<StringArray>().unwrap();

            // Should have 2 keys: level, service
            assert_eq!(keys_array.len(), 2);
            assert!(keys_array.iter().any(|k| k == Some("level")));
            assert!(keys_array.iter().any(|k| k == Some("service")));
        } else {
            panic!("Expected array result");
        }
    }
}
