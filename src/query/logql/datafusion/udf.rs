//! User-defined scalar functions for LogQL operations in DataFusion.
//!
//! This module provides UDFs for LogQL-specific operations, particularly
//! map filtering functions needed for `keep`, `drop`, `by`, and `without`
//! operations.

use std::{any::Any, collections::HashSet, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, GenericListArray, ListArray, MapArray, StringArray, StringBuilder},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::DataType,
    },
    common::{plan_err, Result},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};

/// UDF: `map_keep_keys(map, keys_array)` - keeps only keys present in array.
///
/// # Arguments
/// - `map`: A `Map<String, String>` column
/// - `keys_array`: An `Array<String>` of keys to keep
///
/// # Returns
/// A new map containing only the key-value pairs where the key is in
/// `keys_array`.
///
/// # Example
/// ```sql
/// SELECT map_keep_keys(attributes, ARRAY['level', 'service']) FROM logs
/// -- {level: "info", service: "api", method: "GET"} → {level: "info", service: "api"}
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
            signature: Signature::any(2, Volatility::Immutable),
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
        // Return same map type as input
        if arg_types.is_empty() {
            return plan_err!("map_keep_keys requires at least one argument");
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        filter_map_keys(&args.args, true) // keep=true
    }
}

/// UDF: `map_drop_keys(map, keys_array)` - removes keys present in array.
///
/// # Arguments
/// - `map`: A `Map<String, String>` column
/// - `keys_array`: An `Array<String>` of keys to drop
///
/// # Returns
/// A new map with the key-value pairs removed where the key is in `keys_array`.
///
/// # Example
/// ```sql
/// SELECT map_drop_keys(attributes, ARRAY['method']) FROM logs
/// -- {level: "info", service: "api", method: "GET"} → {level: "info", service: "api"}
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
            signature: Signature::any(2, Volatility::Immutable),
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
        if arg_types.is_empty() {
            return plan_err!("map_drop_keys requires at least one argument");
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        filter_map_keys(&args.args, false) // keep=false (drop mode)
    }
}

/// Core implementation for map key filtering.
///
/// # Arguments
/// - `args`: Two `ColumnarValue`s: the map and the keys array
/// - `keep_mode`: If true, keep only keys in array; if false, drop keys in
///   array
///
/// # Returns
/// A new `MapArray` with filtered entries.
fn filter_map_keys(args: &[ColumnarValue], keep_mode: bool) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return plan_err!(
            "map_{}_keys requires exactly 2 arguments, got {}",
            if keep_mode { "keep" } else { "drop" },
            args.len()
        );
    }

    let arrays = ColumnarValue::values_to_arrays(args)?;
    let map_array = arrays[0]
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| datafusion::error::DataFusionError::Plan("First argument must be a map".to_string()))?;

    // Extract filter keys from the second argument (could be ListArray or
    // StringArray)
    let filter_keys = extract_filter_keys(&arrays[1])?;

    // Build HashSet for O(1) lookup
    let filter_set: HashSet<&str> = filter_keys.iter().map(String::as_str).collect();

    // Process each row and build new MapArray
    let result = build_filtered_map(map_array, &filter_set, keep_mode)?;

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

/// Build a new `MapArray` with filtered entries, preserving original schema
/// metadata.
fn build_filtered_map(map_array: &MapArray, filter_set: &HashSet<&str>, keep_mode: bool) -> Result<MapArray> {
    let offsets = map_array.offsets();
    let keys = map_array
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| datafusion::error::DataFusionError::Plan("Map keys must be strings".to_string()))?;
    let values = map_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| datafusion::error::DataFusionError::Plan("Map values must be strings".to_string()))?;

    // Extract the original entries field from the map's data type to preserve
    // metadata
    let original_entries_field = map_array.entries().fields().clone();

    // Build new arrays for the filtered map
    let mut new_keys = StringBuilder::new();
    let mut new_values = StringBuilder::new();
    let mut new_offsets: Vec<i32> = Vec::with_capacity(map_array.len() + 1);
    new_offsets.push(0);

    let mut current_offset: i32 = 0;

    for row_idx in 0..map_array.len() {
        if map_array.is_null(row_idx) {
            // Null row - keep offset the same
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
            let should_include = if keep_mode { filter_set.contains(key) } else { !filter_set.contains(key) };

            if should_include {
                new_keys.append_value(key);
                if values.is_null(entry_idx) {
                    new_values.append_null();
                } else {
                    new_values.append_value(values.value(entry_idx));
                }
                current_offset += 1;
            }
        }

        new_offsets.push(current_offset);
    }

    // Build the struct array for map entries using the original field definitions
    let new_keys_array: ArrayRef = Arc::new(new_keys.finish());
    let new_values_array: ArrayRef = Arc::new(new_values.finish());

    let struct_array = datafusion::arrow::array::StructArray::try_new(
        original_entries_field,
        vec![new_keys_array, new_values_array],
        None,
    )?;

    // Create the MapArray with the original entries field (preserving metadata)
    let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(new_offsets));

    // Handle nulls from original map
    let null_buffer = if map_array.null_count() > 0 { map_array.nulls().cloned() } else { None };

    // Get the original map field which contains the field name and metadata
    let original_map_field = match map_array.data_type() {
        DataType::Map(field, _) => field.clone(),
        _ => {
            return Err(datafusion::error::DataFusionError::Plan(
                "Expected Map data type".to_string(),
            ))
        },
    };

    Ok(MapArray::new(
        original_map_field,
        offset_buffer,
        struct_array,
        null_buffer,
        false,
    ))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{Field, Fields};

    use super::*;

    /// Helper to create a simple `MapArray` for testing.
    fn create_test_map(entries: &[Vec<(&str, &str)>]) -> MapArray {
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

    /// Helper to create a `ListArray` of filter keys.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn create_filter_keys(keys: &[&str]) -> ListArray {
        let string_array = StringArray::from(keys.to_vec());
        let values: ArrayRef = Arc::new(string_array);

        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, keys.len() as i32]));

        ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            offsets,
            values,
            None,
        )
    }

    #[test]
    fn test_map_keep_keys_basic() {
        let map = create_test_map(&[vec![("level", "info"), ("service", "api"), ("method", "GET")]]);

        let _filter = create_filter_keys(&["level", "service"]);
        let filter_set: HashSet<&str> = ["level", "service"].into_iter().collect();

        let result = build_filtered_map(&map, &filter_set, true).unwrap();

        assert_eq!(result.len(), 1);
        let offsets = result.offsets();
        assert_eq!(offsets[1] - offsets[0], 2); // 2 entries kept
    }

    #[test]
    fn test_map_drop_keys_basic() {
        let map = create_test_map(&[vec![("level", "info"), ("service", "api"), ("method", "GET")]]);

        let filter_set: HashSet<&str> = std::iter::once("method").collect();

        let result = build_filtered_map(&map, &filter_set, false).unwrap();

        assert_eq!(result.len(), 1);
        let offsets = result.offsets();
        assert_eq!(offsets[1] - offsets[0], 2); // 2 entries remain (level,
                                                // service)
    }

    #[test]
    fn test_map_filter_empty_filter() {
        let map = create_test_map(&[vec![("level", "info"), ("service", "api")]]);

        let filter_set: HashSet<&str> = HashSet::new();

        // keep mode with empty filter = keep nothing
        let result_keep = build_filtered_map(&map, &filter_set, true).unwrap();
        let offsets = result_keep.offsets();
        assert_eq!(offsets[1] - offsets[0], 0);

        // drop mode with empty filter = drop nothing (keep all)
        let result_drop = build_filtered_map(&map, &filter_set, false).unwrap();
        let offsets = result_drop.offsets();
        assert_eq!(offsets[1] - offsets[0], 2);
    }

    #[test]
    fn test_map_filter_multiple_rows() {
        let map = create_test_map(
            &[vec![("a", "1"), ("b", "2"), ("c", "3")], vec![("a", "4"), ("d", "5")], vec![(
                "b", "6",
            )]],
        );

        let filter_set: HashSet<&str> = ["a", "b"].into_iter().collect();

        let result = build_filtered_map(&map, &filter_set, true).unwrap();

        assert_eq!(result.len(), 3);
        let offsets = result.offsets();
        assert_eq!(offsets[1] - offsets[0], 2); // row 0: a, b
        assert_eq!(offsets[2] - offsets[1], 1); // row 1: a
        assert_eq!(offsets[3] - offsets[2], 1); // row 2: b
    }
}
