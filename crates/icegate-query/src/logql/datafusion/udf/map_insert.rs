//! Map insertion UDF for adding/updating key-value pairs in maps.

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, MapArray, StringArray, StringBuilder},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::DataType,
    },
    common::{DataFusionError, Result, exec_err, plan_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};

/// UDF: `map_insert(map, key, value)` - inserts a key-value pair into a map.
///
/// # Arguments
/// - `map`: A `Map<String, String>` column
/// - `key`: A `String` - the key to insert
/// - `value`: A `String` - the value to insert
///
/// # Returns
/// A new map with the key-value pair inserted. If the key already exists, the
/// value is updated.
///
/// # Example
/// ```sql
/// SELECT map_insert(attributes, '__error__', 'true')
/// -- Inserts or updates the __error__ key with value 'true'
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapInsert {
    signature: Signature,
}

impl Default for MapInsert {
    fn default() -> Self {
        Self::new()
    }
}

impl MapInsert {
    /// Creates a new `MapInsert` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapInsert {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "map_insert"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return plan_err!("map_insert requires three arguments");
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return exec_err!("map_insert requires three arguments: map, key, value");
        }

        let map_arg = &args.args[0];
        let key_arg = &args.args[1];
        let value_arg = &args.args[2];

        // Extract scalar key and value
        let insert_key = match key_arg {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(None)) => {
                return exec_err!("map_insert key cannot be NULL");
            }
            _ => return exec_err!("map_insert key must be a string scalar"),
        };

        let insert_value = match value_arg {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(None)) => {
                return exec_err!("map_insert value cannot be NULL");
            }
            _ => return exec_err!("map_insert value must be a string scalar"),
        };

        if let ColumnarValue::Array(array) = map_arg {
            let map_array = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| DataFusionError::Execution("Expected MapArray".to_string()))?;

            let result = insert_key_into_map(map_array, &insert_key, &insert_value)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        } else {
            exec_err!("map_insert requires map to be an array (batch)")
        }
    }
}

/// Helper function to insert a key-value pair into each map in the array
fn insert_key_into_map(map_array: &MapArray, insert_key: &str, insert_value: &str) -> Result<MapArray> {
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

        let mut key_found = false;

        // Copy existing entries, updating if key matches
        for entry_idx in start..end {
            if keys.is_null(entry_idx) {
                continue;
            }

            let key = keys.value(entry_idx);

            new_keys.append_value(key);
            if key == insert_key {
                // Update existing key
                new_values.append_value(insert_value);
                key_found = true;
            } else {
                // Copy existing entry
                if values.is_null(entry_idx) {
                    new_values.append_null();
                } else {
                    new_values.append_value(values.value(entry_idx));
                }
            }
            current_offset += 1;
        }

        // If key not found, append it
        if !key_found {
            new_keys.append_value(insert_key);
            new_values.append_value(insert_value);
            current_offset += 1;
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
