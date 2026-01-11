//! User-defined scalar functions for LogQL operations in DataFusion.
//!
//! This module provides UDFs for LogQL-specific operations, particularly
//! map filtering functions needed for `keep`, `drop`, `by`, and `without`
//! operations.

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, GenericListArray, ListArray, MapArray, StringArray, StringBuilder},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::DataType,
    },
    common::{DataFusionError, Result, datatype::DataTypeExt, exec_err, plan_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};

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
            signature: Signature::any(4, Volatility::Immutable),
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
            signature: Signature::any(4, Volatility::Immutable),
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

/// UDF: `date_grid(timestamp, start, end, step, range, offset, inverse)` - calculate step timestamps of
/// aligning grid
///
/// # Arguments
/// - `timestamp`: A `Timestamp` column
/// - `start`: Grid start timestamp
/// - `end`: Grid end timestamp
/// - `step`: Step between grid points
/// - `range`: Range window size as interval
/// - `offset`: Offset for the range window as interval
/// - `inverse`: Boolean - if true, returns grid points NOT covered by the timestamp
///
/// # References
/// - [Loki Metric Query](https://grafana.com/blog/how-to-run-faster-loki-metric-queries-with-more-accurate-results/)
///
/// # Returns
/// A new array with the date bins containing (or not containing, if inverse=true) the timestamp point
///
/// # Example
/// ```sql
/// SELECT timestamp FROM t;
/// -- TIMESTAMP '2025-01-02 00:10:30'
/// -- TIMESTAMP '2025-01-02 00:13:59'
/// -- Normal mode (inverse=false): returns grid points covered by timestamp
/// SELECT date_grid(
///     timestamp,
///     TIMESTAMP '2025-01-02 00:00:00',
///     TIMESTAMP '2025-01-03 00:00:00',
///     INTERVAL '1 minute',
///     INTERVAL '2 minute',
///     INTERVAL '10 seconds',
///     false
/// ) FROM logs;
/// -- [TIMESTAMP '2025-01-02 00:11:00', '2025-01-02 00:12:00']
///
/// -- Inverse mode (inverse=true): returns grid points NOT covered by timestamp
/// SELECT date_grid(
///     timestamp,
///     TIMESTAMP '2025-01-02 00:00:00',
///     TIMESTAMP '2025-01-03 00:05:00',
///     INTERVAL '1 minute',
///     INTERVAL '2 minute',
///     INTERVAL '10 seconds',
///     true
/// ) FROM logs;
/// -- [TIMESTAMP '2025-01-02 00:00:00', '2025-01-02 00:01:00', ..., '2025-01-02 00:10:00', '2025-01-02 00:13:00', ...]
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DateGrid {
    signature: Signature,
}

impl Default for DateGrid {
    fn default() -> Self {
        Self::new()
    }
}

impl DateGrid {
    /// Creates a new `Grid` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(7, Volatility::Immutable),
        }
    }

    /// Extracts interval as microseconds from a `ColumnarValue`.
    fn extract_interval_micros(arg: &ColumnarValue, param_name: &str) -> Result<i64> {
        use datafusion::{arrow::datatypes::IntervalMonthDayNano, common::ScalarValue};

        if let ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(interval))) = arg {
            let IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            } = *interval;

            if months != 0 {
                return plan_err!("Month intervals are not supported, use days or smaller units");
            }

            let day_micros = i64::from(days) * 86_400_000_000;
            let nano_micros = nanoseconds / 1000;

            Ok(day_micros + nano_micros)
        } else {
            plan_err!("{param_name} must be an IntervalMonthDayNano scalar")
        }
    }
}

impl ScalarUDFImpl for DateGrid {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "date_grid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 7 {
            return plan_err!("date_grid requires 7 arguments");
        }
        Ok(DataType::List(arg_types[0].clone().into_nullable_field_ref()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use datafusion::{
            arrow::{
                array::{Array, ListArray, TimestampMicrosecondArray},
                buffer::{OffsetBuffer, ScalarBuffer},
                datatypes::TimeUnit,
            },
            common::ScalarValue,
        };

        // Validate argument count
        if args.args.len() != 7 {
            return plan_err!("date_grid requires 7 arguments");
        }

        // Extract arguments
        let timestamp_array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(|| {
                DataFusionError::Execution("First argument must be TimestampMicrosecond array".to_string())
            })?,
            ColumnarValue::Scalar(_) => {
                return plan_err!("First argument must be an array, not a scalar");
            }
        };

        // Extract scalar parameters
        let start_micros = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(v), _)) => *v,
            _ => return plan_err!("Second argument (start) must be a TimestampMicrosecond scalar"),
        };

        let end_micros = match &args.args[2] {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(v), _)) => *v,
            _ => return plan_err!("Third argument (end) must be a TimestampMicrosecond scalar"),
        };

        // Extract interval parameters and convert to microseconds
        let step_micros = Self::extract_interval_micros(&args.args[3], "step")?;
        let range_micros = Self::extract_interval_micros(&args.args[4], "range")?;
        let offset_micros = Self::extract_interval_micros(&args.args[5], "offset")?;

        // Extract inverse parameter
        let inverse = match &args.args[6] {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(val))) => *val,
            _ => return plan_err!("Seventh argument (inverse) must be a Boolean scalar"),
        };

        // Validate step is positive
        if step_micros <= 0 {
            return plan_err!("step must be positive");
        }

        // Generate grid timestamps
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let num_points = ((end_micros - start_micros) / step_micros + 1) as usize;
        let grid: Vec<i64> = (0..num_points)
            .map(|i| {
                #[allow(clippy::cast_possible_wrap)]
                let idx = i as i64;
                start_micros + idx * step_micros
            })
            .collect();

        let upper_bound_micros = range_micros + offset_micros;

        // Process each timestamp and collect matching grid points
        let mut list_builder: Vec<Vec<i64>> = Vec::with_capacity(timestamp_array.len());

        for i in 0..timestamp_array.len() {
            if timestamp_array.is_null(i) {
                list_builder.push(Vec::new());
            } else {
                let t = timestamp_array.value(i);

                // Grid point g matches if: t + offset <= g <= t + upper_bound
                let lower_grid = t + offset_micros;
                let upper_grid = t + upper_bound_micros;

                // Collect matching grid points based on inverse parameter
                let matches: Vec<i64> = if inverse {
                    // Inverse mode: return grid points NOT in the coverage window
                    grid.iter().copied().filter(|&g| g < lower_grid || g > upper_grid).collect()
                } else {
                    // Normal mode: return grid points in the coverage window
                    // Binary search for first grid point >= lower_grid
                    let start_idx = grid.partition_point(|&g| g < lower_grid);
                    // Collect all matching grid points within the range
                    grid[start_idx..].iter().take_while(|&&g| g <= upper_grid).copied().collect()
                };

                list_builder.push(matches);
            }
        }

        // Build the ListArray result
        let mut offset = 0i32;
        let mut offsets_vec = vec![0i32];
        let mut values_vec = Vec::new();

        for matches in list_builder {
            let len = i32::try_from(matches.len())
                .map_err(|_| DataFusionError::Execution("ListArray overflow".to_string()))?;
            for timestamp in matches {
                values_vec.push(timestamp);
            }
            offset += len;
            offsets_vec.push(offset);
        }

        let values_array = Arc::new(TimestampMicrosecondArray::from(values_vec));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets_vec));

        let field = Arc::new(datafusion::arrow::datatypes::Field::new(
            "", // Empty name to match return_type promise
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true, // Nullable to match return_type promise
        ));

        let list_array = ListArray::new(field, offsets, values_array, None);

        Ok(ColumnarValue::Array(Arc::new(list_array)))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            array::{Float64Array, TimestampMicrosecondArray},
            datatypes::{Field, IntervalMonthDayNano, TimeUnit},
        },
        common::{DataFusionError, Result, ScalarValue, config::ConfigOptions},
    };

    use super::*;

    // DateGrid test helpers

    /// Helper to create `IntervalMonthDayNano` from microseconds.
    ///
    /// # Arguments
    /// - `micros`: Microseconds for the interval (negative values supported)
    ///
    /// # Notes
    /// - Only supports day and nanosecond components (months not supported per udaf.rs:379-382)
    fn create_interval_micros(micros: i64) -> ScalarValue {
        ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
            0,             // months (not supported)
            0,             // days
            micros * 1000, // convert micros to nanos
        )))
    }

    /// Helper to create a timestamp in microseconds from a datetime string.
    ///
    /// # Arguments
    /// - `datetime`: String in format "YYYY-MM-DD HH:MM:SS"
    ///
    /// # Example
    /// ```
    /// let ts = timestamp_micros("2025-01-02 00:10:30");
    /// // Returns: 1735776630000000 (microseconds since epoch)
    /// ```
    fn timestamp_micros(datetime: &str) -> i64 {
        use chrono::NaiveDateTime;

        let dt = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").expect("Invalid datetime format");

        dt.and_utc().timestamp() * 1_000_000 + i64::from(dt.and_utc().timestamp_subsec_micros())
    }

    /// Helper to create `TimestampMicrosecondArray` from datetime strings.
    fn create_timestamps(datetimes: &[&str]) -> TimestampMicrosecondArray {
        let micros: Vec<i64> = datetimes.iter().map(|dt| timestamp_micros(dt)).collect();
        TimestampMicrosecondArray::from(micros)
    }

    /// Helper to execute `date_grid` UDF and return the result as a vector of timestamp arrays.
    ///
    /// # Arguments
    /// - `timestamps`: Input timestamps to process
    /// - `start`: Grid start timestamp
    /// - `end`: Grid end timestamp
    /// - `step`: Step interval between grid points
    /// - `range`: Range window size
    /// - `offset`: Range window offset
    /// - `inverse`: If true, returns grid points NOT covered by timestamps
    ///
    /// # Returns
    /// `Vec` of `Vec<i64>` where each inner vec is the grid timestamps for one input timestamp
    fn execute_date_grid(
        timestamps: &TimestampMicrosecondArray,
        start: i64,
        end: i64,
        step: ScalarValue,
        range: ScalarValue,
        offset: ScalarValue,
        inverse: bool,
    ) -> Result<Vec<Vec<i64>>> {
        let udf = DateGrid::new();

        let timestamp_arg = ColumnarValue::Array(Arc::new(timestamps.clone()) as ArrayRef);
        let start_arg = ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(start), None));
        let end_arg = ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(end), None));
        let step_arg = ColumnarValue::Scalar(step);
        let range_arg = ColumnarValue::Scalar(range);
        let offset_arg = ColumnarValue::Scalar(offset);
        let inverse_arg = ColumnarValue::Scalar(ScalarValue::Boolean(Some(inverse)));

        let return_field = Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ))),
            false,
        ));

        let arg_fields = vec![
            Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )),
            Arc::new(Field::new(
                "start",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )),
            Arc::new(Field::new(
                "end",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )),
            Arc::new(Field::new(
                "step",
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
                false,
            )),
            Arc::new(Field::new(
                "range",
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
                false,
            )),
            Arc::new(Field::new(
                "offset",
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
                false,
            )),
            Arc::new(Field::new("inverse", DataType::Boolean, false)),
        ];

        let args = ScalarFunctionArgs {
            args: vec![
                timestamp_arg,
                start_arg,
                end_arg,
                step_arg,
                range_arg,
                offset_arg,
                inverse_arg,
            ],
            number_rows: timestamps.len(),
            return_field,
            arg_fields,
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args)?;

        // Extract ListArray from result
        match result {
            ColumnarValue::Array(arr) => {
                let list_array = arr
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| DataFusionError::Plan("Expected ListArray".to_string()))?;

                let mut output = Vec::new();
                for i in 0..list_array.len() {
                    if list_array.is_null(i) {
                        output.push(Vec::new());
                    } else {
                        let ts_array = list_array.value(i);
                        let ts_micros = ts_array
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .ok_or_else(|| DataFusionError::Plan("Expected TimestampMicrosecondArray".to_string()))?;

                        let timestamps_vec: Vec<i64> = (0..ts_micros.len()).map(|j| ts_micros.value(j)).collect();
                        output.push(timestamps_vec);
                    }
                }

                Ok(output)
            }
            ColumnarValue::Scalar(_) => Err(DataFusionError::Plan("Expected Array result".to_string())),
        }
    }

    // DateGrid UDF tests

    #[test]
    fn test_date_grid_basic_single_match() {
        // Test basic functionality: single timestamp matching one grid point
        //
        // Grid: 00:00, 00:01, 00:02, 00:03, 00:04, 00:05 (1-minute steps)
        // Timestamp: 00:00:30
        // Range: 1 minute, Offset: 0
        //
        // For t = 00:00:30:
        //   - Lower bound: 00:00:30 + 0 = 00:00:30
        //   - Upper bound: 00:00:30 + 1min = 00:01:30
        //   - Matches: 00:01:00 (only grid point in [00:00:30, 00:01:30])

        let timestamps = create_timestamps(&["2025-01-02 00:00:30"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:05:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(60_000_000); // 1 minute
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:01:00"));
    }

    #[test]
    fn test_date_grid_multiple_matches() {
        // Test timestamp matching multiple grid points
        //
        // Grid: 00:00, 00:01, 00:02, 00:03, 00:04, 00:05 (1-minute steps)
        // Timestamp: 00:00:00
        // Range: 3 minutes, Offset: 0
        //
        // For t = 00:00:00:
        //   - Lower bound: 00:00:00 + 0 = 00:00:00
        //   - Upper bound: 00:00:00 + 3min = 00:03:00
        //   - Matches: 00:00:00, 00:01:00, 00:02:00, 00:03:00

        let timestamps = create_timestamps(&["2025-01-02 00:00:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:05:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(180_000_000); // 3 minutes
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 4);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:00:00"));
        assert_eq!(result[0][1], timestamp_micros("2025-01-02 00:01:00"));
        assert_eq!(result[0][2], timestamp_micros("2025-01-02 00:02:00"));
        assert_eq!(result[0][3], timestamp_micros("2025-01-02 00:03:00"));
    }

    #[test]
    fn test_date_grid_with_offset() {
        // Test offset parameter shifts the range window
        //
        // Grid: 00:00, 00:01, 00:02, 00:03 (1-minute steps)
        // Timestamp: 00:00:00
        // Range: 1 minute, Offset: 30 seconds
        //
        // For t = 00:00:00:
        //   - Lower bound: 00:00:00 + 30s = 00:00:30
        //   - Upper bound: 00:00:00 + 30s + 1min = 00:01:30
        //   - Matches: 00:01:00 (only grid point in [00:00:30, 00:01:30])

        let timestamps = create_timestamps(&["2025-01-02 00:00:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(60_000_000); // 1 minute
        let offset = create_interval_micros(30_000_000); // 30 seconds

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:01:00"));
    }

    #[test]
    fn test_date_grid_negative_offset() {
        // Test negative offset shifts range window backward
        //
        // Grid: 00:00, 00:01, 00:02, 00:03 (1-minute steps)
        // Timestamp: 00:02:00
        // Range: 1 minute, Offset: -30 seconds
        //
        // For t = 00:02:00:
        //   - Lower bound: 00:02:00 - 30s = 00:01:30
        //   - Upper bound: 00:02:00 - 30s + 1min = 00:02:30
        //   - Matches: 00:02:00 (only grid point in [00:01:30, 00:02:30])

        let timestamps = create_timestamps(&["2025-01-02 00:02:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(60_000_000); // 1 minute
        let offset = create_interval_micros(-30_000_000); // -30 seconds

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:02:00"));
    }

    #[test]
    fn test_date_grid_no_matches() {
        // Test timestamp with no matching grid points (outside range window)
        //
        // Grid: 00:00, 00:01, 00:02, 00:03 (1-minute steps)
        // Timestamp: 00:10:00 (far after grid end)
        // Range: 1 minute, Offset: 0
        //
        // For t = 00:10:00:
        //   - Lower bound: 00:10:00 + 0 = 00:10:00
        //   - Upper bound: 00:10:00 + 1min = 00:11:00
        //   - Matches: none (grid ends at 00:03:00)

        let timestamps = create_timestamps(&["2025-01-02 00:10:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(60_000_000); // 1 minute
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 0); // Empty array
    }

    #[test]
    fn test_date_grid_timestamp_before_grid() {
        // Test timestamp before grid start
        //
        // Grid: 01:00, 01:01, 01:02, 01:03 (1-minute steps, starts at 01:00)
        // Timestamp: 00:59:30 (before grid start)
        // Range: 2 minutes, Offset: 0
        //
        // For t = 00:59:30:
        //   - Lower bound: 00:59:30 + 0 = 00:59:30
        //   - Upper bound: 00:59:30 + 2min = 01:01:30
        //   - Matches: 01:00:00, 01:01:00 (first two grid points)

        let timestamps = create_timestamps(&["2025-01-02 00:59:30"]);
        let start = timestamp_micros("2025-01-02 01:00:00");
        let end = timestamp_micros("2025-01-02 01:03:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(120_000_000); // 2 minutes
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 2);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 01:00:00"));
        assert_eq!(result[0][1], timestamp_micros("2025-01-02 01:01:00"));
    }

    #[test]
    fn test_date_grid_exactly_on_grid_point() {
        // Test timestamp exactly matching a grid point
        //
        // Grid: 00:00, 00:01, 00:02, 00:03 (1-minute steps)
        // Timestamp: 00:02:00 (exactly on grid point)
        // Range: 1 minute, Offset: 0
        //
        // For t = 00:02:00:
        //   - Lower bound: 00:02:00 + 0 = 00:02:00
        //   - Upper bound: 00:02:00 + 1min = 00:03:00
        //   - Matches: 00:02:00, 00:03:00 (inclusive boundaries)

        let timestamps = create_timestamps(&["2025-01-02 00:02:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(60_000_000); // 1 minute
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 2);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:02:00"));
        assert_eq!(result[0][1], timestamp_micros("2025-01-02 00:03:00"));
    }

    #[test]
    fn test_date_grid_batch_processing() {
        // Test multiple timestamps in a batch (common use case)
        //
        // Grid: 00:00, 00:05, 00:10, 00:15 (5-minute steps)
        // Timestamps: [00:01:00, 00:07:00, 00:14:00]
        // Range: 5 minutes, Offset: 0
        //
        // For t1 = 00:01:00: matches 00:05:00
        // For t2 = 00:07:00: matches 00:10:00
        // For t3 = 00:14:00: matches 00:15:00

        let timestamps = create_timestamps(&["2025-01-02 00:01:00", "2025-01-02 00:07:00", "2025-01-02 00:14:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:15:00");
        let step = create_interval_micros(300_000_000); // 5 minutes
        let range = create_interval_micros(300_000_000); // 5 minutes
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 3);

        // First timestamp matches 00:05:00
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:05:00"));

        // Second timestamp matches 00:10:00
        assert_eq!(result[1].len(), 1);
        assert_eq!(result[1][0], timestamp_micros("2025-01-02 00:10:00"));

        // Third timestamp matches 00:15:00
        assert_eq!(result[2].len(), 1);
        assert_eq!(result[2][0], timestamp_micros("2025-01-02 00:15:00"));
    }

    #[test]
    fn test_date_grid_subsecond_precision() {
        // Test subsecond (microsecond) precision in grid
        //
        // Grid: 00:00:00.000000, 00:00:00.100000, 00:00:00.200000 (100ms steps)
        // Timestamp: 00:00:00.050000
        // Range: 100ms, Offset: 0
        //
        // For t = 00:00:00.050000:
        //   - Lower bound: 00:00:00.050000
        //   - Upper bound: 00:00:00.150000
        //   - Matches: 00:00:00.100000

        let base = timestamp_micros("2025-01-02 00:00:00");
        let timestamps = TimestampMicrosecondArray::from(vec![base + 50_000]); // +50ms
        let start = base;
        let end = base + 300_000; // +300ms
        let step = create_interval_micros(100_000); // 100ms
        let range = create_interval_micros(100_000); // 100ms
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0][0], base + 100_000); // 00:00:00.100000
    }

    #[test]
    fn test_date_grid_large_grid() {
        // Test performance with large grid (1000+ points)
        //
        // Grid: 00:00 to 16:40 with 1-second steps (1000 points)
        // Timestamp: 08:20:00 (middle of grid)
        // Range: 10 seconds, Offset: 0
        //
        // Should efficiently find ~10 matching grid points

        let timestamps = create_timestamps(&["2025-01-02 08:20:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 16:40:00"); // 1000 minutes = 60000 seconds
        let step = create_interval_micros(1_000_000); // 1 second
        let range = create_interval_micros(10_000_000); // 10 seconds
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 11); // 08:20:00 through 08:20:10 (inclusive)

        // Verify first and last match
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 08:20:00"));
        assert_eq!(result[0][10], timestamp_micros("2025-01-02 08:20:10"));
    }

    #[test]
    fn test_date_grid_edge_case_range_equals_step() {
        // Test edge case: range equals step size
        //
        // Grid: 00:00, 00:01, 00:02, 00:03 (1-minute steps)
        // Timestamp: 00:00:30 (halfway between grid points)
        // Range: 1 minute (same as step), Offset: 0
        //
        // For t = 00:00:30:
        //   - Lower bound: 00:00:30
        //   - Upper bound: 00:01:30
        //   - Matches: 00:01:00 (only one grid point)

        let timestamps = create_timestamps(&["2025-01-02 00:00:30"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(60_000_000); // 1 minute (same as step)
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:01:00"));
    }

    #[test]
    fn test_date_grid_edge_case_zero_range() {
        // Test edge case: zero range (degenerate window)
        //
        // Grid: 00:00, 00:01, 00:02, 00:03 (1-minute steps)
        // Timestamp: 00:01:00 (exactly on grid)
        // Range: 0, Offset: 0
        //
        // For t = 00:01:00:
        //   - Lower bound: 00:01:00
        //   - Upper bound: 00:01:00
        //   - Matches: 00:01:00 (only exact point due to <= boundaries)

        let timestamps = create_timestamps(&["2025-01-02 00:01:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(0); // zero range
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:01:00"));
    }

    #[test]
    fn test_date_grid_range_less_than_step_no_match() {
        // Test edge case: range smaller than step, timestamp falls between grid points
        //
        // Grid: 00:00, 00:05, 00:10, 00:15 (5-minute steps)
        // Timestamp: 00:02:00 (between grid points)
        // Range: 1 minute (much less than 5-minute step), Offset: 0
        //
        // For t = 00:02:00:
        //   - Lower bound: 00:02:00 + 0 = 00:02:00
        //   - Upper bound: 00:02:00 + 1min = 00:03:00
        //   - Grid points in range [00:02:00, 00:03:00]: NONE
        //   - 00:00 < 00:02:00 (not in range)
        //   - 00:05 > 00:03:00 (not in range)
        //   - Matches: [] (empty)

        let timestamps = create_timestamps(&["2025-01-02 00:02:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:15:00");
        let step = create_interval_micros(300_000_000); // 5 minutes
        let range = create_interval_micros(60_000_000); // 1 minute (less than step)
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 0); // No matches
    }

    #[test]
    fn test_date_grid_range_less_than_step_single_match() {
        // Test edge case: range smaller than step, timestamp just before grid point
        //
        // Grid: 00:00, 00:05, 00:10, 00:15 (5-minute steps)
        // Timestamp: 00:04:30 (30 seconds before grid point)
        // Range: 1 minute (much less than 5-minute step), Offset: 0
        //
        // For t = 00:04:30:
        //   - Lower bound: 00:04:30 + 0 = 00:04:30
        //   - Upper bound: 00:04:30 + 1min = 00:05:30
        //   - Grid points in range [00:04:30, 00:05:30]: 00:05:00
        //   - Matches: [00:05:00]

        let timestamps = create_timestamps(&["2025-01-02 00:04:30"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:15:00");
        let step = create_interval_micros(300_000_000); // 5 minutes
        let range = create_interval_micros(60_000_000); // 1 minute (less than step)
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:05:00"));
    }

    #[test]
    fn test_date_grid_range_less_than_step_with_negative_offset() {
        // Test edge case: range smaller than step with negative offset
        //
        // Grid: 00:00, 00:05, 00:10, 00:15 (5-minute steps)
        // Timestamp: 00:05:30 (30 seconds after grid point)
        // Range: 1 minute, Offset: -1 minute
        //
        // For t = 00:05:30:
        //   - Lower bound: 00:05:30 - 1min = 00:04:30
        //   - Upper bound: 00:05:30 - 1min + 1min = 00:05:30
        //   - Grid points in range [00:04:30, 00:05:30]: 00:05:00
        //   - Matches: [00:05:00]

        let timestamps = create_timestamps(&["2025-01-02 00:05:30"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:15:00");
        let step = create_interval_micros(300_000_000); // 5 minutes
        let range = create_interval_micros(60_000_000); // 1 minute
        let offset = create_interval_micros(-60_000_000); // -1 minute

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:05:00"));
    }

    // Error cases

    #[test]
    #[should_panic(expected = "requires 7 arguments")]
    fn test_date_grid_wrong_argument_count() {
        // Test error handling: wrong number of arguments
        // Should panic with message "date_grid requires 6 arguments"

        let udf = DateGrid::new();
        let timestamps = create_timestamps(&["2025-01-02 00:00:00"]);

        // Only provide 3 arguments (should require 6)
        let return_field = Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ))),
            false,
        ));

        let arg_fields = vec![
            Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )),
            Arc::new(Field::new(
                "start",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )),
            Arc::new(Field::new(
                "end",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )),
        ];

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(timestamps) as ArrayRef),
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(0), None)),
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1000), None)),
            ],
            number_rows: 1,
            return_field,
            arg_fields,
            config_options: Arc::new(ConfigOptions::default()),
        };

        // Should panic
        let _result = udf.invoke_with_args(args).unwrap();
    }

    #[test]
    #[should_panic(expected = "step must be positive")]
    fn test_date_grid_negative_step() {
        // Test error handling: negative step interval
        // Should panic with "step must be positive" (from GridAccumulator::new)

        let timestamps = create_timestamps(&["2025-01-02 00:00:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");
        let step = create_interval_micros(-60_000_000); // negative step
        let range = create_interval_micros(60_000_000);
        let offset = create_interval_micros(0);

        // Should panic
        let _result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();
    }

    #[test]
    #[should_panic(expected = "Month intervals are not supported")]
    fn test_date_grid_month_interval() {
        // Test error handling: intervals with months not supported
        // Should panic with "Month intervals are not supported" (from udaf.rs:379-382)

        let timestamps = create_timestamps(&["2025-01-02 00:00:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");

        // Create interval with months (not supported)
        let step = ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
            1, // 1 month (not supported!)
            0, 0,
        )));
        let range = create_interval_micros(60_000_000);
        let offset = create_interval_micros(0);

        // Should panic
        let _result = execute_date_grid(&timestamps, start, end, step, range, offset, false).unwrap();
    }

    // Inverse mode tests

    #[test]
    fn test_date_grid_inverse_basic() {
        // Test inverse mode: returns grid points NOT covered by timestamp
        //
        // Grid: 00:00, 00:01, 00:02, 00:03, 00:04, 00:05 (1-minute steps) = 6 points
        // Timestamp: 00:00:30
        // Range: 1 minute, Offset: 0
        // inverse: true
        //
        // Normal mode matches: 00:01:00 (in [00:00:30, 00:01:30])
        // Inverse mode: ALL grid points EXCEPT 00:01:00
        // Expected: [00:00:00, 00:02:00, 00:03:00, 00:04:00, 00:05:00]

        let timestamps = create_timestamps(&["2025-01-02 00:00:30"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:05:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(60_000_000); // 1 minute
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, true).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 5); // 5 uncovered grid points
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:00:00"));
        assert_eq!(result[0][1], timestamp_micros("2025-01-02 00:02:00"));
        assert_eq!(result[0][2], timestamp_micros("2025-01-02 00:03:00"));
        assert_eq!(result[0][3], timestamp_micros("2025-01-02 00:04:00"));
        assert_eq!(result[0][4], timestamp_micros("2025-01-02 00:05:00"));
    }

    #[test]
    fn test_date_grid_inverse_full_coverage() {
        // Test inverse mode with full coverage: should return empty
        //
        // Grid: 00:00, 00:01, 00:02, 00:03 (1-minute steps) = 4 points
        // Timestamp: 00:00:00
        // Range: 10 minutes (covers entire grid), Offset: 0
        // inverse: true
        //
        // Normal mode matches: ALL grid points [00:00:00, 00:01:00, 00:02:00, 00:03:00]
        // Inverse mode: ALL grid points EXCEPT those covered
        // Expected: [] (empty - all points are covered)

        let timestamps = create_timestamps(&["2025-01-02 00:00:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(600_000_000); // 10 minutes
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, true).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 0); // Empty - all grid points covered
    }

    #[test]
    fn test_date_grid_inverse_no_coverage() {
        // Test inverse mode with no coverage: should return all grid points
        //
        // Grid: 00:00, 00:01, 00:02, 00:03 (1-minute steps) = 4 points
        // Timestamp: 00:10:00 (far after grid end)
        // Range: 1 minute, Offset: 0
        // inverse: true
        //
        // Normal mode matches: [] (no coverage)
        // Inverse mode: ALL grid points (none are covered)
        // Expected: [00:00:00, 00:01:00, 00:02:00, 00:03:00]

        let timestamps = create_timestamps(&["2025-01-02 00:10:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(60_000_000); // 1 minute
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, true).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 4); // All 4 grid points uncovered
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:00:00"));
        assert_eq!(result[0][1], timestamp_micros("2025-01-02 00:01:00"));
        assert_eq!(result[0][2], timestamp_micros("2025-01-02 00:02:00"));
        assert_eq!(result[0][3], timestamp_micros("2025-01-02 00:03:00"));
    }

    #[test]
    fn test_date_grid_inverse_partial_coverage() {
        // Test inverse mode with partial coverage
        //
        // Grid: 00:00, 00:01, 00:02, 00:03, 00:04, 00:05 (1-minute steps) = 6 points
        // Timestamp: 00:00:00
        // Range: 3 minutes, Offset: 0
        // inverse: true
        //
        // Normal mode matches: [00:00:00, 00:01:00, 00:02:00, 00:03:00] (in [00:00:00, 00:03:00])
        // Inverse mode: Grid points NOT in [00:00:00, 00:03:00]
        // Expected: [00:04:00, 00:05:00]

        let timestamps = create_timestamps(&["2025-01-02 00:00:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:05:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(180_000_000); // 3 minutes
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, true).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 2); // 2 uncovered grid points
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:04:00"));
        assert_eq!(result[0][1], timestamp_micros("2025-01-02 00:05:00"));
    }

    #[test]
    fn test_date_grid_inverse_with_offset() {
        // Test inverse mode with offset parameter
        //
        // Grid: 00:00, 00:01, 00:02, 00:03 (1-minute steps) = 4 points
        // Timestamp: 00:00:00
        // Range: 1 minute, Offset: 30 seconds
        // inverse: true
        //
        // Normal mode:
        //   - Lower: 00:00:00 + 30s = 00:00:30
        //   - Upper: 00:00:30 + 1min = 00:01:30
        //   - Matches: 00:01:00
        // Inverse mode: All grid points EXCEPT 00:01:00
        // Expected: [00:00:00, 00:02:00, 00:03:00]

        let timestamps = create_timestamps(&["2025-01-02 00:00:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:03:00");
        let step = create_interval_micros(60_000_000); // 1 minute
        let range = create_interval_micros(60_000_000); // 1 minute
        let offset = create_interval_micros(30_000_000); // 30 seconds

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, true).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 3); // 3 uncovered grid points
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:00:00"));
        assert_eq!(result[0][1], timestamp_micros("2025-01-02 00:02:00"));
        assert_eq!(result[0][2], timestamp_micros("2025-01-02 00:03:00"));
    }

    #[test]
    fn test_date_grid_inverse_batch_processing() {
        // Test inverse mode with multiple timestamps
        //
        // Grid: 00:00, 00:05, 00:10, 00:15 (5-minute steps) = 4 points
        // Timestamps: [00:01:00, 00:07:00]
        // Range: 5 minutes, Offset: 0
        // inverse: true
        //
        // For t1 = 00:01:00:
        //   - Normal mode matches: 00:05:00
        //   - Inverse: [00:00:00, 00:10:00, 00:15:00]
        // For t2 = 00:07:00:
        //   - Normal mode matches: 00:10:00
        //   - Inverse: [00:00:00, 00:05:00, 00:15:00]

        let timestamps = create_timestamps(&["2025-01-02 00:01:00", "2025-01-02 00:07:00"]);
        let start = timestamp_micros("2025-01-02 00:00:00");
        let end = timestamp_micros("2025-01-02 00:15:00");
        let step = create_interval_micros(300_000_000); // 5 minutes
        let range = create_interval_micros(300_000_000); // 5 minutes
        let offset = create_interval_micros(0);

        let result = execute_date_grid(&timestamps, start, end, step, range, offset, true).unwrap();

        assert_eq!(result.len(), 2);

        // First timestamp: inverse excludes 00:05:00
        assert_eq!(result[0].len(), 3);
        assert_eq!(result[0][0], timestamp_micros("2025-01-02 00:00:00"));
        assert_eq!(result[0][1], timestamp_micros("2025-01-02 00:10:00"));
        assert_eq!(result[0][2], timestamp_micros("2025-01-02 00:15:00"));

        // Second timestamp: inverse excludes 00:10:00
        assert_eq!(result[1].len(), 3);
        assert_eq!(result[1][0], timestamp_micros("2025-01-02 00:00:00"));
        assert_eq!(result[1][1], timestamp_micros("2025-01-02 00:05:00"));
        assert_eq!(result[1][2], timestamp_micros("2025-01-02 00:15:00"));
    }

    // ParseNumeric UDF tests

    /// Helper to execute `parse_numeric` UDF
    fn execute_parse_numeric(values: &[Option<&str>]) -> Result<Vec<Option<f64>>> {
        let udf = ParseNumeric::new();
        let string_array = StringArray::from(values.to_vec());
        let arg = ColumnarValue::Array(Arc::new(string_array) as ArrayRef);

        let return_field = Arc::new(Field::new("item", DataType::Float64, true));
        let arg_fields = vec![Arc::new(Field::new("value", DataType::Utf8, true))];

        let args = ScalarFunctionArgs {
            args: vec![arg],
            number_rows: values.len(),
            return_field,
            arg_fields,
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args)?;

        match result {
            ColumnarValue::Array(arr) => {
                let float_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok((0..float_arr.len())
                    .map(|i| {
                        if float_arr.is_null(i) {
                            None
                        } else {
                            Some(float_arr.value(i))
                        }
                    })
                    .collect())
            }
            ColumnarValue::Scalar(_) => Err(DataFusionError::Execution("Expected array result".to_string())),
        }
    }

    #[test]
    fn test_parse_numeric_valid_integers() {
        let result = execute_parse_numeric(&[Some("42"), Some("0"), Some("-123")]).unwrap();
        assert_eq!(result, vec![Some(42.0), Some(0.0), Some(-123.0)]);
    }

    #[test]
    fn test_parse_numeric_valid_floats() {
        let result = execute_parse_numeric(&[Some("3.15"), Some("-2.5"), Some("0.001")]).unwrap();
        assert_eq!(result, vec![Some(3.15), Some(-2.5), Some(0.001)]);
    }

    #[test]
    fn test_parse_numeric_scientific_notation() {
        let result = execute_parse_numeric(&[Some("1e3"), Some("2.5e-2"), Some("-1.5E+4")]).unwrap();
        assert_eq!(result, vec![Some(1000.0), Some(0.025), Some(-15000.0)]);
    }

    #[test]
    fn test_parse_numeric_invalid_returns_null() {
        let result = execute_parse_numeric(&[Some("invalid"), Some("12.34.56"), Some("abc123")]).unwrap();
        assert_eq!(result, vec![None, None, None]);
    }

    #[test]
    fn test_parse_numeric_null_input() {
        let result = execute_parse_numeric(&[None, Some("42"), None]).unwrap();
        assert_eq!(result, vec![None, Some(42.0), None]);
    }

    #[test]
    fn test_parse_numeric_whitespace() {
        let result = execute_parse_numeric(&[Some("  42  "), Some("\t3.15\n"), Some(" ")]).unwrap();
        assert_eq!(result, vec![Some(42.0), Some(3.15), None]);
    }

    // ParseBytes UDF tests

    /// Helper to execute `parse_bytes` UDF
    fn execute_parse_bytes(values: &[Option<&str>]) -> Result<Vec<Option<f64>>> {
        let udf = ParseBytes::new();
        let string_array = StringArray::from(values.to_vec());
        let arg = ColumnarValue::Array(Arc::new(string_array) as ArrayRef);

        let return_field = Arc::new(Field::new("item", DataType::Float64, true));
        let arg_fields = vec![Arc::new(Field::new("value", DataType::Utf8, true))];

        let args = ScalarFunctionArgs {
            args: vec![arg],
            number_rows: values.len(),
            return_field,
            arg_fields,
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args)?;

        match result {
            ColumnarValue::Array(arr) => {
                let float_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok((0..float_arr.len())
                    .map(|i| {
                        if float_arr.is_null(i) {
                            None
                        } else {
                            Some(float_arr.value(i))
                        }
                    })
                    .collect())
            }
            ColumnarValue::Scalar(_) => Err(DataFusionError::Execution("Expected array result".to_string())),
        }
    }

    #[test]
    fn test_parse_bytes_basic_units() {
        let result = execute_parse_bytes(&[Some("100B"), Some("10KB"), Some("5MB")]).unwrap();
        assert_eq!(
            result,
            vec![Some(100.0), Some(10.0 * 1024.0), Some(5.0 * 1024.0 * 1024.0)]
        );
    }

    #[test]
    fn test_parse_bytes_large_units() {
        let result = execute_parse_bytes(&[Some("2GB"), Some("1TB"), Some("0.5PB")]).unwrap();
        assert_eq!(
            result,
            vec![
                Some(2.0 * 1024.0 * 1024.0 * 1024.0),
                Some(1024.0 * 1024.0 * 1024.0 * 1024.0),
                Some(0.5 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0)
            ]
        );
    }

    #[test]
    fn test_parse_bytes_fractional() {
        let result = execute_parse_bytes(&[Some("5.5MB"), Some("1.25GB"), Some("0.1KB")]).unwrap();
        assert_eq!(
            result,
            vec![
                Some(5.5 * 1024.0 * 1024.0),
                Some(1.25 * 1024.0 * 1024.0 * 1024.0),
                Some(0.1 * 1024.0)
            ]
        );
    }

    #[test]
    fn test_parse_bytes_case_insensitive() {
        let result = execute_parse_bytes(&[Some("10kb"), Some("5Mb"), Some("2GB")]).unwrap();
        assert_eq!(
            result,
            vec![
                Some(10.0 * 1024.0),
                Some(5.0 * 1024.0 * 1024.0),
                Some(2.0 * 1024.0 * 1024.0 * 1024.0)
            ]
        );
    }

    #[test]
    fn test_parse_bytes_alternative_formats() {
        let result = execute_parse_bytes(&[Some("10K"), Some("5M"), Some("2KiB")]).unwrap();
        assert_eq!(
            result,
            vec![Some(10.0 * 1024.0), Some(5.0 * 1024.0 * 1024.0), Some(2.0 * 1024.0)]
        );
    }

    #[test]
    fn test_parse_bytes_invalid_returns_null() {
        let result = execute_parse_bytes(&[Some("invalid"), Some("10XB"), Some("abc")]).unwrap();
        assert_eq!(result, vec![None, None, None]);
    }

    #[test]
    fn test_parse_bytes_null_input() {
        let result = execute_parse_bytes(&[None, Some("10KB"), None]).unwrap();
        assert_eq!(result, vec![None, Some(10.0 * 1024.0), None]);
    }

    #[test]
    fn test_parse_bytes_whitespace() {
        let result = execute_parse_bytes(&[Some("  10KB  "), Some("\t5MB\n")]).unwrap();
        assert_eq!(result, vec![Some(10.0 * 1024.0), Some(5.0 * 1024.0 * 1024.0)]);
    }

    // ParseDuration UDF tests

    /// Helper to execute `parse_duration` UDF
    fn execute_parse_duration(values: &[Option<&str>], as_seconds: bool) -> Result<Vec<Option<f64>>> {
        let udf = ParseDuration::new();
        let string_array = StringArray::from(values.to_vec());
        let arg1 = ColumnarValue::Array(Arc::new(string_array) as ArrayRef);
        let arg2 = ColumnarValue::Scalar(ScalarValue::Boolean(Some(as_seconds)));

        let return_field = Arc::new(Field::new("item", DataType::Float64, true));
        let arg_fields = vec![
            Arc::new(Field::new("value", DataType::Utf8, true)),
            Arc::new(Field::new("as_seconds", DataType::Boolean, false)),
        ];

        let func_args = ScalarFunctionArgs {
            args: vec![arg1, arg2],
            number_rows: values.len(),
            return_field,
            arg_fields,
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(func_args)?;

        match result {
            ColumnarValue::Array(arr) => {
                let float_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok((0..float_arr.len())
                    .map(|i| {
                        if float_arr.is_null(i) {
                            None
                        } else {
                            Some(float_arr.value(i))
                        }
                    })
                    .collect())
            }
            ColumnarValue::Scalar(_) => Err(DataFusionError::Execution("Expected array result".to_string())),
        }
    }

    #[test]
    fn test_parse_duration_seconds_basic() {
        let result = execute_parse_duration(&[Some("5s"), Some("10m"), Some("2h")], true).unwrap();
        assert_eq!(result, vec![Some(5.0), Some(600.0), Some(7200.0)]);
    }

    #[test]
    fn test_parse_duration_nanoseconds_basic() {
        let result = execute_parse_duration(&[Some("5s"), Some("10ms"), Some("100ns")], false).unwrap();
        assert_eq!(result, vec![Some(5_000_000_000.0), Some(10_000_000.0), Some(100.0)]);
    }

    #[test]
    fn test_parse_duration_compound() {
        let result = execute_parse_duration(&[Some("1h30m"), Some("1h30m45s"), Some("2m30s")], true).unwrap();
        assert_eq!(result, vec![Some(5400.0), Some(5445.0), Some(150.0)]);
    }

    #[test]
    fn test_parse_duration_fractional() {
        let result = execute_parse_duration(&[Some("1.5s"), Some("2.5m"), Some("0.5h")], true).unwrap();
        assert_eq!(result, vec![Some(1.5), Some(150.0), Some(1800.0)]);
    }

    #[test]
    fn test_parse_duration_milliseconds_microseconds() {
        let result = execute_parse_duration(&[Some("500ms"), Some("100us"), Some("50ns")], false).unwrap();
        assert_eq!(result, vec![Some(500_000_000.0), Some(100_000.0), Some(50.0)]);
    }

    #[test]
    fn test_parse_duration_complex_compound() {
        let result = execute_parse_duration(&[Some("1h2m3s4ms5us6ns")], false).unwrap();
        // 1h = 3,600,000,000,000 ns
        // 2m = 120,000,000,000 ns
        // 3s = 3,000,000,000 ns
        // 4ms = 4,000,000 ns
        // 5us = 5,000 ns
        // 6ns = 6 ns
        let expected = 3_600_000_000_000.0 + 120_000_000_000.0 + 3_000_000_000.0 + 4_000_000.0 + 5_000.0 + 6.0;
        assert_eq!(result, vec![Some(expected)]);
    }

    #[test]
    fn test_parse_duration_invalid_returns_null() {
        let result = execute_parse_duration(&[Some("invalid"), Some("10x"), Some("abc")], true).unwrap();
        assert_eq!(result, vec![None, None, None]);
    }

    #[test]
    fn test_parse_duration_null_input() {
        let result = execute_parse_duration(&[None, Some("5s"), None], true).unwrap();
        assert_eq!(result, vec![None, Some(5.0), None]);
    }

    #[test]
    fn test_parse_duration_whitespace() {
        let result = execute_parse_duration(&[Some("  5s  "), Some("\t10m\n")], true).unwrap();
        assert_eq!(result, vec![Some(5.0), Some(600.0)]);
    }

    #[test]
    fn test_parse_duration_zero() {
        let result = execute_parse_duration(&[Some("0s"), Some("0m"), Some("0h")], true).unwrap();
        assert_eq!(result, vec![Some(0.0), Some(0.0), Some(0.0)]);
    }

    // Map filtering with matchers tests

    /// Helper to create a `MapArray` for testing map filtering.
    fn create_test_map_for_filter(entries: &[Vec<(&str, &str)>]) -> MapArray {
        use std::sync::Arc;

        use datafusion::arrow::{
            array::{ArrayRef, StringBuilder, StructArray},
            buffer::{OffsetBuffer, ScalarBuffer},
            datatypes::{DataType, Fields},
        };

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

        let struct_array = StructArray::try_new(struct_fields, vec![keys_array, values_array], None).unwrap();

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
        use std::sync::Arc;

        use datafusion::arrow::{
            array::{ArrayRef, ListArray, StringArray},
            buffer::{OffsetBuffer, ScalarBuffer},
            datatypes::DataType,
        };

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

/// UDF: `parse_numeric(value)` - parses string to Float64.
///
/// # Arguments
/// - `value`: A `String` column
///
/// # Returns
/// Parsed Float64 value, or NULL if parsing fails.
///
/// # Example
/// ```sql
/// SELECT parse_numeric('42.5') -- 42.5
/// SELECT parse_numeric('invalid') -- NULL
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseNumeric {
    signature: Signature,
}

impl Default for ParseNumeric {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseNumeric {
    /// Creates a new `ParseNumeric` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ParseNumeric {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_numeric"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("parse_numeric requires one argument");
        }

        let value = &args.args[0];

        match value {
            ColumnarValue::Scalar(scalar) => {
                let result = match scalar {
                    datafusion::scalar::ScalarValue::Utf8(Some(s)) => s.trim().parse::<f64>().ok(),
                    _ => None,
                };

                Ok(ColumnarValue::Scalar(result.map_or_else(
                    || datafusion::scalar::ScalarValue::Float64(None),
                    |v| datafusion::scalar::ScalarValue::Float64(Some(v)),
                )))
            }
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?;

                let result: datafusion::arrow::array::Float64Array = string_array
                    .iter()
                    .map(|opt_str| opt_str.and_then(|s| s.trim().parse::<f64>().ok()))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

/// UDF: `parse_bytes(value)` - parses humanized byte string to Float64.
///
/// # Arguments
/// - `value`: A `String` column containing byte values like "10KB", "5.5MB"
///
/// # Returns
/// Parsed byte count as Float64, or NULL if parsing fails.
///
/// # Supported Units
/// B, KB, MB, GB, TB, PB (1024-based), also `KiB`, `MiB`, `GiB`, etc.
///
/// # Example
/// ```sql
/// SELECT parse_bytes('10KB') -- 10240.0
/// SELECT parse_bytes('5.5MB') -- 5767168.0
/// SELECT parse_bytes('invalid') -- NULL
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseBytes {
    signature: Signature,
}

impl Default for ParseBytes {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseBytes {
    /// Creates a new `ParseBytes` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    /// Parse a byte string like "10KB" to float
    fn parse_byte_string(s: &str) -> Option<f64> {
        let s = s.trim();

        // Find where the unit starts (first non-digit, non-dot character)
        let unit_start = s.find(|c: char| !c.is_ascii_digit() && c != '.')?;

        let (num_str, unit_str) = s.split_at(unit_start);
        let num: f64 = num_str.trim().parse().ok()?;

        let multiplier = match unit_str.trim().to_uppercase().as_str() {
            "B" => 1.0,
            "KB" | "K" | "KIB" => 1024.0,
            "MB" | "M" | "MIB" => 1024.0 * 1024.0,
            "GB" | "G" | "GIB" => 1024.0 * 1024.0 * 1024.0,
            "TB" | "T" | "TIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
            "PB" | "P" | "PIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0,
            _ => return None,
        };

        Some(num * multiplier)
    }
}

impl ScalarUDFImpl for ParseBytes {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_bytes"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("parse_bytes requires one argument");
        }

        let value = &args.args[0];

        match value {
            ColumnarValue::Scalar(scalar) => {
                let result = match scalar {
                    datafusion::scalar::ScalarValue::Utf8(Some(s)) => Self::parse_byte_string(s.trim()),
                    _ => None,
                };

                Ok(ColumnarValue::Scalar(result.map_or_else(
                    || datafusion::scalar::ScalarValue::Float64(None),
                    |v| datafusion::scalar::ScalarValue::Float64(Some(v)),
                )))
            }
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?;

                let result: datafusion::arrow::array::Float64Array = string_array
                    .iter()
                    .map(|opt_str| opt_str.and_then(Self::parse_byte_string))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

/// UDF: `parse_duration(value, as_seconds)` - parses Go-style duration to Float64.
///
/// # Arguments
/// - `value`: A `String` column containing durations like "5s", "1h30m", "500ms"
/// - `as_seconds`: Boolean - true to return seconds, false to return nanoseconds
///
/// # Returns
/// Parsed duration as Float64, or NULL if parsing fails.
///
/// # Supported Units
/// ns, us (µs), ms, s, m, h (can be combined like "1h30m45s")
///
/// # Example
/// ```sql
/// SELECT parse_duration('5s', true) -- 5.0 seconds
/// SELECT parse_duration('1h30m', true) -- 5400.0 seconds
/// SELECT parse_duration('500ms', false) -- 500000000.0 nanoseconds
/// SELECT parse_duration('invalid', true) -- NULL
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseDuration {
    signature: Signature,
}

impl Default for ParseDuration {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseDuration {
    /// Creates a new `ParseDuration` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    /// Parse a Go-style duration string like "1h30m45s" to nanoseconds
    fn parse_duration_string(s: &str) -> Option<f64> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }

        let mut total_nanos = 0.0;
        let mut current_num = String::new();

        let chars: Vec<char> = s.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            let c = chars[i];

            if c.is_ascii_digit() || c == '.' {
                current_num.push(c);
                i += 1;
            } else {
                // Found a unit character
                if current_num.is_empty() {
                    return None;
                }

                let num: f64 = current_num.parse().ok()?;
                current_num.clear();

                // Determine the unit
                let unit = if c == 'h' {
                    i += 1;
                    3_600_000_000_000.0 // hours to nanoseconds
                } else if c == 'm' {
                    // Check if next char is 's' (milliseconds)
                    if i + 1 < chars.len() && chars[i + 1] == 's' {
                        i += 2;
                        1_000_000.0 // milliseconds to nanoseconds
                    } else {
                        i += 1;
                        60_000_000_000.0 // minutes to nanoseconds
                    }
                } else if c == 's' {
                    i += 1;
                    1_000_000_000.0 // seconds to nanoseconds
                } else if c == 'n' && i + 1 < chars.len() && chars[i + 1] == 's' {
                    i += 2;
                    1.0 // nanoseconds
                } else if c == 'u' || c == 'µ' {
                    // microseconds (us or µs)
                    if i + 1 < chars.len() && chars[i + 1] == 's' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                    1_000.0 // microseconds to nanoseconds
                } else {
                    return None; // Unknown unit
                };

                total_nanos += num * unit;
            }
        }

        if !current_num.is_empty() {
            // Trailing number without unit
            return None;
        }

        Some(total_nanos)
    }
}

impl ScalarUDFImpl for ParseDuration {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_duration"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("parse_duration requires two arguments");
        }

        let value = &args.args[0];
        let as_seconds = &args.args[1];

        // Extract as_seconds boolean value
        let as_secs = match as_seconds {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Boolean(Some(b))) => *b,
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Boolean(None)) => false,
            _ => return exec_err!("Second argument must be a boolean"),
        };

        match value {
            ColumnarValue::Scalar(scalar) => {
                let result = match scalar {
                    datafusion::scalar::ScalarValue::Utf8(Some(s)) => Self::parse_duration_string(s.trim()).map(
                        |nanos| {
                            if as_secs { nanos / 1_000_000_000.0 } else { nanos }
                        },
                    ),
                    _ => None,
                };

                Ok(ColumnarValue::Scalar(result.map_or_else(
                    || datafusion::scalar::ScalarValue::Float64(None),
                    |v| datafusion::scalar::ScalarValue::Float64(Some(v)),
                )))
            }
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?;

                let result: datafusion::arrow::array::Float64Array = string_array
                    .iter()
                    .map(|opt_str| {
                        opt_str
                            .and_then(Self::parse_duration_string)
                            .map(|nanos| if as_secs { nanos / 1_000_000_000.0 } else { nanos })
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

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
}

/// Build matcher specifications from keys, values, and ops arrays.
///
/// # Errors
///
/// Returns an error if any matcher has inconsistent value/op state (one is `None` while the other is `Some`).
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

        // Only push after validation succeeds
        matchers.push(MatcherSpec {
            key: key.clone(),
            value,
            op,
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
                    // Conditional matching - evaluate the matcher
                    let is_match = evaluate_matcher(map_value, matcher_value, matcher_op)?;

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
fn evaluate_matcher(value: &str, pattern: &str, op: &str) -> Result<bool> {
    match op {
        "=" => Ok(value == pattern),
        "!=" => Ok(value != pattern),
        "=~" => {
            // Regex match
            let re = regex::Regex::new(pattern)
                .map_err(|e| DataFusionError::Execution(format!("Invalid regex pattern '{pattern}': {e}")))?;
            Ok(re.is_match(value))
        }
        "!~" => {
            // Regex not match
            let re = regex::Regex::new(pattern)
                .map_err(|e| DataFusionError::Execution(format!("Invalid regex pattern '{pattern}': {e}")))?;
            Ok(!re.is_match(value))
        }
        _ => exec_err!("Unknown matcher operator: {}", op),
    }
}
