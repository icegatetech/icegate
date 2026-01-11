//! Date grid UDF for aligning timestamps to time-bucketed grids.

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ListArray, TimestampMicrosecondArray},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, TimeUnit},
    },
    common::{DataFusionError, Result, datatype::DataTypeExt, plan_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};

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
        use datafusion::common::ScalarValue;

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
    use super::*;

    #[test]
    fn test_date_grid_creation() {
        let udf = DateGrid::new();
        assert_eq!(udf.name(), "date_grid");
    }

    #[test]
    fn test_extract_interval_micros_days() {
        use datafusion::{arrow::datatypes::IntervalMonthDayNano, common::ScalarValue};

        let interval = IntervalMonthDayNano {
            months: 0,
            days: 1,
            nanoseconds: 0,
        };
        let arg = ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(interval)));
        let result = DateGrid::extract_interval_micros(&arg, "test");
        assert_eq!(result.unwrap(), 86_400_000_000); // 1 day in microseconds
    }

    #[test]
    fn test_extract_interval_micros_rejects_months() {
        use datafusion::{arrow::datatypes::IntervalMonthDayNano, common::ScalarValue};

        let interval = IntervalMonthDayNano {
            months: 1,
            days: 0,
            nanoseconds: 0,
        };
        let arg = ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(interval)));
        let result = DateGrid::extract_interval_micros(&arg, "test");
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("Month intervals are not supported"));
    }
}
