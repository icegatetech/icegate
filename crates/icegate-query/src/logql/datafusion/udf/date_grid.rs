//! Date grid UDF for aligning timestamps to time-bucketed grids.

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, LargeListArray, TimestampMicrosecondArray},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, TimeUnit},
    },
    common::{DataFusionError, Result, datatype::DataTypeExt, plan_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};

/// Precompute grid points as microsecond timestamps.
///
/// Generates evenly-spaced timestamps from `start_micros` to `end_micros`
/// at `step_micros` intervals. Used by both the `DateGrid` UDF and `GridAgg` UDAF.
///
/// # Arguments
/// * `start_micros` - Grid start timestamp in microseconds
/// * `end_micros` - Grid end timestamp in microseconds
/// * `step_micros` - Step interval in microseconds (must be positive)
/// * `max_points` - Maximum allowed grid points to prevent excessive memory allocation
///
/// # Errors
/// Returns error if step is non-positive, time range is invalid, or grid exceeds
/// `max_points` limit.
pub fn compute_grid_points(start_micros: i64, end_micros: i64, step_micros: i64, max_points: i64) -> Result<Vec<i64>> {
    if step_micros <= 0 {
        return plan_err!("step must be positive");
    }
    if end_micros < start_micros {
        return plan_err!("end timestamp must be greater than or equal to start timestamp");
    }

    let time_range = end_micros
        .checked_sub(start_micros)
        .ok_or_else(|| DataFusionError::Plan("Time range calculation overflow".to_string()))?;

    let num_points_i64 = time_range
        .checked_div(step_micros)
        .ok_or_else(|| DataFusionError::Plan("Grid calculation overflow".to_string()))?
        .checked_add(1)
        .ok_or_else(|| DataFusionError::Plan("Grid size calculation overflow".to_string()))?;

    if num_points_i64 > max_points {
        return plan_err!(
            "Grid size too large: {} points exceeds maximum of {}",
            num_points_i64,
            max_points
        );
    }

    let num_points = usize::try_from(num_points_i64)
        .map_err(|_| DataFusionError::Plan("Grid size exceeds usize capacity".to_string()))?;

    let grid: Vec<i64> = (0..num_points)
        .map(|i| {
            #[allow(clippy::cast_possible_wrap)]
            let idx = i as i64;
            start_micros + idx * step_micros
        })
        .collect();

    Ok(grid)
}

/// Find the range of grid indices a timestamp maps to using binary search.
///
/// A timestamp `t` maps to grid point `g` if: `t + offset <= g <= t + offset + range`.
/// Returns `(start_idx, end_idx)` as a half-open range `[start_idx, end_idx)`.
///
/// # Arguments
/// * `timestamp` - The timestamp in microseconds
/// * `grid` - Sorted slice of grid point timestamps in microseconds
/// * `range_micros` - Range window size in microseconds
/// * `offset_micros` - Offset for the range window in microseconds
///
/// # Returns
/// `(start_idx, end_idx)` — indices into `grid` where matching points lie.
/// If no grid points match, `start_idx == end_idx`.
pub fn find_matching_grid_indices(
    timestamp: i64,
    grid: &[i64],
    range_micros: i64,
    offset_micros: i64,
) -> (usize, usize) {
    // Grid point g matches if: t + offset <= g <= t + offset + range
    let Some(lower_grid) = timestamp.checked_add(offset_micros) else {
        return (0, 0);
    };
    let Some(upper_grid) = lower_grid.checked_add(range_micros) else {
        return (0, 0);
    };

    // Negative range would produce an inverted interval — return empty.
    if upper_grid < lower_grid {
        return (0, 0);
    }

    let start_idx = grid.partition_point(|&g| g < lower_grid);
    let end_idx = grid.partition_point(|&g| g <= upper_grid);
    (start_idx, end_idx)
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
    max_grid_points: i64,
}

impl Default for DateGrid {
    fn default() -> Self {
        use crate::logql::planner::QueryContext;
        Self::with_max_grid_points(QueryContext::DEFAULT_MAX_GRID_POINTS)
    }
}

impl DateGrid {
    /// Creates a new `DateGrid` UDF with the default grid point limit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `DateGrid` UDF with a custom grid point limit.
    pub fn with_max_grid_points(max_grid_points: i64) -> Self {
        Self {
            signature: Signature::any(7, Volatility::Immutable),
            max_grid_points,
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

            let day_micros = i64::from(days).checked_mul(86_400_000_000).ok_or_else(|| {
                DataFusionError::Plan("Interval overflow when converting days to microseconds".to_string())
            })?;

            // Round to nearest microsecond instead of truncating
            let nano_micros = if nanoseconds >= 0 {
                (nanoseconds + 500) / 1000
            } else {
                (nanoseconds - 500) / 1000
            };

            // Use checked addition to prevent overflow
            let total = day_micros.checked_add(nano_micros).ok_or_else(|| {
                DataFusionError::Plan("Interval overflow when converting to microseconds".to_string())
            })?;

            Ok(total)
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
        Ok(DataType::LargeList(arg_types[0].clone().into_nullable_field_ref()))
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

        // Compute grid points using shared utility
        let grid = compute_grid_points(start_micros, end_micros, step_micros, self.max_grid_points)?;

        // Process each timestamp and collect matching grid points
        let mut list_builder: Vec<Vec<i64>> = Vec::with_capacity(timestamp_array.len());

        for i in 0..timestamp_array.len() {
            if timestamp_array.is_null(i) {
                list_builder.push(Vec::new());
            } else {
                let t = timestamp_array.value(i);

                let (start_idx, end_idx) = find_matching_grid_indices(t, &grid, range_micros, offset_micros);
                let matches: Vec<i64> = if inverse {
                    // Inverse mode: return grid points NOT in the coverage window
                    grid[..start_idx].iter().chain(grid[end_idx..].iter()).copied().collect()
                } else {
                    // Normal mode: return grid points in the coverage window
                    grid[start_idx..end_idx].to_vec()
                };

                list_builder.push(matches);
            }
        }

        // Build the LargeListArray result (uses i64 offsets to prevent overflow)
        let mut offset = 0i64;
        let mut offsets_vec = vec![0i64];
        let mut values_vec = Vec::new();

        for matches in list_builder {
            let len = i64::try_from(matches.len())
                .map_err(|_| DataFusionError::Execution("List length exceeds i64 capacity".to_string()))?;
            for timestamp in matches {
                values_vec.push(timestamp);
            }
            offset = offset.checked_add(len).ok_or_else(|| {
                DataFusionError::Execution("Offset overflow when building LargeListArray".to_string())
            })?;
            offsets_vec.push(offset);
        }

        let values_array = Arc::new(TimestampMicrosecondArray::from(values_vec));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets_vec));

        let field = Arc::new(datafusion::arrow::datatypes::Field::new(
            "", // Empty name to match return_type promise
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true, // Nullable to match return_type promise
        ));

        let list_array = LargeListArray::new(field, offsets, values_array, None);

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

    #[test]
    fn test_compute_grid_points_basic() {
        // 0, 100, 200, 300, 400, 500
        let grid = compute_grid_points(0, 500, 100, 11_000).unwrap();
        assert_eq!(grid, vec![0, 100, 200, 300, 400, 500]);
    }

    #[test]
    fn test_compute_grid_points_single_point() {
        let grid = compute_grid_points(100, 100, 50, 11_000).unwrap();
        assert_eq!(grid, vec![100]);
    }

    #[test]
    fn test_compute_grid_points_rejects_negative_step() {
        assert!(compute_grid_points(0, 100, -1, 11_000).is_err());
    }

    #[test]
    fn test_compute_grid_points_rejects_inverted_range() {
        assert!(compute_grid_points(100, 0, 10, 11_000).is_err());
    }

    #[test]
    fn test_find_matching_grid_indices_basic() {
        // Grid: [0, 100, 200, 300, 400, 500]
        // Timestamp=50, range=200, offset=0 → matches g where 50 <= g <= 250 → [100, 200]
        let grid = vec![0, 100, 200, 300, 400, 500];
        let (start, end) = find_matching_grid_indices(50, &grid, 200, 0);
        assert_eq!(&grid[start..end], &[100, 200]);
    }

    #[test]
    fn test_find_matching_grid_indices_with_offset() {
        // Grid: [0, 100, 200, 300, 400, 500]
        // Timestamp=50, range=100, offset=50 → matches g where 100 <= g <= 200 → [100, 200]
        let grid = vec![0, 100, 200, 300, 400, 500];
        let (start, end) = find_matching_grid_indices(50, &grid, 100, 50);
        assert_eq!(&grid[start..end], &[100, 200]);
    }

    #[test]
    fn test_find_matching_grid_indices_no_match() {
        let grid = vec![0, 100, 200, 300];
        let (start, end) = find_matching_grid_indices(500, &grid, 100, 0);
        assert_eq!(start, end); // empty range
    }
}
