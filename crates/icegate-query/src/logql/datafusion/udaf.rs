//! User-defined aggregate functions for LogQL operations in DataFusion.
//!
//! This module provides UDAFs for LogQL-specific aggregation operations,
//! particularly time-windowed aggregations like `count_over_time`.
//!
//! # Architecture
//!
//! All range aggregation UDAFs share a common `GridAccumulator` base that
//! handles:
//! - Time grid generation
//! - Timestamp-major iteration with binary search (O(T * log G) where T=timestamps, G=grid)
//! - State serialization for distributed execution
//! - Merge logic for combining partial results
//!
//! Individual UDAFs customize only:
//! - Input handling (timestamp-only vs timestamp+body)
//! - Output evaluation (raw counts, rates, inverted for absent)

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, Float64Array, ListArray, StringArray, StructArray, TimestampMicrosecondArray, UInt64Array,
        },
        buffer::OffsetBuffer,
        datatypes::{DataType, Field, Fields, IntervalMonthDayNano, TimeUnit},
    },
    common::{Result, ScalarValue},
    error::DataFusionError,
    logical_expr::{
        Accumulator, AggregateUDFImpl, Signature, Volatility,
        function::{AccumulatorArgs, StateFieldsArgs},
    },
    physical_plan::PhysicalExpr,
};

// ============================================================================
// GridAccumulator - Shared Base for All Range Aggregations
// ============================================================================

/// Base accumulator for time-bucketed range aggregations.
///
/// Handles grid generation, state serialization, merging, and efficient
/// timestamp-major iteration. Uses **binary search** to find matching grid
/// ranges for each timestamp, which is O(T * (log G + M)) where T=timestamps,
/// G=grid points, M=matching grid points per timestamp.
///
/// # Iteration Strategy
///
/// For sparse data (few timestamps per batch, many grid points):
/// ```text
/// for each timestamp t:           // 1-10 iterations
///     binary_search(grid, t + offset)  // O(log G)
///     increment matching range    // O(M) where M << G typically
/// ```
///
/// This is optimal when `num_timestamps` << `num_grid_points`, which is the
/// typical case for per-file batches (1-10 timestamps) vs query grids (100+).
#[derive(Debug)]
pub struct GridAccumulator {
    /// Grid timestamps (microseconds).
    grid: Vec<i64>,
    /// Accumulated values (counts or byte sums) per grid point.
    values: Vec<u64>,
    /// Offset in microseconds (lower bound of range window).
    offset_micros: i64,
    /// Upper bound in microseconds (range + offset).
    upper_bound_micros: i64,
    /// Range in microseconds (for state serialization and rate calculation).
    range_micros: i64,
}

impl GridAccumulator {
    /// Creates a new grid accumulator with the given time parameters.
    ///
    /// # Arguments
    /// - `start_micros`: Grid start timestamp (microseconds)
    /// - `end_micros`: Grid end timestamp (microseconds)
    /// - `step_micros`: Step between grid points (microseconds)
    /// - `range_micros`: Range window size (microseconds)
    /// - `offset_micros`: Offset for range window (microseconds)
    pub fn new(
        start_micros: i64,
        end_micros: i64,
        step_micros: i64,
        range_micros: i64,
        offset_micros: i64,
    ) -> Result<Self> {
        if step_micros <= 0 {
            return Err(DataFusionError::Plan("step must be positive".to_string()));
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

        let values = vec![0u64; grid.len()];
        let upper_bound_micros = range_micros + offset_micros;

        Ok(Self {
            grid,
            values,
            offset_micros,
            upper_bound_micros,
            range_micros,
        })
    }

    /// Serializes state for distributed execution.
    #[allow(clippy::unnecessary_wraps)] // Required by Accumulator trait interface
    pub fn state(&self) -> Result<Vec<ScalarValue>> {
        let grid_array = TimestampMicrosecondArray::from(self.grid.clone());
        let values_array = UInt64Array::from(self.values.clone());

        let grid_list = ScalarValue::List(Arc::new(array_to_list_array(Arc::new(grid_array))));
        let values_list = ScalarValue::List(Arc::new(array_to_list_array(Arc::new(values_array))));

        Ok(vec![
            grid_list,
            values_list,
            ScalarValue::Int64(Some(self.range_micros)),
            ScalarValue::Int64(Some(self.offset_micros)),
        ])
    }

    /// Merges another accumulator's state into this one.
    pub fn merge(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected ListArray for values".to_string()))?;

        for i in 0..values_list.len() {
            if values_list.is_null(i) {
                continue;
            }

            let other_values = values_list.value(i);
            let other_u64 = other_values
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| DataFusionError::Plan("Expected UInt64Array in values list".to_string()))?;

            // Element-wise addition
            for (j, other_val) in other_u64.iter().enumerate() {
                if let Some(v) = other_val {
                    self.values[j] += v;
                }
            }
        }

        Ok(())
    }

    /// Memory size calculation.
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.grid.capacity() * std::mem::size_of::<i64>()
            + self.values.capacity() * std::mem::size_of::<u64>()
    }

    /// Updates counts using timestamp-major iteration with binary search.
    ///
    /// For each timestamp, finds the range of grid points it affects using
    /// binary search, then increments those grid points. This is efficient
    /// when `num_timestamps` << `num_grid_points` (typical case: 1-10 timestamps
    /// per batch vs 100+ grid points).
    ///
    /// A timestamp `t` matches grid point `g` when: `offset <= (g - t) <= upper_bound`
    /// Rearranging: `t + offset <= g <= t + upper_bound`
    pub fn update_counts(&mut self, timestamps: &TimestampMicrosecondArray) {
        if timestamps.is_empty() {
            return;
        }

        // Timestamp-major iteration: for each timestamp, find matching grid range
        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                continue;
            }
            let t = timestamps.value(i);

            // Grid point g matches if: t + offset <= g <= t + upper_bound
            let lower_grid = t + self.offset_micros;
            let upper_grid = t + self.upper_bound_micros;

            // Binary search for first grid point >= lower_grid
            let start_idx = self.grid.partition_point(|&g| g < lower_grid);

            // Increment counts for all matching grid points
            for idx in start_idx..self.grid.len() {
                if self.grid[idx] > upper_grid {
                    break;
                }
                self.values[idx] += 1;
            }
        }
    }

    /// Updates byte sums using timestamp-major iteration with binary search.
    ///
    /// Similar to `update_counts`, but sums byte lengths of body values
    /// for matching timestamps instead of just counting.
    fn update_bytes(&mut self, timestamps: &TimestampMicrosecondArray, bodies: &StringArray) {
        if timestamps.is_empty() {
            return;
        }

        // Timestamp-major iteration: for each timestamp, find matching grid range
        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || bodies.is_null(i) {
                continue;
            }
            let t = timestamps.value(i);

            #[allow(clippy::cast_possible_truncation)]
            let byte_len = bodies.value(i).len() as u64;

            // Grid point g matches if: t + offset <= g <= t + upper_bound
            let lower_grid = t + self.offset_micros;
            let upper_grid = t + self.upper_bound_micros;

            // Binary search for first grid point >= lower_grid
            let start_idx = self.grid.partition_point(|&g| g < lower_grid);

            // Add byte length for all matching grid points
            for idx in start_idx..self.grid.len() {
                if self.grid[idx] > upper_grid {
                    break;
                }
                self.values[idx] += byte_len;
            }
        }
    }

    /// Builds sparse output as `List<Struct { timestamp, value: UInt64 }>`.
    ///
    /// Only includes grid points where value > 0 (or value == 0 if inverted).
    pub fn build_sparse_u64(&self, invert: bool) -> Result<ScalarValue> {
        let mut timestamps = Vec::new();
        let mut output_values = Vec::new();

        for (i, &grid_ts) in self.grid.iter().enumerate() {
            let value = self.values[i];
            let include = if invert { value == 0 } else { value > 0 };
            if include {
                timestamps.push(grid_ts);
                output_values.push(if invert { 1u64 } else { value });
            }
        }

        build_list_struct_u64(&timestamps, &output_values)
    }

    /// Builds sparse output as `List<Struct { timestamp, value: Float64 }>`.
    ///
    /// Divides values by the given divisor (`range_seconds` for rate).
    pub fn build_sparse_f64(&self, divisor: f64) -> Result<ScalarValue> {
        let mut timestamps = Vec::new();
        let mut output_values = Vec::new();

        for (i, &grid_ts) in self.grid.iter().enumerate() {
            let value = self.values[i];
            if value > 0 {
                timestamps.push(grid_ts);
                #[allow(clippy::cast_precision_loss)]
                output_values.push(value as f64 / divisor);
            }
        }

        build_list_struct_f64(&timestamps, &output_values)
    }

    /// Returns range in seconds (for rate calculations).
    fn range_seconds(&self) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let secs = self.range_micros as f64 / 1_000_000.0;
        secs
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Converts an array to a single-element `ListArray`.
fn array_to_list_array(array: ArrayRef) -> ListArray {
    let field = Arc::new(Field::new("item", array.data_type().clone(), false));
    let offsets = OffsetBuffer::from_lengths([array.len()]);
    ListArray::new(field, offsets, array, None)
}

/// Builds `List<Struct { timestamp: Timestamp, value: UInt64 }>`.
fn build_list_struct_u64(timestamps: &[i64], values: &[u64]) -> Result<ScalarValue> {
    let struct_fields = Fields::from(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value", DataType::UInt64, false),
    ]);

    let ts_array = TimestampMicrosecondArray::from(timestamps.to_vec());
    let val_array = UInt64Array::from(values.to_vec());

    let struct_array = StructArray::new(
        struct_fields.clone(),
        vec![Arc::new(ts_array) as ArrayRef, Arc::new(val_array) as ArrayRef],
        None,
    );

    let list_field = Arc::new(Field::new("item", DataType::Struct(struct_fields), false));
    let offsets = OffsetBuffer::from_lengths([struct_array.len()]);
    let list_array = ListArray::new(list_field, offsets, Arc::new(struct_array), None);

    ScalarValue::try_from_array(&list_array, 0)
}

/// Builds `List<Struct { timestamp: Timestamp, value: Float64 }>`.
fn build_list_struct_f64(timestamps: &[i64], values: &[f64]) -> Result<ScalarValue> {
    let struct_fields = Fields::from(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value", DataType::Float64, false),
    ]);

    let ts_array = TimestampMicrosecondArray::from(timestamps.to_vec());
    let val_array = Float64Array::from(values.to_vec());

    let struct_array = StructArray::new(
        struct_fields.clone(),
        vec![Arc::new(ts_array) as ArrayRef, Arc::new(val_array) as ArrayRef],
        None,
    );

    let list_field = Arc::new(Field::new("item", DataType::Struct(struct_fields), false));
    let offsets = OffsetBuffer::from_lengths([struct_array.len()]);
    let list_array = ListArray::new(list_field, offsets, Arc::new(struct_array), None);

    ScalarValue::try_from_array(&list_array, 0)
}

/// Extracts `Timestamp(Microsecond)` as i64 from a physical expression.
fn extract_timestamp_micros_from_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<i64> {
    if let Some(scalar) = expr.as_any().downcast_ref::<datafusion::physical_expr::expressions::Literal>() {
        match scalar.value() {
            ScalarValue::TimestampMicrosecond(Some(ts), _) => return Ok(*ts),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Expected TimestampMicrosecond, got: {other:?}"
                )));
            }
        }
    }
    Err(DataFusionError::Plan(
        "Expected literal TimestampMicrosecond expression".to_string(),
    ))
}

/// Extracts Interval as i64 microseconds from a physical expression.
fn extract_interval_micros_from_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<i64> {
    if let Some(scalar) = expr.as_any().downcast_ref::<datafusion::physical_expr::expressions::Literal>() {
        match scalar.value() {
            ScalarValue::IntervalMonthDayNano(Some(interval)) => {
                let IntervalMonthDayNano {
                    months,
                    days,
                    nanoseconds,
                } = *interval;

                if months != 0 {
                    return Err(DataFusionError::Plan(
                        "Month intervals are not supported, use days or smaller units".to_string(),
                    ));
                }

                let day_micros = i64::from(days) * 86_400_000_000;
                let nano_micros = nanoseconds / 1000;

                return Ok(day_micros + nano_micros);
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Expected IntervalMonthDayNano, got: {other:?}"
                )));
            }
        }
    }
    Err(DataFusionError::Plan(
        "Expected literal IntervalMonthDayNano expression".to_string(),
    ))
}

/// Returns the standard state fields for all range aggregation UDAFs.
fn standard_state_fields() -> Vec<Arc<Field>> {
    vec![
        Arc::new(Field::new(
            "grid_timestamp",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ))),
            false,
        )),
        Arc::new(Field::new(
            "value",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
            false,
        )),
        Arc::new(Field::new("range_micros", DataType::Int64, false)),
        Arc::new(Field::new("offset_micros", DataType::Int64, false)),
    ]
}

/// Returns output type for `UInt64` value UDAFs.
fn output_type_u64() -> DataType {
    let struct_type = DataType::Struct(Fields::from(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value", DataType::UInt64, false),
    ]));
    DataType::List(Arc::new(Field::new("item", struct_type, false)))
}

/// Returns output type for Float64 value UDAFs.
fn output_type_f64() -> DataType {
    let struct_type = DataType::Struct(Fields::from(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value", DataType::Float64, false),
    ]));
    DataType::List(Arc::new(Field::new("item", struct_type, false)))
}

// ============================================================================
// CountOverTime UDAF
// ============================================================================

/// UDAF: `count_over_time` - counts timestamps in time-bucketed ranges.
///
/// # Arguments
/// 1. `timestamp`: `Timestamp(Microsecond)` - input timestamps
/// 2. `start`: `Timestamp(Microsecond)` - grid start time
/// 3. `end`: `Timestamp(Microsecond)` - grid end time
/// 4. `step`: `Interval` - step between grid points
/// 5. `range`: `Interval` - range window size
/// 6. `offset`: `Interval` - offset for range window
///
/// # Returns
/// `List<Struct { timestamp, value: UInt64 }>` (sparse)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CountOverTime {
    signature: Signature,
}

impl Default for CountOverTime {
    fn default() -> Self {
        Self::new()
    }
}

impl CountOverTime {
    /// Creates a new `CountOverTime` UDAF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(6, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CountOverTime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "count_over_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(output_type_u64())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let start = extract_timestamp_micros_from_expr(&args.exprs[1])?;
        let end = extract_timestamp_micros_from_expr(&args.exprs[2])?;
        let step = extract_interval_micros_from_expr(&args.exprs[3])?;
        let range = extract_interval_micros_from_expr(&args.exprs[4])?;
        let offset = extract_interval_micros_from_expr(&args.exprs[5])?;

        Ok(Box::new(CountOverTimeAccumulator(GridAccumulator::new(
            start, end, step, range, offset,
        )?)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        Ok(standard_state_fields())
    }
}

/// Accumulator for `count_over_time`.
#[derive(Debug)]
struct CountOverTimeAccumulator(GridAccumulator);

impl Accumulator for CountOverTimeAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let timestamps = values[0]
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected TimestampMicrosecondArray".to_string()))?;
        self.0.update_counts(timestamps);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.merge(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.0.build_sparse_u64(false)
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

// ============================================================================
// RateOverTime UDAF
// ============================================================================

/// UDAF: `rate` - counts timestamps and divides by range duration.
///
/// Same arguments as `count_over_time`, returns Float64 values.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RateOverTime {
    signature: Signature,
}

impl Default for RateOverTime {
    fn default() -> Self {
        Self::new()
    }
}

impl RateOverTime {
    /// Creates a new `RateOverTime` UDAF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(6, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for RateOverTime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "rate_over_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(output_type_f64())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let start = extract_timestamp_micros_from_expr(&args.exprs[1])?;
        let end = extract_timestamp_micros_from_expr(&args.exprs[2])?;
        let step = extract_interval_micros_from_expr(&args.exprs[3])?;
        let range = extract_interval_micros_from_expr(&args.exprs[4])?;
        let offset = extract_interval_micros_from_expr(&args.exprs[5])?;

        Ok(Box::new(RateOverTimeAccumulator(GridAccumulator::new(
            start, end, step, range, offset,
        )?)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        Ok(standard_state_fields())
    }
}

/// Accumulator for `rate_over_time`.
#[derive(Debug)]
struct RateOverTimeAccumulator(GridAccumulator);

impl Accumulator for RateOverTimeAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let timestamps = values[0]
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected TimestampMicrosecondArray".to_string()))?;
        self.0.update_counts(timestamps);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.merge(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.0.build_sparse_f64(self.0.range_seconds())
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

// ============================================================================
// BytesOverTime UDAF
// ============================================================================

/// UDAF: `bytes_over_time` - sums byte lengths of body in time ranges.
///
/// # Arguments
/// 1. `timestamp`: `Timestamp(Microsecond)`
/// 2. `body`: `Utf8` - body content to measure
/// 3. `start`, `end`, `step`, `range`, `offset`: same as `count_over_time`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BytesOverTime {
    signature: Signature,
}

impl Default for BytesOverTime {
    fn default() -> Self {
        Self::new()
    }
}

impl BytesOverTime {
    /// Creates a new `BytesOverTime` UDAF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(7, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for BytesOverTime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "bytes_over_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(output_type_u64())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Args: timestamp, body, start, end, step, range, offset
        let start = extract_timestamp_micros_from_expr(&args.exprs[2])?;
        let end = extract_timestamp_micros_from_expr(&args.exprs[3])?;
        let step = extract_interval_micros_from_expr(&args.exprs[4])?;
        let range = extract_interval_micros_from_expr(&args.exprs[5])?;
        let offset = extract_interval_micros_from_expr(&args.exprs[6])?;

        Ok(Box::new(BytesOverTimeAccumulator(GridAccumulator::new(
            start, end, step, range, offset,
        )?)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        Ok(standard_state_fields())
    }
}

/// Accumulator for `bytes_over_time`.
#[derive(Debug)]
struct BytesOverTimeAccumulator(GridAccumulator);

impl Accumulator for BytesOverTimeAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let timestamps = values[0]
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected TimestampMicrosecondArray".to_string()))?;
        let bodies = values[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected StringArray for body".to_string()))?;
        self.0.update_bytes(timestamps, bodies);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.merge(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.0.build_sparse_u64(false)
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

// ============================================================================
// BytesRate UDAF
// ============================================================================

/// UDAF: `bytes_rate` - sums byte lengths and divides by range duration.
///
/// Same arguments as `bytes_over_time`, returns Float64 values.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BytesRate {
    signature: Signature,
}

impl Default for BytesRate {
    fn default() -> Self {
        Self::new()
    }
}

impl BytesRate {
    /// Creates a new `BytesRate` UDAF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(7, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for BytesRate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "bytes_rate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(output_type_f64())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let start = extract_timestamp_micros_from_expr(&args.exprs[2])?;
        let end = extract_timestamp_micros_from_expr(&args.exprs[3])?;
        let step = extract_interval_micros_from_expr(&args.exprs[4])?;
        let range = extract_interval_micros_from_expr(&args.exprs[5])?;
        let offset = extract_interval_micros_from_expr(&args.exprs[6])?;

        Ok(Box::new(BytesRateAccumulator(GridAccumulator::new(
            start, end, step, range, offset,
        )?)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        Ok(standard_state_fields())
    }
}

/// Accumulator for `bytes_rate`.
#[derive(Debug)]
struct BytesRateAccumulator(GridAccumulator);

impl Accumulator for BytesRateAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let timestamps = values[0]
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected TimestampMicrosecondArray".to_string()))?;
        let bodies = values[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected StringArray for body".to_string()))?;
        self.0.update_bytes(timestamps, bodies);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.merge(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.0.build_sparse_f64(self.0.range_seconds())
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

// ============================================================================
// AbsentOverTime UDAF
// ============================================================================

/// UDAF: `absent_over_time` - returns 1 for time ranges with no samples.
///
/// Same arguments as `count_over_time`, but outputs only grid points where
/// count == 0, with value 1.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AbsentOverTime {
    signature: Signature,
}

impl Default for AbsentOverTime {
    fn default() -> Self {
        Self::new()
    }
}

impl AbsentOverTime {
    /// Creates a new `AbsentOverTime` UDAF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(6, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for AbsentOverTime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "absent_over_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(output_type_u64())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let start = extract_timestamp_micros_from_expr(&args.exprs[1])?;
        let end = extract_timestamp_micros_from_expr(&args.exprs[2])?;
        let step = extract_interval_micros_from_expr(&args.exprs[3])?;
        let range = extract_interval_micros_from_expr(&args.exprs[4])?;
        let offset = extract_interval_micros_from_expr(&args.exprs[5])?;

        Ok(Box::new(AbsentOverTimeAccumulator(GridAccumulator::new(
            start, end, step, range, offset,
        )?)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        Ok(standard_state_fields())
    }
}

/// Accumulator for `absent_over_time`.
#[derive(Debug)]
struct AbsentOverTimeAccumulator(GridAccumulator);

impl Accumulator for AbsentOverTimeAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let timestamps = values[0]
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected TimestampMicrosecondArray".to_string()))?;
        self.0.update_counts(timestamps);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.merge(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Invert: output only points where count == 0, with value 1
        self.0.build_sparse_u64(true)
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

// ============================================================================
// ArrayIntersectAgg UDAF
// ============================================================================

/// UDAF: `array_intersect_agg(array_column)` - aggregates multiple arrays and returns their intersection.
///
/// # Purpose
/// Used by `absent_over_time` to find grid points present in ALL inverse arrays.
/// Each row contributes a sorted array of timestamps, and the aggregate returns
/// only timestamps present in every input array.
///
/// # Arguments
/// 1. `array`: `List<Timestamp(Microsecond)>` - input arrays of timestamps
///
/// # Returns
/// `List<Timestamp(Microsecond)>` - intersection of all input arrays
///
/// # Algorithm
/// - State: Current intersection result (starts as first array, then narrows)
/// - Update: intersection = `current_state` ∩ `new_array` (two-pointer merge)
/// - Merge: intersection of partial results from different partitions
/// - Evaluate: Return final intersection array
///
/// # Complexity
/// O(n × m) where n = number of arrays, m = average array length
/// Optimized for sorted arrays using two-pointer technique
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayIntersectAgg {
    signature: Signature,
}

impl Default for ArrayIntersectAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayIntersectAgg {
    /// Creates a new `ArrayIntersectAgg` UDAF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ArrayIntersectAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_intersect_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Return same type as input (List<Timestamp>)
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArrayIntersectAccumulator::new()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        // State is a single List<Timestamp> field containing the current intersection
        Ok(vec![Arc::new(Field::new(
            "intersection",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ))),
            true,
        ))])
    }
}

/// Accumulator for `array_intersect_agg`.
#[derive(Debug)]
struct ArrayIntersectAccumulator {
    /// Current intersection result as raw i64 timestamps (None = first array not yet seen).
    current_intersection: Option<Vec<i64>>,
}

impl ArrayIntersectAccumulator {
    const fn new() -> Self {
        Self {
            current_intersection: None,
        }
    }

    /// Intersect a sorted timestamp array with a sorted Vec using two-pointer algorithm.
    /// Extracts values from the array on-demand, avoiding intermediate Vec allocation.
    /// Both inputs are assumed to be sorted (guaranteed by `date_grid` UDF output).
    fn intersect_ts_array_with_vec(ts_array: &TimestampMicrosecondArray, vec: &[i64]) -> Vec<i64> {
        let mut result = Vec::with_capacity(ts_array.len().min(vec.len()));
        let mut i = 0;
        let mut j = 0;

        while i < ts_array.len() && j < vec.len() {
            let ts_val = ts_array.value(i);
            let vec_val = vec[j];

            match ts_val.cmp(&vec_val) {
                std::cmp::Ordering::Equal => {
                    result.push(ts_val);
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
            }
        }

        result
    }

    /// Convert a Vec of timestamps to a List scalar value.
    fn vec_to_list_scalar(vec: Option<&Vec<i64>>) -> ScalarValue {
        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ));

        let ts_array = vec.map_or_else(
            || Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new())) as ArrayRef,
            |v| Arc::new(TimestampMicrosecondArray::from(v.clone())) as ArrayRef,
        );

        let offsets = OffsetBuffer::from_lengths([ts_array.len()]);
        let list_array = ListArray::new(field, offsets, ts_array, None);
        ScalarValue::List(Arc::new(list_array))
    }

    /// Helper to process a `ListArray` of timestamp arrays and update the intersection.
    /// Used by both `update_batch` and `merge_batch`.
    fn process_list_array(&mut self, list_array: &ListArray, skip_empty: bool) -> Result<()> {
        for row_idx in 0..list_array.len() {
            // Skip null rows, and optionally skip empty rows (for merge_batch)
            if list_array.is_null(row_idx) || (skip_empty && list_array.value_length(row_idx) == 0) {
                continue;
            }

            // Extract timestamp array from this row
            let timestamps_array = list_array.value(row_idx);
            let ts_array = timestamps_array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| DataFusionError::Plan("Expected TimestampMicrosecondArray".to_string()))?;

            // Update intersection
            self.current_intersection = Some(self.current_intersection.as_ref().map_or_else(
                || (0..ts_array.len()).map(|i| ts_array.value(i)).collect(),
                |current| Self::intersect_ts_array_with_vec(ts_array, current),
            ));
        }

        Ok(())
    }
}

impl Accumulator for ArrayIntersectAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        // Extract ListArray of timestamps
        let list_array = values[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected ListArray".to_string()))?;

        self.process_list_array(list_array, false)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Return the final intersection as a List scalar
        Ok(Self::vec_to_list_scalar(self.current_intersection.as_ref()))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .current_intersection
                .as_ref()
                .map_or(0, |v| std::mem::size_of::<i64>() * v.capacity())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Serialize current intersection state
        let state_value = Self::vec_to_list_scalar(self.current_intersection.as_ref());
        Ok(vec![state_value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Merge partial intersection results from different partitions
        // state[0] contains intersection arrays from other accumulators

        let list_array = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected ListArray in state".to_string()))?;

        self.process_list_array(list_array, true)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::Accumulator;

    use super::*;

    #[test]
    fn test_grid_accumulator_creation() {
        let acc = GridAccumulator::new(
            0,         // start
            1_000_000, // end (1 second)
            100_000,   // step (100ms)
            500_000,   // range (500ms)
            0,         // offset
        )
        .unwrap();

        // Should have 11 grid points: 0, 100k, 200k, ..., 1000k
        assert_eq!(acc.grid.len(), 11);
        assert_eq!(acc.values.len(), 11);
    }

    #[test]
    fn test_negative_step_rejected() {
        let result = GridAccumulator::new(0, 1_000_000, -100_000, 500_000, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_step_rejected() {
        let result = GridAccumulator::new(0, 1_000_000, 0, 500_000, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_grid_generation() {
        let acc = GridAccumulator::new(0, 500, 100, 200, 0).unwrap();

        assert_eq!(acc.grid.len(), 6);
        assert_eq!(acc.grid[0], 0);
        assert_eq!(acc.grid[1], 100);
        assert_eq!(acc.grid[5], 500);
    }

    #[test]
    fn test_update_counts_single_timestamp() {
        // Grid: [0, 100, 200], range=150, offset=0
        // A timestamp at t=50 should match grid point 100 (diff=50, 0<=50<=150)
        // and grid point 200 (diff=150, 0<=150<=150)
        // but NOT grid point 0 (diff=-50, not >= 0)
        let mut acc = GridAccumulator::new(0, 200, 100, 150, 0).unwrap();

        let timestamps = TimestampMicrosecondArray::from(vec![50i64]);
        acc.update_counts(&timestamps);

        assert_eq!(acc.values[0], 0); // grid=0, diff=-50, outside [0,150]
        assert_eq!(acc.values[1], 1); // grid=100, diff=50, inside [0,150]
        assert_eq!(acc.values[2], 1); // grid=200, diff=150, inside [0,150]
    }

    #[test]
    fn test_update_counts_multiple_timestamps() {
        // Grid: [0, 100, 200], range=100, offset=0
        let mut acc = GridAccumulator::new(0, 200, 100, 100, 0).unwrap();

        // t=0: matches grid 0 (diff=0), grid 100 (diff=100)
        // t=50: matches grid 50..150 → grid 100 (diff=50)
        // t=150: matches grid 150..250 → grid 200 (diff=50)
        let timestamps = TimestampMicrosecondArray::from(vec![0i64, 50, 150]);
        acc.update_counts(&timestamps);

        assert_eq!(acc.values[0], 1); // only t=0 matches
        assert_eq!(acc.values[1], 2); // t=0 (diff=100) and t=50 (diff=50)
        assert_eq!(acc.values[2], 1); // only t=150 matches (diff=50)
    }

    #[test]
    fn test_update_counts_with_offset() {
        // Grid: [0, 100, 200], range=50, offset=25
        // Range check: 25 <= (grid - t) <= 75
        let mut acc = GridAccumulator::new(0, 200, 100, 50, 25).unwrap();

        // t=50: grid 0 → diff=-50 (no), grid 100 → diff=50 (yes), grid 200 → diff=150
        // (no)
        let timestamps = TimestampMicrosecondArray::from(vec![50i64]);
        acc.update_counts(&timestamps);

        assert_eq!(acc.values[0], 0);
        assert_eq!(acc.values[1], 1);
        assert_eq!(acc.values[2], 0);
    }

    #[test]
    fn test_update_counts_empty() {
        let mut acc = GridAccumulator::new(0, 200, 100, 100, 0).unwrap();

        let timestamps = TimestampMicrosecondArray::from(Vec::<i64>::new());
        acc.update_counts(&timestamps);

        assert_eq!(acc.values[0], 0);
        assert_eq!(acc.values[1], 0);
        assert_eq!(acc.values[2], 0);
    }

    #[test]
    fn test_update_counts_outside_all_ranges() {
        // Grid: [1000, 2000, 3000], range=100, offset=0
        let mut acc = GridAccumulator::new(1000, 3000, 1000, 100, 0).unwrap();

        // t=0: all grid points have diff > 100, so no matches
        let timestamps = TimestampMicrosecondArray::from(vec![0i64]);
        acc.update_counts(&timestamps);

        assert_eq!(acc.values[0], 0);
        assert_eq!(acc.values[1], 0);
        assert_eq!(acc.values[2], 0);
    }

    #[test]
    fn test_boundary_conditions() {
        // Grid: [100], range=50, offset=0 → accepts timestamps where 0 <= (100-t) <= 50
        // i.e., t in [50, 100]
        let mut acc = GridAccumulator::new(100, 100, 100, 50, 0).unwrap();

        assert_eq!(acc.grid.len(), 1);

        // t=49: diff=51, outside range
        // t=50: diff=50, exactly at upper bound (inclusive)
        // t=100: diff=0, exactly at lower bound (inclusive)
        // t=101: diff=-1, outside range
        let timestamps = TimestampMicrosecondArray::from(vec![49i64, 50, 100, 101]);
        acc.update_counts(&timestamps);

        assert_eq!(acc.values[0], 2); // t=50 and t=100 match
    }

    #[test]
    fn test_merge() {
        let mut acc1 = GridAccumulator::new(0, 200, 100, 100, 0).unwrap();
        let mut acc2 = GridAccumulator::new(0, 200, 100, 100, 0).unwrap();

        let ts1 = TimestampMicrosecondArray::from(vec![0i64]);
        acc1.update_counts(&ts1);

        let ts2 = TimestampMicrosecondArray::from(vec![100i64]);
        acc2.update_counts(&ts2);

        let state = acc2.state().unwrap();
        let grid_list = match &state[0] {
            ScalarValue::List(arr) => arr.clone(),
            _ => panic!("Expected List for grid"),
        };
        let values_list = match &state[1] {
            ScalarValue::List(arr) => arr.clone(),
            _ => panic!("Expected List for values"),
        };

        acc1.merge(&[grid_list as ArrayRef, values_list as ArrayRef]).unwrap();

        // acc1 after ts=0: [1, 1, 0]
        // acc2 after ts=100: [0, 1, 1]
        // merged: [1, 2, 1]
        assert_eq!(acc1.values[0], 1);
        assert_eq!(acc1.values[1], 2);
        assert_eq!(acc1.values[2], 1);
    }

    #[test]
    fn test_count_over_time_accumulator() {
        let mut acc = CountOverTimeAccumulator(GridAccumulator::new(0, 200, 100, 100, 0).unwrap());

        let timestamps = TimestampMicrosecondArray::from(vec![50i64]);
        acc.update_batch(&[Arc::new(timestamps) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                assert_eq!(list_arr.len(), 1);
                let inner = list_arr.value(0);
                let struct_arr = inner.as_any().downcast_ref::<StructArray>().expect("Expected StructArray");
                assert_eq!(struct_arr.num_columns(), 2);
                // Sparse: only 1 grid point has count > 0
                assert_eq!(struct_arr.len(), 1);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_state_serialization() {
        let acc = GridAccumulator::new(0, 200, 100, 50, 25).unwrap();
        let state = acc.state().unwrap();

        assert_eq!(state.len(), 4);

        match (&state[2], &state[3]) {
            (ScalarValue::Int64(Some(range)), ScalarValue::Int64(Some(offset))) => {
                assert_eq!(*range, 50);
                assert_eq!(*offset, 25);
            }
            _ => panic!("Expected Int64 values for range and offset"),
        }
    }

    #[test]
    fn test_size_calculation() {
        let acc = GridAccumulator::new(0, 1000, 100, 100, 0).unwrap();
        let size = acc.size();

        assert!(size > 0);
        assert!(size > std::mem::size_of::<GridAccumulator>());
    }

    #[test]
    fn test_update_bytes() {
        let mut acc = GridAccumulator::new(0, 200, 100, 150, 0).unwrap();

        let timestamps = TimestampMicrosecondArray::from(vec![50i64, 50]);
        let bodies = StringArray::from(vec!["hello", "world"]); // 5 + 5 = 10 bytes each match

        acc.update_bytes(&timestamps, &bodies);

        // Both timestamps match grid points 100 and 200
        assert_eq!(acc.values[0], 0);
        assert_eq!(acc.values[1], 10); // 5 + 5
        assert_eq!(acc.values[2], 10); // 5 + 5
    }

    #[test]
    fn test_build_sparse_f64() {
        let mut acc = GridAccumulator::new(0, 200, 100, 100, 0).unwrap();
        acc.values = vec![0, 300, 0]; // Only middle has value

        let result = acc.build_sparse_f64(100.0).unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let inner = list_arr.value(0);
                let struct_arr = inner.as_any().downcast_ref::<StructArray>().expect("Expected StructArray");
                assert_eq!(struct_arr.len(), 1);

                let values = struct_arr.column(1);
                let float_arr = values.as_any().downcast_ref::<Float64Array>().unwrap();
                assert!((float_arr.value(0) - 3.0).abs() < f64::EPSILON); // 300
                // / 100
                // = 3.
                // 0
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_absent_over_time_invert() {
        let mut acc = AbsentOverTimeAccumulator(GridAccumulator::new(0, 200, 100, 100, 0).unwrap());

        // No timestamps - all grid points should be absent
        let timestamps = TimestampMicrosecondArray::from(Vec::<i64>::new());
        acc.update_batch(&[Arc::new(timestamps) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let inner = list_arr.value(0);
                let struct_arr = inner.as_any().downcast_ref::<StructArray>().expect("Expected StructArray");
                // All 3 grid points should be absent (value=1)
                assert_eq!(struct_arr.len(), 3);

                let values = struct_arr.column(1);
                let u64_arr = values.as_any().downcast_ref::<UInt64Array>().unwrap();
                assert_eq!(u64_arr.value(0), 1);
                assert_eq!(u64_arr.value(1), 1);
                assert_eq!(u64_arr.value(2), 1);
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_absent_over_time_partial_coverage() {
        // Test with some timestamps present - some grid points should be absent
        let mut acc = AbsentOverTimeAccumulator(GridAccumulator::new(0, 300, 100, 100, 0).unwrap());

        // Grid: [0, 100, 200, 300]
        // Add timestamp at 50 with range 100:
        //   - Coverage window: [50, 150]
        //   - Matches grid point: 100 (only point in [50, 150])
        // Grid points 0, 200, and 300 should be absent (not covered)
        let timestamps = TimestampMicrosecondArray::from(vec![50i64]);
        acc.update_batch(&[Arc::new(timestamps) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let inner = list_arr.value(0);
                let struct_arr = inner.as_any().downcast_ref::<StructArray>().expect("Expected StructArray");
                // Grid points 0, 200, and 300 should be absent
                assert_eq!(struct_arr.len(), 3);

                let ts_col = struct_arr.column(0);
                let ts_arr = ts_col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                assert_eq!(ts_arr.value(0), 0);
                assert_eq!(ts_arr.value(1), 200);
                assert_eq!(ts_arr.value(2), 300);

                let values = struct_arr.column(1);
                let u64_arr = values.as_any().downcast_ref::<UInt64Array>().unwrap();
                assert_eq!(u64_arr.value(0), 1);
                assert_eq!(u64_arr.value(1), 1);
                assert_eq!(u64_arr.value(2), 1);
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_absent_over_time_full_coverage() {
        // Test with all grid points covered - no absent points
        let mut acc = AbsentOverTimeAccumulator(GridAccumulator::new(0, 200, 100, 100, 0).unwrap());

        // Add timestamps that cover all grid points
        let timestamps = TimestampMicrosecondArray::from(vec![0i64, 100, 200]);
        acc.update_batch(&[Arc::new(timestamps) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let inner = list_arr.value(0);
                let struct_arr = inner.as_any().downcast_ref::<StructArray>().expect("Expected StructArray");
                // No absent points
                assert_eq!(struct_arr.len(), 0);
            }
            _ => panic!("Expected List"),
        }
    }

    // ArrayIntersectAgg UDAF tests

    #[test]
    fn test_array_intersect_agg_two_arrays() {
        // Test intersection of two arrays
        let mut acc = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // First array: [100, 200, 300]
        let arr1 = TimestampMicrosecondArray::from(vec![100i64, 200, 300]);
        let list1 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr1.len()]),
            Arc::new(arr1) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list1) as ArrayRef]).unwrap();

        // Second array: [200, 300, 400]
        let arr2 = TimestampMicrosecondArray::from(vec![200i64, 300, 400]);
        let list2 = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr2.len()]),
            Arc::new(arr2) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list2) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Intersection should be [200, 300]
                assert_eq!(ts_arr.len(), 2);
                assert_eq!(ts_arr.value(0), 200);
                assert_eq!(ts_arr.value(1), 300);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_three_arrays() {
        // Test intersection of three arrays
        let mut acc = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // Array 1: [100, 200, 300, 400]
        let arr1 = TimestampMicrosecondArray::from(vec![100i64, 200, 300, 400]);
        let list1 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr1.len()]),
            Arc::new(arr1) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list1) as ArrayRef]).unwrap();

        // Array 2: [200, 300, 400, 500]
        let arr2 = TimestampMicrosecondArray::from(vec![200i64, 300, 400, 500]);
        let list2 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr2.len()]),
            Arc::new(arr2) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list2) as ArrayRef]).unwrap();

        // Array 3: [300, 400, 500, 600]
        let arr3 = TimestampMicrosecondArray::from(vec![300i64, 400, 500, 600]);
        let list3 = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr3.len()]),
            Arc::new(arr3) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list3) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Intersection of all three should be [300, 400]
                assert_eq!(ts_arr.len(), 2);
                assert_eq!(ts_arr.value(0), 300);
                assert_eq!(ts_arr.value(1), 400);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_no_intersection() {
        // Test arrays with no common elements
        let mut acc = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // Array 1: [100, 200]
        let arr1 = TimestampMicrosecondArray::from(vec![100i64, 200]);
        let list1 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr1.len()]),
            Arc::new(arr1) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list1) as ArrayRef]).unwrap();

        // Array 2: [300, 400]
        let arr2 = TimestampMicrosecondArray::from(vec![300i64, 400]);
        let list2 = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr2.len()]),
            Arc::new(arr2) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list2) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // No intersection
                assert_eq!(ts_arr.len(), 0);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_single_array() {
        // Test with only one array - should return that array
        let mut acc = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        let arr = TimestampMicrosecondArray::from(vec![100i64, 200, 300]);
        let list = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr.len()]),
            Arc::new(arr) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Should return the same array
                assert_eq!(ts_arr.len(), 3);
                assert_eq!(ts_arr.value(0), 100);
                assert_eq!(ts_arr.value(1), 200);
                assert_eq!(ts_arr.value(2), 300);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_empty_array() {
        // Test with empty arrays
        let mut acc = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // Empty array
        let arr = TimestampMicrosecondArray::from(Vec::<i64>::new());
        let list = ListArray::new(field, OffsetBuffer::from_lengths([0]), Arc::new(arr) as ArrayRef, None);
        acc.update_batch(&[Arc::new(list) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Empty result
                assert_eq!(ts_arr.len(), 0);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_merge_batch() {
        // Test merge_batch for distributed execution
        let mut acc1 = ArrayIntersectAccumulator::new();
        let mut acc2 = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // Acc1: intersection of [100, 200, 300] and [200, 300, 400] = [200, 300]
        let arr1 = TimestampMicrosecondArray::from(vec![100i64, 200, 300]);
        let list1 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr1.len()]),
            Arc::new(arr1) as ArrayRef,
            None,
        );
        acc1.update_batch(&[Arc::new(list1) as ArrayRef]).unwrap();

        let arr2 = TimestampMicrosecondArray::from(vec![200i64, 300, 400]);
        let list2 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr2.len()]),
            Arc::new(arr2) as ArrayRef,
            None,
        );
        acc1.update_batch(&[Arc::new(list2) as ArrayRef]).unwrap();

        // Acc2: intersection of [200, 300, 500] and [200, 300, 600] = [200, 300]
        let arr3 = TimestampMicrosecondArray::from(vec![200i64, 300, 500]);
        let list3 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr3.len()]),
            Arc::new(arr3) as ArrayRef,
            None,
        );
        acc2.update_batch(&[Arc::new(list3) as ArrayRef]).unwrap();

        let arr4 = TimestampMicrosecondArray::from(vec![200i64, 300, 600]);
        let list4 = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr4.len()]),
            Arc::new(arr4) as ArrayRef,
            None,
        );
        acc2.update_batch(&[Arc::new(list4) as ArrayRef]).unwrap();

        // Get state from acc2 and merge into acc1
        let state = acc2.state().unwrap();
        let state_list = match &state[0] {
            ScalarValue::List(arr) => arr.clone(),
            _ => panic!("Expected List state"),
        };

        acc1.merge_batch(&[state_list as ArrayRef]).unwrap();

        let result = acc1.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Final intersection: [200, 300] ∩ [200, 300] = [200, 300]
                assert_eq!(ts_arr.len(), 2);
                assert_eq!(ts_arr.value(0), 200);
                assert_eq!(ts_arr.value(1), 300);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }
}
