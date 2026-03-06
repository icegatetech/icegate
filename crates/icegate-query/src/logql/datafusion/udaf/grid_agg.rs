//! Grid-bucketed aggregation UDAF for LogQL range aggregations.
//!
//! Instead of materializing `N_logs * M_grid_points` intermediate rows (via unnest)
//! before aggregation, this UDAF accumulates values directly into grid buckets.
//! The unnest happens post-aggregation where data is already reduced.
//!
//! **Before:** N logs → unnest → N×M rows → GROUP BY → result
//! **After:**  N logs → GROUP BY labels → UDAF buckets internally → unnest → G rows per group

use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Float64Array, Int64Array, ListArray},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field},
    },
    common::{Result, ScalarValue},
    error::DataFusionError,
    logical_expr::{
        Accumulator, AggregateUDFImpl, Signature, Volatility,
        function::{AccumulatorArgs, StateFieldsArgs},
    },
};

use super::super::udf::date_grid::{compute_grid_points, find_matching_grid_indices};

/// Aggregation operation to perform within each grid bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GridAggOp {
    /// `count_over_time`, `rate` — count of log entries per bucket.
    Count,
    /// `sum_over_time`, `bytes_over_time`, `bytes_rate` — sum of values per bucket.
    Sum,
    /// `avg_over_time` — average of values per bucket.
    Avg,
    /// `min_over_time` — minimum value per bucket.
    Min,
    /// `max_over_time` — maximum value per bucket.
    Max,
    /// `stddev_over_time` — population standard deviation per bucket (Welford's algorithm).
    Stddev,
    /// `stdvar_over_time` — population variance per bucket (Welford's algorithm).
    Stdvar,
    /// `first_over_time` — first value by timestamp per bucket.
    First,
    /// `last_over_time` — last value by timestamp per bucket.
    Last,
    /// `quantile_over_time(phi)` — quantile of values per bucket.
    Quantile(OrderedFloat),
    /// `rate_counter` — counter reset detection per bucket.
    RateCounter,
}

/// Wrapper for f64 that implements Eq + Hash for use in enum variants.
///
/// `NaN` is treated as equal to `NaN` for hashing/comparison purposes.
#[derive(Debug, Clone, Copy)]
pub struct OrderedFloat(pub f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat {}

impl std::hash::Hash for OrderedFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

/// Shared grid configuration for all accumulators.
#[derive(Debug, Clone)]
struct GridContext {
    /// Precomputed grid points in microseconds.
    grid: Arc<Vec<i64>>,
    /// Range window size in microseconds.
    range_micros: i64,
    /// Offset for the range window in microseconds.
    offset_micros: i64,
}

/// Grid-bucketed aggregation UDAF.
///
/// Accumulates values directly into grid-aligned time buckets without
/// materializing the cross-product of logs and grid points.
///
/// # Input Signature
/// - Count: `(Timestamp)` — only needs timestamps to count
/// - All others: `(Timestamp, Float64)` — timestamp + value to aggregate
///
/// # Return Type
/// `List<Float64>` — one value per grid point, NULL for empty buckets
#[derive(Debug)]
pub struct GridAgg {
    op: GridAggOp,
    grid: Arc<Vec<i64>>,
    range_micros: i64,
    offset_micros: i64,
    signature: Signature,
}

impl GridAgg {
    /// Creates a new `GridAgg` UDAF.
    ///
    /// # Arguments
    /// * `op` - The aggregation operation to perform
    /// * `start_micros` - Grid start timestamp in microseconds
    /// * `end_micros` - Grid end timestamp in microseconds
    /// * `step_micros` - Step interval in microseconds
    /// * `range_micros` - Range window size in microseconds
    /// * `offset_micros` - Offset for the range window in microseconds
    /// * `max_grid_points` - Maximum allowed grid points to prevent excessive memory allocation
    ///
    /// # Errors
    /// Returns error if grid computation fails (invalid parameters).
    pub fn new(
        op: GridAggOp,
        start_micros: i64,
        end_micros: i64,
        step_micros: i64,
        range_micros: i64,
        offset_micros: i64,
        max_grid_points: i64,
    ) -> Result<Self> {
        let grid = compute_grid_points(start_micros, end_micros, step_micros, max_grid_points)?;

        // Count only needs timestamp; all others need (timestamp, value)
        let num_args = if matches!(op, GridAggOp::Count) { 1 } else { 2 };

        Ok(Self {
            op,
            grid: Arc::new(grid),
            range_micros,
            offset_micros,
            signature: Signature::any(num_args, Volatility::Immutable),
        })
    }

    /// Returns the precomputed grid points.
    pub fn grid_points(&self) -> &[i64] {
        &self.grid
    }
}

impl AggregateUDFImpl for GridAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "grid_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new("item", DataType::Float64, true))))
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let ctx = GridContext {
            grid: Arc::clone(&self.grid),
            range_micros: self.range_micros,
            offset_micros: self.offset_micros,
        };

        let acc: Box<dyn Accumulator> = match &self.op {
            GridAggOp::Count => Box::new(CountGridAccumulator::new(ctx)),
            GridAggOp::Sum => Box::new(SumGridAccumulator::new(ctx)),
            GridAggOp::Avg => Box::new(AvgGridAccumulator::new(ctx)),
            GridAggOp::Min => Box::new(MinGridAccumulator::new(ctx)),
            GridAggOp::Max => Box::new(MaxGridAccumulator::new(ctx)),
            GridAggOp::Stddev => Box::new(WelfordGridAccumulator::new(ctx, false)),
            GridAggOp::Stdvar => Box::new(WelfordGridAccumulator::new(ctx, true)),
            GridAggOp::First => Box::new(FirstGridAccumulator::new(ctx)),
            GridAggOp::Last => Box::new(LastGridAccumulator::new(ctx)),
            GridAggOp::Quantile(phi) => Box::new(QuantileGridAccumulator::new(ctx, phi.0)),
            GridAggOp::RateCounter => Box::new(RateCounterGridAccumulator::new(ctx)),
        };

        Ok(acc)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        // State is serialized as List<Float64> arrays — specific fields depend on the op.
        // For simplicity, we use a fixed set of state fields that all variants can use:
        // - field 0: List<Float64> for primary values per bucket
        // - field 1: List<Int64> for counts/timestamps per bucket
        // - field 2: List<Float64> for secondary values per bucket (Welford m2, quantile values, etc.)
        Ok(vec![
            Arc::new(Field::new(
                "grid_values",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )),
            Arc::new(Field::new(
                "grid_counts",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            )),
            Arc::new(Field::new(
                "grid_secondary",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            )),
        ])
    }
}

// Required for DataFusion UDAF hashing
impl PartialEq for GridAgg {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op
            && *self.grid == *other.grid
            && self.range_micros == other.range_micros
            && self.offset_micros == other.offset_micros
    }
}

impl Eq for GridAgg {}

impl std::hash::Hash for GridAgg {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.op).hash(state);
        self.grid.hash(state);
        self.range_micros.hash(state);
        self.offset_micros.hash(state);
    }
}

// ============================================================================
// Helper functions for state serialization
// ============================================================================

/// Build a `List<Float64>` scalar from a `Vec<Option<f64>>`.
fn f64_vec_to_list_scalar(values: &[Option<f64>]) -> ScalarValue {
    let field = Arc::new(Field::new("item", DataType::Float64, true));
    let array: Float64Array = values.iter().copied().collect();
    let offsets = OffsetBuffer::from_lengths([array.len()]);
    let list = ListArray::new(field, offsets, Arc::new(array), None);
    ScalarValue::List(Arc::new(list))
}

/// Build a `List<Int64>` scalar from a `Vec<Option<i64>>`.
fn i64_vec_to_list_scalar(values: &[Option<i64>]) -> ScalarValue {
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let array: Int64Array = values.iter().copied().collect();
    let offsets = OffsetBuffer::from_lengths([array.len()]);
    let list = ListArray::new(field, offsets, Arc::new(array), None);
    ScalarValue::List(Arc::new(list))
}

/// Extract `Vec<Option<f64>>` from a `ListArray` at a given row.
///
/// # Errors
/// Returns error if the inner array is not `Float64Array`.
fn extract_f64_list(list_array: &ListArray, row: usize) -> Result<Vec<Option<f64>>> {
    let values = list_array.value(row);
    let f64_array = values
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Execution("Expected Float64Array in state".to_string()))?;
    Ok((0..f64_array.len())
        .map(|i| {
            if f64_array.is_null(i) {
                None
            } else {
                Some(f64_array.value(i))
            }
        })
        .collect())
}

/// Extract `Vec<Option<i64>>` from a `ListArray` at a given row.
///
/// # Errors
/// Returns error if the inner array is not `Int64Array`.
fn extract_i64_list(list_array: &ListArray, row: usize) -> Result<Vec<Option<i64>>> {
    let values = list_array.value(row);
    let i64_array = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Execution("Expected Int64Array in state".to_string()))?;
    Ok((0..i64_array.len())
        .map(|i| {
            if i64_array.is_null(i) {
                None
            } else {
                Some(i64_array.value(i))
            }
        })
        .collect())
}

/// Extract timestamp values from `TimestampMicrosecondArray` column.
fn extract_timestamps(values: &[ArrayRef]) -> Result<&datafusion::arrow::array::TimestampMicrosecondArray> {
    values[0]
        .as_any()
        .downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>()
        .ok_or_else(|| DataFusionError::Execution("Expected TimestampMicrosecondArray for first argument".to_string()))
}

/// Extract Float64 values from the second argument column.
fn extract_values(values: &[ArrayRef]) -> Result<&Float64Array> {
    values[1]
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Execution("Expected Float64Array for second argument".to_string()))
}

/// Build the final `List<Float64>` result from per-bucket `Option<f64>`.
fn build_result_list(bucket_values: &[Option<f64>]) -> ScalarValue {
    f64_vec_to_list_scalar(bucket_values)
}

// ============================================================================
// Count accumulator
// ============================================================================

/// Accumulator for `count_over_time` / `rate`.
///
/// State per bucket: `i64` count of log entries.
#[derive(Debug)]
struct CountGridAccumulator {
    ctx: GridContext,
    /// Count per grid bucket.
    counts: Vec<i64>,
}

impl CountGridAccumulator {
    fn new(ctx: GridContext) -> Self {
        let n = ctx.grid.len();
        Self {
            ctx,
            counts: vec![0; n],
        }
    }
}

impl Accumulator for CountGridAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let timestamps = extract_timestamps(values)?;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            let (start_idx, end_idx) =
                find_matching_grid_indices(ts, &self.ctx.grid, self.ctx.range_micros, self.ctx.offset_micros);
            for bucket in &mut self.counts[start_idx..end_idx] {
                *bucket += 1;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<Option<f64>> = self
            .counts
            .iter()
            .map(|&c| {
                if c == 0 {
                    None
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    Some(c as f64)
                }
            })
            .collect();
        Ok(build_result_list(&values))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.counts.capacity() * std::mem::size_of::<i64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Primary: empty (not used), Counts: the counts, Secondary: empty
        let empty_f64 = f64_vec_to_list_scalar(&[]);
        let counts: Vec<Option<i64>> = self.counts.iter().map(|&c| Some(c)).collect();
        let counts_scalar = i64_vec_to_list_scalar(&counts);
        Ok(vec![empty_f64.clone(), counts_scalar, empty_f64])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for counts state".to_string()))?;

        for row in 0..counts_list.len() {
            if counts_list.is_null(row) {
                continue;
            }
            let other_counts = extract_i64_list(counts_list, row)?;
            for (i, c) in other_counts.into_iter().enumerate() {
                if let Some(c) = c {
                    if i < self.counts.len() {
                        self.counts[i] += c;
                    }
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for CountGridAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountGridAccumulator")
    }
}

// ============================================================================
// Sum accumulator
// ============================================================================

/// Accumulator for `sum_over_time` / `bytes_over_time` / `bytes_rate`.
///
/// State per bucket: `f64` sum + `i64` count.
#[derive(Debug)]
struct SumGridAccumulator {
    ctx: GridContext,
    sums: Vec<f64>,
    counts: Vec<i64>,
}

impl SumGridAccumulator {
    fn new(ctx: GridContext) -> Self {
        let n = ctx.grid.len();
        Self {
            ctx,
            sums: vec![0.0; n],
            counts: vec![0; n],
        }
    }
}

impl Accumulator for SumGridAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let timestamps = extract_timestamps(values)?;
        let vals = extract_values(values)?;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || vals.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            let val = vals.value(i);
            let (start_idx, end_idx) =
                find_matching_grid_indices(ts, &self.ctx.grid, self.ctx.range_micros, self.ctx.offset_micros);
            for j in start_idx..end_idx {
                self.sums[j] += val;
                self.counts[j] += 1;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<Option<f64>> = self
            .counts
            .iter()
            .zip(self.sums.iter())
            .map(|(&c, &s)| if c == 0 { None } else { Some(s) })
            .collect();
        Ok(build_result_list(&values))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.sums.capacity() * std::mem::size_of::<f64>()
            + self.counts.capacity() * std::mem::size_of::<i64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sums: Vec<Option<f64>> = self.sums.iter().map(|&s| Some(s)).collect();
        let counts: Vec<Option<i64>> = self.counts.iter().map(|&c| Some(c)).collect();
        Ok(vec![
            f64_vec_to_list_scalar(&sums),
            i64_vec_to_list_scalar(&counts),
            f64_vec_to_list_scalar(&[]),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let sums_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for sums state".to_string()))?;
        let counts_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for counts state".to_string()))?;

        for row in 0..sums_list.len() {
            if sums_list.is_null(row) {
                continue;
            }
            let other_sums = extract_f64_list(sums_list, row)?;
            let other_counts = extract_i64_list(counts_list, row)?;
            for (i, (s, c)) in other_sums.into_iter().zip(other_counts).enumerate() {
                if let (Some(s), Some(c)) = (s, c) {
                    if i < self.sums.len() {
                        self.sums[i] += s;
                        self.counts[i] += c;
                    }
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for SumGridAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SumGridAccumulator")
    }
}

// ============================================================================
// Avg accumulator
// ============================================================================

/// Accumulator for `avg_over_time`.
///
/// State per bucket: `f64` sum + `i64` count. Evaluate: sum/count.
#[derive(Debug)]
struct AvgGridAccumulator {
    ctx: GridContext,
    sums: Vec<f64>,
    counts: Vec<i64>,
}

impl AvgGridAccumulator {
    fn new(ctx: GridContext) -> Self {
        let n = ctx.grid.len();
        Self {
            ctx,
            sums: vec![0.0; n],
            counts: vec![0; n],
        }
    }
}

impl Accumulator for AvgGridAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let timestamps = extract_timestamps(values)?;
        let vals = extract_values(values)?;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || vals.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            let val = vals.value(i);
            let (start_idx, end_idx) =
                find_matching_grid_indices(ts, &self.ctx.grid, self.ctx.range_micros, self.ctx.offset_micros);
            for j in start_idx..end_idx {
                self.sums[j] += val;
                self.counts[j] += 1;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<Option<f64>> = self
            .counts
            .iter()
            .zip(self.sums.iter())
            .map(|(&c, &s)| {
                if c == 0 {
                    None
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    Some(s / c as f64)
                }
            })
            .collect();
        Ok(build_result_list(&values))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.sums.capacity() * std::mem::size_of::<f64>()
            + self.counts.capacity() * std::mem::size_of::<i64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sums: Vec<Option<f64>> = self.sums.iter().map(|&s| Some(s)).collect();
        let counts: Vec<Option<i64>> = self.counts.iter().map(|&c| Some(c)).collect();
        Ok(vec![
            f64_vec_to_list_scalar(&sums),
            i64_vec_to_list_scalar(&counts),
            f64_vec_to_list_scalar(&[]),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let sums_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for sums state".to_string()))?;
        let counts_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for counts state".to_string()))?;

        for row in 0..sums_list.len() {
            if sums_list.is_null(row) {
                continue;
            }
            let other_sums = extract_f64_list(sums_list, row)?;
            let other_counts = extract_i64_list(counts_list, row)?;
            for (i, (s, c)) in other_sums.into_iter().zip(other_counts).enumerate() {
                if let (Some(s), Some(c)) = (s, c) {
                    if i < self.sums.len() {
                        self.sums[i] += s;
                        self.counts[i] += c;
                    }
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for AvgGridAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AvgGridAccumulator")
    }
}

// ============================================================================
// Min accumulator
// ============================================================================

/// Accumulator for `min_over_time`.
///
/// State per bucket: `f64` min + `bool` `has_value`.
#[derive(Debug)]
struct MinGridAccumulator {
    ctx: GridContext,
    mins: Vec<f64>,
    has_value: Vec<bool>,
}

impl MinGridAccumulator {
    fn new(ctx: GridContext) -> Self {
        let n = ctx.grid.len();
        Self {
            ctx,
            mins: vec![f64::INFINITY; n],
            has_value: vec![false; n],
        }
    }
}

impl Accumulator for MinGridAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let timestamps = extract_timestamps(values)?;
        let vals = extract_values(values)?;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || vals.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            let val = vals.value(i);
            let (start_idx, end_idx) =
                find_matching_grid_indices(ts, &self.ctx.grid, self.ctx.range_micros, self.ctx.offset_micros);
            for j in start_idx..end_idx {
                if val < self.mins[j] {
                    self.mins[j] = val;
                }
                self.has_value[j] = true;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<Option<f64>> = self
            .has_value
            .iter()
            .zip(self.mins.iter())
            .map(|(&has, &m)| if has { Some(m) } else { None })
            .collect();
        Ok(build_result_list(&values))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.mins.capacity() * std::mem::size_of::<f64>()
            + self.has_value.capacity() * std::mem::size_of::<bool>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mins: Vec<Option<f64>> = self
            .has_value
            .iter()
            .zip(self.mins.iter())
            .map(|(&has, &m)| if has { Some(m) } else { None })
            .collect();
        let has_vals: Vec<Option<i64>> = self.has_value.iter().map(|&h| Some(i64::from(h))).collect();
        Ok(vec![
            f64_vec_to_list_scalar(&mins),
            i64_vec_to_list_scalar(&has_vals),
            f64_vec_to_list_scalar(&[]),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let mins_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for mins state".to_string()))?;
        let has_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for has_value state".to_string()))?;

        for row in 0..mins_list.len() {
            if mins_list.is_null(row) {
                continue;
            }
            let other_mins = extract_f64_list(mins_list, row)?;
            let other_has = extract_i64_list(has_list, row)?;
            for (i, (m, h)) in other_mins.into_iter().zip(other_has).enumerate() {
                if let (Some(m), Some(h)) = (m, h) {
                    if i < self.mins.len() && h != 0 {
                        if m < self.mins[i] || !self.has_value[i] {
                            self.mins[i] = m;
                        }
                        self.has_value[i] = true;
                    }
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for MinGridAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MinGridAccumulator")
    }
}

// ============================================================================
// Max accumulator
// ============================================================================

/// Accumulator for `max_over_time`.
///
/// State per bucket: `f64` max + `bool` `has_value`.
#[derive(Debug)]
struct MaxGridAccumulator {
    ctx: GridContext,
    maxs: Vec<f64>,
    has_value: Vec<bool>,
}

impl MaxGridAccumulator {
    fn new(ctx: GridContext) -> Self {
        let n = ctx.grid.len();
        Self {
            ctx,
            maxs: vec![f64::NEG_INFINITY; n],
            has_value: vec![false; n],
        }
    }
}

impl Accumulator for MaxGridAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let timestamps = extract_timestamps(values)?;
        let vals = extract_values(values)?;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || vals.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            let val = vals.value(i);
            let (start_idx, end_idx) =
                find_matching_grid_indices(ts, &self.ctx.grid, self.ctx.range_micros, self.ctx.offset_micros);
            for j in start_idx..end_idx {
                if val > self.maxs[j] {
                    self.maxs[j] = val;
                }
                self.has_value[j] = true;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<Option<f64>> = self
            .has_value
            .iter()
            .zip(self.maxs.iter())
            .map(|(&has, &m)| if has { Some(m) } else { None })
            .collect();
        Ok(build_result_list(&values))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.maxs.capacity() * std::mem::size_of::<f64>()
            + self.has_value.capacity() * std::mem::size_of::<bool>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let maxs: Vec<Option<f64>> = self
            .has_value
            .iter()
            .zip(self.maxs.iter())
            .map(|(&has, &m)| if has { Some(m) } else { None })
            .collect();
        let has_vals: Vec<Option<i64>> = self.has_value.iter().map(|&h| Some(i64::from(h))).collect();
        Ok(vec![
            f64_vec_to_list_scalar(&maxs),
            i64_vec_to_list_scalar(&has_vals),
            f64_vec_to_list_scalar(&[]),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let maxs_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for maxs state".to_string()))?;
        let has_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for has_value state".to_string()))?;

        for row in 0..maxs_list.len() {
            if maxs_list.is_null(row) {
                continue;
            }
            let other_maxs = extract_f64_list(maxs_list, row)?;
            let other_has = extract_i64_list(has_list, row)?;
            for (i, (m, h)) in other_maxs.into_iter().zip(other_has).enumerate() {
                if let (Some(m), Some(h)) = (m, h) {
                    if i < self.maxs.len() && h != 0 {
                        if m > self.maxs[i] || !self.has_value[i] {
                            self.maxs[i] = m;
                        }
                        self.has_value[i] = true;
                    }
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for MaxGridAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MaxGridAccumulator")
    }
}

// ============================================================================
// Welford accumulator (stddev / stdvar)
// ============================================================================

/// Accumulator for `stddev_over_time` / `stdvar_over_time` using Welford's online algorithm.
///
/// State per bucket: `i64` count + `f64` mean + `f64` m2.
/// Merge uses Chan et al. parallel combination formula.
///
/// Note: Uses population stddev/stdvar (divide by n) to match Prometheus/Loki behavior,
/// not sample stddev (divide by n-1).
#[derive(Debug)]
struct WelfordGridAccumulator {
    ctx: GridContext,
    counts: Vec<i64>,
    means: Vec<f64>,
    m2s: Vec<f64>,
    /// If true, evaluate returns variance; if false, returns stddev.
    is_variance: bool,
}

impl WelfordGridAccumulator {
    fn new(ctx: GridContext, is_variance: bool) -> Self {
        let n = ctx.grid.len();
        Self {
            ctx,
            counts: vec![0; n],
            means: vec![0.0; n],
            m2s: vec![0.0; n],
            is_variance,
        }
    }
}

impl Accumulator for WelfordGridAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let timestamps = extract_timestamps(values)?;
        let vals = extract_values(values)?;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || vals.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            let val = vals.value(i);
            let (start_idx, end_idx) =
                find_matching_grid_indices(ts, &self.ctx.grid, self.ctx.range_micros, self.ctx.offset_micros);
            // Welford's online algorithm for each matching bucket
            for j in start_idx..end_idx {
                self.counts[j] += 1;
                let delta = val - self.means[j];
                #[allow(clippy::cast_precision_loss)]
                let new_mean = self.means[j] + delta / self.counts[j] as f64;
                let delta2 = val - new_mean;
                self.m2s[j] += delta * delta2;
                self.means[j] = new_mean;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<Option<f64>> = self
            .counts
            .iter()
            .zip(self.m2s.iter())
            .map(|(&c, &m2)| {
                // Population stddev/stdvar: need at least 1 sample (Prometheus uses n, not n-1)
                if c == 0 {
                    None
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    let variance = m2 / c as f64;
                    if self.is_variance {
                        Some(variance)
                    } else {
                        Some(variance.sqrt())
                    }
                }
            })
            .collect();
        Ok(build_result_list(&values))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.counts.capacity() * std::mem::size_of::<i64>()
            + self.means.capacity() * std::mem::size_of::<f64>()
            + self.m2s.capacity() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let means: Vec<Option<f64>> = self.means.iter().map(|&m| Some(m)).collect();
        let counts: Vec<Option<i64>> = self.counts.iter().map(|&c| Some(c)).collect();
        let m2s: Vec<Option<f64>> = self.m2s.iter().map(|&m| Some(m)).collect();
        Ok(vec![
            f64_vec_to_list_scalar(&means),
            i64_vec_to_list_scalar(&counts),
            f64_vec_to_list_scalar(&m2s),
        ])
    }

    #[allow(clippy::similar_names)]
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let means_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for means state".to_string()))?;
        let counts_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for counts state".to_string()))?;
        let m2s_list = states[2]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for m2s state".to_string()))?;

        for row in 0..means_list.len() {
            if means_list.is_null(row) {
                continue;
            }
            let other_means = extract_f64_list(means_list, row)?;
            let other_counts = extract_i64_list(counts_list, row)?;
            let other_m2s = extract_f64_list(m2s_list, row)?;

            for (i, ((m, c), m2)) in other_means.into_iter().zip(other_counts).zip(other_m2s).enumerate() {
                if let (Some(other_mean), Some(other_count), Some(other_m2)) = (m, c, m2) {
                    if i < self.counts.len() && other_count > 0 {
                        // Chan et al. parallel combination formula
                        let n_a = self.counts[i];
                        let n_b = other_count;
                        if n_a == 0 {
                            self.counts[i] = n_b;
                            self.means[i] = other_mean;
                            self.m2s[i] = other_m2;
                        } else {
                            let n_ab = n_a + n_b;
                            let delta = other_mean - self.means[i];
                            #[allow(clippy::cast_precision_loss)]
                            let new_mean = self.means[i].mul_add(n_a as f64, other_mean * n_b as f64) / n_ab as f64;
                            #[allow(clippy::cast_precision_loss)]
                            let new_m2 = self.m2s[i] + other_m2 + delta * delta * n_a as f64 * n_b as f64 / n_ab as f64;
                            self.counts[i] = n_ab;
                            self.means[i] = new_mean;
                            self.m2s[i] = new_m2;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for WelfordGridAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WelfordGridAccumulator({})",
            if self.is_variance { "var" } else { "stddev" }
        )
    }
}

// ============================================================================
// First accumulator
// ============================================================================

/// Accumulator for `first_over_time`.
///
/// State per bucket: `i64` timestamp + `f64` value. Keeps the earliest timestamp.
#[derive(Debug)]
struct FirstGridAccumulator {
    ctx: GridContext,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    has_value: Vec<bool>,
}

impl FirstGridAccumulator {
    fn new(ctx: GridContext) -> Self {
        let n = ctx.grid.len();
        Self {
            ctx,
            timestamps: vec![i64::MAX; n],
            values: vec![0.0; n],
            has_value: vec![false; n],
        }
    }
}

impl Accumulator for FirstGridAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let timestamps = extract_timestamps(values)?;
        let vals = extract_values(values)?;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || vals.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            let val = vals.value(i);
            let (start_idx, end_idx) =
                find_matching_grid_indices(ts, &self.ctx.grid, self.ctx.range_micros, self.ctx.offset_micros);
            for j in start_idx..end_idx {
                if ts < self.timestamps[j] {
                    self.timestamps[j] = ts;
                    self.values[j] = val;
                }
                self.has_value[j] = true;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<Option<f64>> = self
            .has_value
            .iter()
            .zip(self.values.iter())
            .map(|(&has, &v)| if has { Some(v) } else { None })
            .collect();
        Ok(build_result_list(&values))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.timestamps.capacity() * std::mem::size_of::<i64>()
            + self.values.capacity() * std::mem::size_of::<f64>()
            + self.has_value.capacity() * std::mem::size_of::<bool>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<Option<f64>> = self
            .has_value
            .iter()
            .zip(self.values.iter())
            .map(|(&has, &v)| if has { Some(v) } else { None })
            .collect();
        let ts: Vec<Option<i64>> = self
            .has_value
            .iter()
            .zip(self.timestamps.iter())
            .map(|(&has, &t)| if has { Some(t) } else { None })
            .collect();
        Ok(vec![
            f64_vec_to_list_scalar(&values),
            i64_vec_to_list_scalar(&ts),
            f64_vec_to_list_scalar(&[]),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let vals_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for values state".to_string()))?;
        let ts_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for timestamps state".to_string()))?;

        for row in 0..vals_list.len() {
            if vals_list.is_null(row) {
                continue;
            }
            let other_vals = extract_f64_list(vals_list, row)?;
            let other_ts = extract_i64_list(ts_list, row)?;
            for (i, (v, t)) in other_vals.into_iter().zip(other_ts).enumerate() {
                if let (Some(v), Some(t)) = (v, t) {
                    if i < self.timestamps.len() && (t < self.timestamps[i] || !self.has_value[i]) {
                        self.timestamps[i] = t;
                        self.values[i] = v;
                        self.has_value[i] = true;
                    }
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for FirstGridAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FirstGridAccumulator")
    }
}

// ============================================================================
// Last accumulator
// ============================================================================

/// Accumulator for `last_over_time`.
///
/// State per bucket: `i64` timestamp + `f64` value. Keeps the latest timestamp.
#[derive(Debug)]
struct LastGridAccumulator {
    ctx: GridContext,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    has_value: Vec<bool>,
}

impl LastGridAccumulator {
    fn new(ctx: GridContext) -> Self {
        let n = ctx.grid.len();
        Self {
            ctx,
            timestamps: vec![i64::MIN; n],
            values: vec![0.0; n],
            has_value: vec![false; n],
        }
    }
}

impl Accumulator for LastGridAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let timestamps = extract_timestamps(values)?;
        let vals = extract_values(values)?;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || vals.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            let val = vals.value(i);
            let (start_idx, end_idx) =
                find_matching_grid_indices(ts, &self.ctx.grid, self.ctx.range_micros, self.ctx.offset_micros);
            for j in start_idx..end_idx {
                if ts > self.timestamps[j] {
                    self.timestamps[j] = ts;
                    self.values[j] = val;
                }
                self.has_value[j] = true;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<Option<f64>> = self
            .has_value
            .iter()
            .zip(self.values.iter())
            .map(|(&has, &v)| if has { Some(v) } else { None })
            .collect();
        Ok(build_result_list(&values))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.timestamps.capacity() * std::mem::size_of::<i64>()
            + self.values.capacity() * std::mem::size_of::<f64>()
            + self.has_value.capacity() * std::mem::size_of::<bool>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<Option<f64>> = self
            .has_value
            .iter()
            .zip(self.values.iter())
            .map(|(&has, &v)| if has { Some(v) } else { None })
            .collect();
        let ts: Vec<Option<i64>> = self
            .has_value
            .iter()
            .zip(self.timestamps.iter())
            .map(|(&has, &t)| if has { Some(t) } else { None })
            .collect();
        Ok(vec![
            f64_vec_to_list_scalar(&values),
            i64_vec_to_list_scalar(&ts),
            f64_vec_to_list_scalar(&[]),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let vals_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for values state".to_string()))?;
        let ts_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for timestamps state".to_string()))?;

        for row in 0..vals_list.len() {
            if vals_list.is_null(row) {
                continue;
            }
            let other_vals = extract_f64_list(vals_list, row)?;
            let other_ts = extract_i64_list(ts_list, row)?;
            for (i, (v, t)) in other_vals.into_iter().zip(other_ts).enumerate() {
                if let (Some(v), Some(t)) = (v, t) {
                    if i < self.timestamps.len() && (t > self.timestamps[i] || !self.has_value[i]) {
                        self.timestamps[i] = t;
                        self.values[i] = v;
                        self.has_value[i] = true;
                    }
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for LastGridAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LastGridAccumulator")
    }
}

// ============================================================================
// Quantile accumulator
// ============================================================================

/// Accumulator for `quantile_over_time(phi)`.
///
/// State per bucket: `Vec<f64>` of all values (sorted on evaluate).
#[derive(Debug)]
struct QuantileGridAccumulator {
    ctx: GridContext,
    /// Per-bucket collection of values.
    buckets: Vec<Vec<f64>>,
    /// Quantile parameter (0.0 to 1.0).
    phi: f64,
}

impl QuantileGridAccumulator {
    fn new(ctx: GridContext, phi: f64) -> Self {
        let n = ctx.grid.len();
        Self {
            buckets: vec![Vec::new(); n],
            ctx,
            phi,
        }
    }

    /// Compute quantile via linear interpolation (matching Prometheus behavior).
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn compute_quantile(sorted: &mut [f64], phi: f64) -> f64 {
        if sorted.is_empty() {
            return f64::NAN;
        }
        sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        if sorted.len() == 1 {
            return sorted[0];
        }

        // Prometheus-style quantile: phi * (n-1) index with linear interpolation
        #[allow(clippy::cast_precision_loss)]
        let rank = phi * (sorted.len() - 1) as f64;
        let lower = rank.floor() as usize;
        let upper = rank.ceil() as usize;

        if lower == upper || upper >= sorted.len() {
            sorted[lower.min(sorted.len() - 1)]
        } else {
            let frac = rank - rank.floor();
            sorted[lower].mul_add(1.0 - frac, sorted[upper] * frac)
        }
    }
}

impl Accumulator for QuantileGridAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let timestamps = extract_timestamps(values)?;
        let vals = extract_values(values)?;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || vals.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            let val = vals.value(i);
            let (start_idx, end_idx) =
                find_matching_grid_indices(ts, &self.ctx.grid, self.ctx.range_micros, self.ctx.offset_micros);
            for j in start_idx..end_idx {
                self.buckets[j].push(val);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<Option<f64>> = self
            .buckets
            .iter_mut()
            .map(|bucket| {
                if bucket.is_empty() {
                    None
                } else {
                    Some(Self::compute_quantile(bucket, self.phi))
                }
            })
            .collect();
        Ok(build_result_list(&values))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .buckets
                .iter()
                .map(|b| b.capacity() * std::mem::size_of::<f64>())
                .sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Serialize all bucket values as a flat List<Float64> with bucket lengths in counts
        let flat_values: Vec<Option<f64>> = self.buckets.iter().flat_map(|b| b.iter().map(|&v| Some(v))).collect();
        #[allow(clippy::cast_possible_wrap)]
        let lengths: Vec<Option<i64>> = self.buckets.iter().map(|b| Some(b.len() as i64)).collect();
        Ok(vec![
            f64_vec_to_list_scalar(&flat_values),
            i64_vec_to_list_scalar(&lengths),
            f64_vec_to_list_scalar(&[]),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let vals_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for values state".to_string()))?;
        let lengths_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for lengths state".to_string()))?;

        for row in 0..vals_list.len() {
            if vals_list.is_null(row) {
                continue;
            }
            let flat_vals = extract_f64_list(vals_list, row)?;
            let lengths = extract_i64_list(lengths_list, row)?;

            // Reconstruct per-bucket values from flat array + lengths
            let mut offset = 0usize;
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            for (i, len) in lengths.into_iter().enumerate() {
                if let Some(len) = len {
                    let len = len as usize;
                    if i < self.buckets.len() {
                        for &v in &flat_vals[offset..offset + len] {
                            if let Some(v) = v {
                                self.buckets[i].push(v);
                            }
                        }
                    }
                    offset += len;
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for QuantileGridAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QuantileGridAccumulator(phi={})", self.phi)
    }
}

// ============================================================================
// RateCounter accumulator
// ============================================================================

/// Accumulator for `rate_counter`.
///
/// Stores `(timestamp, value)` pairs per bucket, then on evaluate:
/// sorts by timestamp, detects counter resets, and sums deltas.
#[derive(Debug)]
struct RateCounterGridAccumulator {
    ctx: GridContext,
    /// Per-bucket (timestamp, value) entries.
    buckets: Vec<Vec<(i64, f64)>>,
}

impl RateCounterGridAccumulator {
    fn new(ctx: GridContext) -> Self {
        let n = ctx.grid.len();
        Self {
            buckets: vec![Vec::new(); n],
            ctx,
        }
    }

    /// Compute rate counter value: sort by timestamp, detect resets, sum deltas.
    fn compute_rate_counter(entries: &mut [(i64, f64)]) -> f64 {
        if entries.is_empty() {
            return 0.0;
        }

        entries.sort_unstable_by_key(|&(ts, _)| ts);

        let mut total_delta = 0.0;
        for i in 1..entries.len() {
            let prev_val = entries[i - 1].1;
            let curr_val = entries[i].1;

            if curr_val < prev_val {
                // Counter reset detected: assume counter started from 0
                total_delta += curr_val;
            } else {
                total_delta += curr_val - prev_val;
            }
        }

        total_delta
    }
}

impl Accumulator for RateCounterGridAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let timestamps = extract_timestamps(values)?;
        let vals = extract_values(values)?;

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || vals.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            let val = vals.value(i);
            let (start_idx, end_idx) =
                find_matching_grid_indices(ts, &self.ctx.grid, self.ctx.range_micros, self.ctx.offset_micros);
            for j in start_idx..end_idx {
                self.buckets[j].push((ts, val));
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values: Vec<Option<f64>> = self
            .buckets
            .iter_mut()
            .map(|bucket| {
                if bucket.is_empty() {
                    None
                } else {
                    Some(Self::compute_rate_counter(bucket))
                }
            })
            .collect();
        Ok(build_result_list(&values))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .buckets
                .iter()
                .map(|b| b.capacity() * std::mem::size_of::<(i64, f64)>())
                .sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Serialize as flat timestamps + flat values + bucket lengths
        let flat_ts: Vec<Option<f64>> = self
            .buckets
            .iter()
            .flat_map(|b| {
                b.iter().map(|&(ts, _)| {
                    #[allow(clippy::cast_precision_loss)]
                    Some(ts as f64)
                })
            })
            .collect();
        let flat_vals: Vec<Option<f64>> = self.buckets.iter().flat_map(|b| b.iter().map(|&(_, v)| Some(v))).collect();
        #[allow(clippy::cast_possible_wrap)]
        let lengths: Vec<Option<i64>> = self.buckets.iter().map(|b| Some(b.len() as i64)).collect();
        Ok(vec![
            f64_vec_to_list_scalar(&flat_ts),
            i64_vec_to_list_scalar(&lengths),
            f64_vec_to_list_scalar(&flat_vals),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let ts_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for timestamps state".to_string()))?;
        let lengths_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for lengths state".to_string()))?;
        let vals_list = states[2]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected ListArray for values state".to_string()))?;

        for row in 0..ts_list.len() {
            if ts_list.is_null(row) {
                continue;
            }
            let flat_ts = extract_f64_list(ts_list, row)?;
            let flat_vals = extract_f64_list(vals_list, row)?;
            let lengths = extract_i64_list(lengths_list, row)?;

            let mut offset = 0usize;
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            for (i, len) in lengths.into_iter().enumerate() {
                if let Some(len) = len {
                    let len = len as usize;
                    if i < self.buckets.len() {
                        for j in offset..offset + len {
                            if let (Some(ts), Some(val)) =
                                (flat_ts.get(j).copied().flatten(), flat_vals.get(j).copied().flatten())
                            {
                                #[allow(clippy::cast_possible_truncation)]
                                self.buckets[i].push((ts as i64, val));
                            }
                        }
                    }
                    offset += len;
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for RateCounterGridAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RateCounterGridAccumulator")
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use datafusion::arrow::array::TimestampMicrosecondArray;

    use super::*;

    /// Helper to create a `GridContext` for testing.
    fn test_ctx(start: i64, end: i64, step: i64, range: i64, offset: i64) -> GridContext {
        GridContext {
            grid: Arc::new(compute_grid_points(start, end, step, 11_000).unwrap()),
            range_micros: range,
            offset_micros: offset,
        }
    }

    #[test]
    fn test_count_accumulator_basic() {
        // Grid: [0, 100, 200], range=100, offset=0
        // Timestamp 50: matches grid points where 50 <= g <= 150 → [100]
        // Timestamp 150: matches where 150 <= g <= 250 → [200]
        let ctx = test_ctx(0, 200, 100, 100, 0);
        let mut acc = CountGridAccumulator::new(ctx);

        let ts = Arc::new(TimestampMicrosecondArray::from(vec![50, 150])) as ArrayRef;
        acc.update_batch(&[ts]).unwrap();

        let result = acc.evaluate().unwrap();
        if let ScalarValue::List(list) = result {
            let vals = list.value(0);
            let f64_arr = vals.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(f64_arr.len(), 3);
            assert!(f64_arr.is_null(0)); // bucket 0: no matches
            assert_eq!(f64_arr.value(1), 1.0); // bucket 100: 1 match
            assert_eq!(f64_arr.value(2), 1.0); // bucket 200: 1 match
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_sum_accumulator_basic() {
        let ctx = test_ctx(0, 200, 100, 100, 0);
        let mut acc = SumGridAccumulator::new(ctx);

        let ts = Arc::new(TimestampMicrosecondArray::from(vec![50, 50])) as ArrayRef;
        let vals = Arc::new(Float64Array::from(vec![10.0, 20.0])) as ArrayRef;
        acc.update_batch(&[ts, vals]).unwrap();

        let result = acc.evaluate().unwrap();
        if let ScalarValue::List(list) = result {
            let arr = list.value(0);
            let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!(f64_arr.is_null(0));
            assert_eq!(f64_arr.value(1), 30.0); // 10 + 20
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_avg_accumulator_basic() {
        let ctx = test_ctx(0, 100, 100, 200, 0);
        let mut acc = AvgGridAccumulator::new(ctx);

        let ts = Arc::new(TimestampMicrosecondArray::from(vec![50, 50])) as ArrayRef;
        let vals = Arc::new(Float64Array::from(vec![10.0, 30.0])) as ArrayRef;
        acc.update_batch(&[ts, vals]).unwrap();

        let result = acc.evaluate().unwrap();
        if let ScalarValue::List(list) = result {
            let arr = list.value(0);
            let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            // ts=50 maps to g where 50<=g<=250 but grid only has [0, 100]
            // g=100 matches (50 <= 100 <= 250), g=0 does not (50 > 0)
            assert_eq!(f64_arr.value(1), 20.0); // avg(10, 30) = 20
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_min_max_accumulator() {
        let ctx = test_ctx(100, 100, 100, 200, 0);
        let mut min_acc = MinGridAccumulator::new(ctx.clone());
        let mut max_acc = MaxGridAccumulator::new(ctx);

        let ts = Arc::new(TimestampMicrosecondArray::from(vec![0, 0, 0])) as ArrayRef;
        let vals = Arc::new(Float64Array::from(vec![5.0, 2.0, 8.0])) as ArrayRef;

        min_acc.update_batch(&[Arc::clone(&ts), Arc::clone(&vals)]).unwrap();
        max_acc.update_batch(&[ts, vals]).unwrap();

        if let ScalarValue::List(list) = min_acc.evaluate().unwrap() {
            let arr = list.value(0);
            let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(f64_arr.value(0), 2.0);
        }

        if let ScalarValue::List(list) = max_acc.evaluate().unwrap() {
            let arr = list.value(0);
            let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(f64_arr.value(0), 8.0);
        }
    }

    #[test]
    fn test_first_last_accumulator() {
        let ctx = test_ctx(100, 100, 100, 200, 0);
        let mut first_acc = FirstGridAccumulator::new(ctx.clone());
        let mut last_acc = LastGridAccumulator::new(ctx);

        // ts=10 → val=5.0, ts=20 → val=3.0, ts=5 → val=9.0
        let ts = Arc::new(TimestampMicrosecondArray::from(vec![10, 20, 5])) as ArrayRef;
        let vals = Arc::new(Float64Array::from(vec![5.0, 3.0, 9.0])) as ArrayRef;

        first_acc.update_batch(&[Arc::clone(&ts), Arc::clone(&vals)]).unwrap();
        last_acc.update_batch(&[ts, vals]).unwrap();

        if let ScalarValue::List(list) = first_acc.evaluate().unwrap() {
            let arr = list.value(0);
            let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(f64_arr.value(0), 9.0); // first by ts=5
        }

        if let ScalarValue::List(list) = last_acc.evaluate().unwrap() {
            let arr = list.value(0);
            let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(f64_arr.value(0), 3.0); // last by ts=20
        }
    }

    #[test]
    fn test_quantile_accumulator() {
        let ctx = test_ctx(100, 100, 100, 200, 0);
        let mut acc = QuantileGridAccumulator::new(ctx, 0.5);

        let ts = Arc::new(TimestampMicrosecondArray::from(vec![10, 20, 30, 40])) as ArrayRef;
        let vals = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
        acc.update_batch(&[ts, vals]).unwrap();

        if let ScalarValue::List(list) = acc.evaluate().unwrap() {
            let arr = list.value(0);
            let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(f64_arr.value(0), 2.5); // median of [1,2,3,4]
        }
    }

    #[test]
    fn test_rate_counter_accumulator() {
        let ctx = test_ctx(100, 100, 100, 200, 0);
        let mut acc = RateCounterGridAccumulator::new(ctx);

        // Counter: 10, 20, 5 (reset!), 15
        // Deltas: (20-10)=10, reset→5, (15-5)=10 → total=25
        let ts = Arc::new(TimestampMicrosecondArray::from(vec![10, 20, 30, 40])) as ArrayRef;
        let vals = Arc::new(Float64Array::from(vec![10.0, 20.0, 5.0, 15.0])) as ArrayRef;
        acc.update_batch(&[ts, vals]).unwrap();

        if let ScalarValue::List(list) = acc.evaluate().unwrap() {
            let arr = list.value(0);
            let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(f64_arr.value(0), 25.0);
        }
    }

    #[test]
    fn test_empty_buckets_are_null() {
        // Grid: [0, 100, 200], but only ts=150 → matches [200] only
        let ctx = test_ctx(0, 200, 100, 100, 0);
        let mut acc = CountGridAccumulator::new(ctx);

        let ts = Arc::new(TimestampMicrosecondArray::from(vec![150])) as ArrayRef;
        acc.update_batch(&[ts]).unwrap();

        if let ScalarValue::List(list) = acc.evaluate().unwrap() {
            let arr = list.value(0);
            let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!(f64_arr.is_null(0)); // bucket 0: empty
            assert!(f64_arr.is_null(1)); // bucket 100: empty
            assert_eq!(f64_arr.value(2), 1.0); // bucket 200: 1 match
        }
    }
}
