//! Explicit-bucket histogram buckets as `{min, max, count}` objects, derived per row from the
//! stored `explicit_bounds` and `bucket_counts`.
//!
//! Bucket `i` of a histogram with `n` counts runs from bound `i - 1` to bound `i`; the first
//! bucket's `min` is [`UNBOUNDED_BUCKET_MIN`] and the last bucket's `max` is
//! [`UNBOUNDED_BUCKET_MAX`], as JSON holds no infinity. `_source` rendering and the
//! `histogram_buckets` read expression share [`HistogramBounds`], so both answer the same buckets.

use std::sync::Arc;

use datafusion::arrow::array::builder::NullBufferBuilder;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Builder, Int64Builder, ListArray, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{ArrowNativeType as _, DataType, Field, FieldRef, Fields, Float64Type, Int64Type};
use datafusion::common::{Result as DataFusionResult, exec_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::field::FieldType;

/// The `min` of a histogram's first bucket, which has no lower bound: the most negative finite
/// `f32`, widened to `f64`. Chosen to follow `createBuckets` of Data Prepper's
/// `data-prepper-plugins/otel-proto-common/.../codec/OTelProtoOpensearchCodec.java`; IceGate's
/// contract is the value itself, pinned by
/// `ss4o_metrics_renders_histogram_buckets_as_min_max_count_objects`.
pub(crate) const UNBOUNDED_BUCKET_MIN: f64 = -UNBOUNDED_BUCKET_MAX;

/// The `max` of a histogram's last bucket, which has no upper bound: the most positive finite
/// `f32`, widened to `f64` (see [`UNBOUNDED_BUCKET_MIN`]). `f64::from` is not `const`; the
/// widening is exact either way.
pub(crate) const UNBOUNDED_BUCKET_MAX: f64 = f32::MAX as f64;

/// The lower bound member of a bucket object.
pub(crate) const BUCKET_MIN_MEMBER: &str = "min";
/// The upper bound member of a bucket object.
pub(crate) const BUCKET_MAX_MEMBER: &str = "max";
/// The count member of a bucket object.
pub(crate) const BUCKET_COUNT_MEMBER: &str = "count";

/// The members of a bucket object with their field types, in `_mapping` order.
pub(crate) const BUCKET_MEMBERS: [(&str, FieldType); 3] = [
    (BUCKET_MIN_MEMBER, FieldType::Double),
    (BUCKET_MAX_MEMBER, FieldType::Double),
    (BUCKET_COUNT_MEMBER, FieldType::Long),
];

/// A stored histogram row that reads as no buckets: its lists contradict each other, which ingest
/// never writes (it drops such a point, `histogram_buckets_consistent` in icegate-ingest's metrics
/// transform), or a column holds another type. Either is a broken invariant, never a value to
/// show.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum HistogramShapeError {
    /// The bounds are not exactly one fewer than the counts.
    #[error("a histogram row has {bucket_count} bucket counts and {bound_count:?} explicit bounds, not one fewer")]
    BoundCount {
        /// The row's number of bucket counts.
        bucket_count: usize,
        /// The row's number of explicit bounds; `None` for a NULL list.
        bound_count: Option<usize>,
    },
    /// An explicit bound is NULL.
    #[error("a histogram row holds a NULL explicit bound")]
    NullBound,
    /// A stored list is not of the type the histogram columns hold.
    #[error("a histogram column holds {found}, not {expected}")]
    UnexpectedType {
        /// The type the column must hold.
        expected: &'static str,
        /// The type it holds.
        found: DataType,
    },
}

/// The explicit bounds of one histogram row, checked against its bucket count.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HistogramBounds<'a> {
    /// The row's bounds, one fewer than its buckets (or none for a row with no bucket).
    bounds: &'a [f64],
}

impl<'a> HistogramBounds<'a> {
    /// The bounds of a row with `bucket_count` buckets. A row with no bucket needs no bound and
    /// ignores `bounds`.
    ///
    /// # Errors
    ///
    /// [`HistogramShapeError::BoundCount`] when the row has buckets and `bounds` is NULL or not one
    /// shorter than them.
    pub(crate) fn try_new(bounds: Option<&'a [f64]>, bucket_count: usize) -> Result<Self, HistogramShapeError> {
        if bucket_count == 0 {
            return Ok(Self { bounds: &[] });
        }
        match bounds {
            Some(bounds) if bounds.len() + 1 == bucket_count => Ok(Self { bounds }),
            _ => Err(HistogramShapeError::BoundCount {
                bucket_count,
                bound_count: bounds.map(<[f64]>::len),
            }),
        }
    }

    /// The `(min, max)` of bucket `index`, which must be below the bucket count.
    pub(crate) fn read_bucket_range(self, index: usize) -> (f64, f64) {
        let min = index
            .checked_sub(1)
            .and_then(|previous| self.bounds.get(previous))
            .copied()
            .unwrap_or(UNBOUNDED_BUCKET_MIN);
        let max = self.bounds.get(index).copied().unwrap_or(UNBOUNDED_BUCKET_MAX);
        (min, max)
    }
}

/// The explicit bounds at `row` of the `List<Float64>` column `bound_lists`, borrowed in place;
/// `None` for a NULL list.
///
/// # Errors
///
/// [`HistogramShapeError::UnexpectedType`] for a column of another type,
/// [`HistogramShapeError::NullBound`] for a NULL element.
pub(crate) fn read_row_bounds(bound_lists: &dyn Array, row: usize) -> Result<Option<&[f64]>, HistogramShapeError> {
    let lists = read_list_array(bound_lists, "a list of doubles")?;
    if lists.is_null(row) {
        return Ok(None);
    }
    let values = lists
        .values()
        .as_primitive_opt::<Float64Type>()
        .ok_or_else(|| report_unexpected_type("a list of doubles", bound_lists.data_type()))?;
    let offsets = lists.value_offsets();
    let (start, end) = (offsets[row].as_usize(), offsets[row + 1].as_usize());
    if values
        .nulls()
        .is_some_and(|nulls| (start..end).any(|index| nulls.is_null(index)))
    {
        return Err(HistogramShapeError::NullBound);
    }
    Ok(Some(&values.values()[start..end]))
}

/// `array` as a `List` array.
fn read_list_array<'a>(array: &'a dyn Array, expected: &'static str) -> Result<&'a ListArray, HistogramShapeError> {
    array
        .as_list_opt::<i32>()
        .ok_or_else(|| report_unexpected_type(expected, array.data_type()))
}

/// The [`HistogramShapeError::UnexpectedType`] for a column holding `found`.
fn report_unexpected_type(expected: &'static str, found: &DataType) -> HistogramShapeError {
    HistogramShapeError::UnexpectedType {
        expected,
        found: found.clone(),
    }
}

/// The type of a row's buckets: a list of `{min: Float64, max: Float64, count: Int64}`, `count`
/// NULL where the stored count is.
fn build_bucket_list_type() -> DataType {
    DataType::List(build_bucket_item_field())
}

/// The item field of a row's bucket list.
fn build_bucket_item_field() -> FieldRef {
    Arc::new(Field::new_list_field(DataType::Struct(build_bucket_fields()), true))
}

/// The members of a bucket struct.
fn build_bucket_fields() -> Fields {
    Fields::from(vec![
        Field::new(BUCKET_MIN_MEMBER, DataType::Float64, false),
        Field::new(BUCKET_MAX_MEMBER, DataType::Float64, false),
        Field::new(BUCKET_COUNT_MEMBER, DataType::Int64, true),
    ])
}

/// A row's buckets read from its explicit bounds and bucket counts: a list of
/// `{min: Float64, max: Float64, count: Int64}`, NULL where the counts are NULL, empty where they
/// are empty. The query fails on a row whose lists contradict each other (see
/// [`HistogramShapeError`]).
pub(crate) fn build_histogram_buckets_expr(bounds: Expr, counts: Expr) -> Expr {
    ScalarUDF::new_from_impl(HistogramBucketsUdf {
        signature: Signature::any(2, Volatility::Immutable),
    })
    .call(vec![bounds, counts])
}

/// The `histogram_buckets(explicit_bounds, bucket_counts)` UDF of [`build_histogram_buckets_expr`].
#[derive(Debug, PartialEq, Eq, Hash)]
struct HistogramBucketsUdf {
    /// Two arguments, checked when invoked; immutable.
    signature: Signature,
}

impl ScalarUDFImpl for HistogramBucketsUdf {
    fn name(&self) -> &'static str {
        "histogram_buckets"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(build_bucket_list_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let [bounds, counts] = args.args.as_slice() else {
            return exec_err!("histogram_buckets takes 2 arguments, got {}", args.args.len());
        };
        let bound_lists = bounds.to_array(args.number_rows)?;
        let count_lists = counts.to_array(args.number_rows)?;
        let buckets = build_bucket_lists(&bound_lists, &count_lists).map_err(|error| {
            // Execution errors carry no structured payload; the shape error's text is the reason.
            DataFusionError::Execution(error.to_string())
        })?;
        Ok(ColumnarValue::Array(buckets))
    }
}

/// The buckets of every row of `bound_lists` and `count_lists`, one list per row.
///
/// # Errors
///
/// The [`HistogramShapeError`] of the first row whose lists contradict each other, or a column of
/// another type.
fn build_bucket_lists(bound_lists: &ArrayRef, count_lists: &ArrayRef) -> Result<ArrayRef, HistogramShapeError> {
    let counts = read_list_array(count_lists.as_ref(), "a list of longs")?;
    let count_values = counts
        .values()
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| report_unexpected_type("a list of longs", count_lists.data_type()))?;
    let row_count = counts.len();
    let value_capacity = count_values.len();
    let mut mins = Float64Builder::with_capacity(value_capacity);
    let mut maxs = Float64Builder::with_capacity(value_capacity);
    let mut bucket_counts = Int64Builder::with_capacity(value_capacity);
    let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut nulls = NullBufferBuilder::new(row_count);
    let mut bucket_total: i32 = 0;
    let count_offsets = counts.value_offsets();
    for row in 0..row_count {
        if counts.is_null(row) {
            nulls.append_null();
            offsets.push(bucket_total);
            continue;
        }
        let (start, end) = (count_offsets[row].as_usize(), count_offsets[row + 1].as_usize());
        let bounds = HistogramBounds::try_new(read_row_bounds(bound_lists.as_ref(), row)?, end - start)?;
        for (bucket_index, value_index) in (start..end).enumerate() {
            let (min, max) = bounds.read_bucket_range(bucket_index);
            mins.append_value(min);
            maxs.append_value(max);
            bucket_counts.append_option(count_values.is_valid(value_index).then(|| count_values.value(value_index)));
        }
        // A row's buckets are its counts, so the running total stays within the counts' own
        // `i32` offsets.
        bucket_total += count_offsets[row + 1] - count_offsets[row];
        offsets.push(bucket_total);
        nulls.append_non_null();
    }
    let bucket_structs = StructArray::new(
        build_bucket_fields(),
        vec![
            Arc::new(mins.finish()),
            Arc::new(maxs.finish()),
            Arc::new(bucket_counts.finish()),
        ],
        None,
    );
    Ok(Arc::new(ListArray::new(
        build_bucket_item_field(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(bucket_structs),
        nulls.finish(),
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, ArrayRef, ListArray, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Float64Type, Int64Type, Schema};
    use datafusion::prelude::{SessionContext, ident};
    use serde_json::{Value, json};

    use super::{HistogramBounds, HistogramShapeError, build_histogram_buckets_expr};
    use crate::opensearch::search::render_array_value;

    const BOUNDS: &str = "bounds";
    const COUNTS: &str = "counts";

    /// The `histogram_buckets` of each row of `bounds` and `counts`, rendered as JSON.
    async fn evaluate_buckets(
        bounds: Vec<Option<Vec<Option<f64>>>>,
        counts: Vec<Option<Vec<Option<i64>>>>,
    ) -> Result<Vec<Value>, String> {
        let bound_lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(bounds));
        let count_lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(counts));
        let schema = Arc::new(Schema::new(vec![
            Field::new(BOUNDS, bound_lists.data_type().clone(), true),
            Field::new(COUNTS, count_lists.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![bound_lists, count_lists]).expect("batch");
        let batches = SessionContext::new()
            .read_batch(batch)
            .expect("frame")
            .select(vec![build_histogram_buckets_expr(ident(BOUNDS), ident(COUNTS))])
            .expect("project")
            .collect()
            .await
            .map_err(|error| error.to_string())?;
        Ok(batches
            .iter()
            .flat_map(|batch| {
                let buckets = Arc::clone(batch.column(0));
                (0..buckets.len()).map(move |row| render_array_value(buckets.as_ref(), row).expect("render"))
            })
            .collect())
    }

    #[tokio::test]
    async fn histogram_buckets_run_between_bounds_with_open_ends_and_follow_null_and_empty_counts() {
        let rows = evaluate_buckets(
            vec![Some(vec![Some(1.0), Some(5.0)]), Some(vec![]), None, Some(vec![]), None],
            vec![
                Some(vec![Some(2), Some(3), Some(4)]),
                Some(vec![Some(9)]),
                None,
                Some(vec![]),
                Some(vec![]),
            ],
        )
        .await
        .expect("buckets");

        assert_eq!(
            rows,
            vec![
                json!([
                    {"min": -3.402_823_466_385_288_6e38, "max": 1.0, "count": 2},
                    {"min": 1.0, "max": 5.0, "count": 3},
                    {"min": 5.0, "max": 3.402_823_466_385_288_6e38, "count": 4},
                ]),
                json!([{"min": -3.402_823_466_385_288_6e38, "max": 3.402_823_466_385_288_6e38, "count": 9}]),
                Value::Null,
                json!([]),
                json!([]),
            ]
        );
    }

    #[tokio::test]
    async fn histogram_buckets_fail_the_query_on_contradicting_lists() {
        for (bounds, counts) in [
            (Some(vec![Some(1.0)]), Some(vec![Some(1), Some(2), Some(3)])),
            (None, Some(vec![Some(1), Some(2)])),
            (Some(vec![None]), Some(vec![Some(1), Some(2)])),
        ] {
            let error = evaluate_buckets(vec![bounds.clone()], vec![counts.clone()])
                .await
                .expect_err("contradicting lists");

            assert!(error.contains("histogram row"), "{bounds:?} {counts:?}: {error}");
        }
    }

    #[test]
    fn histogram_bounds_accept_exactly_one_bound_fewer_than_buckets() {
        assert_eq!(
            HistogramBounds::try_new(Some(&[1.0]), 3).map(|_| ()),
            Err(HistogramShapeError::BoundCount {
                bucket_count: 3,
                bound_count: Some(1),
            })
        );
        assert_eq!(
            HistogramBounds::try_new(None, 1).map(|_| ()),
            Err(HistogramShapeError::BoundCount {
                bucket_count: 1,
                bound_count: None,
            })
        );
        let bounds = HistogramBounds::try_new(Some(&[1.0, 5.0]), 3).expect("bounds");
        assert_eq!(bounds.read_bucket_range(1), (1.0, 5.0));
        // The open ends are the most negative and most positive finite `f32`.
        assert_eq!(bounds.read_bucket_range(0).0.to_bits(), f64::from(f32::MIN).to_bits());
        assert_eq!(bounds.read_bucket_range(2).1.to_bits(), f64::from(f32::MAX).to_bits());
    }
}
