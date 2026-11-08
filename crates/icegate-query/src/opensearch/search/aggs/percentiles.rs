//! `percentiles` and `percentile_ranks`: request specs, the two aggregates that
//! compute them, and their rendering.
//!
//! `percentiles` is approximate by contract: one t-digest per bucket and value
//! estimates every requested percent in one pass, within a stated rank tolerance
//! the tests pin. Several `percentiles` of one stage aggregate that read the same
//! value (field and `missing`) share that digest over the union of their percents
//! ([`SharedDigest`]). The partial states of one group merge across partitions, pinned
//! by `tests::partial_states_of_one_group_merge_across_partitions`. `percentile_ranks` is
//! exact: one count per interval between the requested values, summed at the end.
//!
//! Keys of keyed results use [`format_double_key`]: `25` renders as `"25.0"`.
//! A bucket without a value renders every percent and every rank as `null`.

use std::num::FpCategory;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type, Schema};
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::functions_nested::expr_fn::{array_element, make_array};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Expr, Signature, Volatility, col, lit,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_functions_aggregate_common::tdigest::{DEFAULT_MAX_SIZE, TDigest};
use serde_json::{Map, Value, json};

use super::metrics::{MetricSpec, Statistic};
use super::parse::{
    IGNORED_EXECUTION_KEYS, MetricPath, expect_flag, expect_object, expect_string, read_lenient_number,
    read_strict_number, report_missing_key, report_unsupported_key,
};
use super::plan::{name_statistic_column, name_value_column};
use crate::opensearch::error::OpenSearchError;

/// Percents a `percentiles` aggregation computes when the request names none.
///
/// The list holds 50 so that an order path `<id>[50.0]` resolves against a
/// `percentiles` sent without `percents`.
pub(crate) const DEFAULT_PERCENTS: [f64; 7] = [1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0];

/// The request type name of `percentiles`.
const PERCENTILES: &str = "percentiles";

/// The request type name of `percentile_ranks`.
const PERCENTILE_RANKS: &str = "percentile_ranks";

/// A `percentiles` aggregation: approximate quantiles of one numeric field.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PercentilesSpec {
    /// The field and its `missing` value.
    pub(crate) metric: MetricSpec,
    /// Requested percents in `[0, 100]`, in request order; a percent may repeat.
    pub(crate) percents: Vec<f64>,
    /// `true` renders `values` as an object keyed by percent, `false` as an array.
    pub(crate) is_keyed: bool,
}

/// A `percentile_ranks` aggregation: the exact share, in percent, of values at
/// or below each requested value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PercentileRanksSpec {
    /// The field; `missing` is never set for this kind.
    pub(crate) metric: MetricSpec,
    /// Requested values, in request order.
    pub(crate) values: Vec<f64>,
    /// `true` renders `values` as an object keyed by value, `false` as an array.
    pub(crate) is_keyed: bool,
}

/// Parses the `percentiles` aggregation `name`: `field`, `missing`, `percents`, `keyed`.
///
/// `percents` defaults to [`DEFAULT_PERCENTS`] and `keyed` to `true`.
///
/// # Errors
///
/// `parsing_exception` for a body that is not an object, a missing `field`, an unknown key
/// (named under `construct`), a percent that is neither a JSON number nor a numeric string or
/// lies outside `[0, 100]`, an empty or non-array `percents`, or a non-boolean `keyed`.
pub(super) fn parse_percentiles(name: &str, value: &Value) -> Result<PercentilesSpec, OpenSearchError> {
    let body = expect_object(name, PERCENTILES, value)?;
    let mut field = None;
    let mut missing = None;
    let mut percents = None;
    let mut is_keyed = true;
    for (key, value) in body {
        match key.as_str() {
            "field" => field = Some(expect_string(name, key, value)?.to_string()),
            "missing" => missing = Some(read_lenient_number(name, key, value)?),
            "percents" => percents = Some(read_percents(name, value)?),
            "keyed" => is_keyed = expect_flag(name, key, value)?,
            ignored if IGNORED_EXECUTION_KEYS.contains(&ignored) => {}
            unknown => return Err(report_unsupported_key(PERCENTILES, name, unknown)),
        }
    }
    Ok(PercentilesSpec {
        metric: MetricSpec {
            field: field.ok_or_else(|| report_missing_key(PERCENTILES, name, "field"))?,
            missing,
        },
        percents: percents.unwrap_or_else(|| DEFAULT_PERCENTS.to_vec()),
        is_keyed,
    })
}

/// Parses the `percentile_ranks` aggregation `name`: `field`, `values`, `keyed`.
///
/// `keyed` defaults to `true`.
///
/// # Errors
///
/// `parsing_exception` for a body that is not an object, a missing `field` or `values`, an
/// unknown key (`missing` included, named under `construct`), an empty or non-array `values`,
/// a value that is not a JSON number, or a non-boolean `keyed`.
pub(super) fn parse_percentile_ranks(name: &str, value: &Value) -> Result<PercentileRanksSpec, OpenSearchError> {
    let body = expect_object(name, PERCENTILE_RANKS, value)?;
    let mut field = None;
    let mut values = None;
    let mut is_keyed = true;
    for (key, value) in body {
        match key.as_str() {
            "field" => field = Some(expect_string(name, key, value)?.to_string()),
            "values" => {
                let items = value.as_array().filter(|items| !items.is_empty()).ok_or_else(|| {
                    OpenSearchError::parsing(format!("[values] of [{name}] must be a non-empty array"))
                })?;
                values = Some(
                    items
                        .iter()
                        .map(|item| read_strict_number(name, key, item))
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            "keyed" => is_keyed = expect_flag(name, key, value)?,
            ignored if IGNORED_EXECUTION_KEYS.contains(&ignored) => {}
            unknown => return Err(report_unsupported_key(PERCENTILE_RANKS, name, unknown)),
        }
    }
    Ok(PercentileRanksSpec {
        metric: MetricSpec {
            field: field.ok_or_else(|| report_missing_key(PERCENTILE_RANKS, name, "field"))?,
            missing: None,
        },
        values: values.ok_or_else(|| report_missing_key(PERCENTILE_RANKS, name, "values"))?,
        is_keyed,
    })
}

/// `percents` of the aggregation `name`: a non-empty array of numbers or numeric strings in
/// `[0, 100]`.
fn read_percents(name: &str, value: &Value) -> Result<Vec<f64>, OpenSearchError> {
    let items = value
        .as_array()
        .filter(|items| !items.is_empty())
        .ok_or_else(|| OpenSearchError::parsing(format!("[percents] of [{name}] must be a non-empty array")))?;
    items
        .iter()
        .map(|item| {
            let percent = read_lenient_number(name, "percents", item)?;
            if (0.0..=100.0).contains(&percent) {
                Ok(percent)
            } else {
                Err(OpenSearchError::parsing(format!(
                    "[percents] of [{name}]: percent [{percent}] must be between 0 and 100"
                )))
            }
        })
        .collect()
}

/// Splits a `<id>[<percent>]` or `<id>['<percent>']` metric path into the
/// aggregation id and the percent in [`format_double_key`] form; `None` for
/// every other path form.
///
/// # Errors
///
/// `parsing_exception` for an empty id, an unclosed bracket, or a bracket that
/// does not hold a finite number.
pub(super) fn split_percent_path(text: &str) -> Result<Option<(&str, String)>, OpenSearchError> {
    let Some((aggregation, rest)) = text.split_once('[') else {
        return Ok(None);
    };
    let path_error = || OpenSearchError::parsing(format!("invalid metric path [{text}]"));
    let bracket_text = rest.strip_suffix(']').ok_or_else(path_error)?;
    let percent_text = bracket_text
        .strip_prefix('\'')
        .and_then(|quoted_text| quoted_text.strip_suffix('\''))
        .unwrap_or(bracket_text);
    let percent: f64 = percent_text.parse().map_err(|_| path_error())?;
    if aggregation.is_empty() || !percent.is_finite() {
        return Err(path_error());
    }
    Ok(Some((aggregation, format_double_key(percent))))
}

/// Renders a double the way keyed results name it: plain decimal with at least
/// one fraction digit for zero and magnitudes in `[10^-3, 10^7)`,
/// `<mantissa>E<exponent>` otherwise (`25` → `"25.0"`, `10^7` → `"1.0E7"`).
pub(crate) fn format_double_key(value: f64) -> String {
    let is_zero = matches!(value.classify(), FpCategory::Zero);
    if is_zero || (1.0e-3..1.0e7).contains(&value.abs()) {
        // `Debug` prints the shortest round-trip digits and keeps `.0`.
        return format!("{value:?}");
    }
    let scientific_text = format!("{value:e}");
    let (mantissa, exponent) = scientific_text.split_once('e').unwrap_or((scientific_text.as_str(), "0"));
    if mantissa.contains('.') {
        format!("{mantissa}E{exponent}")
    } else {
        format!("{mantissa}.0E{exponent}")
    }
}

/// The `multi_quantile` aggregate: `multi_quantile(value, p_1, …, p_n)`.
///
/// `value` and every `p_i` are numeric and coerced to `Float64`; every `p_i` must be
/// a constant percent in `[0, 100]`. The result is a `List<Float64>` with one estimate per `p_i`, in
/// argument order, or `NULL` for a group without a finite value.
pub(crate) fn build_multi_quantile_udf() -> AggregateUDF {
    AggregateUDF::new_from_impl(MultiQuantile {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

/// The `multi_quantile` aggregate estimating `percents` of a per-row `value`.
pub(super) fn plan_percentiles_aggregate(percents: &[f64], value: Expr) -> Expr {
    let mut arguments = Vec::with_capacity(percents.len() + 1);
    arguments.push(value);
    arguments.extend(percents.iter().map(|percent| lit(*percent)));
    build_multi_quantile_udf().call(arguments)
}

/// The `percentiles` metrics of one stage aggregate that read the same value, and
/// the one digest that answers them all.
///
/// The digest estimates the union of the members' percents, each percent once. When the
/// digest's list is exactly one member's list (one member, no repeated percent), it is that
/// member's `quantiles` statistic itself. Otherwise it is a hidden column, and a projection
/// after the aggregate gives each member a `quantiles` list of its own percents in request
/// order, a repeated percent read at each of its positions, so every reader of the statistic
/// is unchanged.
#[derive(Debug)]
pub(super) struct SharedDigest<'a> {
    /// The value input every member reads.
    metric: &'a MetricSpec,
    /// The first member; the digest reads its value column.
    owner: usize,
    /// The union of the members' percents, in first-request order, without repeats.
    percents: Vec<f64>,
    /// Each member and, per percent of its spec, that percent's 1-based element in
    /// the digest's list (the index `array_element` takes).
    members: Vec<(usize, Vec<i64>)>,
}

impl SharedDigest<'_> {
    /// The digest's aggregate expression, named as its single member's `quantiles`
    /// statistic when it answers that member as it is, or as the hidden shared column.
    pub(super) fn plan_digest(&self) -> Expr {
        let digest = plan_percentiles_aggregate(&self.percents, col(name_value_column(self.owner)));
        if self.is_answered_by_its_digest_list() {
            digest.alias(name_statistic_column(self.owner, Statistic::Quantiles))
        } else {
            digest.alias(name_shared_digest_column(self.owner))
        }
    }

    /// The `quantiles` statistic of every member, projected over the aggregate's output;
    /// nothing when the digest is its single member's statistic.
    pub(super) fn plan_member_quantiles(&self) -> Vec<Expr> {
        if self.is_answered_by_its_digest_list() {
            return Vec::new();
        }
        let digest = col(name_shared_digest_column(self.owner));
        self.members
            .iter()
            .map(|(metric, elements)| {
                let estimates = elements
                    .iter()
                    .map(|element| array_element(digest.clone(), lit(*element)))
                    .collect();
                make_array(estimates).alias(name_statistic_column(*metric, Statistic::Quantiles))
            })
            .collect()
    }

    /// Whether the digest's list is its one member's list as requested: one member whose
    /// percents hold no repeat.
    const fn is_answered_by_its_digest_list(&self) -> bool {
        matches!(self.members.as_slice(), [(_, elements)] if elements.len() == self.percents.len())
    }

    /// The 1-based element of `percent` in the digest's list, adding it to the union
    /// when no member asked for it yet.
    fn add_percent(&mut self, percent: f64) -> Result<i64, OpenSearchError> {
        let index = self
            .percents
            .iter()
            .position(|known_percent| known_percent.total_cmp(&percent).is_eq())
            .unwrap_or_else(|| {
                self.percents.push(percent);
                self.percents.len() - 1
            });
        i64::try_from(index + 1)
            .map_err(|_| OpenSearchError::internal("a digest lists more percents than a list index holds"))
    }
}

/// Groups `metrics` (each `percentiles` metric of one stage with its spec, by
/// ascending metric index) by the value they read: one [`SharedDigest`] per value.
///
/// # Errors
///
/// `internal` for more percents than a list index holds.
pub(super) fn group_shared_digests(
    metrics: Vec<(usize, &PercentilesSpec)>,
) -> Result<Vec<SharedDigest<'_>>, OpenSearchError> {
    let mut digests: Vec<SharedDigest<'_>> = Vec::new();
    for (metric, spec) in metrics {
        let position = digests
            .iter()
            .position(|digest| *digest.metric == spec.metric)
            .unwrap_or_else(|| {
                digests.push(SharedDigest {
                    metric: &spec.metric,
                    owner: metric,
                    percents: Vec::with_capacity(spec.percents.len()),
                    members: Vec::new(),
                });
                digests.len() - 1
            });
        let digest = &mut digests[position];
        let elements = spec
            .percents
            .iter()
            .map(|percent| digest.add_percent(*percent))
            .collect::<Result<Vec<_>, _>>()?;
        digest.members.push((metric, elements));
    }
    Ok(digests)
}

/// Column of a shared digest in the stage aggregate's output; never rendered.
fn name_shared_digest_column(owner: usize) -> String {
    format!("__digest_{owner}")
}

/// The `percentile_ranks` aggregate over a metric's per-row `value`: a
/// `List<Int64>` holding the count of values, then, per requested value in
/// request order, the count of values at or below it; `NULL` without a value.
/// A `NaN` is no value: it counts neither at a threshold nor in the total.
pub(super) fn plan_rank_counts_aggregate(spec: &PercentileRanksSpec, value: Expr) -> Expr {
    let mut arguments = Vec::with_capacity(spec.values.len() + 1);
    arguments.push(value);
    arguments.extend(spec.values.iter().map(|threshold| lit(*threshold)));
    AggregateUDF::new_from_impl(RankCounts {
        signature: Signature::user_defined(Volatility::Immutable),
    })
    .call(arguments)
}

/// The element of the `quantiles` list a `terms` order path names: by
/// `<id>[p]` / `<id>['p']` (`path.percent`) or `<id>.p` (`path.statistic`).
///
/// # Errors
///
/// `aggregation_execution_exception` when the path names no percent or a
/// percent `spec` does not compute.
pub(super) fn find_percentile_order_element(
    spec: &PercentilesSpec,
    path: &MetricPath,
) -> Result<usize, OpenSearchError> {
    let expected_key = match (&path.percent, &path.statistic) {
        (Some(percent), _) => Some(percent.clone()),
        (None, Some(statistic)) => statistic
            .parse::<f64>()
            .ok()
            .filter(|percent| percent.is_finite())
            .map(format_double_key),
        (None, None) => None,
    };
    expected_key
        .and_then(|expected_key| {
            spec.percents
                .iter()
                .position(|percent| format_double_key(*percent) == expected_key)
        })
        .ok_or_else(|| {
            OpenSearchError::aggregation_execution(&format!(
                "order path of [{}] names no percent the percentiles aggregation computes",
                path.aggregation
            ))
        })
}

/// Element `element` of a `quantiles` statistic as a `Float64` (NULL for a
/// bucket without a value).
///
/// # Errors
///
/// `internal` when `value` is not the list `multi_quantile` produces, holds no element
/// `element`, or holds a non-finite estimate there.
pub(super) fn select_quantile_element(value: &ScalarValue, element: usize) -> Result<ScalarValue, OpenSearchError> {
    let Some(estimates) = read_quantile_estimates(value)? else {
        return Ok(ScalarValue::Float64(None));
    };
    if element >= estimates.len() {
        return Err(OpenSearchError::internal(format!(
            "quantiles statistic holds {} estimates, so it has no element {element}",
            estimates.len()
        )));
    }
    if estimates.is_null(element) {
        return Ok(ScalarValue::Float64(None));
    }
    check_finite_estimate(estimates.value(element)).map(|estimate| ScalarValue::Float64(Some(estimate)))
}

/// Decodes a `quantiles` statistic; `None` for a bucket without a value. Every
/// estimate returned is finite or NULL.
///
/// # Errors
///
/// `internal` when `value` is not a list of `Float64`, and for a non-finite estimate:
/// a `null` would claim the bucket has no value.
pub(super) fn decode_quantiles(value: &ScalarValue) -> Result<Option<Vec<Option<f64>>>, OpenSearchError> {
    let Some(estimates) = read_quantile_estimates(value)? else {
        return Ok(None);
    };
    estimates
        .iter()
        .map(|estimate| estimate.map(check_finite_estimate).transpose())
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// The estimates of a `quantiles` statistic; `None` for a bucket without a value.
///
/// # Errors
///
/// `internal` when `value` is not a list of `Float64`.
fn read_quantile_estimates(value: &ScalarValue) -> Result<Option<Float64Array>, OpenSearchError> {
    let list = match value {
        ScalarValue::Null => return Ok(None),
        ScalarValue::List(list) => list,
        other => {
            return Err(OpenSearchError::internal(format!(
                "quantiles statistic [{other}] is not a list"
            )));
        }
    };
    if list.is_empty() || list.is_null(0) {
        return Ok(None);
    }
    // The clone shares the list's buffers; it only outlives the sliced element array.
    list.value(0)
        .as_primitive_opt::<Float64Type>()
        .cloned()
        .map(Some)
        .ok_or_else(|| OpenSearchError::internal("quantiles statistic holds non-Float64 estimates"))
}

/// `estimate` when it is finite.
///
/// # Errors
///
/// `internal` for a non-finite estimate: a `null` would claim the bucket has no value.
fn check_finite_estimate(estimate: f64) -> Result<f64, OpenSearchError> {
    if estimate.is_finite() {
        Ok(estimate)
    } else {
        Err(OpenSearchError::internal(format!(
            "quantile estimate [{estimate}] is not finite"
        )))
    }
}

/// Decodes a `rank_counts` statistic (`[count, at_or_below_1, …]`); `None` for
/// a bucket without a value.
///
/// # Errors
///
/// `internal` when `value` is not a list of non-null `Int64`.
pub(super) fn decode_rank_counts(value: &ScalarValue) -> Result<Option<Vec<i64>>, OpenSearchError> {
    let list = match value {
        ScalarValue::Null => return Ok(None),
        ScalarValue::List(list) => list,
        other => {
            return Err(OpenSearchError::internal(format!(
                "rank_counts statistic [{other}] is not a list"
            )));
        }
    };
    if list.is_empty() || list.is_null(0) {
        return Ok(None);
    }
    let items = list.value(0);
    let counts = items
        .as_primitive_opt::<Int64Type>()
        .filter(|counts| counts.null_count() == 0)
        .ok_or_else(|| OpenSearchError::internal("rank_counts statistic holds non-Int64 counts"))?;
    Ok(Some(counts.values().to_vec()))
}

/// Renders a `percentiles` result; `quantiles` is `None` for a bucket without a value.
///
/// The estimates are finite or NULL, as [`decode_quantiles`] returns them; a NULL renders
/// `null`.
pub(super) fn render_percentiles(spec: &PercentilesSpec, quantiles: Option<&[Option<f64>]>) -> Value {
    render_values(&spec.percents, spec.is_keyed, |index| {
        quantiles.and_then(|estimates| estimates.get(index).copied().flatten())
    })
}

/// Renders a `percentile_ranks` result from `[count, at_or_below_1, …]`;
/// `counts` is `None` for a bucket without a value. A rank is
/// `100 · at_or_below / count`, exact and within `[0, 100]`.
pub(super) fn render_percentile_ranks(spec: &PercentileRanksSpec, counts: Option<&[i64]>) -> Value {
    let (total, at_or_below_counts) = counts.and_then(<[i64]>::split_first).unzip();
    render_values(&spec.values, spec.is_keyed, |index| {
        let total = total.copied().filter(|total| *total > 0)?;
        let matching_count = at_or_below_counts.and_then(|counts| counts.get(index).copied())?;
        #[expect(clippy::cast_precision_loss, reason = "a bucket's row count is far below 2^53")]
        let share = 100.0 * matching_count as f64 / total as f64;
        Some(share)
    })
}

/// The `values` member shared by both kinds: keyed by [`format_double_key`]
/// or an array of `{key, value}` in request order. Both callers pass finite
/// values, so a value renders as a JSON number and only a missing one as `null`.
fn render_values(keys: &[f64], is_keyed: bool, value_at: impl Fn(usize) -> Option<f64>) -> Value {
    if is_keyed {
        let values: Map<String, Value> = keys
            .iter()
            .enumerate()
            .map(|(index, key)| (format_double_key(*key), json!(value_at(index))))
            .collect();
        json!({ "values": values })
    } else {
        let values: Vec<Value> = keys
            .iter()
            .enumerate()
            .map(|(index, key)| json!({ "key": key, "value": value_at(index) }))
            .collect();
        json!({ "values": values })
    }
}

/// The constant `Float64` arguments after the first, as both aggregates take them.
fn read_constant_arguments(name: &str, exprs: &[Arc<dyn PhysicalExpr>]) -> DataFusionResult<Vec<f64>> {
    // A constant evaluates against an empty batch; a column reference cannot.
    let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let constants = exprs
        .iter()
        .skip(1)
        .map(|expr| match expr.evaluate(&empty_batch)? {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(constant))) if constant.is_finite() => Ok(constant),
            _ => Err(DataFusionError::Plan(format!(
                "{name} takes constant finite Float64 arguments after the value"
            ))),
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    if constants.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{name} needs at least one argument after the value"
        )));
    }
    Ok(constants)
}

/// Every argument of the aggregate `name` as a `Float64`: a value and at least one constant
/// after it, all numeric (or NULL).
fn coerce_to_doubles(name: &str, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
    if arg_types.len() < 2 {
        return Err(DataFusionError::Plan(format!(
            "{name} takes a value and at least one argument after it"
        )));
    }
    if let Some(other_type) = arg_types.iter().find(|arg_type| !(arg_type.is_numeric() || arg_type.is_null())) {
        return Err(DataFusionError::Plan(format!(
            "{name} takes numeric arguments, got {other_type}"
        )));
    }
    Ok(vec![DataType::Float64; arg_types.len()])
}

/// `List<item>` with nullable items.
fn build_list_type(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(item, true)))
}

/// The `multi_quantile` aggregate function.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MultiQuantile {
    signature: Signature,
}

impl AggregateUDFImpl for MultiQuantile {
    fn name(&self) -> &'static str {
        "multi_quantile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(build_list_type(DataType::Float64))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        coerce_to_doubles(self.name(), arg_types)
    }

    /// The digest's own six-column state (`TDigest::to_scalar_state`).
    fn state_fields(&self, args: StateFieldsArgs) -> DataFusionResult<Vec<FieldRef>> {
        Ok(vec![
            Field::new(format_state_name(args.name, "max_size"), DataType::UInt64, false),
            Field::new(format_state_name(args.name, "sum"), DataType::Float64, false),
            Field::new(format_state_name(args.name, "count"), DataType::Float64, false),
            Field::new(format_state_name(args.name, "max"), DataType::Float64, false),
            Field::new(format_state_name(args.name, "min"), DataType::Float64, false),
            Field::new_list(
                format_state_name(args.name, "centroids"),
                Field::new_list_field(DataType::Float64, true),
                false,
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> DataFusionResult<Box<dyn Accumulator>> {
        let percents = read_constant_arguments("multi_quantile", args.exprs)?;
        if let Some(outside_percent) = percents.iter().find(|percent| !(0.0..=100.0).contains(*percent)) {
            return Err(DataFusionError::Plan(format!(
                "multi_quantile percent {outside_percent} is outside [0, 100]"
            )));
        }
        Ok(Box::new(MultiQuantileAccumulator {
            digest: TDigest::new(DEFAULT_MAX_SIZE),
            percents,
        }))
    }
}

/// One group's digest and the percents it answers.
#[derive(Debug)]
struct MultiQuantileAccumulator {
    digest: TDigest,
    percents: Vec<f64>,
}

impl Accumulator for MultiQuantileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        let Some(input) = values.first() else {
            return Ok(());
        };
        // `merge_unsorted_f64` takes an owned `Vec`, so the finite values are collected once,
        // skipping NULLs and non-finite values in the same pass.
        let finite_values: Vec<f64> = input
            .as_primitive_opt::<Float64Type>()
            .ok_or_else(|| DataFusionError::Execution("multi_quantile input is not Float64".to_owned()))?
            .iter()
            .flatten()
            .filter(|value| value.is_finite())
            .collect();
        if !finite_values.is_empty() {
            self.digest = self.digest.merge_unsorted_f64(finite_values);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        // The count is a sum of positive weights: below one means no value.
        if self.digest.count() < 1.0 {
            return ScalarValue::try_from(&build_list_type(DataType::Float64));
        }
        let estimates: Vec<ScalarValue> = self
            .percents
            .iter()
            .map(|percent| ScalarValue::Float64(Some(self.digest.estimate_quantile(percent / 100.0))))
            .collect();
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &estimates,
            &DataType::Float64,
        )))
    }

    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok(self.digest.to_scalar_state())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        let Some(first_state_column) = states.first() else {
            return Ok(());
        };
        let mut digests = Vec::with_capacity(first_state_column.len() + 1);
        for row in 0..first_state_column.len() {
            let state = states
                .iter()
                .map(|state_column| ScalarValue::try_from_array(state_column, row))
                .collect::<DataFusionResult<Vec<_>>>()?;
            // The state comes from `state` above, the only input this decoder trusts.
            digests.push(TDigest::from_scalar_state(&state));
        }
        digests.push(self.digest.clone());
        self.digest = TDigest::merge_digests(&digests);
        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.digest.size() - size_of_val(&self.digest) + self.percents.capacity() * size_of::<f64>()
    }
}

/// The `rank_counts` aggregate function behind `percentile_ranks`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RankCounts {
    signature: Signature,
}

impl AggregateUDFImpl for RankCounts {
    fn name(&self) -> &'static str {
        "rank_counts"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(build_list_type(DataType::Int64))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        coerce_to_doubles(self.name(), arg_types)
    }

    /// One count per interval between the sorted thresholds.
    fn state_fields(&self, args: StateFieldsArgs) -> DataFusionResult<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new_list(
            format_state_name(args.name, "interval_counts"),
            Field::new_list_field(DataType::Int64, true),
            false,
        ))])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> DataFusionResult<Box<dyn Accumulator>> {
        let thresholds = read_constant_arguments("rank_counts", args.exprs)?;
        let mut sorted_thresholds: Vec<(f64, usize)> = thresholds.into_iter().zip(0..).collect();
        sorted_thresholds.sort_by(|left, right| left.0.total_cmp(&right.0));
        Ok(Box::new(RankCountsAccumulator {
            interval_counts: vec![0; sorted_thresholds.len() + 1],
            sorted_thresholds,
        }))
    }
}

/// One group's value counts per threshold interval.
#[derive(Debug)]
struct RankCountsAccumulator {
    /// The thresholds ascending, each with its position in the request.
    sorted_thresholds: Vec<(f64, usize)>,
    /// `interval_counts[p]`: values above the first `p` sorted thresholds and at
    /// or below the others (the last interval: above every threshold). A value
    /// costs one binary search; the counts per threshold are prefix sums.
    interval_counts: Vec<i64>,
}

impl Accumulator for RankCountsAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        let Some(input) = values.first() else {
            return Ok(());
        };
        let numbers = input
            .as_primitive_opt::<Float64Type>()
            .ok_or_else(|| DataFusionError::Execution("rank_counts input is not Float64".to_owned()))?;
        // A `NaN` orders below no threshold, so the search would count it at or below every
        // one; it is no value. Infinities order correctly and count.
        for value in numbers.iter().flatten().filter(|value| !value.is_nan()) {
            let interval = self.sorted_thresholds.partition_point(|(threshold, _)| *threshold < value);
            let count = self
                .interval_counts
                .get_mut(interval)
                .ok_or_else(|| DataFusionError::Execution(format!("rank_counts holds no interval {interval}")))?;
            *count += 1;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        let total: i64 = self.interval_counts.iter().sum();
        if total == 0 {
            return ScalarValue::try_from(&build_list_type(DataType::Int64));
        }
        let mut at_or_below_counts = vec![0_i64; self.sorted_thresholds.len()];
        let mut running_count = 0_i64;
        for ((_, position), count) in self.sorted_thresholds.iter().zip(&self.interval_counts) {
            running_count += count;
            let slot = at_or_below_counts.get_mut(*position).ok_or_else(|| {
                DataFusionError::Execution(format!("rank_counts holds no threshold at position {position}"))
            })?;
            *slot = running_count;
        }
        let counts: Vec<ScalarValue> = std::iter::once(total)
            .chain(at_or_below_counts)
            .map(|count| ScalarValue::Int64(Some(count)))
            .collect();
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &counts,
            &DataType::Int64,
        )))
    }

    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        let counts: Vec<ScalarValue> = self
            .interval_counts
            .iter()
            .map(|count| ScalarValue::Int64(Some(*count)))
            .collect();
        Ok(vec![ScalarValue::List(ScalarValue::new_list_nullable(
            &counts,
            &DataType::Int64,
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        let Some(state_column) = states.first() else {
            return Ok(());
        };
        let lists = state_column
            .as_list_opt::<i32>()
            .ok_or_else(|| DataFusionError::Execution("rank_counts state is not a list".to_owned()))?;
        for row in 0..lists.len() {
            if lists.is_null(row) {
                continue;
            }
            let items = lists.value(row);
            let counts = items
                .as_primitive_opt::<Int64Type>()
                .ok_or_else(|| DataFusionError::Execution("rank_counts state is not Int64".to_owned()))?;
            if counts.len() != self.interval_counts.len() {
                return Err(DataFusionError::Execution(format!(
                    "rank_counts state holds {} interval counts where {} are expected",
                    counts.len(),
                    self.interval_counts.len()
                )));
            }
            for (total, count) in self.interval_counts.iter_mut().zip(counts.values()) {
                *total += count;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.sorted_thresholds.capacity() * size_of::<(f64, usize)>()
            + self.interval_counts.capacity() * size_of::<i64>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::StatusCode;
    use datafusion::arrow::array::{Float64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::MemTable;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::col;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use serde_json::json;

    use super::super::metrics::{MetricKind, render_metric};
    use super::super::parse::AggregationKind;
    use super::super::plan::{Granularity, MetricSlot};
    use super::*;
    use crate::opensearch::error::render_envelope;

    /// The status and `error.type` an error renders.
    async fn render_refusal(error: OpenSearchError) -> (StatusCode, Value) {
        let (status, body) = render_envelope(error).await;
        (status, body["error"]["type"].clone())
    }

    fn build_metric_spec(field: &str) -> MetricSpec {
        MetricSpec {
            field: field.to_owned(),
            missing: None,
        }
    }

    /// Plans `aggregate` (built over the column `value`) grouped by `group`, ordered by group,
    /// over a memory table holding one partition of `(groups, values)` rows per entry of
    /// `partitions`.
    async fn plan_by_group(
        partitions: Vec<(Vec<&str>, Vec<Option<f64>>)>,
        aggregate: Expr,
    ) -> (Arc<dyn ExecutionPlan>, Arc<TaskContext>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let partition_batches: Vec<Vec<RecordBatch>> = partitions
            .into_iter()
            .map(|(groups, values)| {
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(groups)),
                        Arc::new(Float64Array::from(values)),
                    ],
                )
                .expect("batch matches the schema");
                vec![batch]
            })
            .collect();
        // As many target partitions as the widest table here, so the planner keeps each table
        // partition at one partial aggregate instead of spreading it over more.
        let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        context
            .register_table(
                "rows",
                Arc::new(MemTable::try_new(schema, partition_batches).expect("memory table")),
            )
            .expect("table registers");
        let plan = context
            .table("rows")
            .await
            .expect("table resolves")
            .aggregate(vec![col("group")], vec![aggregate.alias("result")])
            .expect("aggregate plans")
            .sort(vec![col("group").sort(true, false)])
            .expect("sort plans")
            .create_physical_plan()
            .await
            .expect("physical plan");
        (plan, context.task_ctx())
    }

    /// The input partition count of every partial aggregate of `plan`, in plan order.
    fn list_partial_aggregate_partitions(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
        let mut partition_counts = Vec::new();
        plan.apply(|node| {
            if let Some(aggregate) = node.as_ref().downcast_ref::<AggregateExec>()
                && matches!(aggregate.mode(), AggregateMode::Partial)
            {
                partition_counts.push(aggregate.input().output_partitioning().partition_count());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("walk the plan");
        partition_counts
    }

    /// Runs a plan of [`plan_by_group`]; returns each group's result, ordered by group.
    async fn collect_groups(plan: Arc<dyn ExecutionPlan>, task: Arc<TaskContext>) -> Vec<(String, ScalarValue)> {
        let batches = collect(plan, task).await.expect("aggregate runs");
        let mut results = Vec::new();
        for batch in &batches {
            let names = batch.column(0).as_string::<i32>();
            for row in 0..batch.num_rows() {
                let result = ScalarValue::try_from_array(batch.column(1).as_ref(), row).expect("a result");
                results.push((names.value(row).to_owned(), result));
            }
        }
        results
    }

    /// Runs `aggregate` (built over the column `value`) grouped by `group` over
    /// the given rows, one partition; returns each group's result, ordered by group.
    async fn aggregate_by_group(
        groups: Vec<&str>,
        values: Vec<Option<f64>>,
        aggregate: Expr,
    ) -> Vec<(String, ScalarValue)> {
        let (plan, task) = plan_by_group(vec![(groups, values)], aggregate).await;
        collect_groups(plan, task).await
    }

    fn plan_quantiles_call(percents: &[f64]) -> Expr {
        plan_percentiles_aggregate(percents, col("value"))
    }

    #[tokio::test]
    async fn multi_quantile_returns_every_requested_percent_in_order_and_null_for_an_empty_group() {
        // Group "a" holds one distinct value, so every quantile is that value;
        // group "b" holds only NULLs, so it has no estimate at all.
        let results = aggregate_by_group(
            vec!["a", "a", "a", "b"],
            vec![Some(250.0), Some(250.0), Some(250.0), None],
            plan_quantiles_call(&[99.0, 50.0, 1.0]),
        )
        .await;
        let decoded_quantiles: Vec<(String, Option<Vec<Option<f64>>>)> = results
            .iter()
            .map(|(group, value)| (group.clone(), decode_quantiles(value).expect("a quantile list")))
            .collect();
        assert_eq!(
            decoded_quantiles,
            vec![
                ("a".to_owned(), Some(vec![Some(250.0), Some(250.0), Some(250.0)])),
                ("b".to_owned(), None),
            ]
        );
    }

    #[tokio::test]
    async fn percentile_values_lie_within_the_stated_tolerance_of_exact_quantiles() {
        // Stated tolerance: the share of values at or below each estimate differs
        // from the requested percent / 100 by at most 0.02. The oracle is the
        // sorted input, independent of the digest.
        const RANK_TOLERANCE: f64 = 0.02;
        let percents = [1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0];
        let mut random = StdRng::seed_from_u64(20_261_008);
        // A uniform group and a heavy right tail (cubed uniform), 10 001 rows each.
        let mut groups = Vec::new();
        let mut values = Vec::new();
        let mut tail = Vec::new();
        let mut uniform = Vec::new();
        for _ in 0..10_001 {
            let draw: f64 = random.random_range(0.0..10_000.0);
            let cubed = draw.powi(3) / 1.0e8;
            groups.push("tail");
            values.push(Some(cubed));
            tail.push(cubed);
            groups.push("uniform");
            values.push(Some(draw));
            uniform.push(draw);
        }
        tail.sort_by(f64::total_cmp);
        uniform.sort_by(f64::total_cmp);
        let results = aggregate_by_group(groups, values, plan_quantiles_call(&percents)).await;
        assert_eq!(results.len(), 2);
        let group_names: Vec<&str> = results.iter().map(|(group, _)| group.as_str()).collect();
        assert_eq!(group_names, vec!["tail", "uniform"]);
        for ((group, result), sorted_values) in results.iter().zip([&tail, &uniform]) {
            let estimates = decode_quantiles(result)
                .expect("a quantile list")
                .expect("a non-empty group has estimates");
            assert_eq!(estimates.len(), percents.len(), "group {group}");
            for (percent, estimate) in percents.iter().zip(estimates) {
                let estimate = estimate.expect("every percent has an estimate");
                let at_or_below_count = sorted_values.partition_point(|value| *value <= estimate);
                #[expect(clippy::cast_precision_loss, reason = "10 001 rows are exact in f64")]
                let rank = at_or_below_count as f64 / sorted_values.len() as f64;
                assert!(
                    (rank - percent / 100.0).abs() <= RANK_TOLERANCE,
                    "group {group}: percent {percent} estimated {estimate} at rank {rank}"
                );
            }
        }
    }

    #[tokio::test]
    async fn rank_counts_count_each_threshold_exactly_and_skip_nulls() {
        // Values 100, 200, 300, 400, a NULL and a NaN; thresholds in request order
        // 250, 400, 50: 2, 4 and 0 values lie at or below them. The NaN is no value,
        // so it counts neither at a threshold nor in the total.
        let spec = PercentileRanksSpec {
            metric: build_metric_spec("value"),
            values: vec![250.0, 400.0, 50.0],
            is_keyed: false,
        };
        let results = aggregate_by_group(
            vec!["a", "a", "a", "a", "a", "a", "b"],
            vec![
                Some(400.0),
                Some(100.0),
                None,
                Some(f64::NAN),
                Some(300.0),
                Some(200.0),
                None,
            ],
            plan_rank_counts_aggregate(&spec, col("value")),
        )
        .await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "a");
        assert_eq!(
            decode_rank_counts(&results[0].1).expect("a count list"),
            Some(vec![4, 2, 4, 0])
        );
        assert_eq!(results[1].0, "b");
        assert_eq!(decode_rank_counts(&results[1].1).expect("a count list"), None);
    }

    #[tokio::test]
    async fn partial_states_of_one_group_merge_across_partitions() {
        // Group "a" holds 100 and 200 in the first partition and 300 and 400 in the second, so
        // its result merges two partial states. Of its four values, 2, 4 and 0 lie at or below
        // 250, 400 and 50; percent 0 is the minimum 100 and percent 100 the maximum 400 exactly.
        // Each answer needs both states.
        let partitions = || {
            vec![
                (vec!["a", "a"], vec![Some(100.0), Some(200.0)]),
                (vec!["a", "a"], vec![Some(300.0), Some(400.0)]),
            ]
        };
        let spec = PercentileRanksSpec {
            metric: build_metric_spec("value"),
            values: vec![250.0, 400.0, 50.0],
            is_keyed: false,
        };
        let (plan, task) = plan_by_group(partitions(), plan_rank_counts_aggregate(&spec, col("value"))).await;
        // One partial aggregate reads both partitions, one partial state per partition.
        assert_eq!(list_partial_aggregate_partitions(&plan), vec![2]);
        let results = collect_groups(plan, task).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "a");
        assert_eq!(
            decode_rank_counts(&results[0].1).expect("a count list"),
            Some(vec![4, 2, 4, 0])
        );
        let (plan, task) = plan_by_group(partitions(), plan_quantiles_call(&[0.0, 100.0])).await;
        assert_eq!(list_partial_aggregate_partitions(&plan), vec![2]);
        let results = collect_groups(plan, task).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "a");
        assert_eq!(
            decode_quantiles(&results[0].1).expect("a quantile list"),
            Some(vec![Some(100.0), Some(400.0)])
        );
    }

    #[test]
    fn multi_quantile_merges_every_state_of_a_batch_into_the_digest_it_holds() {
        // Four partial states over 100, 400, 200 and 300. The first merge reads the minimum and
        // the maximum in two rows of one batch; the second merges the other two into the digest
        // the first left. Percents 0 and 100 are exact, so a dropped row or a dropped digest
        // moves an answer.
        let build_digest_state = |value: f64| {
            let mut accumulator = MultiQuantileAccumulator {
                digest: TDigest::new(DEFAULT_MAX_SIZE),
                percents: vec![0.0, 100.0],
            };
            let input: ArrayRef = Arc::new(Float64Array::from(vec![value]));
            accumulator.update_batch(&[input]).expect("update the digest");
            accumulator.state().expect("a digest state")
        };
        let build_state_columns = |values: &[f64]| -> Vec<ArrayRef> {
            let states: Vec<Vec<ScalarValue>> = values.iter().map(|value| build_digest_state(*value)).collect();
            (0..states[0].len())
                .map(|column| {
                    ScalarValue::iter_to_array(states.iter().map(|state| state[column].clone()))
                        .expect("a state column")
                })
                .collect()
        };
        let mut accumulator = MultiQuantileAccumulator {
            digest: TDigest::new(DEFAULT_MAX_SIZE),
            percents: vec![0.0, 100.0],
        };
        accumulator
            .merge_batch(&build_state_columns(&[100.0, 400.0]))
            .expect("merge the first states");
        accumulator
            .merge_batch(&build_state_columns(&[200.0, 300.0]))
            .expect("merge the second states");
        let result = accumulator.evaluate().expect("an estimate list");
        assert_eq!(
            decode_quantiles(&result).expect("a quantile list"),
            Some(vec![Some(100.0), Some(400.0)])
        );
    }

    #[test]
    fn a_rank_counts_state_of_another_interval_count_is_an_execution_error() {
        // Two thresholds make three intervals; the state holds two.
        let mut accumulator = RankCountsAccumulator {
            sorted_thresholds: vec![(250.0, 0), (400.0, 1)],
            interval_counts: vec![0; 3],
        };
        let state = ScalarValue::List(ScalarValue::new_list_nullable(
            &[ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(1))],
            &DataType::Int64,
        ))
        .to_array()
        .expect("a state array");
        let error = accumulator
            .merge_batch(&[state])
            .expect_err("a state of another length is refused");
        assert!(matches!(error, DataFusionError::Execution(_)), "{error}");
    }

    #[test]
    fn percentiles_parse_numbers_and_numeric_strings_and_default_to_the_stated_list() {
        let stated_spec = parse_percentiles(
            "1",
            &json!({"field": "duration_micros", "percents": [25, "50.5", "99"]}),
        )
        .expect("numbers and numeric strings are percents");
        assert_eq!(stated_spec.percents, vec![25.0, 50.5, 99.0]);
        assert!(stated_spec.is_keyed);
        assert_eq!(stated_spec.metric, build_metric_spec("duration_micros"));
        let default_spec = parse_percentiles(
            "1",
            &json!({"field": "duration_micros", "keyed": false, "missing": "0"}),
        )
        .expect("percents are optional");
        assert_eq!(default_spec.percents, vec![1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0]);
        assert!(!default_spec.is_keyed);
        assert_eq!(default_spec.metric.missing, Some(0.0));
    }

    #[tokio::test]
    async fn a_percent_outside_zero_to_one_hundred_or_of_another_type_is_a_parsing_exception() {
        for body in [
            json!({"field": "duration_micros", "percents": [101]}),
            json!({"field": "duration_micros", "percents": [-1]}),
            json!({"field": "duration_micros", "percents": ["fifty"]}),
            json!({"field": "duration_micros", "percents": [true]}),
            json!({"field": "duration_micros", "percents": []}),
            json!({"field": "duration_micros", "percents": 50}),
        ] {
            let error = parse_percentiles("1", &body).expect_err("refused");
            assert_eq!(
                render_refusal(error).await,
                (StatusCode::BAD_REQUEST, json!("parsing_exception")),
                "{body}"
            );
        }
    }

    #[tokio::test]
    async fn a_malformed_percentiles_body_is_a_parsing_exception_naming_an_unknown_key() {
        for body in [
            json!({"field": "duration_micros", "keyed": "yes"}),
            json!({"field": "duration_micros", "compression": 100}),
            json!({"percents": [50]}),
        ] {
            let error = parse_percentiles("1", &body).expect_err("refused");
            assert_eq!(
                render_refusal(error).await,
                (StatusCode::BAD_REQUEST, json!("parsing_exception")),
                "{body}"
            );
        }
        // An unknown key is named under `construct`.
        let error = parse_percentiles("1", &json!({"field": "duration_micros", "compression": 100}))
            .expect_err("an unknown key is refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], json!("parsing_exception"));
        assert_eq!(body["error"]["construct"], json!("compression"));
    }

    #[tokio::test]
    async fn percentile_ranks_take_json_numbers_only() {
        let spec = parse_percentile_ranks(
            "1",
            &json!({"field": "duration_micros", "values": [250, 400.5], "keyed": false}),
        )
        .expect("numbers are values");
        assert_eq!(spec.values, vec![250.0, 400.5]);
        assert!(!spec.is_keyed);
        for body in [
            json!({"field": "duration_micros", "values": ["250"]}),
            json!({"field": "duration_micros", "values": []}),
            json!({"field": "duration_micros"}),
            json!({"field": "duration_micros", "values": [1], "missing": 0}),
        ] {
            let error = parse_percentile_ranks("1", &body).expect_err("refused");
            assert_eq!(
                render_refusal(error).await,
                (StatusCode::BAD_REQUEST, json!("parsing_exception")),
                "{body}"
            );
        }
        // `missing` is an unknown key of this kind, named under `construct`.
        let error = parse_percentile_ranks("1", &json!({"field": "duration_micros", "values": [1], "missing": 0}))
            .expect_err("missing is refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], json!("parsing_exception"));
        assert_eq!(body["error"]["construct"], json!("missing"));
    }

    #[test]
    fn double_keys_render_one_decimal_or_scientific_as_the_contract_states() {
        let cases = [
            (25.0, "25.0"),
            (50.0, "50.0"),
            (50.5, "50.5"),
            (99.9, "99.9"),
            (0.0, "0.0"),
            (100.0, "100.0"),
            (0.001, "0.001"),
            (1_500_000.0, "1500000.0"),
            (10_000_000.0, "1.0E7"),
            (0.0001, "1.0E-4"),
            (12_345_678.9, "1.23456789E7"),
        ];
        for (value, expected) in cases {
            assert_eq!(format_double_key(value), expected, "{value}");
        }
    }

    #[test]
    fn percentiles_render_keyed_keys_and_an_array_when_keyed_is_false() {
        let keyed_spec = PercentilesSpec {
            metric: build_metric_spec("duration_micros"),
            percents: vec![25.0, 50.5],
            is_keyed: true,
        };
        assert_eq!(
            render_percentiles(&keyed_spec, Some(&[Some(100.0), Some(250.0)])),
            json!({"values": {"25.0": 100.0, "50.5": 250.0}})
        );
        assert_eq!(
            render_percentiles(&keyed_spec, None),
            json!({"values": {"25.0": null, "50.5": null}})
        );
        let listed_spec = PercentilesSpec {
            is_keyed: false,
            ..keyed_spec
        };
        assert_eq!(
            render_percentiles(&listed_spec, Some(&[Some(100.0), Some(250.0)])),
            json!({"values": [{"key": 25.0, "value": 100.0}, {"key": 50.5, "value": 250.0}]})
        );
    }

    #[test]
    fn percentile_ranks_render_exact_shares_and_null_without_values() {
        let spec = PercentileRanksSpec {
            metric: build_metric_spec("duration_micros"),
            values: vec![250.0, 400.0, 50.0],
            is_keyed: true,
        };
        assert_eq!(
            render_percentile_ranks(&spec, Some(&[4, 2, 4, 0])),
            json!({"values": {"250.0": 50.0, "400.0": 100.0, "50.0": 0.0}})
        );
        assert_eq!(
            render_percentile_ranks(&spec, None),
            json!({"values": {"250.0": null, "400.0": null, "50.0": null}})
        );
    }

    #[tokio::test]
    async fn a_non_finite_quantile_estimate_is_an_internal_error() {
        // A non-finite estimate is refused: a `null` would claim the bucket has no value.
        let list = ScalarValue::List(ScalarValue::new_list_nullable(
            &[
                ScalarValue::Float64(Some(1.0)),
                ScalarValue::Float64(Some(f64::INFINITY)),
            ],
            &DataType::Float64,
        ));
        let error = decode_quantiles(&list).expect_err("an infinite estimate is refused");
        assert_eq!(
            render_refusal(error).await,
            (StatusCode::INTERNAL_SERVER_ERROR, json!("exception"))
        );
    }

    #[tokio::test]
    async fn an_order_element_reads_its_estimate_and_refuses_a_missing_or_non_finite_one() {
        let list = ScalarValue::List(ScalarValue::new_list_nullable(
            &[
                ScalarValue::Float64(Some(100.0)),
                ScalarValue::Float64(None),
                ScalarValue::Float64(Some(f64::INFINITY)),
            ],
            &DataType::Float64,
        ));
        assert_eq!(
            select_quantile_element(&list, 0).expect("a finite estimate"),
            ScalarValue::Float64(Some(100.0))
        );
        assert_eq!(
            select_quantile_element(&list, 1).expect("a NULL estimate"),
            ScalarValue::Float64(None)
        );
        assert_eq!(
            select_quantile_element(&ScalarValue::Null, 0).expect("a bucket without a value"),
            ScalarValue::Float64(None)
        );
        // Element 2 is infinite; element 3 does not exist.
        for element in [2, 3] {
            let error = select_quantile_element(&list, element).expect_err("refused");
            assert_eq!(
                render_refusal(error).await,
                (StatusCode::INTERNAL_SERVER_ERROR, json!("exception")),
                "element {element}"
            );
        }
    }

    #[tokio::test]
    async fn a_list_statistic_of_another_length_than_its_request_is_an_internal_error() {
        // One estimate for two percents; two counts where the total and two ranks make three.
        let metric_spec = build_metric_spec("duration_micros");
        let percentiles = AggregationKind::Percentiles(PercentilesSpec {
            metric: metric_spec.clone(),
            percents: vec![50.0, 99.0],
            is_keyed: true,
        });
        let ranks = AggregationKind::PercentileRanks(PercentileRanksSpec {
            metric: metric_spec.clone(),
            values: vec![250.0, 400.0],
            is_keyed: true,
        });
        let one_estimate = ScalarValue::List(ScalarValue::new_list_nullable(
            &[ScalarValue::Float64(Some(100.0))],
            &DataType::Float64,
        ));
        let two_counts = ScalarValue::List(ScalarValue::new_list_nullable(
            &[ScalarValue::Int64(Some(4)), ScalarValue::Int64(Some(2))],
            &DataType::Int64,
        ));
        for (kind, aggregation, statistic) in [
            (MetricKind::Percentiles, &percentiles, one_estimate),
            (MetricKind::PercentileRanks, &ranks, two_counts),
        ] {
            let slot = MetricSlot {
                name: "1",
                kind,
                spec: &metric_spec,
                aggregation,
                granularity: Granularity::Top,
            };
            let error = render_metric(&slot, false, &|_| Ok(statistic.clone())).expect_err("refused");
            assert_eq!(
                render_refusal(error).await,
                (StatusCode::INTERNAL_SERVER_ERROR, json!("exception")),
                "{kind:?}"
            );
        }
    }

    #[tokio::test]
    async fn percent_paths_split_bracketed_and_quoted_percents_into_their_key_form() {
        assert_eq!(
            split_percent_path("1[50.0]").expect("valid"),
            Some(("1", "50.0".to_owned()))
        );
        assert_eq!(
            split_percent_path("1['50.5']").expect("valid"),
            Some(("1", "50.5".to_owned()))
        );
        assert_eq!(
            split_percent_path("1[50]").expect("valid"),
            Some(("1", "50.0".to_owned()))
        );
        assert_eq!(split_percent_path("1-orderAgg").expect("valid"), None);
        assert_eq!(split_percent_path("1.95").expect("valid"), None);
        for invalid_path in ["1[50.0", "1[]", "1['fifty']", "1[NaN]", "1[inf]", "[50]"] {
            let error = split_percent_path(invalid_path).expect_err("an invalid percent path is refused");
            assert_eq!(
                render_refusal(error).await,
                (StatusCode::BAD_REQUEST, json!("parsing_exception")),
                "{invalid_path}"
            );
        }
    }

    #[tokio::test]
    async fn an_order_path_selects_the_element_of_its_percent() {
        let spec = PercentilesSpec {
            metric: build_metric_spec("duration_micros"),
            percents: vec![25.0, 50.0, 99.0],
            is_keyed: true,
        };
        let path = |statistic: Option<&str>, percent: Option<&str>| MetricPath {
            aggregation: "1".to_owned(),
            statistic: statistic.map(str::to_owned),
            percent: percent.map(str::to_owned),
        };
        assert_eq!(
            find_percentile_order_element(&spec, &path(None, Some("50.0"))).expect("computed"),
            1
        );
        assert_eq!(
            find_percentile_order_element(&spec, &path(Some("99"), None)).expect("computed"),
            2
        );
        for unknown_path in [path(None, Some("75.0")), path(Some("max"), None), path(None, None)] {
            let error = find_percentile_order_element(&spec, &unknown_path).expect_err("no such percent");
            assert_eq!(
                render_refusal(error).await,
                (StatusCode::BAD_REQUEST, json!("aggregation_execution_exception")),
                "{unknown_path:?}"
            );
        }
    }

    #[test]
    fn percentiles_reading_the_same_value_share_one_digest_over_the_union_of_their_percents() {
        let spec = |field: &str, percents: &[f64]| PercentilesSpec {
            metric: build_metric_spec(field),
            percents: percents.to_vec(),
            is_keyed: true,
        };
        let first_duration_spec = spec("duration_micros", &[50.0, 99.0]);
        let score_spec = spec("score", &[50.0]);
        let second_duration_spec = spec("duration_micros", &[99.0, 25.0]);
        let digests = group_shared_digests(vec![
            (0, &first_duration_spec),
            (2, &score_spec),
            (3, &second_duration_spec),
        ])
        .expect("percents group");
        let summary: Vec<(usize, Vec<f64>, Vec<(usize, Vec<i64>)>)> = digests
            .iter()
            .map(|digest| (digest.owner, digest.percents.clone(), digest.members.clone()))
            .collect();
        // Metrics 0 and 3 read duration_micros: the union is 50, 99, 25 in first-request
        // order, and each member reads its own percents by 1-based list element.
        assert_eq!(
            summary,
            vec![
                (0, vec![50.0, 99.0, 25.0], vec![(0, vec![1, 2]), (3, vec![2, 3])]),
                (2, vec![50.0], vec![(2, vec![1])]),
            ]
        );
    }
}
