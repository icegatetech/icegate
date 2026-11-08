//! Pipeline aggregations, run in Rust over the assembled and rendered tree.
//!
//! A parent pipeline (`cumulative_sum`, `derivative`, `serial_diff`, `moving_fn`, `moving_avg`,
//! `bucket_script`) reads one value per bucket of the multi-bucket aggregation it is a child of
//! and writes its result into each bucket; all but `bucket_script` need a `histogram` or
//! `date_histogram` parent, and `derivative.unit` a `date_histogram`. A sibling pipeline
//! (`avg_bucket`, `sum_bucket`, `min_bucket`, `max_bucket`) reads the buckets of the sibling
//! multi-bucket aggregation its path starts with and writes one result beside it. Paths resolve
//! against the rendered JSON, so a pipeline may read a metric, a bucket's `doc_count`, a
//! single-bucket child (`filter>_count`) or another pipeline's output.
//!
//! A bucket has no value at a path when it is empty (`doc_count` 0) or the value there is `null`;
//! under `gap_policy: insert_zeros` such a bucket counts as 0 instead. `cumulative_sum` takes no
//! `gap_policy`: a bucket without a value adds 0 to its total. The bare `_count` path is exempt
//! from every gap rule: a bucket's doc count is always a value, so an empty bucket's `_count`
//! reads 0, under either policy, and the empty-bucket renderings below do not apply to it.
//!
//! `format` is accepted and ignored on every pipeline body: pipelines render no
//! `value_as_string`.
//!
//! Every structural error is refused by [`validate_pipelines`] before any plan executes, so the
//! answer to a malformed request never depends on the data: the placement, every element and
//! target of every path (after a sibling path's first, multi-bucket aggregation, a single-bucket
//! aggregation before each `>`; at the end an aggregation of its level that renders a number,
//! with a key for a multi-value metric and only a key the metric renders), and cycles between
//! pipelines. [`apply_pipelines`] then computes the values; its errors are a backstop for a
//! rendered tree that does not match the validated request.
//!
//! Rendering on empty and first buckets:
//! - `cumulative_sum`: `{value}` on every bucket, the running total.
//! - `derivative`: no key on the first bucket; otherwise `v_i − v_{i−1}`, or `{value: null}` when
//!   either is missing; with `unit`, `normalized_value` is the value per unit of key distance
//!   (`null` with the value).
//! - `serial_diff`: `v_i − v_{i−lag}`; no key while `i < lag` or when either value is missing.
//! - `moving_fn`: no key on a bucket without a value; otherwise the function over the values of
//!   up to `window` preceding buckets that have one (oldest first), `{value: null}` when there are
//!   none.
//! - `moving_avg`: as `moving_fn`, but no key when there are none.
//! - `bucket_script`: no key when a variable has no value (so none on an empty bucket under
//!   `skip`); a non-finite result renders `{value: null}`.
//! - `avg_bucket` and `min_bucket` / `max_bucket` over no values render `null` (the latter with
//!   `keys: []`), `sum_bucket` renders 0.0; `min_bucket` and `max_bucket` list the keys of every
//!   bucket holding the extreme.

use std::cmp::Ordering;
use std::collections::VecDeque;

use serde_json::{Map, Number, Value, json};

use super::metrics::{EXTENDED_STATS_MEMBERS, STATS_MEMBERS};
use super::parse::{
    AggregationKind, CalendarUnit, HistogramInterval, IGNORED_EXECUTION_KEYS, NamedAggregation,
    classify_legacy_interval, expect_object, expect_string, read_lenient_number, read_strict_number,
    report_missing_key, report_unsupported_key,
};
use super::percentiles::format_double_key;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::script::{
    ArithmeticExpr, MovingFunction, VALUE_VARIABLE, read_script_source, recognize_arithmetic_script,
    recognize_moving_function,
};
use crate::opensearch::search::doc_id::{
    MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MILLISECOND, MICROS_PER_MINUTE, MICROS_PER_SECOND,
};

/// Keys of a `cumulative_sum` body; it takes no `gap_policy`.
const CUMULATIVE_SUM_KEYS: &[&str] = &["buckets_path", "format"];
/// Keys of a `derivative` body.
const DERIVATIVE_KEYS: &[&str] = &["buckets_path", "gap_policy", "format", "unit"];
/// Keys of a `serial_diff` body.
const SERIAL_DIFF_KEYS: &[&str] = &["buckets_path", "gap_policy", "format", "lag"];
/// Keys of a `moving_fn` body.
const MOVING_FN_KEYS: &[&str] = &["buckets_path", "gap_policy", "format", "window", "script"];
/// Keys of a legacy `moving_avg` body.
const MOVING_AVG_KEYS: &[&str] = &[
    "buckets_path",
    "gap_policy",
    "format",
    "window",
    "model",
    "settings",
    "minimize",
];
/// Keys of a `bucket_script` body.
const BUCKET_SCRIPT_KEYS: &[&str] = &["buckets_path", "gap_policy", "format", "script"];
/// Keys of a sibling pipeline body.
const SIBLING_KEYS: &[&str] = &["buckets_path", "gap_policy", "format"];

/// `moving_avg.window` when absent.
const DEFAULT_MOVING_AVG_WINDOW: usize = 5;
/// `moving_avg` `settings.alpha` when absent.
const DEFAULT_ALPHA: f64 = 0.3;
/// `moving_avg` `settings.beta` when absent.
const DEFAULT_BETA: f64 = 0.1;
/// Largest `window` and `lag`.
const MAX_PIPELINE_SPAN: f64 = 65_535.0;
/// Microseconds in a week, the longest calendar unit a `derivative.unit` may name.
const MICROS_PER_WEEK: i64 = 7 * MICROS_PER_DAY;
/// Longest `derivative.unit`, in milliseconds: one week.
const MAX_UNIT_MILLIS: f64 = convert_micros_to_millis(MICROS_PER_WEEK);

/// What a bucket without a value contributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GapPolicy {
    /// Nothing: the pipeline treats the bucket as missing.
    Skip,
    /// The value 0.
    InsertZeros,
}

/// A parsed `buckets_path`: the `>`-separated aggregations, then the target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BucketsPath {
    /// The aggregations before the last `>`, outermost first.
    pub(crate) aggregations: Vec<String>,
    /// The last element.
    pub(crate) target: PathTarget,
}

/// The last element of a `buckets_path`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathTarget {
    /// `_count`: the bucket's `doc_count`.
    Count,
    /// `<id>`, `<id>.<key>`, `<id>[<key>]` or `<id>['<key>']`.
    Metric {
        /// The metric (or pipeline) aggregation's name.
        aggregation: String,
        /// The value of a multi-value metric; `None` reads `value`.
        key: Option<String>,
    },
}

impl BucketsPath {
    /// The aggregation the path starts from, if any (a bare `_count` names none).
    fn find_first_aggregation(&self) -> Option<&str> {
        match (self.aggregations.first(), &self.target) {
            (Some(first_aggregation), _) => Some(first_aggregation),
            (None, PathTarget::Metric { aggregation, .. }) => Some(aggregation),
            (None, PathTarget::Count) => None,
        }
    }
}

/// Parses a `buckets_path`.
///
/// # Errors
///
/// `parsing_exception` for an empty path or element, or an unclosed or empty bracket.
pub(crate) fn parse_buckets_path(text: &str) -> Result<BucketsPath, OpenSearchError> {
    let build_path_error = || OpenSearchError::parsing(format!("invalid buckets_path [{text}]"));
    let mut elements: Vec<&str> = text.split('>').collect();
    let last_element = elements.pop().ok_or_else(build_path_error)?;
    if last_element.is_empty() || elements.iter().any(|element| element.is_empty()) {
        return Err(build_path_error());
    }
    let target = if last_element == "_count" {
        PathTarget::Count
    } else if let Some((aggregation, bracketed_key)) = last_element.split_once('[') {
        let inner_key = bracketed_key.strip_suffix(']').ok_or_else(build_path_error)?;
        let key = inner_key
            .strip_prefix('\'')
            .and_then(|quoted_key| quoted_key.strip_suffix('\''))
            .unwrap_or(inner_key);
        if aggregation.is_empty() || key.is_empty() {
            return Err(build_path_error());
        }
        PathTarget::Metric {
            aggregation: aggregation.to_owned(),
            key: Some(key.to_owned()),
        }
    } else if let Some((aggregation, key)) = last_element.split_once('.') {
        if aggregation.is_empty() || key.is_empty() {
            return Err(build_path_error());
        }
        PathTarget::Metric {
            aggregation: aggregation.to_owned(),
            key: Some(key.to_owned()),
        }
    } else {
        PathTarget::Metric {
            aggregation: last_element.to_owned(),
            key: None,
        }
    };
    Ok(BucketsPath {
        aggregations: elements.into_iter().map(str::to_owned).collect(),
        target,
    })
}

/// `cumulative_sum` and the sibling pipelines: one path and a gap policy.
#[derive(Debug, Clone)]
pub(crate) struct PipelineSpec {
    /// The value read per bucket.
    pub(crate) buckets_path: BucketsPath,
    /// What a bucket without a value contributes; always `Skip` for `cumulative_sum`, which adds
    /// 0 for it.
    pub(crate) gap_policy: GapPolicy,
}

/// A `derivative`.
#[derive(Debug, Clone)]
pub(crate) struct DerivativeSpec {
    /// The value read per bucket.
    pub(crate) buckets_path: BucketsPath,
    /// What a bucket without a value contributes.
    pub(crate) gap_policy: GapPolicy,
    /// `unit` in milliseconds; adds `normalized_value` when set.
    pub(crate) unit_millis: Option<f64>,
}

/// A `serial_diff`.
#[derive(Debug, Clone)]
pub(crate) struct SerialDiffSpec {
    /// The value read per bucket.
    pub(crate) buckets_path: BucketsPath,
    /// What a bucket without a value contributes.
    pub(crate) gap_policy: GapPolicy,
    /// Buckets between the two values differenced.
    pub(crate) lag: usize,
}

/// A `moving_fn`, and (as [`MovingAvgSpec`]) a legacy `moving_avg`.
#[derive(Debug, Clone)]
pub(crate) struct MovingFnSpec {
    /// The value read per bucket.
    pub(crate) buckets_path: BucketsPath,
    /// What a bucket without a value contributes.
    pub(crate) gap_policy: GapPolicy,
    /// Preceding values the function reads.
    pub(crate) window: usize,
    /// The recognized function or model.
    pub(crate) function: MovingFunction,
}

/// A legacy `moving_avg`: the same computation as `moving_fn`, rendered without a key where the
/// window is empty.
pub(crate) type MovingAvgSpec = MovingFnSpec;

/// A `bucket_script`.
#[derive(Debug, Clone)]
pub(crate) struct BucketScriptSpec {
    /// Script variable names and the paths they read, in request order.
    pub(crate) variables: Vec<(String, BucketsPath)>,
    /// What a bucket without a value contributes.
    pub(crate) gap_policy: GapPolicy,
    /// The recognized arithmetic over `params.<variable>`.
    pub(crate) script: ArithmeticExpr,
}

/// Parses the `cumulative_sum` aggregation `name`.
///
/// # Errors
///
/// `parsing_exception` for an unknown key (`gap_policy` included) or a missing or invalid
/// `buckets_path`.
pub(super) fn parse_cumulative_sum(name: &str, value: &Value) -> Result<PipelineSpec, OpenSearchError> {
    let body = read_pipeline_body(name, "cumulative_sum", value, CUMULATIVE_SUM_KEYS)?;
    Ok(PipelineSpec {
        buckets_path: read_buckets_path(name, "cumulative_sum", body)?,
        gap_policy: GapPolicy::Skip,
    })
}

/// Parses the `derivative` aggregation `name`.
///
/// # Errors
///
/// `parsing_exception` for an unknown key, an invalid path or gap policy, or a `unit` that is not
/// a fixed or calendar duration of at most [`MAX_UNIT_MILLIS`].
pub(super) fn parse_derivative(name: &str, value: &Value) -> Result<DerivativeSpec, OpenSearchError> {
    let body = read_pipeline_body(name, "derivative", value, DERIVATIVE_KEYS)?;
    let unit_millis = body
        .get("unit")
        .map(|unit| parse_unit_millis(name, expect_string(name, "unit", unit)?))
        .transpose()?;
    Ok(DerivativeSpec {
        buckets_path: read_buckets_path(name, "derivative", body)?,
        gap_policy: read_gap_policy(name, body)?,
        unit_millis,
    })
}

/// Parses the `serial_diff` aggregation `name` (`lag` defaults to 1).
///
/// # Errors
///
/// `parsing_exception` for an unknown key, an invalid path or gap policy, or a `lag` that is not a
/// whole JSON number from 1 to [`MAX_PIPELINE_SPAN`].
pub(super) fn parse_serial_diff(name: &str, value: &Value) -> Result<SerialDiffSpec, OpenSearchError> {
    let body = read_pipeline_body(name, "serial_diff", value, SERIAL_DIFF_KEYS)?;
    let lag = match body.get("lag") {
        None => 1,
        Some(lag) => parse_span(name, "lag", read_strict_number(name, "lag", lag)?)?,
    };
    Ok(SerialDiffSpec {
        buckets_path: read_buckets_path(name, "serial_diff", body)?,
        gap_policy: read_gap_policy(name, body)?,
        lag,
    })
}

/// Parses the `moving_fn` aggregation `name`; `window` (a JSON number or a numeric string) and
/// `script` are required.
///
/// # Errors
///
/// `parsing_exception` for an unknown key, an invalid path or gap policy, or a missing or invalid
/// `window` or `script`; `illegal_argument_exception` for a script other than the recognized
/// `MovingFunctions.*` forms.
pub(super) fn parse_moving_fn(name: &str, value: &Value) -> Result<MovingFnSpec, OpenSearchError> {
    let body = read_pipeline_body(name, "moving_fn", value, MOVING_FN_KEYS)?;
    let window = body
        .get("window")
        .ok_or_else(|| report_missing_key("moving_fn", name, "window"))?;
    let window = parse_span(name, "window", read_lenient_number(name, "window", window)?)?;
    let script = body
        .get("script")
        .ok_or_else(|| report_missing_key("moving_fn", name, "script"))?;
    Ok(MovingFnSpec {
        buckets_path: read_buckets_path(name, "moving_fn", body)?,
        gap_policy: read_gap_policy(name, body)?,
        window,
        function: recognize_moving_function(read_script_source(name, script)?)?,
    })
}

/// Parses the legacy `moving_avg` aggregation `name`: `model` (default `simple`), `window` (a JSON
/// number, default [`DEFAULT_MOVING_AVG_WINDOW`]), `settings.{alpha, beta}` (defaults
/// [`DEFAULT_ALPHA`] and [`DEFAULT_BETA`]) and `minimize: false`.
///
/// # Errors
///
/// `parsing_exception` for an unknown key (`predict` included), an invalid path, gap policy or
/// `window`, a model other than `simple`, `linear`, `ewma` and `holt`, a `settings` key other
/// than `alpha` and `beta` or a factor outside `[0, 1]`, and `minimize` other than `false`.
pub(super) fn parse_moving_avg(name: &str, value: &Value) -> Result<MovingAvgSpec, OpenSearchError> {
    let body = read_pipeline_body(name, "moving_avg", value, MOVING_AVG_KEYS)?;
    let window = match body.get("window") {
        None => DEFAULT_MOVING_AVG_WINDOW,
        Some(window) => parse_span(name, "window", read_strict_number(name, "window", window)?)?,
    };
    let settings = body
        .get("settings")
        .map(|settings| expect_object(name, "settings", settings))
        .transpose()?;
    let unknown_setting =
        settings.and_then(|settings| settings.keys().find(|key| !matches!(key.as_str(), "alpha" | "beta")));
    if let Some(key) = unknown_setting {
        return Err(report_unsupported_key("settings", name, key));
    }
    let read_factor = |key: &str, default_factor: f64| -> Result<f64, OpenSearchError> {
        let Some(factor) = settings.and_then(|settings| settings.get(key)) else {
            return Ok(default_factor);
        };
        let factor = read_strict_number(name, key, factor)?;
        if (0.0..=1.0).contains(&factor) {
            Ok(factor)
        } else {
            Err(OpenSearchError::parsing(format!(
                "[{key}] of [{name}] must be between 0 and 1"
            )))
        }
    };
    let model = body.get("model").map(|model| expect_string(name, "model", model)).transpose()?;
    let function = match model {
        None | Some("simple") => MovingFunction::UnweightedAvg,
        Some("linear") => MovingFunction::LinearWeightedAvg,
        Some("ewma") => MovingFunction::Ewma {
            alpha: read_factor("alpha", DEFAULT_ALPHA)?,
        },
        Some("holt") => MovingFunction::Holt {
            alpha: read_factor("alpha", DEFAULT_ALPHA)?,
            beta: read_factor("beta", DEFAULT_BETA)?,
        },
        Some(other_model) => {
            return Err(OpenSearchError::parsing(format!(
                "[model] of [{name}] is [{other_model}], which is not supported: use simple, linear, ewma or holt"
            )));
        }
    };
    match body.get("minimize") {
        None | Some(Value::Bool(false)) => {}
        Some(_) => {
            return Err(OpenSearchError::parsing(format!(
                "[minimize] of [{name}] must be false: IceGate does not fit model parameters"
            )));
        }
    }
    Ok(MovingFnSpec {
        buckets_path: read_buckets_path(name, "moving_avg", body)?,
        gap_policy: read_gap_policy(name, body)?,
        window,
        function,
    })
}

/// Parses the `bucket_script` aggregation `name`; `buckets_path` maps variable names to paths.
///
/// # Errors
///
/// `parsing_exception` for an unknown key, a `buckets_path` that is not a non-empty object of
/// string paths or that names a variable `_value` (under `construct`), an invalid path or gap
/// policy, or a missing or malformed `script`;
/// `illegal_argument_exception` for a script outside the arithmetic grammar or naming another
/// variable (`_value` included).
pub(super) fn parse_bucket_script(name: &str, value: &Value) -> Result<BucketScriptSpec, OpenSearchError> {
    let body = read_pipeline_body(name, "bucket_script", value, BUCKET_SCRIPT_KEYS)?;
    let paths = body
        .get("buckets_path")
        .map(|paths| expect_object(name, "buckets_path", paths))
        .transpose()?
        .filter(|paths| !paths.is_empty())
        .ok_or_else(|| {
            OpenSearchError::parsing(format!(
                "[buckets_path] of [{name}] must be a non-empty object of variable names to paths"
            ))
        })?;
    let variables = paths
        .iter()
        .map(|(variable, path)| -> Result<(String, BucketsPath), OpenSearchError> {
            // `_value` exists only in metric value scripts: a variable of that name would let a
            // bucket_script read it where `params._value` is refused.
            if variable == VALUE_VARIABLE {
                return Err(OpenSearchError::unsupported_construct(
                    variable,
                    format!(
                        "[buckets_path] of [{name}] names the variable [{variable}], which a bucket_script cannot \
                         read: name the variable otherwise and read it as params.<name>"
                    ),
                ));
            }
            Ok((
                variable.clone(),
                parse_buckets_path(expect_string(name, variable, path)?)?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let variable_names: Vec<&str> = variables.iter().map(|(variable, _)| variable.as_str()).collect();
    let script = body
        .get("script")
        .ok_or_else(|| report_missing_key("bucket_script", name, "script"))?;
    let script = recognize_arithmetic_script(read_script_source(name, script)?, &variable_names)?;
    Ok(BucketScriptSpec {
        gap_policy: read_gap_policy(name, body)?,
        variables,
        script,
    })
}

/// Parses the sibling pipeline `name` of kind `kind_name` (`avg_bucket`, `sum_bucket`,
/// `min_bucket` or `max_bucket`).
///
/// # Errors
///
/// `parsing_exception` for an unknown key or an invalid path or gap policy.
pub(super) fn parse_sibling_pipeline(
    name: &str,
    kind_name: &str,
    value: &Value,
) -> Result<PipelineSpec, OpenSearchError> {
    let body = read_pipeline_body(name, kind_name, value, SIBLING_KEYS)?;
    Ok(PipelineSpec {
        buckets_path: read_buckets_path(name, kind_name, body)?,
        gap_policy: read_gap_policy(name, body)?,
    })
}

/// The required single-string `buckets_path` of the pipeline `name` of kind `kind_name`.
fn read_buckets_path(name: &str, kind_name: &str, body: &Map<String, Value>) -> Result<BucketsPath, OpenSearchError> {
    let path = body
        .get("buckets_path")
        .ok_or_else(|| report_missing_key(kind_name, name, "buckets_path"))?;
    parse_buckets_path(expect_string(name, "buckets_path", path)?)
}

/// `gap_policy` of the pipeline `name`: `skip` when absent.
fn read_gap_policy(name: &str, body: &Map<String, Value>) -> Result<GapPolicy, OpenSearchError> {
    let policy = body
        .get("gap_policy")
        .map(|policy| expect_string(name, "gap_policy", policy))
        .transpose()?;
    match policy {
        None | Some("skip") => Ok(GapPolicy::Skip),
        Some("insert_zeros") => Ok(GapPolicy::InsertZeros),
        Some(other_policy) => Err(OpenSearchError::parsing(format!(
            "[gap_policy] of [{name}] is [{other_policy}]; use skip or insert_zeros"
        ))),
    }
}

/// The object body of the pipeline `name` of kind `kind_name`, whose keys all lie in `accepted`
/// or the ignored execution hints.
fn read_pipeline_body<'v>(
    name: &str,
    kind_name: &str,
    value: &'v Value,
    accepted: &[&str],
) -> Result<&'v Map<String, Value>, OpenSearchError> {
    let body = expect_object(name, kind_name, value)?;
    if let Some(key) = body
        .keys()
        .find(|key| !accepted.contains(&key.as_str()) && !IGNORED_EXECUTION_KEYS.contains(&key.as_str()))
    {
        return Err(report_unsupported_key(kind_name, name, key));
    }
    Ok(body)
}

/// The `window` or `lag` (`key`) of the pipeline `name`: a whole number from 1 to
/// [`MAX_PIPELINE_SPAN`].
fn parse_span(name: &str, key: &str, number: f64) -> Result<usize, OpenSearchError> {
    let is_whole = number.trunc().total_cmp(&number).is_eq();
    if !is_whole || !(1.0..=MAX_PIPELINE_SPAN).contains(&number) {
        return Err(OpenSearchError::parsing(format!(
            "[{key}] of [{name}] must be a whole number between 1 and {MAX_PIPELINE_SPAN}"
        )));
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "the number is whole, positive and at most MAX_PIPELINE_SPAN, so the conversion is exact"
    )]
    let span = number as usize;
    Ok(span)
}

/// `derivative.unit` of the pipeline `name` in milliseconds: a single fixed or calendar duration
/// of at most [`MAX_UNIT_MILLIS`] (`1s`, `60s`, `1m`, `1h`, `1d`, `1w`, `7d`).
fn parse_unit_millis(name: &str, unit: &str) -> Result<f64, OpenSearchError> {
    let build_unit_refusal = || {
        OpenSearchError::parsing(format!(
            "[unit] of [{name}] is [{unit}]; use a fixed or calendar duration of at most one week"
        ))
    };
    let millis = match classify_legacy_interval(unit).map_err(|_| build_unit_refusal())? {
        HistogramInterval::Calendar(calendar_unit) => {
            find_calendar_unit_millis(calendar_unit).ok_or_else(build_unit_refusal)?
        }
        HistogramInterval::Fixed(duration) => duration.as_secs_f64() * convert_micros_to_millis(MICROS_PER_SECOND),
    };
    if millis > 0.0 && millis <= MAX_UNIT_MILLIS {
        Ok(millis)
    } else {
        Err(build_unit_refusal())
    }
}

/// Milliseconds in one calendar `unit`; `None` for a month, a quarter and a year, whose length
/// varies.
const fn find_calendar_unit_millis(unit: CalendarUnit) -> Option<f64> {
    let micros = match unit {
        CalendarUnit::Second => MICROS_PER_SECOND,
        CalendarUnit::Minute => MICROS_PER_MINUTE,
        CalendarUnit::Hour => MICROS_PER_HOUR,
        CalendarUnit::Day => MICROS_PER_DAY,
        CalendarUnit::Week => MICROS_PER_WEEK,
        CalendarUnit::Month | CalendarUnit::Quarter | CalendarUnit::Year => return None,
    };
    Some(convert_micros_to_millis(micros))
}

/// `micros` microseconds in milliseconds.
#[expect(
    clippy::cast_precision_loss,
    reason = "only the lengths of calendar units up to a week are converted, far below 2^53 microseconds, so the \
              conversion is exact"
)]
const fn convert_micros_to_millis(micros: i64) -> f64 {
    micros as f64 / MICROS_PER_MILLISECOND as f64
}

/// A parent pipeline, borrowed from its `AggregationKind`.
enum ParentPipeline<'a> {
    /// `cumulative_sum`.
    CumulativeSum(&'a PipelineSpec),
    /// `derivative`.
    Derivative(&'a DerivativeSpec),
    /// `serial_diff`.
    SerialDiff(&'a SerialDiffSpec),
    /// `moving_fn`: renders `null` over an empty window.
    MovingFn(&'a MovingFnSpec),
    /// `moving_avg`: renders no key over an empty window.
    MovingAvg(&'a MovingAvgSpec),
    /// `bucket_script`: the one parent pipeline allowed under any multi-bucket parent.
    BucketScript(&'a BucketScriptSpec),
}

/// The statistic a sibling pipeline computes over its buckets' values.
#[derive(Clone, Copy)]
enum SiblingStatistic {
    /// `avg_bucket`: the mean, `null` over no values.
    Avg,
    /// `sum_bucket`: the sum, 0 over no values.
    Sum,
    /// `min_bucket`: the minimum and the keys holding it.
    Min,
    /// `max_bucket`: the maximum and the keys holding it.
    Max,
}

impl AggregationKind {
    /// Whether the kind is a pipeline: computed over the rendered tree after assembly, never
    /// planned.
    pub(crate) const fn is_pipeline(&self) -> bool {
        matches!(
            self,
            Self::CumulativeSum(_)
                | Self::Derivative(_)
                | Self::SerialDiff(_)
                | Self::MovingFn(_)
                | Self::MovingAvg(_)
                | Self::BucketScript(_)
                | Self::AvgBucket(_)
                | Self::SumBucket(_)
                | Self::MinBucket(_)
                | Self::MaxBucket(_)
        )
    }

    /// The parent pipeline this kind is, if it is one.
    const fn as_parent_pipeline(&self) -> Option<ParentPipeline<'_>> {
        match self {
            Self::CumulativeSum(spec) => Some(ParentPipeline::CumulativeSum(spec)),
            Self::Derivative(spec) => Some(ParentPipeline::Derivative(spec)),
            Self::SerialDiff(spec) => Some(ParentPipeline::SerialDiff(spec)),
            Self::MovingFn(spec) => Some(ParentPipeline::MovingFn(spec)),
            Self::MovingAvg(spec) => Some(ParentPipeline::MovingAvg(spec)),
            Self::BucketScript(spec) => Some(ParentPipeline::BucketScript(spec)),
            Self::AvgBucket(_)
            | Self::SumBucket(_)
            | Self::MinBucket(_)
            | Self::MaxBucket(_)
            | Self::DateHistogram(_)
            | Self::Terms(_)
            | Self::Histogram(_)
            | Self::Range(_)
            | Self::DateRange(_)
            | Self::Filters(_)
            | Self::Filter(_)
            | Self::Avg(_)
            | Self::Sum(_)
            | Self::Min(_)
            | Self::Max(_)
            | Self::ValueCount(_)
            | Self::Stats(_)
            | Self::Cardinality(_)
            | Self::Percentiles(_)
            | Self::PercentileRanks(_)
            | Self::ExtendedStats(_)
            | Self::TopHits(_) => None,
        }
    }

    /// The sibling pipeline this kind is, if it is one.
    const fn as_sibling_pipeline(&self) -> Option<(SiblingStatistic, &PipelineSpec)> {
        match self {
            Self::AvgBucket(spec) => Some((SiblingStatistic::Avg, spec)),
            Self::SumBucket(spec) => Some((SiblingStatistic::Sum, spec)),
            Self::MinBucket(spec) => Some((SiblingStatistic::Min, spec)),
            Self::MaxBucket(spec) => Some((SiblingStatistic::Max, spec)),
            Self::CumulativeSum(_)
            | Self::Derivative(_)
            | Self::SerialDiff(_)
            | Self::MovingFn(_)
            | Self::MovingAvg(_)
            | Self::BucketScript(_)
            | Self::DateHistogram(_)
            | Self::Terms(_)
            | Self::Histogram(_)
            | Self::Range(_)
            | Self::DateRange(_)
            | Self::Filters(_)
            | Self::Filter(_)
            | Self::Avg(_)
            | Self::Sum(_)
            | Self::Min(_)
            | Self::Max(_)
            | Self::ValueCount(_)
            | Self::Stats(_)
            | Self::Cardinality(_)
            | Self::Percentiles(_)
            | Self::PercentileRanks(_)
            | Self::ExtendedStats(_)
            | Self::TopHits(_) => None,
        }
    }

    /// Whether the kind renders `buckets`: the hosts a parent pipeline may sit under and the
    /// aggregations a sibling pipeline's path may start with.
    const fn is_multi_bucket(&self) -> bool {
        match self {
            Self::Terms(_)
            | Self::DateHistogram(_)
            | Self::Histogram(_)
            | Self::Range(_)
            | Self::DateRange(_)
            | Self::Filters(_) => true,
            Self::Filter(_)
            | Self::Avg(_)
            | Self::Sum(_)
            | Self::Min(_)
            | Self::Max(_)
            | Self::ValueCount(_)
            | Self::Stats(_)
            | Self::Cardinality(_)
            | Self::Percentiles(_)
            | Self::PercentileRanks(_)
            | Self::ExtendedStats(_)
            | Self::TopHits(_)
            | Self::CumulativeSum(_)
            | Self::Derivative(_)
            | Self::SerialDiff(_)
            | Self::MovingFn(_)
            | Self::MovingAvg(_)
            | Self::BucketScript(_)
            | Self::AvgBucket(_)
            | Self::SumBucket(_)
            | Self::MinBucket(_)
            | Self::MaxBucket(_) => false,
        }
    }

    /// Whether the kind renders one bucket (`doc_count` and its sub-aggregations) a path may
    /// step into with `>`.
    const fn is_single_bucket(&self) -> bool {
        match self {
            Self::Filter(_) => true,
            Self::Terms(_)
            | Self::DateHistogram(_)
            | Self::Histogram(_)
            | Self::Range(_)
            | Self::DateRange(_)
            | Self::Filters(_)
            | Self::Avg(_)
            | Self::Sum(_)
            | Self::Min(_)
            | Self::Max(_)
            | Self::ValueCount(_)
            | Self::Stats(_)
            | Self::Cardinality(_)
            | Self::Percentiles(_)
            | Self::PercentileRanks(_)
            | Self::ExtendedStats(_)
            | Self::TopHits(_)
            | Self::CumulativeSum(_)
            | Self::Derivative(_)
            | Self::SerialDiff(_)
            | Self::MovingFn(_)
            | Self::MovingAvg(_)
            | Self::BucketScript(_)
            | Self::AvgBucket(_)
            | Self::SumBucket(_)
            | Self::MinBucket(_)
            | Self::MaxBucket(_) => false,
        }
    }

    /// Whether the rendered aggregation carries a number at `key`: `value` (or no key) for a
    /// single-value metric or a pipeline, a member of `stats` and `extended_stats`, a requested
    /// percent or value of `percentiles` and `percentile_ranks`, and `normalized_value` of a
    /// `derivative` with a `unit`. A bucket aggregation, `top_hits`, the keys of an extreme and a
    /// date's `value_as_string` carry none.
    fn has_numeric_value(&self, key: Option<&str>) -> bool {
        match self {
            Self::Avg(_)
            | Self::Sum(_)
            | Self::Min(_)
            | Self::Max(_)
            | Self::ValueCount(_)
            | Self::Cardinality(_)
            | Self::CumulativeSum(_)
            | Self::SerialDiff(_)
            | Self::MovingFn(_)
            | Self::MovingAvg(_)
            | Self::BucketScript(_)
            | Self::AvgBucket(_)
            | Self::SumBucket(_)
            | Self::MinBucket(_)
            | Self::MaxBucket(_) => matches!(key, None | Some("value")),
            Self::Derivative(spec) => {
                matches!(key, None | Some("value")) || (key == Some("normalized_value") && spec.unit_millis.is_some())
            }
            Self::Stats(_) => key.is_some_and(|key| STATS_MEMBERS.contains(&key)),
            Self::ExtendedStats(_) => key.is_some_and(|key| EXTENDED_STATS_MEMBERS.contains(&key)),
            Self::Percentiles(spec) => key.is_some_and(|key| is_requested_key(key, &spec.percents)),
            Self::PercentileRanks(spec) => key.is_some_and(|key| is_requested_key(key, &spec.values)),
            Self::DateHistogram(_)
            | Self::Terms(_)
            | Self::Histogram(_)
            | Self::Range(_)
            | Self::DateRange(_)
            | Self::Filters(_)
            | Self::Filter(_)
            | Self::TopHits(_) => false,
        }
    }
}

/// Whether `key` names one of `requested` (percents or values) as their rendered keys do.
fn is_requested_key(key: &str, requested: &[f64]) -> bool {
    let Some(key_text) = parse_number_key(key) else {
        return false;
    };
    requested
        .iter()
        .any(|requested_number| format_double_key(*requested_number) == key_text)
}

/// `key` as a percent or value in the form its rendered key takes (`"99"` → `"99.0"`); `None`
/// for a key that is no finite number.
fn parse_number_key(key: &str) -> Option<String> {
    key.parse::<f64>()
        .ok()
        .filter(|number| number.is_finite())
        .map(format_double_key)
}

/// Whether `kind` is a parent pipeline (an `order_pipelines` selector).
const fn is_parent(kind: &AggregationKind) -> bool {
    kind.as_parent_pipeline().is_some()
}

/// Whether `kind` is a sibling pipeline (an `order_pipelines` selector).
const fn is_sibling(kind: &AggregationKind) -> bool {
    kind.as_sibling_pipeline().is_some()
}

/// Every path a pipeline of `kind` reads; none for any other kind.
fn list_buckets_paths(kind: &AggregationKind) -> Vec<&BucketsPath> {
    match (kind.as_parent_pipeline(), kind.as_sibling_pipeline()) {
        (Some(ParentPipeline::CumulativeSum(spec)), _) | (_, Some((_, spec))) => vec![&spec.buckets_path],
        (Some(ParentPipeline::Derivative(spec)), _) => vec![&spec.buckets_path],
        (Some(ParentPipeline::SerialDiff(spec)), _) => vec![&spec.buckets_path],
        (Some(ParentPipeline::MovingFn(spec) | ParentPipeline::MovingAvg(spec)), _) => vec![&spec.buckets_path],
        (Some(ParentPipeline::BucketScript(spec)), _) => spec.variables.iter().map(|(_, path)| path).collect(),
        (None, None) => Vec::new(),
    }
}

/// The first aggregation named by each path of `pipeline`.
fn list_referenced_names(pipeline: &NamedAggregation) -> Vec<&str> {
    list_buckets_paths(&pipeline.kind)
        .into_iter()
        .filter_map(BucketsPath::find_first_aggregation)
        .collect()
}

/// Checks the structure of every pipeline of the request before any plan runs.
///
/// # Errors
///
/// `parsing_exception` for a pipeline with sub-aggregations; `aggregation_execution_exception`
/// whose `aggregation` names the pipeline for a parent pipeline outside a multi-bucket
/// aggregation (or, except `bucket_script`, outside a `histogram` or `date_histogram`), a
/// `derivative.unit` outside a `date_histogram`, a sibling path that does not start with a
/// multi-bucket sibling and `>`, a path element before a `>` that names no single-bucket
/// aggregation of its level, a target that names no aggregation of its level or no number of it
/// (a multi-value metric without a key, a key the metric does not render, a bucket aggregation,
/// `top_hits`), and pipelines naming each other in a cycle.
pub(crate) fn validate_pipelines(aggregations: &[NamedAggregation]) -> Result<(), OpenSearchError> {
    validate_level(None, aggregations)
}

/// [`validate_pipelines`] for the children of `host` (`None` at the top level).
fn validate_level(host: Option<&NamedAggregation>, children: &[NamedAggregation]) -> Result<(), OpenSearchError> {
    for child in children {
        if child.kind.is_pipeline() && !child.children.is_empty() {
            return Err(OpenSearchError::parsing(format!(
                "[{}] is a pipeline aggregation and cannot have sub-aggregations",
                child.name
            )));
        }
        if let Some(pipeline) = child.kind.as_parent_pipeline() {
            validate_parent_placement(host, &child.name, &pipeline)?;
            for path in list_buckets_paths(&child.kind) {
                validate_buckets_path(children, &child.name, &path.aggregations, &path.target)?;
            }
        }
        if let Some((_, spec)) = child.kind.as_sibling_pipeline() {
            let (first_aggregation, later_elements) =
                find_sibling_bucket_aggregation(children, &child.name, &spec.buckets_path)?;
            validate_buckets_path(
                &first_aggregation.children,
                &child.name,
                later_elements,
                &spec.buckets_path.target,
            )?;
        }
        validate_level(Some(child), &child.children)?;
    }
    order_pipelines(children, is_parent)?;
    order_pipelines(children, is_sibling)?;
    Ok(())
}

/// A parent pipeline needs a multi-bucket host; all but `bucket_script` need a `histogram` or
/// `date_histogram`, and `derivative.unit` a `date_histogram`.
fn validate_parent_placement(
    host: Option<&NamedAggregation>,
    name: &str,
    pipeline: &ParentPipeline<'_>,
) -> Result<(), OpenSearchError> {
    let Some(host) = host.filter(|host| host.kind.is_multi_bucket()) else {
        let host_clause = host.map_or_else(String::new, |host| format!(", not of [{}]", host.name));
        return Err(OpenSearchError::invalid_pipeline(
            name,
            &format!("[{name}] must be a child of a multi-bucket aggregation{host_clause}"),
        ));
    };
    let is_date_histogram = matches!(host.kind, AggregationKind::DateHistogram(_));
    let is_histogram = is_date_histogram || matches!(host.kind, AggregationKind::Histogram(_));
    match pipeline {
        ParentPipeline::BucketScript(_) => Ok(()),
        ParentPipeline::Derivative(spec) if spec.unit_millis.is_some() && !is_date_histogram => {
            Err(OpenSearchError::invalid_pipeline(
                name,
                &format!("[{name}] [unit] needs a date_histogram parent, not [{}]", host.name),
            ))
        }
        ParentPipeline::CumulativeSum(_)
        | ParentPipeline::Derivative(_)
        | ParentPipeline::SerialDiff(_)
        | ParentPipeline::MovingFn(_)
        | ParentPipeline::MovingAvg(_)
            if is_histogram =>
        {
            Ok(())
        }
        ParentPipeline::CumulativeSum(_)
        | ParentPipeline::Derivative(_)
        | ParentPipeline::SerialDiff(_)
        | ParentPipeline::MovingFn(_)
        | ParentPipeline::MovingAvg(_) => Err(OpenSearchError::invalid_pipeline(
            name,
            &format!(
                "[{name}] must be a child of a histogram or date_histogram aggregation, not of [{}]",
                host.name
            ),
        )),
    }
}

/// The multi-bucket sibling a sibling pipeline's path starts with, and the path elements after it.
fn find_sibling_bucket_aggregation<'a>(
    siblings: &'a [NamedAggregation],
    name: &str,
    path: &'a BucketsPath,
) -> Result<(&'a NamedAggregation, &'a [String]), OpenSearchError> {
    let (first_name, later_elements) = path.aggregations.split_first().ok_or_else(|| {
        OpenSearchError::invalid_pipeline(
            name,
            &format!("[{name}] buckets_path must start with a multi-bucket aggregation and '>'"),
        )
    })?;
    let first_aggregation = siblings
        .iter()
        .find(|sibling| sibling.name == *first_name)
        .filter(|sibling| sibling.kind.is_multi_bucket())
        .ok_or_else(|| {
            OpenSearchError::invalid_pipeline(
                name,
                &format!(
                    "[{name}] buckets_path must start with a multi-bucket aggregation, and [{first_name}] is none"
                ),
            )
        })?;
    Ok((first_aggregation, later_elements))
}

/// Checks that the path `elements` > `target` of the pipeline `owner` resolves among `children`:
/// each element names a single-bucket aggregation, whose children the next element resolves
/// among, and the target names an aggregation of the last level that renders a number there.
fn validate_buckets_path(
    children: &[NamedAggregation],
    owner: &str,
    elements: &[String],
    target: &PathTarget,
) -> Result<(), OpenSearchError> {
    let mut level = children;
    for element in elements {
        let aggregation = level
            .iter()
            .find(|child| child.name == *element)
            .filter(|child| child.kind.is_single_bucket())
            .ok_or_else(|| {
                OpenSearchError::invalid_pipeline(
                    owner,
                    &format!(
                        "[{owner}] buckets_path element [{element}] must name a single-bucket aggregation of its level"
                    ),
                )
            })?;
        level = &aggregation.children;
    }
    let PathTarget::Metric { aggregation, key } = target else {
        return Ok(());
    };
    let target_aggregation = level.iter().find(|child| child.name == *aggregation).ok_or_else(|| {
        OpenSearchError::invalid_pipeline(
            owner,
            &format!("No aggregation found for path [{aggregation}] of [{owner}]"),
        )
    })?;
    if target_aggregation.kind.has_numeric_value(key.as_deref()) {
        return Ok(());
    }
    let written_target = key
        .as_deref()
        .map_or_else(|| aggregation.clone(), |key| format!("{aggregation}.{key}"));
    Err(OpenSearchError::invalid_pipeline(
        owner,
        &format!(
            "[{owner}] buckets_path [{written_target}] names no number: a multi-value metric needs one of the \
             keys it renders"
        ),
    ))
}

/// Runs every pipeline of the request over `rendered`, the assembled `aggregations` object, in
/// place. The tree passed [`validate_pipelines`].
///
/// # Errors
///
/// `aggregation_execution_exception` when the rendered tree does not hold what a validated path
/// names, such as a multi-value metric where a single value is read or a single-bucket element
/// that renders as none; `500` for an aggregation of the request that was not rendered.
pub(crate) fn apply_pipelines(
    aggregations: &[NamedAggregation],
    rendered: &mut Map<String, Value>,
) -> Result<(), OpenSearchError> {
    let schedule = build_level_schedule(aggregations)?;
    apply_level(&schedule, rendered)
}

/// The pipelines of one level of the request in the order they run, and the schedules of the
/// level's hosts. The order depends only on the request, so it is computed once per request and
/// shared by every rendered bucket of the level.
struct LevelSchedule<'a> {
    /// The aggregations of the level that have children and are no pipeline, each with the
    /// schedule of its children.
    hosts: Vec<HostSchedule<'a>>,
    /// The sibling pipelines of the level, each after every sibling pipeline it reads.
    sibling_pipelines: Vec<&'a NamedAggregation>,
    /// The parent pipelines of the level, which run across the buckets of the host the level
    /// belongs to, each after every parent pipeline it reads.
    parent_pipelines: Vec<&'a NamedAggregation>,
}

/// A host aggregation and the schedule of its children.
struct HostSchedule<'a> {
    /// The aggregation whose rendered buckets (or single bucket) hold its children.
    host: &'a NamedAggregation,
    /// The schedule of the host's children.
    children: LevelSchedule<'a>,
}

/// The schedule of `aggregations` and of every level below them.
///
/// # Errors
///
/// `aggregation_execution_exception` naming the first pipeline of a cycle, which
/// [`validate_pipelines`] has already refused.
fn build_level_schedule(aggregations: &[NamedAggregation]) -> Result<LevelSchedule<'_>, OpenSearchError> {
    let hosts = aggregations
        .iter()
        .filter(|aggregation| !aggregation.kind.is_pipeline() && !aggregation.children.is_empty())
        .map(|host| {
            Ok(HostSchedule {
                host,
                children: build_level_schedule(&host.children)?,
            })
        })
        .collect::<Result<Vec<_>, OpenSearchError>>()?;
    Ok(LevelSchedule {
        hosts,
        sibling_pipelines: order_pipelines(aggregations, is_sibling)?,
        parent_pipelines: order_pipelines(aggregations, is_parent)?,
    })
}

/// The pipelines among `children` that `select` keeps, ordered so that each runs after every
/// selected pipeline its paths name. A level holds a handful of pipelines, so the quadratic search
/// is bounded by the request's size.
///
/// # Errors
///
/// `aggregation_execution_exception` naming the first pipeline of a cycle.
fn order_pipelines(
    children: &[NamedAggregation],
    select: fn(&AggregationKind) -> bool,
) -> Result<Vec<&NamedAggregation>, OpenSearchError> {
    let mut unordered_pipelines: Vec<&NamedAggregation> = children.iter().filter(|child| select(&child.kind)).collect();
    let mut ordered_pipelines = Vec::with_capacity(unordered_pipelines.len());
    while !unordered_pipelines.is_empty() {
        let ready_index = unordered_pipelines.iter().position(|candidate| {
            list_referenced_names(candidate)
                .iter()
                .all(|name| !unordered_pipelines.iter().any(|other| other.name == *name))
        });
        let Some(index) = ready_index else {
            let names: Vec<&str> = unordered_pipelines.iter().map(|pipeline| pipeline.name.as_str()).collect();
            let first_name = names.first().copied().unwrap_or_default();
            return Err(OpenSearchError::invalid_pipeline(
                first_name,
                &format!(
                    "pipeline aggregations [{}] name each other in a cycle",
                    names.join(", ")
                ),
            ));
        };
        ordered_pipelines.push(unordered_pipelines.remove(index));
    }
    Ok(ordered_pipelines)
}

/// Runs the pipelines under every host of `schedule`, then the sibling pipelines of this level,
/// over the rendered `level`.
fn apply_level(schedule: &LevelSchedule<'_>, level: &mut Map<String, Value>) -> Result<(), OpenSearchError> {
    for host_schedule in &schedule.hosts {
        let host_name = &host_schedule.host.name;
        let node = level
            .get_mut(host_name)
            .ok_or_else(|| OpenSearchError::internal(format!("aggregation [{host_name}] was not rendered")))?;
        apply_host(host_schedule, node)?;
    }
    for pipeline in &schedule.sibling_pipelines {
        let result = run_sibling(pipeline, level)?;
        level.insert(pipeline.name.clone(), result);
    }
    Ok(())
}

/// One bucket of a rendered multi-bucket aggregation.
type BucketEntry<'a> = &'a mut Map<String, Value>;

/// Runs the pipelines inside the rendered `node` of `host_schedule`'s host: first every level
/// below each bucket, then the parent pipelines across the buckets.
fn apply_host(host_schedule: &HostSchedule<'_>, node: &mut Value) -> Result<(), OpenSearchError> {
    let host_name = &host_schedule.host.name;
    let children = &host_schedule.children;
    if node.get("buckets").is_none() {
        // A single-bucket host: validation refused any parent pipeline under it.
        let object = node
            .as_object_mut()
            .ok_or_else(|| OpenSearchError::internal(format!("aggregation [{host_name}] rendered as a non-object")))?;
        return apply_level(children, object);
    }
    let mut buckets = collect_bucket_entries(host_name, node)?;
    for entry in &mut buckets {
        apply_level(children, entry)?;
    }
    for pipeline in &children.parent_pipelines {
        run_parent(pipeline, &mut buckets)?;
    }
    Ok(())
}

/// The error of a rendered aggregation `name` that holds no buckets where a path reads some.
fn report_missing_buckets(name: &str) -> OpenSearchError {
    OpenSearchError::aggregation_execution(&format!("[{name}] is not a multi-bucket aggregation"))
}

/// The buckets of a rendered multi-bucket aggregation, in rendered order.
fn collect_bucket_entries<'a>(name: &str, node: &'a mut Value) -> Result<Vec<BucketEntry<'a>>, OpenSearchError> {
    match node.get_mut("buckets") {
        Some(Value::Array(buckets)) => buckets
            .iter_mut()
            .map(|bucket| bucket.as_object_mut().ok_or_else(|| report_missing_buckets(name)))
            .collect(),
        Some(Value::Object(buckets)) => buckets
            .values_mut()
            .map(|bucket| bucket.as_object_mut().ok_or_else(|| report_missing_buckets(name)))
            .collect(),
        None | Some(Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)) => {
            Err(report_missing_buckets(name))
        }
    }
}

/// [`collect_bucket_entries`] for reading, each bucket with its name when the buckets are keyed.
fn collect_bucket_views<'a>(
    name: &str,
    node: &'a Value,
) -> Result<Vec<(Option<&'a str>, &'a Map<String, Value>)>, OpenSearchError> {
    match node.get("buckets") {
        Some(Value::Array(buckets)) => buckets
            .iter()
            .map(|bucket| {
                bucket
                    .as_object()
                    .map(|object| (None, object))
                    .ok_or_else(|| report_missing_buckets(name))
            })
            .collect(),
        Some(Value::Object(buckets)) => buckets
            .iter()
            .map(|(key, bucket)| {
                bucket
                    .as_object()
                    .map(|object| (Some(key.as_str()), object))
                    .ok_or_else(|| report_missing_buckets(name))
            })
            .collect(),
        None | Some(Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)) => {
            Err(report_missing_buckets(name))
        }
    }
}

/// A pipeline's number: `null` when there is none or it is not finite (a `bucket_script` dividing
/// by zero), which JSON cannot carry.
fn render_number(value: Option<f64>) -> Value {
    value.and_then(Number::from_f64).map_or(Value::Null, Value::Number)
}

/// Runs one parent pipeline across `buckets`, writing into each bucket. Its placement under its
/// host passed [`validate_pipelines`].
fn run_parent(pipeline: &NamedAggregation, buckets: &mut [BucketEntry<'_>]) -> Result<(), OpenSearchError> {
    let name = &pipeline.name;
    let kind = pipeline
        .kind
        .as_parent_pipeline()
        .ok_or_else(|| OpenSearchError::internal(format!("[{name}] is not a parent pipeline")))?;
    match kind {
        ParentPipeline::CumulativeSum(spec) => {
            let values = read_series(buckets, &spec.buckets_path, spec.gap_policy)?;
            let mut total = 0.0;
            for (entry, value) in buckets.iter_mut().zip(values) {
                total += value.unwrap_or(0.0);
                entry.insert(name.clone(), json!({ "value": render_number(Some(total)) }));
            }
        }
        ParentPipeline::Derivative(spec) => run_derivative(name, spec, buckets)?,
        ParentPipeline::SerialDiff(spec) => {
            let values = read_series(buckets, &spec.buckets_path, spec.gap_policy)?;
            for (index, entry) in buckets.iter_mut().enumerate() {
                let Some(earlier_index) = index.checked_sub(spec.lag) else {
                    continue;
                };
                let current_value = values.get(index).copied().flatten();
                let previous_value = values.get(earlier_index).copied().flatten();
                if let (Some(current_value), Some(previous_value)) = (current_value, previous_value) {
                    entry.insert(
                        name.clone(),
                        json!({ "value": render_number(Some(current_value - previous_value)) }),
                    );
                }
            }
        }
        ParentPipeline::MovingFn(spec) => run_moving_function(name, spec, buckets, true)?,
        ParentPipeline::MovingAvg(spec) => run_moving_function(name, spec, buckets, false)?,
        ParentPipeline::BucketScript(spec) => {
            for entry in buckets.iter_mut() {
                let mut values = Vec::with_capacity(spec.variables.len());
                for (variable, path) in &spec.variables {
                    let value = read_bucket_value(entry, &path.aggregations, &path.target, spec.gap_policy)?;
                    values.push((variable.as_str(), value));
                }
                if values.iter().any(|(_, value)| value.is_none()) {
                    continue;
                }
                let result = spec.script.evaluate(&|variable| {
                    values
                        .iter()
                        .find(|(candidate, _)| *candidate == variable)
                        .and_then(|(_, value)| *value)
                });
                entry.insert(name.clone(), json!({ "value": render_number(result) }));
            }
        }
    }
    Ok(())
}

/// `derivative` across `buckets` (a `unit` only ever reaches here under a `date_histogram`, which
/// [`validate_pipelines`] checked).
fn run_derivative(name: &str, spec: &DerivativeSpec, buckets: &mut [BucketEntry<'_>]) -> Result<(), OpenSearchError> {
    let values = read_series(buckets, &spec.buckets_path, spec.gap_policy)?;
    // Keys are read only to normalize by `unit`.
    let keys: Vec<Option<f64>> = if spec.unit_millis.is_some() {
        buckets.iter().map(|entry| entry.get("key").and_then(Value::as_f64)).collect()
    } else {
        Vec::new()
    };
    for (index, entry) in buckets.iter_mut().enumerate().skip(1) {
        let current_value = values.get(index).copied().flatten();
        let previous_value = values.get(index - 1).copied().flatten();
        let difference = current_value
            .zip(previous_value)
            .map(|(current_value, previous_value)| current_value - previous_value);
        let mut result = Map::new();
        result.insert("value".to_owned(), render_number(difference));
        if let Some(unit_millis) = spec.unit_millis {
            let key_distance = keys
                .get(index)
                .copied()
                .flatten()
                .zip(keys.get(index - 1).copied().flatten())
                .map(|(current_key, previous_key)| current_key - previous_key);
            let normalized_value = difference
                .zip(key_distance)
                .map(|(difference, key_distance)| difference * unit_millis / key_distance);
            result.insert("normalized_value".to_owned(), render_number(normalized_value));
        }
        entry.insert(name.to_owned(), Value::Object(result));
    }
    Ok(())
}

/// `moving_fn` (`renders_empty_window`) and `moving_avg` across `buckets`.
fn run_moving_function(
    name: &str,
    spec: &MovingFnSpec,
    buckets: &mut [BucketEntry<'_>],
    renders_empty_window: bool,
) -> Result<(), OpenSearchError> {
    let values = read_series(buckets, &spec.buckets_path, spec.gap_policy)?;
    // A window is at most `MAX_PIPELINE_SPAN` values, and never more than the buckets.
    let mut window: VecDeque<f64> = VecDeque::with_capacity(spec.window.min(buckets.len()));
    for (entry, value) in buckets.iter_mut().zip(values) {
        let Some(current_value) = value else {
            continue;
        };
        let result = spec.function.apply(window.make_contiguous());
        if result.is_some() || renders_empty_window {
            entry.insert(name.to_owned(), json!({ "value": render_number(result) }));
        }
        if window.len() == spec.window {
            window.pop_front();
        }
        window.push_back(current_value);
    }
    Ok(())
}

/// Runs one sibling pipeline against the buckets its path names at `level`.
fn run_sibling(pipeline: &NamedAggregation, level: &Map<String, Value>) -> Result<Value, OpenSearchError> {
    let (statistic, spec) = pipeline
        .kind
        .as_sibling_pipeline()
        .ok_or_else(|| OpenSearchError::internal(format!("[{}] is not a sibling pipeline", pipeline.name)))?;
    let path = &spec.buckets_path;
    // `validate_pipelines` checked that the path starts with a multi-bucket sibling.
    let Some((first_aggregation, later_elements)) = path.aggregations.split_first() else {
        return Err(OpenSearchError::internal(format!(
            "[{}] has an unvalidated buckets_path",
            pipeline.name
        )));
    };
    let node = level
        .get(first_aggregation)
        .ok_or_else(|| OpenSearchError::internal(format!("aggregation [{first_aggregation}] was not rendered")))?;
    let mut valued_buckets: Vec<ValuedBucket<'_>> = Vec::new();
    for (keyed_name, bucket) in collect_bucket_views(first_aggregation, node)? {
        if let Some(value) = read_bucket_value(bucket, later_elements, &path.target, spec.gap_policy)? {
            valued_buckets.push((keyed_name, bucket, value));
        }
    }
    Ok(match statistic {
        SiblingStatistic::Avg => {
            let (total, count) = valued_buckets
                .iter()
                .fold((0.0, 0.0), |(total, count), (_, _, value)| (total + value, count + 1.0));
            json!({ "value": render_number((count > 0.0).then_some(total / count)) })
        }
        // A fold from +0.0, not `sum`, whose empty sum is -0.0.
        SiblingStatistic::Sum => {
            let total = valued_buckets.iter().fold(0.0, |total, (_, _, value)| total + value);
            json!({ "value": render_number(Some(total)) })
        }
        SiblingStatistic::Min => render_extreme(&valued_buckets, Ordering::Less),
        SiblingStatistic::Max => render_extreme(&valued_buckets, Ordering::Greater),
    })
}

/// A rendered bucket a sibling pipeline reads a value from: its name when the buckets are keyed,
/// the bucket, and the value.
type ValuedBucket<'a> = (Option<&'a str>, &'a Map<String, Value>, f64);

/// `{value, keys}` of the extreme value in `direction`, listing every bucket holding it; only
/// those buckets render their key text.
fn render_extreme(valued_buckets: &[ValuedBucket<'_>], direction: Ordering) -> Value {
    let extreme = valued_buckets
        .iter()
        .map(|(_, _, value)| *value)
        .reduce(|extreme_value, value| {
            if value.total_cmp(&extreme_value) == direction {
                value
            } else {
                extreme_value
            }
        });
    let keys: Vec<String> = extreme
        .map(|extreme| {
            valued_buckets
                .iter()
                .filter(|(_, _, value)| value.total_cmp(&extreme).is_eq())
                .map(|(keyed_name, bucket, _)| render_bucket_key_text(*keyed_name, bucket))
                .collect()
        })
        .unwrap_or_default();
    json!({ "value": render_number(extreme), "keys": keys })
}

/// The text a sibling pipeline lists for a bucket: `key_as_string`, else the key, else the keyed
/// bucket's name.
fn render_bucket_key_text(keyed_name: Option<&str>, bucket: &Map<String, Value>) -> String {
    match (bucket.get("key_as_string"), bucket.get("key"), keyed_name) {
        (Some(Value::String(text)), _, _) | (_, Some(Value::String(text)), _) => text.clone(),
        (_, Some(Value::Number(number)), _) => number.to_string(),
        (_, _, Some(name)) => name.to_owned(),
        _ => String::new(),
    }
}

/// One value per bucket at `path`, after the gap policy.
fn read_series(
    buckets: &[BucketEntry<'_>],
    path: &BucketsPath,
    gap_policy: GapPolicy,
) -> Result<Vec<Option<f64>>, OpenSearchError> {
    buckets
        .iter()
        .map(|entry| read_bucket_value(entry, &path.aggregations, &path.target, gap_policy))
        .collect()
}

/// A bucket's value at a path, after the gap policy: an empty bucket and a `null` value have no
/// value under `skip` and 0 under `insert_zeros`. The bare `_count` path is exempt: a doc count is
/// always a value, 0 for an empty bucket.
fn read_bucket_value(
    bucket: &Map<String, Value>,
    aggregations: &[String],
    target: &PathTarget,
    gap_policy: GapPolicy,
) -> Result<Option<f64>, OpenSearchError> {
    let is_doc_count = aggregations.is_empty() && matches!(target, PathTarget::Count);
    let is_gap = !is_doc_count && bucket.get("doc_count").and_then(Value::as_u64) == Some(0);
    let value = if is_gap {
        None
    } else {
        resolve_value(bucket, aggregations, target)?
    };
    Ok(match (value, gap_policy) {
        (Some(value), _) => Some(value),
        (None, GapPolicy::Skip) => None,
        (None, GapPolicy::InsertZeros) => Some(0.0),
    })
}

/// The value a path names inside one bucket, before the gap policy. An absent target (a pipeline
/// that wrote no key there) has no value.
fn resolve_value(
    bucket: &Map<String, Value>,
    aggregations: &[String],
    target: &PathTarget,
) -> Result<Option<f64>, OpenSearchError> {
    let mut node = bucket;
    for name in aggregations {
        node = node
            .get(name)
            .and_then(Value::as_object)
            .filter(|child| child.contains_key("doc_count") && !child.contains_key("buckets"))
            .ok_or_else(|| {
                OpenSearchError::aggregation_execution(&format!(
                    "buckets_path element [{name}] must name a single-bucket aggregation"
                ))
            })?;
    }
    match target {
        PathTarget::Count => Ok(node.get("doc_count").and_then(Value::as_f64)),
        PathTarget::Metric { aggregation, key } => node.get(aggregation).map_or(Ok(None), |metric_node| {
            read_metric_value(aggregation, metric_node, key.as_deref())
        }),
    }
}

/// The value `key` names in a rendered metric: `value` without a key, a member such as `avg`, or a
/// percent of a `values` object or array.
fn read_metric_value(aggregation: &str, node: &Value, key: Option<&str>) -> Result<Option<f64>, OpenSearchError> {
    let Some(key) = key else {
        return node.get("value").map(Value::as_f64).ok_or_else(|| {
            OpenSearchError::aggregation_execution(&format!(
                "[{aggregation}] is a multi-value aggregation: name one of its values, such as [{aggregation}.avg]"
            ))
        });
    };
    if let Some(member) = node.get(key) {
        return Ok(member.as_f64());
    }
    let percent_key = parse_number_key(key);
    let found_value = match (node.get("values"), percent_key) {
        (Some(Value::Object(values)), Some(percent_key)) => values.get(&percent_key).map(Value::as_f64),
        (Some(Value::Array(items)), Some(percent_key)) => items
            .iter()
            .find(|item| {
                item.get("key").and_then(Value::as_f64).map(format_double_key).as_deref() == Some(percent_key.as_str())
            })
            .map(|item| item.get("value").and_then(Value::as_f64)),
        _ => None,
    };
    found_value.ok_or_else(|| OpenSearchError::aggregation_execution(&format!("[{aggregation}] has no value [{key}]")))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::super::parse::parse_aggregations;
    use super::*;
    use crate::opensearch::error::render_envelope;

    /// The status and `error` object an error renders.
    async fn render_error(error: OpenSearchError) -> (StatusCode, Value) {
        let (status, body) = render_envelope(error).await;
        (status, body["error"].clone())
    }

    /// The `error.type` an error renders.
    async fn render_error_type(error: OpenSearchError) -> Value {
        render_error(error).await.1["type"].clone()
    }

    /// Parses `request`, validates its pipelines and runs them over `rendered`, as
    /// `execute_aggregations` does around the executed plans.
    fn run_pipelines(request: &Value, mut rendered: Value) -> Result<Value, OpenSearchError> {
        let tree = parse_aggregations(request)?;
        validate_pipelines(&tree.aggregations)?;
        let level = rendered.as_object_mut().expect("rendered aggregations are an object");
        apply_pipelines(&tree.aggregations, level)?;
        Ok(rendered)
    }

    /// A rendered date histogram `"2"` over five minutes whose metric `"1"` is
    /// [_, 10, 25, _, 5] and whose doc counts are [0, 2, 3, 0, 1].
    fn build_gappy_histogram() -> Value {
        json!({"2": {"buckets": [
            {"key": 0, "doc_count": 0, "1": {"value": null}},
            {"key": 60_000, "doc_count": 2, "1": {"value": 10.0}},
            {"key": 120_000, "doc_count": 3, "1": {"value": 25.0}},
            {"key": 180_000, "doc_count": 0, "1": {"value": null}},
            {"key": 240_000, "doc_count": 1, "1": {"value": 5.0}}
        ]}})
    }

    /// Adds each pipeline of the object `pipelines` to the `children` object of a request.
    fn merge_children(children: &mut Value, pipelines: &Value) {
        for (name, body) in pipelines.as_object().expect("pipelines object") {
            children[name] = body.clone();
        }
    }

    /// A request: date histogram `"2"` with metric `"1"` and the `pipelines`.
    fn build_histogram_request(pipelines: &Value) -> Value {
        let mut children = json!({"1": {"avg": {"field": "duration_micros"}}});
        merge_children(&mut children, pipelines);
        json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1m"}, "aggs": children}})
    }

    /// A request: hourly date histogram `"hours"` with the single-bucket `"f"`, the `terms` `"t"`,
    /// the `stats` `"s"`, the `percentiles` `"pct"` (only 50 requested) and the `pipelines`.
    fn build_hours_request(pipelines: &Value) -> Value {
        let mut children = json!({
            "f": {"filter": {"match_all": {}}},
            "t": {"terms": {"field": "service_name"}},
            "s": {"stats": {"field": "duration_micros"}},
            "pct": {"percentiles": {"field": "duration_micros", "percents": [50]}}
        });
        merge_children(&mut children, pipelines);
        json!({"hours": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h"}, "aggs": children}})
    }

    /// The value each bucket of `"2"` carries under `name`, or `None` when the
    /// bucket has no such key.
    fn read_column(rendered: &Value, name: &str) -> Vec<Option<Value>> {
        rendered["2"]["buckets"]
            .as_array()
            .expect("buckets")
            .iter()
            .map(|bucket| bucket.get(name).cloned())
            .collect()
    }

    /// Checks that parsing `request` is a 400 `parsing_exception`.
    async fn assert_parse_refused(request: &Value) {
        let error = parse_aggregations(request).expect_err("refused");
        let (status, error) = render_error(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{request}");
        assert_eq!(error["type"], json!("parsing_exception"), "{request}");
    }

    #[tokio::test]
    async fn buckets_path_forms_resolve_count_ids_keys_and_nested_paths() {
        let build_metric_target = |aggregation: &str, key: Option<&str>| PathTarget::Metric {
            aggregation: aggregation.to_owned(),
            key: key.map(str::to_owned),
        };
        let cases = [
            ("_count", vec![], PathTarget::Count),
            ("1", vec![], build_metric_target("1", None)),
            ("1-metric", vec![], build_metric_target("1-metric", None)),
            ("1.avg", vec![], build_metric_target("1", Some("avg"))),
            ("1[99.0]", vec![], build_metric_target("1", Some("99.0"))),
            ("1['99.9']", vec![], build_metric_target("1", Some("99.9"))),
            ("a>b", vec!["a"], build_metric_target("b", None)),
            ("a>_count", vec!["a"], PathTarget::Count),
            ("a>b>c.max", vec!["a", "b"], build_metric_target("c", Some("max"))),
        ];
        for (text, aggregations, target) in cases {
            let path = parse_buckets_path(text).expect("a valid path");
            assert_eq!(
                path.aggregations,
                aggregations.iter().map(|name| (*name).to_owned()).collect::<Vec<_>>(),
                "{text}"
            );
            assert_eq!(path.target, target, "{text}");
        }
        for invalid_path in ["", "a>", ">b", "a>>b", "1[99", "1[]"] {
            let error = parse_buckets_path(invalid_path).expect_err("refused");
            assert_eq!(
                render_error_type(error).await,
                json!("parsing_exception"),
                "{invalid_path}"
            );
        }
        let bucket = json!({
            "doc_count": 4,
            "1": {"value": 2.5},
            "2": {"avg": 3.0, "count": 4},
            "3": {"values": {"99.0": 7.0}},
            "4": {"values": [{"key": 50.0, "value": 6.0}]},
            "f": {"doc_count": 1, "5": {"value": 9.0}}
        });
        let bucket = bucket.as_object().expect("object");
        let resolve_path = |text: &str| {
            let path = parse_buckets_path(text).expect("valid");
            resolve_value(bucket, &path.aggregations, &path.target)
        };
        for (text, expected) in [
            ("_count", Some(4.0)),
            ("1", Some(2.5)),
            ("2.avg", Some(3.0)),
            ("3.99", Some(7.0)),
            ("3[99.0]", Some(7.0)),
            ("4.50", Some(6.0)),
            ("f>5", Some(9.0)),
            ("f>_count", Some(1.0)),
            ("absent", None),
        ] {
            assert_eq!(resolve_path(text).expect("resolves"), expected, "{text}");
        }
        for refused_path in ["2", "3.42", "1>x"] {
            let error = resolve_path(refused_path).expect_err("refused");
            assert_eq!(
                render_error_type(error).await,
                json!("aggregation_execution_exception"),
                "{refused_path}"
            );
        }
    }

    #[test]
    fn pipelines_over_empty_and_first_buckets_render_as_the_contract_states() {
        let request = build_histogram_request(&json!({
            "cs": {"cumulative_sum": {"buckets_path": "1", "format": "0.00"}},
            "d": {"derivative": {"buckets_path": "1"}},
            "sd": {"serial_diff": {"buckets_path": "1"}},
            "mf": {"moving_fn": {"buckets_path": "1", "window": 2, "script": "MovingFunctions.unweightedAvg(values)"}},
            "ma": {"moving_avg": {"buckets_path": "1", "window": 2}},
            "bs": {"bucket_script": {"buckets_path": {"a": "1", "c": "_count"}, "script": "params.a / params.c", "format": "0.00"}}
        }));
        // `format` is accepted and ignored: no bucket gains a `value_as_string`.
        let rendered = run_pipelines(&request, build_gappy_histogram()).expect("pipelines run");
        let build_value_object = |number: Value| Some(json!({"value": number}));
        assert_eq!(
            read_column(&rendered, "cs"),
            vec![
                build_value_object(json!(0.0)),
                build_value_object(json!(10.0)),
                build_value_object(json!(35.0)),
                build_value_object(json!(35.0)),
                build_value_object(json!(40.0))
            ]
        );
        assert_eq!(
            read_column(&rendered, "d"),
            vec![
                None,
                build_value_object(json!(null)),
                build_value_object(json!(15.0)),
                build_value_object(json!(null)),
                build_value_object(json!(null))
            ]
        );
        assert_eq!(
            read_column(&rendered, "sd"),
            vec![None, None, build_value_object(json!(15.0)), None, None]
        );
        assert_eq!(
            read_column(&rendered, "mf"),
            vec![
                None,
                build_value_object(json!(null)),
                build_value_object(json!(10.0)),
                None,
                build_value_object(json!(17.5))
            ]
        );
        assert_eq!(
            read_column(&rendered, "ma"),
            vec![
                None,
                None,
                build_value_object(json!(10.0)),
                None,
                build_value_object(json!(17.5))
            ]
        );
        assert_eq!(
            read_column(&rendered, "bs"),
            vec![
                None,
                build_value_object(json!(5.0)),
                build_value_object(json!(8.333_333_333_333_334)),
                None,
                build_value_object(json!(5.0))
            ]
        );
    }

    #[tokio::test]
    async fn cumulative_sum_derivative_and_serial_diff_match_hand_computed_series() {
        let rendered = json!({"2": {"buckets": [
            {"key": 0, "doc_count": 1, "1": {"value": 3.0}},
            {"key": 60_000, "doc_count": 1, "1": {"value": 7.0}},
            {"key": 120_000, "doc_count": 1, "1": {"value": 4.0}},
            {"key": 180_000, "doc_count": 1, "1": {"value": 10.0}}
        ]}});
        let request = build_histogram_request(&json!({
            "cs": {"cumulative_sum": {"buckets_path": "1"}},
            "d": {"derivative": {"buckets_path": "1"}},
            "sd": {"serial_diff": {"buckets_path": "1", "lag": 2}}
        }));
        let rendered = run_pipelines(&request, rendered).expect("pipelines run");
        let build_value_object = |number: f64| Some(json!({"value": number}));
        assert_eq!(
            read_column(&rendered, "cs"),
            vec![
                build_value_object(3.0),
                build_value_object(10.0),
                build_value_object(14.0),
                build_value_object(24.0)
            ]
        );
        assert_eq!(
            read_column(&rendered, "d"),
            vec![
                None,
                build_value_object(4.0),
                build_value_object(-3.0),
                build_value_object(6.0)
            ]
        );
        assert_eq!(
            read_column(&rendered, "sd"),
            vec![None, None, build_value_object(1.0), build_value_object(3.0)]
        );
        // insert_zeros counts empty buckets as 0 instead of leaving a gap.
        let zeros =
            build_histogram_request(&json!({"d": {"derivative": {"buckets_path": "1", "gap_policy": "insert_zeros"}}}));
        let rendered = run_pipelines(&zeros, build_gappy_histogram()).expect("pipelines run");
        assert_eq!(
            read_column(&rendered, "d"),
            vec![
                None,
                build_value_object(10.0),
                build_value_object(15.0),
                build_value_object(-25.0),
                build_value_object(5.0)
            ]
        );
        // `_count` is never a gap, under either policy: doc counts 0, 2, 3, 0, 1 differ
        // by 2, 1, −3, 1.
        let counts = build_histogram_request(&json!({"d": {"derivative": {"buckets_path": "_count"}}}));
        let rendered = run_pipelines(&counts, build_gappy_histogram()).expect("pipelines run");
        assert_eq!(
            read_column(&rendered, "d"),
            vec![
                None,
                build_value_object(2.0),
                build_value_object(1.0),
                build_value_object(-3.0),
                build_value_object(1.0)
            ]
        );
        // `lag` is a whole number from 1 to 65535: 65535 parses, 0 and 65536 are refused.
        let longest_lag =
            build_histogram_request(&json!({"sd": {"serial_diff": {"buckets_path": "1", "lag": 65_535}}}));
        let rendered = run_pipelines(&longest_lag, build_gappy_histogram()).expect("pipelines run");
        assert_eq!(read_column(&rendered, "sd"), vec![None, None, None, None, None]);
        for lag in [0, 65_536] {
            let refused_request =
                build_histogram_request(&json!({"sd": {"serial_diff": {"buckets_path": "1", "lag": lag}}}));
            assert_parse_refused(&refused_request).await;
        }
        // `cumulative_sum` takes no gap policy.
        let refused_request =
            build_histogram_request(&json!({"c": {"cumulative_sum": {"buckets_path": "1", "gap_policy": "skip"}}}));
        assert_parse_refused(&refused_request).await;
    }

    #[tokio::test]
    async fn derivative_with_unit_adds_a_normalized_value() {
        let rendered = json!({"2": {"buckets": [
            {"key": 0, "doc_count": 1, "1": {"value": 60.0}},
            {"key": 60_000, "doc_count": 1, "1": {"value": 180.0}},
            {"key": 120_000, "doc_count": 1, "1": {"value": 120.0}},
            {"key": 180_000, "doc_count": 0, "1": {"value": null}}
        ]}});
        let request = build_histogram_request(&json!({
            "per_second": {"derivative": {"buckets_path": "1", "unit": "1s"}},
            "per_minute": {"derivative": {"buckets_path": "1", "unit": "1m"}},
            "per_week": {"derivative": {"buckets_path": "1", "unit": "1w"}},
            "per_seven_days": {"derivative": {"buckets_path": "1", "unit": "7d"}}
        }));
        let rendered = run_pipelines(&request, rendered).expect("pipelines run");
        // Keys 60 000 ms apart: per second divides by 60, per minute by 1, per week
        // (604 800 000 ms) multiplies by 10 080.
        assert_eq!(
            read_column(&rendered, "per_second"),
            vec![
                None,
                Some(json!({"value": 120.0, "normalized_value": 2.0})),
                Some(json!({"value": -60.0, "normalized_value": -1.0})),
                Some(json!({"value": null, "normalized_value": null}))
            ]
        );
        assert_eq!(
            read_column(&rendered, "per_minute"),
            vec![
                None,
                Some(json!({"value": 120.0, "normalized_value": 120.0})),
                Some(json!({"value": -60.0, "normalized_value": -60.0})),
                Some(json!({"value": null, "normalized_value": null}))
            ]
        );
        for name in ["per_week", "per_seven_days"] {
            assert_eq!(
                read_column(&rendered, name)[1],
                Some(json!({"value": 120.0, "normalized_value": 1_209_600.0})),
                "{name}"
            );
        }
        for unit in ["8d", "1M", "1y", "fortnight"] {
            let refused_request =
                build_histogram_request(&json!({"d": {"derivative": {"buckets_path": "1", "unit": unit}}}));
            let error = run_pipelines(&refused_request, build_gappy_histogram()).expect_err("refused");
            let (status, error) = render_error(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{unit}");
            assert_eq!(error["type"], json!("parsing_exception"), "{unit}");
        }
    }

    #[tokio::test]
    async fn moving_fn_and_moving_avg_match_hand_computed_windows() {
        let rendered = json!({"2": {"buckets": [
            {"key": 0, "doc_count": 1, "1": {"value": 100.0}},
            {"key": 60_000, "doc_count": 1, "1": {"value": 300.0}},
            {"key": 120_000, "doc_count": 1, "1": {"value": 500.0}},
            {"key": 180_000, "doc_count": 1, "1": {"value": 700.0}}
        ]}});
        let request = build_histogram_request(&json!({
            "unweighted": {"moving_fn": {"buckets_path": "1", "window": "3", "script": "MovingFunctions.unweightedAvg(values)"}},
            "linear": {"moving_fn": {"buckets_path": "1", "window": 3, "script": {"source": "MovingFunctions.linearWeightedAvg(values)"}}},
            "ewma": {"moving_fn": {"buckets_path": "1", "window": 3, "script": "MovingFunctions.ewma(values, 0.5)"}},
            "holt": {"moving_fn": {"buckets_path": "1", "window": 3, "script": "MovingFunctions.holt(values, 0.5, 0.5)"}},
            "widest": {"moving_fn": {"buckets_path": "1", "window": 65_535, "script": "MovingFunctions.unweightedAvg(values)"}},
            "simple_avg": {"moving_avg": {"buckets_path": "1", "window": 3, "model": "simple"}},
            "linear_avg": {"moving_avg": {"buckets_path": "1", "window": 3, "model": "linear"}},
            "ewma_avg": {"moving_avg": {"buckets_path": "1", "window": 3, "model": "ewma", "settings": {"alpha": 0.5}}},
            "holt_avg": {"moving_avg": {"buckets_path": "1", "window": 3, "model": "holt", "settings": {"alpha": 0.5, "beta": 0.5}, "minimize": false}}
        }));
        let rendered = run_pipelines(&request, rendered).expect("pipelines run");
        let build_value_object = |number: f64| Some(json!({"value": number}));
        let empty_window = Some(json!({"value": null}));
        // Each bucket reads the up-to-three values before it (oldest first). Linear
        // weights start their total at 1: [100] → 100 / 2, [100, 300] → 700 / 4,
        // [100, 300, 500] → 2200 / 7.
        assert_eq!(
            read_column(&rendered, "unweighted"),
            vec![
                empty_window.clone(),
                build_value_object(100.0),
                build_value_object(200.0),
                build_value_object(300.0)
            ]
        );
        assert_eq!(
            read_column(&rendered, "linear"),
            vec![
                empty_window.clone(),
                build_value_object(50.0),
                build_value_object(175.0),
                build_value_object(314.285_714_285_714_3)
            ]
        );
        assert_eq!(
            read_column(&rendered, "ewma"),
            vec![
                empty_window.clone(),
                build_value_object(100.0),
                build_value_object(200.0),
                build_value_object(350.0)
            ]
        );
        assert_eq!(
            read_column(&rendered, "holt"),
            vec![
                empty_window.clone(),
                build_value_object(100.0),
                build_value_object(200.0),
                build_value_object(375.0)
            ]
        );
        // The widest window reads every preceding value: 100, 200, 300.
        assert_eq!(
            read_column(&rendered, "widest"),
            vec![
                empty_window,
                build_value_object(100.0),
                build_value_object(200.0),
                build_value_object(300.0)
            ]
        );
        assert_eq!(
            read_column(&rendered, "simple_avg"),
            vec![
                None,
                build_value_object(100.0),
                build_value_object(200.0),
                build_value_object(300.0)
            ]
        );
        assert_eq!(
            read_column(&rendered, "linear_avg"),
            vec![
                None,
                build_value_object(50.0),
                build_value_object(175.0),
                build_value_object(314.285_714_285_714_3)
            ]
        );
        assert_eq!(
            read_column(&rendered, "ewma_avg"),
            vec![
                None,
                build_value_object(100.0),
                build_value_object(200.0),
                build_value_object(350.0)
            ]
        );
        assert_eq!(
            read_column(&rendered, "holt_avg"),
            vec![
                None,
                build_value_object(100.0),
                build_value_object(200.0),
                build_value_object(375.0)
            ]
        );
        for refused_body in [
            json!({"moving_avg": {"buckets_path": "1", "model": "holt_winters"}}),
            json!({"moving_avg": {"buckets_path": "1", "minimize": true}}),
            json!({"moving_avg": {"buckets_path": "1", "predict": 3}}),
            json!({"moving_avg": {"buckets_path": "1", "window": "5"}}),
            json!({"moving_fn": {"buckets_path": "1", "script": "MovingFunctions.unweightedAvg(values)"}}),
            json!({"moving_fn": {"buckets_path": "1", "window": 0, "script": "MovingFunctions.unweightedAvg(values)"}}),
            json!({"moving_fn": {"buckets_path": "1", "window": 2.5, "script": "MovingFunctions.unweightedAvg(values)"}}),
            json!({"moving_fn": {"buckets_path": "1", "window": 65_536, "script": "MovingFunctions.unweightedAvg(values)"}}),
        ] {
            let refused_request = json!({"2": {
                "date_histogram": {"field": "timestamp", "fixed_interval": "1m"},
                "aggs": {"1": {"avg": {"field": "duration_micros"}}, "p": refused_body}
            }});
            assert_parse_refused(&refused_request).await;
        }
    }

    #[tokio::test]
    async fn bucket_script_evaluates_its_arithmetic_over_named_paths() {
        let rendered = json!({"2": {"buckets": [
            {"key": "checkout", "doc_count": 4, "errors": {"doc_count": 1}, "1": {"value": 8.0}},
            {"key": "cart", "doc_count": 2, "errors": {"doc_count": 0}, "1": {"value": 0.0}}
        ]}});
        let request = json!({"2": {
            "terms": {"field": "service_name"},
            "aggs": {
                "errors": {"filter": {"term": {"status_code": 2}}},
                "1": {"sum": {"field": "duration_micros"}},
                "rate": {"bucket_script": {
                    "buckets_path": {"errors": "errors>_count", "total": "_count"},
                    "script": "params.errors / params.total"
                }},
                "per_error": {"bucket_script": {
                    "buckets_path": {"sum": "1", "errors": "errors>_count"},
                    "script": {"source": "params.sum / params.errors", "lang": "painless"}
                }}
            }
        }});
        let rendered = run_pipelines(&request, rendered).expect("pipelines run");
        assert_eq!(
            read_column(&rendered, "rate"),
            vec![Some(json!({"value": 0.25})), Some(json!({"value": 0.0}))]
        );
        // 0.0 / 0: an infinite or NaN result renders null.
        assert_eq!(
            read_column(&rendered, "per_error"),
            vec![Some(json!({"value": 8.0})), Some(json!({"value": null}))]
        );
        // `_value` exists only in metric value scripts, so no `buckets_path` variable takes its
        // name, which `params._value` could not read either.
        let value_variable = build_histogram_request(&json!({
            "b": {"bucket_script": {"buckets_path": {"_value": "1"}, "script": "_value * 2"}}
        }));
        let error = parse_aggregations(&value_variable).expect_err("refused");
        let (status, error) = render_error(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(error["type"], json!("parsing_exception"));
        assert_eq!(error["construct"], json!("_value"));
    }

    #[test]
    fn sibling_bucket_pipelines_read_hoisted_helper_aggregations() {
        let rendered = json!({"1-bucket": {"buckets": [
            {"key": 0, "key_as_string": "a", "doc_count": 2, "1-metric": {"value": 4.0}},
            {"key": 60_000, "key_as_string": "b", "doc_count": 0, "1-metric": {"value": null}},
            {"key": 120_000, "key_as_string": "c", "doc_count": 1, "1-metric": {"value": 8.0}},
            {"key": 180_000, "key_as_string": "d", "doc_count": 3, "1-metric": {"value": 8.0}}
        ]}});
        let request = json!({
            "1-bucket": {"date_histogram": {"field": "timestamp", "fixed_interval": "1m"}, "aggs": {"1-metric": {"max": {"field": "duration_micros"}}}},
            "avg": {"avg_bucket": {"buckets_path": "1-bucket>1-metric"}},
            "sum": {"sum_bucket": {"buckets_path": "1-bucket>1-metric"}},
            "min": {"min_bucket": {"buckets_path": "1-bucket>1-metric"}},
            "max": {"max_bucket": {"buckets_path": "1-bucket>1-metric"}},
            "avg_count": {"avg_bucket": {"buckets_path": "1-bucket>_count"}},
            "sum_count": {"sum_bucket": {"buckets_path": "1-bucket>_count", "gap_policy": "insert_zeros"}}
        });
        let rendered = run_pipelines(&request, rendered).expect("pipelines run");
        assert_eq!(rendered["avg"], json!({"value": 6.666_666_666_666_667}));
        assert_eq!(rendered["sum"], json!({"value": 20.0}));
        assert_eq!(rendered["min"], json!({"value": 4.0, "keys": ["a"]}));
        assert_eq!(rendered["max"], json!({"value": 8.0, "keys": ["c", "d"]}));
        // `_count` is never a gap: the empty bucket's 0 counts, (2 + 0 + 1 + 3) / 4.
        assert_eq!(rendered["avg_count"], json!({"value": 1.5}));
        assert_eq!(rendered["sum_count"], json!({"value": 6.0}));
        let empty_histogram = json!({"1-bucket": {"buckets": []}});
        let rendered = run_pipelines(&request, empty_histogram).expect("pipelines run");
        assert_eq!(rendered["avg"], json!({"value": null}));
        // A sum over no values is 0.0, not -0.0: the sign is part of the rendered number.
        assert_eq!(rendered["sum"], json!({"value": 0.0}));
        assert_eq!(
            serde_json::to_string(&rendered["sum"]["value"]).expect("a number"),
            "0.0"
        );
        assert_eq!(rendered["min"], json!({"value": null, "keys": []}));
    }

    #[tokio::test]
    async fn a_misplaced_pipeline_is_refused_before_anything_executes() {
        let build_minute_histogram_request = |pipelines: Value| json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1m"}, "aggs": pipelines}});
        // Each refused request with the pipeline its refusal names.
        let refused_cases = [
            // A parent pipeline at the top level.
            (json!({"d": {"derivative": {"buckets_path": "_count"}}}), "d"),
            // A histogram-only pipeline under terms.
            (
                json!({"2": {"terms": {"field": "service_name"}, "aggs": {"d": {"derivative": {"buckets_path": "_count"}}}}}),
                "d",
            ),
            // A bucket_script under a single-bucket aggregation.
            (
                json!({"2": {"filter": {"match_all": {}}, "aggs": {
                    "1": {"avg": {"field": "duration_micros"}},
                    "s": {"bucket_script": {"buckets_path": {"a": "1"}, "script": "params.a"}}
                }}}),
                "s",
            ),
            // A derivative unit under a numeric histogram.
            (
                json!({"2": {"histogram": {"field": "duration_micros", "interval": 100}, "aggs": {
                    "d": {"derivative": {"buckets_path": "_count", "unit": "1s"}}
                }}}),
                "d",
            ),
            // A sibling pipeline whose path does not start with an aggregation and '>'.
            (
                json!({"1": {"avg": {"field": "duration_micros"}}, "m": {"max_bucket": {"buckets_path": "1"}}}),
                "m",
            ),
            // A sibling pipeline whose path starts with a metric.
            (
                json!({"1": {"avg": {"field": "duration_micros"}}, "m": {"max_bucket": {"buckets_path": "1>x"}}}),
                "m",
            ),
            // A path naming no aggregation.
            (
                build_minute_histogram_request(json!({"d": {"derivative": {"buckets_path": "absent"}}})),
                "d",
            ),
            // Two pipelines naming each other: the refusal names the first of the cycle.
            (
                build_minute_histogram_request(json!({
                    "a": {"derivative": {"buckets_path": "b"}},
                    "b": {"derivative": {"buckets_path": "a"}}
                })),
                "a",
            ),
        ];
        for (request, pipeline) in refused_cases {
            let tree = parse_aggregations(&request).expect("the request parses");
            let error = validate_pipelines(&tree.aggregations).expect_err("refused");
            let (status, error) = render_error(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{request}");
            assert_eq!(error["type"], json!("aggregation_execution_exception"), "{request}");
            assert_eq!(error["aggregation"], json!(pipeline), "{request}");
        }
        // Paths that would resolve to nothing in every bucket are refused before any data is
        // read, so even an all-empty histogram answers the 400.
        let empty_histogram = json!({"hours": {"buckets": []}});
        for (request, pipeline) in [
            (
                {
                    let mut request = build_hours_request(&json!({}));
                    request["m"] = json!({"max_bucket": {"buckets_path": "hours>typo"}});
                    request
                },
                "m",
            ),
            (
                build_hours_request(&json!({"d": {"derivative": {"buckets_path": "typo"}}})),
                "d",
            ),
            (
                build_hours_request(&json!({"d": {"derivative": {"buckets_path": "f>typo"}}})),
                "d",
            ),
            (
                build_hours_request(&json!({"d": {"derivative": {"buckets_path": "s"}}})),
                "d",
            ),
            (
                build_hours_request(&json!({"d": {"derivative": {"buckets_path": "s.median"}}})),
                "d",
            ),
            (
                build_hours_request(&json!({"d": {"derivative": {"buckets_path": "t>_count"}}})),
                "d",
            ),
            (
                build_hours_request(&json!({"d": {"derivative": {"buckets_path": "f"}}})),
                "d",
            ),
            (
                build_hours_request(&json!({"d": {"derivative": {"buckets_path": "pct.99"}}})),
                "d",
            ),
            (
                build_hours_request(&json!({"d": {"derivative": {"buckets_path": "pct"}}})),
                "d",
            ),
        ] {
            let error = run_pipelines(&request, empty_histogram.clone()).expect_err("refused");
            let (status, error) = render_error(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{request}");
            assert_eq!(error["type"], json!("aggregation_execution_exception"), "{request}");
            assert_eq!(error["aggregation"], json!(pipeline), "{request}");
        }
        let with_children = build_minute_histogram_request(json!({
            "d": {"derivative": {"buckets_path": "_count"}, "aggs": {"x": {"avg": {"field": "duration_micros"}}}}
        }));
        let tree = parse_aggregations(&with_children).expect("the request parses");
        let error = validate_pipelines(&tree.aggregations).expect_err("refused");
        assert_eq!(render_error_type(error).await, json!("parsing_exception"));
        let valid_request = build_hours_request(&json!({
            "d": {"derivative": {"buckets_path": "1", "unit": "1s"}},
            "1": {"avg": {"field": "duration_micros"}},
            "s_avg": {"derivative": {"buckets_path": "s.avg"}},
            "p50": {"derivative": {"buckets_path": "pct[50.0]"}},
            "errors": {"derivative": {"buckets_path": "f>_count"}},
            "b": {"bucket_script": {"buckets_path": {"a": "1"}, "script": "params.a"}}
        }));
        let tree = parse_aggregations(&valid_request).expect("the request parses");
        validate_pipelines(&tree.aggregations).expect("a well-placed tree passes");
    }

    #[test]
    fn pipelines_naming_each_other_run_in_dependency_order() {
        // `dd` (declared first) reads `d`, so `d` must run before it.
        let request = build_histogram_request(&json!({
            "dd": {"derivative": {"buckets_path": "d"}},
            "d": {"derivative": {"buckets_path": "1"}}
        }));
        let rendered = json!({"2": {"buckets": [
            {"key": 0, "doc_count": 1, "1": {"value": 1.0}},
            {"key": 60_000, "doc_count": 1, "1": {"value": 4.0}},
            {"key": 120_000, "doc_count": 1, "1": {"value": 9.0}}
        ]}});
        let rendered = run_pipelines(&request, rendered).expect("pipelines run");
        assert_eq!(
            read_column(&rendered, "d"),
            vec![None, Some(json!({"value": 3.0})), Some(json!({"value": 5.0}))]
        );
        // `dd` at bucket 1 reads `d`'s missing first value: null; bucket 2: 5 − 3.
        assert_eq!(
            read_column(&rendered, "dd"),
            vec![None, Some(json!({"value": null})), Some(json!({"value": 2.0}))]
        );
    }
}
