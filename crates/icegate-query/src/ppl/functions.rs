//! The PPL function library: each supported function as a DataFusion expression.
//!
//! The library is the table of `ppl/README.md`. A function outside it is refused by name rather
//! than guessed, and every function states its NULL behaviour. Array functions return NULL for a
//! NULL list; on an empty list `exists` is false, `forall` true, `array_length` 0, `mvjoin` the
//! empty string, `filter` and `transform` the empty list, and `mvindex` NULL for a single index (an
//! index outside the list) and the empty list for a range.

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::functions::expr_fn::{
    abs, btrim, ceil, coalesce, floor, lower, ltrim, md5, nullif, octet_length, regexp_replace, round, rtrim, strpos,
    substr, substring, to_char, upper,
};
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::functions_nested::expr_fn::{
    array_any_match, array_element, array_filter, array_length, array_slice, array_to_string, array_transform,
};
use datafusion::logical_expr::expr::{Lambda, Like};
use datafusion::logical_expr::{Expr, ExprSchemable, Operator, binary_expr, cast, lit, not, when};

use super::ast::{ArithmeticOperator, CastTarget};
use super::error::PplError;
use crate::opensearch::search::aggs::build_multi_quantile_udf;
use crate::opensearch::search::doc_id::{
    MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MILLISECOND, MICROS_PER_MINUTE, MICROS_PER_SECOND, MICROS_PER_WEEK,
};

/// The most fraction digits a timestamp literal may carry: columns hold microseconds.
const MAX_FRACTION_DIGITS: usize = 6;

/// The `chrono` format that renders a timestamp in the PPL output form with six fraction digits;
/// trailing zeros are then trimmed.
const TIMESTAMP_TEXT_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";

/// What a function call is lowered against.
pub(crate) struct FunctionContext<'a> {
    /// The columns of the plan at the point of the call.
    pub(crate) schema: &'a DFSchema,
    /// The request time, taken once per request: `now()` and `unix_timestamp()` return it.
    pub(crate) now: DateTime<Utc>,
}

/// Lower the call of function `name` (lower-cased) with lowered `arguments`.
///
/// # Errors
///
/// Returns [`PplError::UnsupportedFunction`] naming a function outside the library, and
/// [`PplError::Semantic`] for a wrong number of arguments, an argument that must be a literal and
/// is not, or a literal the function cannot take.
pub(crate) fn lower_function_call(
    name: &str,
    arguments: Vec<Expr>,
    context: &FunctionContext<'_>,
) -> Result<Expr, PplError> {
    match name {
        "if" => {
            let [condition, then_value, else_value] = take_arguments(name, arguments)?;
            Ok(when(condition, then_value).otherwise(else_value)?)
        }
        "coalesce" => {
            if arguments.is_empty() {
                return Err(build_wrong_arity_error(name, "at least 1", 0));
            }
            Ok(coalesce(arguments))
        }
        "ifnull" => {
            let [value, fallback] = take_arguments(name, arguments)?;
            Ok(coalesce(vec![value, fallback]))
        }
        "nullif" => {
            let [value, null_value] = take_arguments(name, arguments)?;
            Ok(nullif(value, null_value))
        }
        "isnull" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(value.is_null())
        }
        "isnotnull" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(value.is_not_null())
        }
        "concat" => lower_concat(arguments),
        "lower" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(lower(value))
        }
        "upper" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(upper(value))
        }
        "trim" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(btrim(vec![value]))
        }
        "ltrim" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(ltrim(vec![value]))
        }
        "rtrim" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(rtrim(vec![value]))
        }
        "substring" | "substr" => lower_substring(name, arguments),
        "length" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(octet_length(value))
        }
        "locate" => {
            let [needle, haystack] = take_arguments(name, arguments)?;
            Ok(strpos(haystack, needle))
        }
        "like" => lower_like(arguments),
        "md5" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(md5(value))
        }
        "abs" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(abs(value))
        }
        "floor" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(cast(floor(value), DataType::Int64))
        }
        "ceil" | "ceiling" => {
            let [value] = take_arguments(name, arguments)?;
            Ok(cast(ceil(value), DataType::Int64))
        }
        "round" => lower_round(arguments),
        "mod" => {
            let [dividend, divisor] = take_arguments(name, arguments)?;
            Ok(lower_modulo(dividend, divisor))
        }
        "timestamp" => {
            let [value] = take_arguments(name, arguments)?;
            lower_timestamp(value, context)
        }
        "timestampdiff" => lower_timestampdiff(arguments, context),
        "date_format" => {
            let [value, format_pattern] = take_arguments(name, arguments)?;
            lower_date_format(value, &format_pattern, context)
        }
        "now" => {
            let [] = take_arguments(name, arguments)?;
            Ok(build_request_time_literal(context))
        }
        "unix_timestamp" => lower_unix_timestamp(arguments, context),
        "from_unixtime" => lower_from_unixtime(arguments, context),
        "array_length" => {
            let [list] = take_arguments(name, arguments)?;
            Ok(cast(array_length(list), DataType::Int64))
        }
        "mvjoin" => {
            let [list, separator] = take_arguments(name, arguments)?;
            Ok(array_to_string(list, separator))
        }
        "mvindex" => lower_mvindex(arguments, context),
        "exists" => {
            let [list, predicate] = take_arguments(name, arguments)?;
            Ok(array_any_match(list, predicate))
        }
        "forall" => {
            let [list, predicate] = take_arguments(name, arguments)?;
            Ok(not(array_any_match(list, negate_lambda(predicate)?)))
        }
        "filter" => {
            let [list, predicate] = take_arguments(name, arguments)?;
            Ok(array_filter(list, predicate))
        }
        "transform" => {
            let [list, mapping] = take_arguments(name, arguments)?;
            Ok(array_transform(list, mapping))
        }
        other => Err(PplError::UnsupportedFunction {
            function: other.to_string(),
        }),
    }
}

/// How an aggregation of `stats` is computed.
#[derive(Debug, Clone)]
pub(crate) enum PlannedAggregate {
    /// An aggregate of the grouped aggregation itself.
    Grouped(Expr),
    /// A percentile: a multi-quantile aggregate of the grouped aggregation whose one element is
    /// the value.
    Percentile(Expr),
    /// The exact count of distinct non-NULL values of the expression, computed in its own
    /// two-level branch.
    DistinctCount(Expr),
    /// The distinct non-NULL values of the expression, sorted, computed in its own branch.
    DistinctValues(Expr),
}

/// Plan the aggregate `name` over lowered `arguments`: `count()` counts rows; `count(x)`,
/// `sum`, `avg`, `min` and `max` skip NULL (`min` and `max` compare strings byte-wise); `dc`
/// counts distinct values exactly; `values` collects them, distinct, sorted and NULL-free, with
/// no cap; `percentile(x, p)` is approximate, `p` in 0..=100.
///
/// # Errors
///
/// Returns [`PplError::UnsupportedFunction`] for another aggregate, and [`PplError::Semantic`]
/// for a wrong number of arguments or a percentile outside 0..=100.
pub(crate) fn plan_aggregate_call(name: &str, arguments: Vec<Expr>) -> Result<PlannedAggregate, PplError> {
    match name {
        "count" => match <[Expr; 0]>::try_from(arguments) {
            Ok([]) => Ok(PlannedAggregate::Grouped(count(lit(1_i64)))),
            Err(arguments) => {
                let [value] = take_arguments(name, arguments)?;
                Ok(PlannedAggregate::Grouped(count(value)))
            }
        },
        "sum" => Ok(PlannedAggregate::Grouped(sum(take_value(name, arguments)?))),
        "avg" => Ok(PlannedAggregate::Grouped(avg(take_value(name, arguments)?))),
        "min" => Ok(PlannedAggregate::Grouped(min(take_value(name, arguments)?))),
        "max" => Ok(PlannedAggregate::Grouped(max(take_value(name, arguments)?))),
        "dc" | "distinct_count" => Ok(PlannedAggregate::DistinctCount(take_value(name, arguments)?)),
        "values" => Ok(PlannedAggregate::DistinctValues(take_value(name, arguments)?)),
        "percentile" | "percentile_approx" => {
            let [value, percent] = take_arguments(name, arguments)?;
            let percent = match percent {
                Expr::Literal(ScalarValue::Float64(Some(percent)), _) => percent,
                #[allow(clippy::cast_precision_loss)]
                Expr::Literal(ScalarValue::Int64(Some(percent)), _) => percent as f64,
                _ => {
                    return Err(PplError::Semantic {
                        reason: "percentile takes a numeric literal percent".to_string(),
                    });
                }
            };
            if !(0.0..=100.0).contains(&percent) {
                return Err(PplError::Semantic {
                    reason: format!("percentile [{percent}] is outside 0..=100"),
                });
            }
            Ok(PlannedAggregate::Percentile(
                build_multi_quantile_udf().call(vec![cast(value, DataType::Float64), lit(percent)]),
            ))
        }
        other => Err(PplError::UnsupportedFunction {
            function: other.to_string(),
        }),
    }
}

/// The one argument of an aggregate that takes exactly one.
fn take_value(function: &str, arguments: Vec<Expr>) -> Result<Expr, PplError> {
    let [value] = take_arguments(function, arguments)?;
    Ok(value)
}

/// Whether `name` is an array function whose second argument is a lambda over the elements of
/// its first; the planner binds the lambda's parameter to the element type before lowering it.
pub(crate) fn is_higher_order_function(name: &str) -> bool {
    matches!(name, "exists" | "forall" | "filter" | "transform")
}

/// The CAST of `value` to `target`. A string literal cast to `timestamp` folds to a microsecond
/// literal; a timestamp cast to `string` renders as the PPL output does.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] for a timestamp cast of a value that is neither a string nor a
/// timestamp, or of a malformed timestamp literal.
pub(crate) fn lower_cast(value: Expr, target: CastTarget, context: &FunctionContext<'_>) -> Result<Expr, PplError> {
    match target {
        CastTarget::Double => Ok(cast(value, DataType::Float64)),
        CastTarget::Long => Ok(cast(value, DataType::Int64)),
        CastTarget::Int => Ok(cast(value, DataType::Int32)),
        CastTarget::Boolean => Ok(cast(value, DataType::Boolean)),
        CastTarget::Timestamp => lower_timestamp(value, context),
        CastTarget::String => {
            if matches!(value.get_type(context.schema)?, DataType::Timestamp(_, _)) {
                Ok(render_timestamp_text(value))
            } else {
                Ok(cast(value, DataType::Utf8))
            }
        }
    }
}

/// `left <operator> right`; a zero divisor of `/` and `%` yields NULL rather than an error.
pub(crate) fn lower_arithmetic(operator: ArithmeticOperator, left: Expr, right: Expr) -> Expr {
    match operator {
        ArithmeticOperator::Add => binary_expr(left, Operator::Plus, right),
        ArithmeticOperator::Subtract => binary_expr(left, Operator::Minus, right),
        ArithmeticOperator::Multiply => binary_expr(left, Operator::Multiply, right),
        ArithmeticOperator::Divide => binary_expr(left, Operator::Divide, nullif(right, lit(0_i64))),
        ArithmeticOperator::Modulo => lower_modulo(left, right),
    }
}

/// `dividend % divisor`, NULL when the divisor is zero.
fn lower_modulo(dividend: Expr, divisor: Expr) -> Expr {
    binary_expr(dividend, Operator::Modulo, nullif(divisor, lit(0_i64)))
}

/// Parse a timestamp literal: `yyyy-MM-dd`, or `yyyy-MM-dd HH:mm:ss` (a `T` may separate date and
/// time) with up to six fraction digits, read as UTC; the result is microseconds since the epoch.
/// Surrounding whitespace is ignored.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] for any other form, and for more than six fraction digits:
/// columns hold microseconds, so a finer literal could only be truncated.
pub(crate) fn parse_timestamp_literal(text: &str) -> Result<i64, PplError> {
    let build_malformed_error = || PplError::Semantic {
        reason: format!("[{text}] is not a timestamp of the form yyyy-MM-dd HH:mm:ss[.SSSSSS]"),
    };
    let trimmed_text = text.trim();
    if let Ok(date) = NaiveDate::parse_from_str(trimmed_text, "%Y-%m-%d") {
        let midnight = date.and_hms_opt(0, 0, 0).ok_or_else(build_malformed_error)?;
        return Ok(midnight.and_utc().timestamp_micros());
    }
    let (whole_seconds_text, fraction_text) = trimmed_text.split_once('.').unwrap_or((trimmed_text, ""));
    // A point with no digit after it is no form of the stated ones.
    if fraction_text.is_empty() && trimmed_text.contains('.') {
        return Err(build_malformed_error());
    }
    if fraction_text.len() > MAX_FRACTION_DIGITS {
        return Err(PplError::Semantic {
            reason: format!(
                "[{text}] has {} fraction digits; timestamps hold at most {MAX_FRACTION_DIGITS}",
                fraction_text.len()
            ),
        });
    }
    if !fraction_text.chars().all(|digit| digit.is_ascii_digit()) {
        return Err(build_malformed_error());
    }
    let whole_seconds = NaiveDateTime::parse_from_str(whole_seconds_text, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(whole_seconds_text, "%Y-%m-%dT%H:%M:%S"))
        .map_err(|_| build_malformed_error())?;
    let fraction_micros = if fraction_text.is_empty() {
        0
    } else {
        // Right-padding to six digits reads `.5` as 500 000 µs.
        format!("{fraction_text:0<MAX_FRACTION_DIGITS$}")
            .parse::<i64>()
            .map_err(|_| build_malformed_error())?
    };
    Ok(whole_seconds.and_utc().timestamp_micros() + fraction_micros)
}

/// `timestamp(value)`: a string literal folds to a microsecond literal, so a comparison with a
/// timestamp column stays a literal comparison the scans can prune with; a timestamp or a string
/// column converts to `Timestamp(µs)`.
fn lower_timestamp(value: Expr, context: &FunctionContext<'_>) -> Result<Expr, PplError> {
    if let Some(text) = match_string_literal(&value) {
        let micros = parse_timestamp_literal(text)?;
        return Ok(lit(ScalarValue::TimestampMicrosecond(Some(micros), None)));
    }
    match value.get_type(context.schema)? {
        DataType::Timestamp(_, _) | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Date32 => {
            Ok(cast(value, DataType::Timestamp(TimeUnit::Microsecond, None)))
        }
        other => Err(PplError::Semantic {
            reason: format!("timestamp() takes a string or a timestamp, not {other}; use from_unixtime for numbers"),
        }),
    }
}

/// `timestampdiff(<unit>, start, end)`: `end - start` in whole units, truncated toward zero, as a
/// bigint. The fixed units are supported; `MONTH`, `QUARTER` and `YEAR` are refused.
fn lower_timestampdiff(arguments: Vec<Expr>, context: &FunctionContext<'_>) -> Result<Expr, PplError> {
    let [unit, start, end] = take_arguments("timestampdiff", arguments)?;
    let unit = read_string_literal("timestampdiff", &unit)?;
    let unit_micros: i64 = match unit.as_str() {
        "MICROSECOND" => 1,
        "MILLISECOND" => MICROS_PER_MILLISECOND,
        "SECOND" => MICROS_PER_SECOND,
        "MINUTE" => MICROS_PER_MINUTE,
        "HOUR" => MICROS_PER_HOUR,
        "DAY" => MICROS_PER_DAY,
        "WEEK" => MICROS_PER_WEEK,
        other => {
            return Err(PplError::Semantic {
                reason: format!("timestampdiff with the unit {other} is not supported"),
            });
        }
    };
    let convert_to_micros =
        |value: Expr| -> Result<Expr, PplError> { Ok(cast(lower_timestamp(value, context)?, DataType::Int64)) };
    let difference = binary_expr(convert_to_micros(end)?, Operator::Minus, convert_to_micros(start)?);
    if unit_micros == 1 {
        return Ok(difference);
    }
    Ok(binary_expr(difference, Operator::Divide, lit(unit_micros)))
}

/// `date_format(value, format)` with MySQL-style specifiers, rendered in UTC.
fn lower_date_format(value: Expr, format_pattern: &Expr, context: &FunctionContext<'_>) -> Result<Expr, PplError> {
    let format_pattern = read_string_literal("date_format", format_pattern)?;
    let chrono_format = translate_date_format(&format_pattern)?;
    Ok(to_char(lower_timestamp(value, context)?, lit(chrono_format)))
}

/// The `chrono` format of a MySQL-style `date_format` pattern.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] for a specifier outside the supported set and for a pattern
/// that ends with a lone `%`.
fn translate_date_format(format_pattern: &str) -> Result<String, PplError> {
    let mut translated_format = String::with_capacity(format_pattern.len());
    let mut characters = format_pattern.chars();
    while let Some(character) = characters.next() {
        if character != '%' {
            translated_format.push(character);
            continue;
        }
        let specifier = characters.next().ok_or_else(|| PplError::Semantic {
            reason: format!("date_format pattern [{format_pattern}] ends with %"),
        })?;
        let replacement = match specifier {
            'Y' => "%Y",
            'y' => "%y",
            'm' => "%m",
            'c' => "%-m",
            'd' => "%d",
            'e' => "%-d",
            'H' => "%H",
            'k' => "%-H",
            'h' | 'I' => "%I",
            'l' => "%-I",
            'i' => "%M",
            's' | 'S' => "%S",
            'f' => "%6f",
            'p' => "%p",
            'M' => "%B",
            'b' => "%b",
            'W' => "%A",
            'a' => "%a",
            'j' => "%j",
            'T' => "%H:%M:%S",
            'r' => "%I:%M:%S %p",
            '%' => "%%",
            other => {
                return Err(PplError::Semantic {
                    reason: format!("date_format does not support the specifier %{other}"),
                });
            }
        };
        translated_format.push_str(replacement);
    }
    Ok(translated_format)
}

/// The request time as a microsecond timestamp literal.
fn build_request_time_literal(context: &FunctionContext<'_>) -> Expr {
    lit(ScalarValue::TimestampMicrosecond(
        Some(context.now.timestamp_micros()),
        None,
    ))
}

/// The microseconds of one second as a double, the divisor and multiplier between seconds and
/// microseconds.
fn build_micros_per_second_double() -> Expr {
    cast(lit(MICROS_PER_SECOND), DataType::Float64)
}

/// `unix_timestamp([value])`: seconds since the epoch as a double; without an argument, of the
/// request time, read exactly as `unix_timestamp(now())` reads it.
fn lower_unix_timestamp(arguments: Vec<Expr>, context: &FunctionContext<'_>) -> Result<Expr, PplError> {
    let value = match <[Expr; 0]>::try_from(arguments) {
        Ok([]) => build_request_time_literal(context),
        Err(arguments) => {
            let [value] = take_arguments("unix_timestamp", arguments)?;
            value
        }
    };
    let micros = cast(
        cast(lower_timestamp(value, context)?, DataType::Int64),
        DataType::Float64,
    );
    Ok(binary_expr(micros, Operator::Divide, build_micros_per_second_double()))
}

/// `from_unixtime(seconds[, format])`: the timestamp `seconds` after the epoch, rounded to the
/// microsecond; with a format, rendered by `date_format`.
fn lower_from_unixtime(arguments: Vec<Expr>, context: &FunctionContext<'_>) -> Result<Expr, PplError> {
    let (seconds, format_pattern) = match <[Expr; 2]>::try_from(arguments) {
        Ok([seconds, format_pattern]) => (seconds, Some(format_pattern)),
        Err(arguments) => {
            let [seconds] = take_arguments("from_unixtime", arguments)?;
            (seconds, None)
        }
    };
    let micros = round(vec![binary_expr(
        cast(seconds, DataType::Float64),
        Operator::Multiply,
        build_micros_per_second_double(),
    )]);
    let timestamp = cast(
        cast(micros, DataType::Int64),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    match format_pattern {
        Some(format_pattern) => lower_date_format(timestamp, &format_pattern, context),
        None => Ok(timestamp),
    }
}

/// `concat(a, b, …)`: NULL when any argument is NULL; non-string arguments are cast to strings.
fn lower_concat(arguments: Vec<Expr>) -> Result<Expr, PplError> {
    let mut parts = arguments.into_iter().map(|argument| cast(argument, DataType::Utf8));
    let first_part = parts.next().ok_or_else(|| build_wrong_arity_error("concat", "at least 1", 0))?;
    Ok(parts.fold(first_part, |concatenation, part| {
        binary_expr(concatenation, Operator::StringConcat, part)
    }))
}

/// `substring(text, start[, length])`, 1-based.
fn lower_substring(name: &str, arguments: Vec<Expr>) -> Result<Expr, PplError> {
    match <[Expr; 3]>::try_from(arguments) {
        Ok([text, start, length]) => Ok(substring(
            text,
            cast(start, DataType::Int64),
            cast(length, DataType::Int64),
        )),
        Err(arguments) => {
            let [text, start] = take_arguments(name, arguments)?;
            Ok(substr(text, cast(start, DataType::Int64)))
        }
    }
}

/// `like(text, pattern[, case_sensitive])`: `%` matches any run and `_` one character, `\`
/// escapes. Without the third argument the match ignores case.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] when the third argument is not a boolean literal, and for
/// another arity.
fn lower_like(arguments: Vec<Expr>) -> Result<Expr, PplError> {
    let (text, pattern, is_case_sensitive) = match <[Expr; 3]>::try_from(arguments) {
        Ok([text, pattern, case_sensitivity_flag]) => {
            let Expr::Literal(ScalarValue::Boolean(Some(is_case_sensitive)), _) = case_sensitivity_flag else {
                return Err(PplError::Semantic {
                    reason: "like takes true or false as its third argument".to_string(),
                });
            };
            (text, pattern, is_case_sensitive)
        }
        Err(arguments) => {
            let [text, pattern] = take_arguments("like", arguments)?;
            (text, pattern, false)
        }
    };
    Ok(Expr::Like(Like::new(
        false,
        Box::new(text),
        Box::new(pattern),
        Some('\\'),
        !is_case_sensitive,
    )))
}

/// `round(value[, digits])` as a double.
fn lower_round(arguments: Vec<Expr>) -> Result<Expr, PplError> {
    match <[Expr; 2]>::try_from(arguments) {
        Ok([value, digits]) => Ok(round(vec![
            cast(value, DataType::Float64),
            cast(digits, DataType::Int64),
        ])),
        Err(arguments) => {
            let [value] = take_arguments("round", arguments)?;
            Ok(round(vec![cast(value, DataType::Float64)]))
        }
    }
}

/// `mvindex(list, index[, end])`: 0-based indexes, negative ones counting from the end; with
/// `end`, the elements from `index` to `end` inclusive.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] when an index is not an integer literal, and for another
/// arity.
fn lower_mvindex(arguments: Vec<Expr>, context: &FunctionContext<'_>) -> Result<Expr, PplError> {
    // DataFusion's array indexes are 1-based; a negative index counts from the end in both.
    let convert_to_one_based = |index: &Expr| -> Result<i64, PplError> {
        let Expr::Literal(ScalarValue::Int64(Some(index)), _) = index else {
            return Err(PplError::Semantic {
                reason: "mvindex takes integer literals as indexes".to_string(),
            });
        };
        Ok(if *index >= 0 { index + 1 } else { *index })
    };
    match <[Expr; 3]>::try_from(arguments) {
        Ok([list, start, end]) => Ok(array_slice(
            cast_to_sliced_list_type(list, context)?,
            lit(convert_to_one_based(&start)?),
            lit(convert_to_one_based(&end)?),
            None,
        )),
        Err(arguments) => {
            let [list, index] = take_arguments("mvindex", arguments)?;
            Ok(array_element(list, lit(convert_to_one_based(&index)?)))
        }
    }
}

/// `list` cast to a list of nullable, default-named elements before it is sliced. The slice's
/// planned type must equal the type it executes to; normalising the input's element field this way
/// keeps the two equal whatever the table's element field is (a table's list element is non-null
/// and named by the table). Pinned by the integration test
/// `array_functions_return_null_on_a_null_list_and_a_value_on_an_empty_list`, whose
/// `mvindex(finish_reasons, 0, 1)` on a table list fails without this cast.
fn cast_to_sliced_list_type(list: Expr, context: &FunctionContext<'_>) -> Result<Expr, PplError> {
    let build_element_field = |element: &Field| Arc::new(Field::new_list_field(element.data_type().clone(), true));
    Ok(match list.get_type(context.schema)? {
        DataType::List(element) => cast(list, DataType::List(build_element_field(&element))),
        DataType::LargeList(element) => cast(list, DataType::LargeList(build_element_field(&element))),
        _ => list,
    })
}

/// The lambda `x -> not body` of the lambda `x -> body`. The planner refuses a non-lambda second
/// argument of an array function before lowering the call, so the refusal here guards a defect.
fn negate_lambda(predicate: Expr) -> Result<Expr, PplError> {
    let Expr::Lambda(Lambda { params, body }) = predicate else {
        return Err(PplError::Internal {
            reason: "forall lowered without a lambda as its second argument".to_string(),
        });
    };
    Ok(Expr::Lambda(Lambda::new(params, not(*body))))
}

/// A timestamp as `yyyy-MM-dd HH:mm:ss[.f]`, trailing zeros of the fraction (and a bare dot)
/// removed.
fn render_timestamp_text(value: Expr) -> Expr {
    let text = to_char(value, lit(TIMESTAMP_TEXT_FORMAT));
    regexp_replace(text, lit(r"\.?0+$"), lit(""), None)
}

/// The text of a string-literal argument of `function`.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] for any other argument.
fn read_string_literal(function: &str, value: &Expr) -> Result<String, PplError> {
    match_string_literal(value)
        .map(str::to_string)
        .ok_or_else(|| PplError::Semantic {
            reason: format!("{function} takes a string literal here"),
        })
}

/// The text of a non-null string literal of any DataFusion string type (`Utf8`, `LargeUtf8`,
/// `Utf8View`); `None` for every other expression. Every PPL fold of a string literal matches it
/// through this one function, so no fold misses a string type the lowering may produce.
pub(crate) fn match_string_literal(expression: &Expr) -> Option<&str> {
    match expression {
        Expr::Literal(
            ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)) | ScalarValue::Utf8View(Some(text)),
            _,
        ) => Some(text),
        _ => None,
    }
}

/// The arguments of a call that takes exactly `N` of them.
fn take_arguments<const N: usize>(function: &str, arguments: Vec<Expr>) -> Result<[Expr; N], PplError> {
    let count = arguments.len();
    <[Expr; N]>::try_from(arguments).map_err(|_| build_wrong_arity_error(function, &N.to_string(), count))
}

/// The refusal of a call of `function` with `actual` arguments where it takes `expected`.
fn build_wrong_arity_error(function: &str, expected: &str, actual: usize) -> PplError {
    PplError::Semantic {
        reason: format!("function [{function}] takes {expected} argument(s), not {actual}"),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    #[test]
    fn timestamp_literals_parse_to_utc_microseconds_as_stated() {
        // 2026-10-08T00:00:00Z is 1_791_417_600 s after the epoch.
        let midnight_micros = 1_791_417_600_000_000_i64;
        let noon_micros = midnight_micros + 12 * 3_600_000_000;
        let cases = [
            ("2026-10-08", midnight_micros),
            ("2026-10-08 12:00:00", noon_micros),
            ("2026-10-08 12:00:00.5", noon_micros + 500_000),
            ("2026-10-08 12:00:00.123", noon_micros + 123_000),
            ("2026-10-08T12:00:00.000001", noon_micros + 1),
            (" 2026-10-08 12:00:00.123456 ", noon_micros + 123_456),
        ];
        for (text, expected) in cases {
            assert_eq!(parse_timestamp_literal(text), Ok(expected), "literal {text:?}");
        }
        for text in [
            "2026-10-08 12:00:00.1234567",
            "08/10/2026",
            "2026-10-08 12:00",
            "2026-10-08 12:00:00.x",
            "2026-10-08 12:00:00.",
        ] {
            assert!(
                matches!(parse_timestamp_literal(text), Err(PplError::Semantic { .. })),
                "literal {text:?}"
            );
        }
    }

    #[test]
    fn date_format_specifiers_translate_to_chrono_as_stated() {
        assert_eq!(
            translate_date_format("%Y-%m-%d %H:%i:%s.%f %p %%"),
            Ok("%Y-%m-%d %H:%M:%S.%6f %p %%".to_string())
        );
        assert_eq!(translate_date_format("%T %r"), Ok("%H:%M:%S %I:%M:%S %p".to_string()));
        assert!(matches!(translate_date_format("%U"), Err(PplError::Semantic { .. })));
        assert!(matches!(translate_date_format("100%"), Err(PplError::Semantic { .. })));
    }

    #[test]
    fn timestamp_literals_written_from_random_instants_parse_back_to_them() {
        // Instants on both sides of the epoch, written in every accepted form by chrono, which is
        // independent of the parser: the full form, the `T` form, the form with trailing zeros of
        // the fraction dropped (as the jdbc rendering writes it) and the date alone (midnight).
        let mut rng = StdRng::seed_from_u64(0x7157_A3D0);
        let lower_micros = Utc
            .with_ymd_and_hms(1900, 1, 1, 0, 0, 0)
            .single()
            .expect("1900")
            .timestamp_micros();
        let upper_micros = Utc
            .with_ymd_and_hms(2200, 1, 1, 0, 0, 0)
            .single()
            .expect("2200")
            .timestamp_micros();
        for _ in 0..2_000 {
            let micros = rng.random_range(lower_micros..upper_micros);
            let instant = DateTime::from_timestamp_micros(micros).expect("instant in range");
            let full_text = instant.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            assert_eq!(parse_timestamp_literal(&full_text), Ok(micros), "literal {full_text}");
            let t_separated_text = instant.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
            assert_eq!(
                parse_timestamp_literal(&t_separated_text),
                Ok(micros),
                "literal {t_separated_text}"
            );
            let trimmed_text = full_text.trim_end_matches('0').trim_end_matches('.');
            assert_eq!(
                parse_timestamp_literal(trimmed_text),
                Ok(micros),
                "literal {trimmed_text}"
            );
            let date_text = instant.format("%Y-%m-%d").to_string();
            let midnight_micros = micros - micros.rem_euclid(86_400_000_000);
            assert_eq!(
                parse_timestamp_literal(&date_text),
                Ok(midnight_micros),
                "literal {date_text}"
            );
        }
    }
}
