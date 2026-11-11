//! Encoded and derived fields of the non-native profiles, and the field tables those profiles
//! declare.
//!
//! An encoded field shows a stored column under a reversible conversion: hex ids, enum names,
//! NULL as `""` or `0`, microseconds as RFC 3339 text. Each one declares its filter inverse, so a
//! predicate on the value a client sees lowers to a predicate on the stored column and the scan
//! still prunes with it. A derived field is a per-row expression over stored columns with a
//! declared type; its range bounds rewrite to the source column where the expression is
//! monotonic. Each profile index is one [`ProfileTable`]: resolution, field listing, `_source`
//! and `fields` all read that table, so a field is declared once.

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema, TimeUnit, TimestampMicrosecondType};
use datafusion::functions::core::expr_fn::{coalesce, get_field};
use datafusion::functions_nested::expr_fn::cardinality;
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::{Expr, cast, in_list, lit};
use datafusion::prelude::ident;
use icegate_common::schema::{
    AGGREGATION_TEMPORALITY_CUMULATIVE, AGGREGATION_TEMPORALITY_DELTA, COL_AGGREGATION_TEMPORALITY, COL_ATTRIBUTES,
    COL_DROPPED_ATTRIBUTES_COUNT, COL_DURATION_MICROS, COL_EVENTS, COL_EXEMPLARS, COL_LINKS, COL_METRIC_TYPE, COL_NAME,
    COL_QUANTILE_VALUES, COL_RESOURCE_ATTRIBUTES, COL_SCALE, COL_SPAN_ID, COL_TIMESTAMP, COL_TRACE_ID, COL_TRACE_STATE,
    COL_VALUE_DOUBLE, COL_VALUE_INT, METRIC_TYPE_EXPONENTIAL_HISTOGRAM, METRIC_TYPE_GAUGE, METRIC_TYPE_HISTOGRAM,
    METRIC_TYPE_SUM, METRIC_TYPE_SUMMARY, QUANTILE_FIELD_QUANTILE, QUANTILE_FIELD_VALUE, promoted_resource_keys,
};
use serde_json::{Map, Number, Value};

use super::cross_row::{CrossRowField, TraceRoots, render_cross_row_field};
use super::data_prepper::DURATION_IN_NANOS_FIELD;
use super::field::{
    FieldType, FieldValue, IndexField, ResolvedField, SelectedField, covers_every_key, glob_matches_some_extension,
};
use super::map_key_cache::MapKeys;
use super::native::{METRIC_VALUE_FIELD, find_promoted_resource_key, native_column_type};
use crate::engine::fixed_binary::decode_fixed_binary_literal;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::glob::matches_glob;
use crate::opensearch::search::request::SourceFilter;
use crate::opensearch::search::{render_array_value, render_iso_micros};
use crate::query_dsl::ast::RangeBound;
use crate::query_dsl::literal::{parse_java_double, step_exclusive_double};
use crate::query_dsl::lower::{classify_bound_side, render_literal_text};

/// The `OTel` span kinds by stored integer. Ingest stores kind `0` as NULL; both read as
/// `SPAN_KIND_UNSPECIFIED`.
const SPAN_KINDS: [(i32, &str); 6] = [
    (0, "SPAN_KIND_UNSPECIFIED"),
    (1, "SPAN_KIND_INTERNAL"),
    (2, "SPAN_KIND_SERVER"),
    (3, "SPAN_KIND_CLIENT"),
    (4, "SPAN_KIND_PRODUCER"),
    (5, "SPAN_KIND_CONSUMER"),
];

/// The metric types as `metric_type` stores them and the `kind` names this profile renders for
/// them; [`METRIC_TYPE_SUMMARY`] renders no `kind`.
const METRIC_KINDS: [(&str, &str); 4] = [
    (METRIC_TYPE_GAUGE, "GAUGE"),
    (METRIC_TYPE_SUM, "SUM"),
    (METRIC_TYPE_HISTOGRAM, "HISTOGRAM"),
    (METRIC_TYPE_EXPONENTIAL_HISTOGRAM, "EXPONENTIAL_HISTOGRAM"),
];

/// The aggregation temporalities as `aggregation_temporality` stores them and the
/// `aggregationTemporality` names this profile renders.
const TEMPORALITIES: [(&str, &str); 2] = [
    (AGGREGATION_TEMPORALITY_DELTA, "AGGREGATION_TEMPORALITY_DELTA"),
    (AGGREGATION_TEMPORALITY_CUMULATIVE, "AGGREGATION_TEMPORALITY_CUMULATIVE"),
];

/// How an escaping profile writes a dot inside a stored attribute key.
const ESCAPED_KEY_DOT: char = '@';

/// How a profile spells a stored attribute key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttributeKeys {
    /// Each `.` written as `@`, so a dotted stored key stays one property when a client splits
    /// the document on dots; a stored key holding `@` is refused.
    Escaped,
    /// Stored keys as they are; a key with dots stays one flat key.
    Stored,
}

impl AttributeKeys {
    /// The spelling of the stored `key`.
    ///
    /// # Errors
    ///
    /// Returns the 400 of [`escape_attribute_key`] under `Escaped`.
    fn spell_key(self, key: &str) -> Result<String, OpenSearchError> {
        match self {
            Self::Escaped => escape_attribute_key(key),
            Self::Stored => Ok(key.to_string()),
        }
    }

    /// The stored key a requested `spelled_key` names.
    fn read_stored_key(self, spelled_key: &str) -> String {
        match self {
            Self::Escaped => unescape_attribute_key(spelled_key),
            Self::Stored => spelled_key.to_string(),
        }
    }
}

/// Nanoseconds per microsecond: `durationInNanos` is `duration_micros` times this.
pub(super) const NANOS_PER_MICRO: i64 = 1_000;

/// The smallest `f64` above every `i64`: 2^63. `[-2^63, 2^63)` converts to `i64` exactly when
/// integral.
const TWO_TO_THE_63: f64 = 9_223_372_036_854_775_808.0;

/// A stored value shown under a reversible conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EncodedField {
    /// A fixed-width binary id as lowercase hex; NULL renders `""`.
    HexIdOrEmpty {
        /// The stored id column.
        column: &'static str,
        /// The column's width in bytes, from its Arrow type.
        width: i32,
    },
    /// A string whose NULL renders `""`.
    EmptyWhenNull {
        /// The stored string column.
        column: &'static str,
    },
    /// An `OTel` span kind as its `SPAN_KIND_*` name.
    SpanKindName {
        /// The stored integer kind column.
        column: &'static str,
    },
    /// A microsecond timestamp as RFC 3339 UTC text with a literal `Z` and six fraction digits.
    /// Filters lower as the stored date column (see [`ResolvedField::into_lowering_view`]).
    Rfc3339Time {
        /// The stored timestamp column.
        column: &'static str,
    },
    /// A status code whose NULL renders `0`.
    StatusCodeOrZero {
        /// The stored integer status column.
        column: &'static str,
    },
    /// One stored key of an attribute MAP, named with each `.` written as `@`. Filters lower as
    /// the stored MAP key (see [`ResolvedField::into_lowering_view`]).
    EscapedMapKey {
        /// The stored MAP column.
        column: &'static str,
        /// The stored key, dots included.
        key: String,
    },
    /// The span's events: rendered in `_source`, never searchable.
    Events,
    /// The span's links: rendered in `_source`, never searchable.
    Links,
    /// The metric type as its `kind` name; `summary` renders absent.
    MetricKindName,
    /// The aggregation temporality as its `aggregationTemporality` name; NULL renders absent.
    TemporalityName,
    /// Exponential-histogram buckets as `{count, min, max}` objects. Rendered only: the bounds
    /// come from the row's scale and offset, which rendering reads through the profile table.
    ExponentialBuckets {
        /// The stored bucket counts.
        counts_column: &'static str,
    },
    /// Summary quantiles as `{quantile, value}` objects. Rendered only.
    QuantileValues,
    /// Exemplars as `{time, traceId, spanId}` objects. Rendered only.
    Exemplars,
}

/// A per-row expression over stored columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DerivedField {
    /// `duration_micros × 1000`: a span's duration in nanoseconds at microsecond precision.
    DurationInNanos,
    /// The length of a list column: absent for a NULL list, `0` for an empty one.
    ArrayLength {
        /// The stored list column.
        column: &'static str,
        /// The field name a request uses for the length; refusals name it.
        field: &'static str,
    },
    /// A metric point's value: `coalesce(value_double, cast(value_int as double))`, NULL on
    /// histogram and summary rows.
    MetricValue,
}

impl EncodedField {
    /// The client-visible value as an expression over the stored columns, for sorts and
    /// aggregations. Ids stay bytes (their renderers write hex); NULL becomes `""`, `0` or
    /// `SPAN_KIND_UNSPECIFIED` where the encoding says so, and an enum name with no stored match
    /// reads NULL.
    pub(crate) fn build_read_expr(&self) -> Expr {
        match self {
            Self::HexIdOrEmpty { column, .. } | Self::Rfc3339Time { column } => ident(*column),
            Self::EmptyWhenNull { column } => coalesce(vec![ident(*column), lit("")]),
            Self::SpanKindName { column } => build_span_kind_name_expr(column),
            Self::StatusCodeOrZero { column } => coalesce(vec![ident(*column), lit(0_i32)]),
            Self::EscapedMapKey { column, key } => get_field(ident(*column), key.as_str()),
            Self::Events => ident(COL_EVENTS),
            Self::Links => ident(COL_LINKS),
            Self::MetricKindName => build_enum_name_expr(COL_METRIC_TYPE, &METRIC_KINDS),
            Self::TemporalityName => build_enum_name_expr(COL_AGGREGATION_TEMPORALITY, &TEMPORALITIES),
            Self::ExponentialBuckets { counts_column } => ident(*counts_column),
            Self::QuantileValues => ident(COL_QUANTILE_VALUES),
            Self::Exemplars => ident(COL_EXEMPLARS),
        }
    }

    /// Whether the field has no filter inverse: it renders in `_source` and no query reaches it.
    pub(crate) const fn is_render_only(&self) -> bool {
        matches!(
            self,
            Self::ExponentialBuckets { .. } | Self::QuantileValues | Self::Exemplars
        )
    }
}

impl DerivedField {
    /// The derived value per row.
    pub(crate) fn build_read_expr(self) -> Expr {
        match self {
            Self::DurationInNanos => ident(COL_DURATION_MICROS) * lit(NANOS_PER_MICRO),
            Self::ArrayLength { column, .. } => cast(cardinality(ident(column)), DataType::Int64),
            Self::MetricValue => coalesce(vec![
                ident(COL_VALUE_DOUBLE),
                cast(ident(COL_VALUE_INT), DataType::Float64),
            ]),
        }
    }
}

/// `CASE WHEN kind IS NULL THEN 'SPAN_KIND_UNSPECIFIED' WHEN kind = 0 THEN … END`; an integer
/// outside the `OTel` kinds reads NULL.
fn build_span_kind_name_expr(column: &str) -> Expr {
    let mut when_then = vec![(Box::new(ident(column).is_null()), Box::new(lit(SPAN_KINDS[0].1)))];
    when_then.extend(
        SPAN_KINDS
            .iter()
            .map(|(kind, name)| (Box::new(ident(column).eq(lit(*kind))), Box::new(lit(*name)))),
    );
    Expr::Case(Case::new(None, when_then, None))
}

/// `CASE column WHEN stored THEN name … END` over `names`; a stored value outside `names` reads
/// NULL.
fn build_enum_name_expr(column: &'static str, names: &[(&'static str, &'static str)]) -> Expr {
    let when_then = names
        .iter()
        .map(|(stored_value, name)| (Box::new(lit(*stored_value)), Box::new(lit(*name))))
        .collect();
    Expr::Case(Case::new(Some(Box::new(ident(column))), when_then, None))
}

/// Rows whose stored value in `column` is one whose name in `names` satisfies `is_selected`;
/// `false` when no name does.
fn select_enum_names(column: &'static str, names: &[(&str, &str)], is_selected: impl Fn(&str) -> bool) -> Expr {
    let stored_values: Vec<Expr> = names
        .iter()
        .filter(|(_, name)| is_selected(name))
        .map(|(stored_value, _)| lit(*stored_value))
        .collect();
    if stored_values.is_empty() {
        lit(false)
    } else {
        in_list(ident(column), stored_values, false)
    }
}

/// The texts of a range's lower and upper bounds, each with whether it is inclusive; `None` on
/// an unbounded side.
type TextBounds = (Option<(bool, String)>, Option<(bool, String)>);

/// The bounds of a range on the field the request named `field_name`, as texts.
///
/// # Errors
///
/// Returns `parsing_exception` naming `field_name` for a bound that is no string, number or
/// boolean.
fn read_text_bounds(
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
    field_name: &str,
) -> Result<TextBounds, OpenSearchError> {
    let read_bound_text = |bound: Option<&RangeBound>| {
        bound
            .map(|bound| render_literal_text(&bound.value, field_name).map(|text| (bound.is_inclusive, text)))
            .transpose()
    };
    Ok((read_bound_text(lower)?, read_bound_text(upper)?))
}

/// Whether `name` lies inside `bounds`, compared lexicographically like a keyword range.
fn is_text_in_range(name: &str, bounds: &TextBounds) -> bool {
    bounds.0.as_ref().is_none_or(|(is_inclusive, text)| {
        if *is_inclusive {
            name >= text.as_str()
        } else {
            name > text.as_str()
        }
    }) && bounds.1.as_ref().is_none_or(|(is_inclusive, text)| {
        if *is_inclusive {
            name <= text.as_str()
        } else {
            name < text.as_str()
        }
    })
}

/// The name of the stored `value` among `names`; `None` when it has none.
fn find_enum_name<'n>(names: &[(&str, &'n str)], value: &str) -> Option<&'n str> {
    names
        .iter()
        .find(|(stored_value, _)| *stored_value == value)
        .map(|(_, name)| *name)
}

/// The escaped spelling of a stored attribute key: each `.` written as `@`.
///
/// # Errors
///
/// Returns [`OpenSearchError::unescapable_attribute_key`] when the key holds a literal `@`: the
/// spelling would read back as a different key.
pub(crate) fn escape_attribute_key(key: &str) -> Result<String, OpenSearchError> {
    if key.contains(ESCAPED_KEY_DOT) {
        return Err(OpenSearchError::unescapable_attribute_key(key));
    }
    Ok(key.replace('.', "@"))
}

/// The stored attribute key an escaped spelling names: each `@` read as `.`.
pub(crate) fn unescape_attribute_key(name: &str) -> String {
    name.replace(ESCAPED_KEY_DOT, ".")
}

/// `micros` since the epoch as RFC 3339 UTC text with a literal `Z` and six fraction digits, the
/// spelling a native `_source` gives a timestamp.
///
/// # Errors
///
/// Returns `500` for an instant outside chrono's range, which no stored timestamp reaches.
pub(crate) fn format_rfc3339_micros(micros: i64) -> Result<String, OpenSearchError> {
    render_iso_micros(micros)
}

/// A hex literal of `width` bytes for the id field the request named `field_name`.
///
/// # Errors
///
/// Returns the [`OpenSearchError::malformed_field_literal`] 400 naming `field_name` for a text
/// that is no hex of `width` bytes.
fn decode_hex_term(text: &str, width: i32, field_name: &str, index: &str) -> Result<Expr, OpenSearchError> {
    decode_fixed_binary_literal(text, &DataType::FixedSizeBinary(width))
        .map(lit)
        .map_err(|error| {
            OpenSearchError::malformed_field_literal(field_name, index, &format!("holds {width}-byte ids: {error}"))
        })
}

/// The term `text` as a finite number, per Java `Double.parseDouble`.
///
/// # Errors
///
/// Returns the [`OpenSearchError::malformed_field_literal`] 400 naming `field_name` for a text
/// that is no finite number.
fn parse_finite_term(text: &str, field_name: &str, index: &str) -> Result<f64, OpenSearchError> {
    parse_java_double(text).filter(|number| number.is_finite()).ok_or_else(|| {
        OpenSearchError::malformed_field_literal(field_name, index, &format!("cannot hold the value [{text}]"))
    })
}

/// Rows whose stored kind is one of `kinds`; kind `0` also selects the NULL rows it is stored as.
fn select_span_kinds(column: &str, kinds: &[i32]) -> Expr {
    let kind_column = ident(column);
    let kind_predicate = if kinds.is_empty() {
        lit(false)
    } else {
        in_list(
            kind_column.clone(),
            kinds.iter().map(|kind| lit(*kind)).collect(),
            false,
        )
    };
    if kinds.contains(&0) {
        kind_predicate.or(kind_column.is_null())
    } else {
        kind_predicate
    }
}

/// The stored kinds whose names satisfy `is_selected`.
fn find_span_kinds(is_selected: impl Fn(&str) -> bool) -> Vec<i32> {
    SPAN_KINDS
        .iter()
        .filter(|(_, name)| is_selected(name))
        .map(|(kind, _)| *kind)
        .collect()
}

/// `stored_predicate` (a predicate over non-NULL values; `None` admits every one of them),
/// widened to the NULL rows when the value they render as lies inside the selection.
fn include_null_rows(column_value: Expr, stored_predicate: Option<Expr>, is_null_inside: bool) -> Expr {
    let stored_predicate = stored_predicate.unwrap_or_else(|| column_value.clone().is_not_null());
    if is_null_inside {
        stored_predicate.or(column_value.is_null())
    } else {
        stored_predicate
    }
}

/// The comparison of `column_value` with `literal` that one side of a range asks for.
fn compare_with_bound(column_value: Expr, literal: Expr, is_lower: bool, is_inclusive: bool) -> Expr {
    match (is_lower, is_inclusive) {
        (true, true) => column_value.gt_eq(literal),
        (true, false) => column_value.gt(literal),
        (false, true) => column_value.lt_eq(literal),
        (false, false) => column_value.lt(literal),
    }
}

/// Lower `term value` on an encoded field to a predicate on its stored column. `field_name` is
/// the name the request used for the field: every refusal names it, never the stored column.
///
/// # Errors
///
/// Returns `parsing_exception` for a value that is no string, number or boolean, the
/// [`OpenSearchError::malformed_field_literal`] 400 for a malformed hex id or status code, and
/// `500` for an encoding that lowers through its stored column instead (times and escaped keys,
/// see [`ResolvedField::into_lowering_view`]) and for a render-only field (see
/// [`EncodedField::is_render_only`]), which the leaf lowering refuses before it gets here.
pub(crate) fn lower_encoded_term(
    field: &EncodedField,
    field_name: &str,
    value: &Value,
    index: &str,
) -> Result<Expr, OpenSearchError> {
    match field {
        EncodedField::HexIdOrEmpty { column, width } => {
            let text = render_literal_text(value, field_name)?;
            if text.is_empty() {
                return Ok(ident(*column).is_null());
            }
            Ok(ident(*column).eq(decode_hex_term(&text, *width, field_name, index)?))
        }
        EncodedField::EmptyWhenNull { column } => {
            let text = render_literal_text(value, field_name)?;
            // A stored empty string renders `""` as NULL does, so `""` selects both.
            Ok(if text.is_empty() {
                ident(*column).is_null().or(ident(*column).eq(lit(text)))
            } else {
                ident(*column).eq(lit(text))
            })
        }
        EncodedField::SpanKindName { column } => {
            let text = render_literal_text(value, field_name)?;
            Ok(select_span_kinds(column, &find_span_kinds(|name| name == text)))
        }
        EncodedField::StatusCodeOrZero { column } => {
            let number = parse_finite_term(&render_literal_text(value, field_name)?, field_name, index)?;
            if !is_integral(number) || !(f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&number) {
                return Ok(lit(false));
            }
            let code = i32::try_from(convert_integral_double(number))
                .map_err(|_| OpenSearchError::internal(format!("status code {number} left the Int32 range")))?;
            Ok(if code == 0 {
                ident(*column).is_null().or(ident(*column).eq(lit(0_i32)))
            } else {
                ident(*column).eq(lit(code))
            })
        }
        EncodedField::MetricKindName => {
            let text = render_literal_text(value, field_name)?;
            Ok(select_enum_names(COL_METRIC_TYPE, &METRIC_KINDS, |name| name == text))
        }
        EncodedField::TemporalityName => {
            let text = render_literal_text(value, field_name)?;
            Ok(select_enum_names(COL_AGGREGATION_TEMPORALITY, &TEMPORALITIES, |name| {
                name == text
            }))
        }
        EncodedField::Events | EncodedField::Links => Ok(lit(false)),
        EncodedField::Rfc3339Time { column } | EncodedField::EscapedMapKey { column, .. } => {
            Err(report_stored_view_field(column))
        }
        EncodedField::ExponentialBuckets { .. } | EncodedField::QuantileValues | EncodedField::Exemplars => {
            Err(report_render_only_field(field_name))
        }
    }
}

/// Lower a range on an encoded field to a predicate on its stored column. A NULL that renders as
/// `""` or `0` lies inside the range exactly when that rendered value does. `field_name` is the
/// name the request used for the field, as in [`lower_encoded_term`].
///
/// # Errors
///
/// As [`lower_encoded_term`], for each bound.
pub(crate) fn lower_encoded_range(
    field: &EncodedField,
    field_name: &str,
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
    index: &str,
) -> Result<Expr, OpenSearchError> {
    match field {
        EncodedField::HexIdOrEmpty { column, width } => {
            let column_value = ident(*column);
            let mut bounds: Vec<Expr> = Vec::with_capacity(2);
            for (is_lower, bound) in [(true, lower), (false, upper)] {
                if let Some(bound) = bound {
                    let literal = decode_hex_term(
                        &render_literal_text(&bound.value, field_name)?,
                        *width,
                        field_name,
                        index,
                    )?;
                    bounds.push(compare_with_bound(
                        column_value.clone(),
                        literal,
                        is_lower,
                        bound.is_inclusive,
                    ));
                }
            }
            // `""` sorts before every hex id, so a NULL id is inside exactly when no lower bound exists.
            Ok(include_null_rows(
                column_value,
                bounds.into_iter().reduce(Expr::and),
                lower.is_none(),
            ))
        }
        EncodedField::EmptyWhenNull { column } => {
            let column_value = ident(*column);
            let mut bounds: Vec<Expr> = Vec::with_capacity(2);
            // `""` lies above the lower bound only when the bound is an inclusive `""`, and below
            // the upper bound unless the bound is an exclusive `""`.
            let mut is_empty_inside = true;
            for (is_lower, bound) in [(true, lower), (false, upper)] {
                if let Some(bound) = bound {
                    let text = render_literal_text(&bound.value, field_name)?;
                    is_empty_inside &= if is_lower {
                        bound.is_inclusive && text.is_empty()
                    } else {
                        bound.is_inclusive || !text.is_empty()
                    };
                    bounds.push(compare_with_bound(
                        column_value.clone(),
                        lit(text),
                        is_lower,
                        bound.is_inclusive,
                    ));
                }
            }
            Ok(include_null_rows(
                column_value,
                bounds.into_iter().reduce(Expr::and),
                is_empty_inside,
            ))
        }
        EncodedField::SpanKindName { column } => {
            let bounds = read_text_bounds(lower, upper, field_name)?;
            Ok(select_span_kinds(
                column,
                &find_span_kinds(|name| is_text_in_range(name, &bounds)),
            ))
        }
        EncodedField::StatusCodeOrZero { column } => {
            let Some(bounds) = integer_bounds(lower, upper, field_name, index)? else {
                return Ok(lit(false));
            };
            let column_value = ident(*column);
            Ok(
                match build_integer_range(column_value.clone(), bounds, IntegerWidth::I32) {
                    Some(stored_predicate) if bounds.contains(0) => stored_predicate.or(column_value.is_null()),
                    Some(stored_predicate) => stored_predicate,
                    None => lit(false),
                },
            )
        }
        EncodedField::MetricKindName => {
            let bounds = read_text_bounds(lower, upper, field_name)?;
            Ok(select_enum_names(COL_METRIC_TYPE, &METRIC_KINDS, |name| {
                is_text_in_range(name, &bounds)
            }))
        }
        EncodedField::TemporalityName => {
            let bounds = read_text_bounds(lower, upper, field_name)?;
            Ok(select_enum_names(COL_AGGREGATION_TEMPORALITY, &TEMPORALITIES, |name| {
                is_text_in_range(name, &bounds)
            }))
        }
        EncodedField::Events | EncodedField::Links => Ok(lit(false)),
        EncodedField::Rfc3339Time { column } | EncodedField::EscapedMapKey { column, .. } => {
            Err(report_stored_view_field(column))
        }
        EncodedField::ExponentialBuckets { .. } | EncodedField::QuantileValues | EncodedField::Exemplars => {
            Err(report_render_only_field(field_name))
        }
    }
}

/// Lower `exists` on an encoded field. Every encoding that renders NULL as a value is present on
/// every row; events and links exist when the list holds an element; an enum name exists where
/// the stored value has a name.
///
/// # Errors
///
/// Returns `500` for an encoding that lowers through its stored column instead, and for a
/// render-only field (see [`report_render_only_field`]), which this lowering names by its
/// encoding: it receives no field name.
pub(crate) fn lower_encoded_exists(field: &EncodedField) -> Result<Expr, OpenSearchError> {
    match field {
        EncodedField::HexIdOrEmpty { .. }
        | EncodedField::EmptyWhenNull { .. }
        | EncodedField::SpanKindName { .. }
        | EncodedField::StatusCodeOrZero { .. } => Ok(lit(true)),
        EncodedField::Events => Ok(cardinality(ident(COL_EVENTS)).gt(lit(0_u64))),
        EncodedField::Links => Ok(cardinality(ident(COL_LINKS)).gt(lit(0_u64))),
        EncodedField::MetricKindName => Ok(select_enum_names(COL_METRIC_TYPE, &METRIC_KINDS, |_| true)),
        EncodedField::TemporalityName => Ok(select_enum_names(COL_AGGREGATION_TEMPORALITY, &TEMPORALITIES, |_| true)),
        EncodedField::Rfc3339Time { column } | EncodedField::EscapedMapKey { column, .. } => {
            Err(report_stored_view_field(column))
        }
        EncodedField::ExponentialBuckets { .. } => Err(report_render_only_field("exponential buckets")),
        EncodedField::QuantileValues => Err(report_render_only_field("quantile values")),
        EncodedField::Exemplars => Err(report_render_only_field("exemplars")),
    }
}

/// The `500` for an encoding whose filters lower through its stored column: reaching the
/// encoded lowering means a caller skipped `into_lowering_view`.
fn report_stored_view_field(column: &str) -> OpenSearchError {
    OpenSearchError::internal(format!(
        "the encoded field over column `{column}` lowers through its stored column, not its encoding"
    ))
}

/// The `500` for a render-only field that reached a filter lowering: the leaf lowering refuses
/// those with a 400 naming the field before it lowers anything. `subject` is the field name the
/// request sent where the caller has it, and otherwise the field's encoding; never a stored
/// column.
fn report_render_only_field(subject: &str) -> OpenSearchError {
    OpenSearchError::internal(format!("a render-only field ({subject}) reached a filter lowering"))
}

/// Lower `term value` on a derived field to a predicate on its source columns. Every refusal names
/// the derived field, never its source column. A value the field cannot take (a fractional
/// duration in nanoseconds or list length, a negative length) matches nothing. The metric value
/// matches `value_double` under IEEE totalOrder and `value_int` when the value is an integer
/// other than `-0.0`, which sorts below every integer `0`.
///
/// # Errors
///
/// Returns `parsing_exception` for a value that is no string, number or boolean, and the
/// [`OpenSearchError::malformed_field_literal`] 400 for a value that is not a finite number.
pub(crate) fn lower_derived_term(field: DerivedField, value: &Value, index: &str) -> Result<Expr, OpenSearchError> {
    match field {
        DerivedField::DurationInNanos => {
            let nanos = parse_finite_term(
                &render_literal_text(value, DURATION_IN_NANOS_FIELD)?,
                DURATION_IN_NANOS_FIELD,
                index,
            )?;
            if !is_integral(nanos) || !(-TWO_TO_THE_63..TWO_TO_THE_63).contains(&nanos) {
                return Ok(lit(false));
            }
            let nanos = convert_integral_double(nanos);
            if nanos.rem_euclid(NANOS_PER_MICRO) != 0 {
                return Ok(lit(false));
            }
            Ok(ident(COL_DURATION_MICROS).eq(lit(nanos.div_euclid(NANOS_PER_MICRO))))
        }
        DerivedField::ArrayLength { field: field_name, .. } => {
            let length = parse_finite_term(&render_literal_text(value, field_name)?, field_name, index)?;
            if !is_integral(length) || !(0.0..TWO_TO_THE_63).contains(&length) {
                return Ok(lit(false));
            }
            Ok(field.build_read_expr().eq(lit(convert_integral_double(length))))
        }
        DerivedField::MetricValue => {
            let number = parse_finite_term(
                &render_literal_text(value, METRIC_VALUE_FIELD)?,
                METRIC_VALUE_FIELD,
                index,
            )?;
            let double_side = ident(COL_VALUE_DOUBLE).eq(lit(number));
            // `-0.0` sorts just below `+0.0`, so no integer equals it.
            let is_integer_match =
                is_integral(number) && !is_negative_zero(number) && (-TWO_TO_THE_63..TWO_TO_THE_63).contains(&number);
            Ok(if is_integer_match {
                double_side.or(ident(COL_VALUE_INT).eq(lit(convert_integral_double(number))))
            } else {
                double_side
            })
        }
    }
}

/// Lower a range on a derived field. `durationInNanos` is monotonic in `duration_micros`, so
/// the nanosecond bounds rewrite to microsecond bounds on the stored column and keep pruning. A
/// list length compares its integer bounds with the length per row, which no scan can prune by.
/// The metric value is `value_double` inside the bounds (exclusive bounds stepped to the adjacent
/// double, IEEE totalOrder) or `value_int` inside the integer bounds (see [`integer_bounds`]).
///
/// # Errors
///
/// As [`integer_bounds`], naming the derived field, never its source column.
pub(crate) fn lower_derived_range(
    field: DerivedField,
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
    index: &str,
) -> Result<Expr, OpenSearchError> {
    match field {
        DerivedField::DurationInNanos => {
            let Some(nanos) = integer_bounds(lower, upper, DURATION_IN_NANOS_FIELD, index)? else {
                return Ok(lit(false));
            };
            // micros · 1000 ≥ min ⇔ micros ≥ ⌈min / 1000⌉; micros · 1000 ≤ max ⇔ micros ≤ ⌊max / 1000⌋.
            let micros = IntegerBounds {
                min: nanos
                    .min
                    .map(|min| min.div_euclid(NANOS_PER_MICRO) + i64::from(min.rem_euclid(NANOS_PER_MICRO) != 0)),
                max: nanos.max.map(|max| max.div_euclid(NANOS_PER_MICRO)),
            };
            Ok(
                build_integer_range(ident(COL_DURATION_MICROS), micros, IntegerWidth::I64)
                    .unwrap_or_else(|| lit(false)),
            )
        }
        DerivedField::ArrayLength { field: field_name, .. } => {
            let Some(lengths) = integer_bounds(lower, upper, field_name, index)? else {
                return Ok(lit(false));
            };
            Ok(build_integer_range(field.build_read_expr(), lengths, IntegerWidth::I64).unwrap_or_else(|| lit(false)))
        }
        DerivedField::MetricValue => {
            let value_double_expr = ident(COL_VALUE_DOUBLE);
            let mut double_bounds: Vec<Expr> = Vec::with_capacity(2);
            for (is_lower, bound) in [(true, lower), (false, upper)] {
                if let Some(bound) = bound {
                    let value = read_double_bound(bound, METRIC_VALUE_FIELD, index)?;
                    let literal = lit(step_exclusive_double(value, classify_bound_side(is_lower, bound)));
                    double_bounds.push(if is_lower {
                        value_double_expr.clone().gt_eq(literal)
                    } else {
                        value_double_expr.clone().lt_eq(literal)
                    });
                }
            }
            let double_side = double_bounds
                .into_iter()
                .reduce(Expr::and)
                .unwrap_or_else(|| value_double_expr.is_not_null());
            let integer_side = integer_bounds(lower, upper, METRIC_VALUE_FIELD, index)?
                .and_then(|bounds| build_integer_range(ident(COL_VALUE_INT), bounds, IntegerWidth::I64));
            Ok(match integer_side {
                Some(integer_side) => double_side.or(integer_side),
                None => double_side,
            })
        }
    }
}

/// Lower `exists` on a derived field: present wherever its source column is; a list length
/// wherever the list is not NULL, and the metric value wherever either value column is set. The
/// list test reads the length, so it stays out of the scan, which cannot evaluate a predicate on
/// a list column.
pub(crate) fn lower_derived_exists(field: DerivedField) -> Expr {
    match field {
        DerivedField::DurationInNanos => ident(COL_DURATION_MICROS).is_not_null(),
        DerivedField::ArrayLength { .. } => field.build_read_expr().is_not_null(),
        DerivedField::MetricValue => ident(COL_VALUE_DOUBLE).is_not_null().or(ident(COL_VALUE_INT).is_not_null()),
    }
}

/// The integers a numeric range admits, both ends inclusive; `None` on a side is unbounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IntegerBounds {
    /// The smallest admitted integer.
    pub(crate) min: Option<i64>,
    /// The largest admitted integer.
    pub(crate) max: Option<i64>,
}

impl IntegerBounds {
    /// Whether `value` lies inside.
    pub(crate) fn contains(self, value: i64) -> bool {
        self.min.is_none_or(|min| value >= min) && self.max.is_none_or(|max| value <= max)
    }
}

/// One side of an integer range.
enum IntegerEdge {
    /// No integer constrains this side.
    Unbounded,
    /// The first (or last) admitted integer.
    At(i64),
    /// No integer can satisfy this side.
    Empty,
}

/// The integers `lower` and `upper` admit when an integer `n` is compared as the double `n`
/// under IEEE totalOrder, where `-0.0` sorts just below `+0.0` (so `lte -0.0` admits `n < 0` and
/// `gt -0.0` admits `n ≥ 0`). Each bound parses as a Java double; bounds are not
/// finiteness-checked. `Ok(None)` when no integer lies inside.
///
/// # Errors
///
/// Returns `parsing_exception` for a bound that is no string, number or boolean, and the
/// [`OpenSearchError::malformed_field_literal`] 400 naming `field` for a bound that is not a
/// number.
pub(crate) fn integer_bounds(
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
    field: &str,
    index: &str,
) -> Result<Option<IntegerBounds>, OpenSearchError> {
    let mut bounds = IntegerBounds { min: None, max: None };
    if let Some(bound) = lower {
        match find_integer_lower_edge(read_double_bound(bound, field, index)?, bound.is_inclusive) {
            IntegerEdge::Unbounded => {}
            IntegerEdge::At(min) => bounds.min = Some(min),
            IntegerEdge::Empty => return Ok(None),
        }
    }
    if let Some(bound) = upper {
        match find_integer_upper_edge(read_double_bound(bound, field, index)?, bound.is_inclusive) {
            IntegerEdge::Unbounded => {}
            IntegerEdge::At(max) => bounds.max = Some(max),
            IntegerEdge::Empty => return Ok(None),
        }
    }
    if let (Some(min), Some(max)) = (bounds.min, bounds.max)
        && min > max
    {
        return Ok(None);
    }
    Ok(Some(bounds))
}

/// A range bound as a double, per Java `Double.parseDouble`.
fn read_double_bound(bound: &RangeBound, field: &str, index: &str) -> Result<f64, OpenSearchError> {
    let text = render_literal_text(&bound.value, field)?;
    parse_java_double(&text).ok_or_else(|| {
        OpenSearchError::malformed_field_literal(field, index, &format!("cannot hold the bound [{text}]"))
    })
}

/// Whether `value` is the double `-0.0`.
const fn is_negative_zero(value: f64) -> bool {
    value.to_bits() == (-0.0_f64).to_bits()
}

/// Whether `value` is a finite integral double.
const fn is_integral(value: f64) -> bool {
    value.is_finite() && value.trunc().to_bits() == value.to_bits()
}

/// `value` as an `i64`; callers pass an integral double inside `[-2^63, 2^63)`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "callers pass an integral double inside [-2^63, 2^63), which converts exactly"
)]
const fn convert_integral_double(value: f64) -> i64 {
    value as i64
}

/// The first integer `n` with `n ≥ value` (`n > value` when exclusive).
fn find_integer_lower_edge(value: f64, is_inclusive: bool) -> IntegerEdge {
    if value.is_nan() || value >= TWO_TO_THE_63 {
        return IntegerEdge::Empty;
    }
    if is_negative_zero(value) {
        return IntegerEdge::At(0);
    }
    let first_integer = value.ceil();
    if first_integer < -TWO_TO_THE_63 {
        return IntegerEdge::Unbounded;
    }
    let first_integer = convert_integral_double(first_integer);
    if !is_inclusive && is_integral(value) {
        return first_integer.checked_add(1).map_or(IntegerEdge::Empty, IntegerEdge::At);
    }
    IntegerEdge::At(first_integer)
}

/// The last integer `n` with `n ≤ value` (`n < value` when exclusive).
fn find_integer_upper_edge(value: f64, is_inclusive: bool) -> IntegerEdge {
    if value.is_nan() || value < -TWO_TO_THE_63 {
        return IntegerEdge::Empty;
    }
    if is_negative_zero(value) {
        return IntegerEdge::At(-1);
    }
    let last_integer = value.floor();
    if last_integer >= TWO_TO_THE_63 {
        return IntegerEdge::Unbounded;
    }
    let last_integer = convert_integral_double(last_integer);
    if !is_inclusive && is_integral(value) {
        return last_integer.checked_sub(1).map_or(IntegerEdge::Empty, IntegerEdge::At);
    }
    IntegerEdge::At(last_integer)
}

/// The integer type of a column a range compares, so literals keep the column's type and the
/// scan can prune with them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntegerWidth {
    /// `Int32`.
    I32,
    /// `Int64`.
    I64,
}

/// Where an integer lies against the values a column width holds.
enum WidthPlacement {
    /// Below the width's smallest value.
    Below,
    /// One of the width's values, as a literal of the width.
    Within {
        /// The value as a literal of the width.
        literal: Expr,
        /// Whether it is the width's smallest value.
        is_width_min: bool,
        /// Whether it is the width's largest value.
        is_width_max: bool,
    },
    /// Above the width's largest value.
    Above,
}

/// Place `value` against `width`. The conversion to the width is the range check, so no value
/// outside the width ever becomes a literal.
fn place_in_width(value: i64, width: IntegerWidth) -> WidthPlacement {
    match width {
        IntegerWidth::I32 => match i32::try_from(value) {
            Ok(narrow_value) => WidthPlacement::Within {
                literal: lit(narrow_value),
                is_width_min: narrow_value == i32::MIN,
                is_width_max: narrow_value == i32::MAX,
            },
            Err(_) if value < 0 => WidthPlacement::Below,
            Err(_) => WidthPlacement::Above,
        },
        IntegerWidth::I64 => WidthPlacement::Within {
            literal: lit(value),
            is_width_min: value == i64::MIN,
            is_width_max: value == i64::MAX,
        },
    }
}

/// `column_value` inside `bounds` with literals of `width`; `None` when no value of that width
/// lies inside. A bound at or beyond the width's edge on its own side constrains nothing, and a
/// range with no constraint left selects the non-NULL values.
pub(crate) fn build_integer_range(column_value: Expr, bounds: IntegerBounds, width: IntegerWidth) -> Option<Expr> {
    let mut conjuncts: Vec<Expr> = Vec::with_capacity(2);
    if let Some(min) = bounds.min {
        match place_in_width(min, width) {
            WidthPlacement::Above => return None,
            WidthPlacement::Within {
                literal,
                is_width_min: false,
                ..
            } => conjuncts.push(column_value.clone().gt_eq(literal)),
            WidthPlacement::Within { .. } | WidthPlacement::Below => {}
        }
    }
    if let Some(max) = bounds.max {
        match place_in_width(max, width) {
            WidthPlacement::Below => return None,
            WidthPlacement::Within {
                literal,
                is_width_max: false,
                ..
            } => conjuncts.push(column_value.clone().lt_eq(literal)),
            WidthPlacement::Within { .. } | WidthPlacement::Above => {}
        }
    }
    Some(
        conjuncts
            .into_iter()
            .reduce(Expr::and)
            .unwrap_or_else(|| column_value.is_not_null()),
    )
}

/// The `500` for an array whose type the renderer did not expect.
fn report_unexpected_type(expected: &str, actual: &DataType) -> OpenSearchError {
    OpenSearchError::internal(format!("expected {expected} column, found {actual}"))
}

/// The string at `row`, or `None` when it is NULL. Accepts every Arrow string encoding a scan
/// may yield (`Utf8`, `LargeUtf8`, `Utf8View`).
pub(super) fn read_text(array: &ArrayRef, row: usize) -> Result<Option<&str>, OpenSearchError> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string_opt::<i32>().map(|strings| strings.value(row))),
        DataType::LargeUtf8 => Ok(array.as_string_opt::<i64>().map(|strings| strings.value(row))),
        DataType::Utf8View => Ok(array.as_string_view_opt().map(|strings| strings.value(row))),
        other => Err(report_unexpected_type("a string", other)),
    }
}

/// The 32-bit integer at `row`, or `None` when it is NULL.
fn read_i32(array: &ArrayRef, row: usize) -> Result<Option<i32>, OpenSearchError> {
    if array.is_null(row) {
        return Ok(None);
    }
    array
        .as_primitive_opt::<Int32Type>()
        .map(|values| Some(values.value(row)))
        .ok_or_else(|| report_unexpected_type("an Int32", array.data_type()))
}

/// The 64-bit integer at `row`, or `None` when it is NULL.
fn read_i64(array: &ArrayRef, row: usize) -> Result<Option<i64>, OpenSearchError> {
    if array.is_null(row) {
        return Ok(None);
    }
    array
        .as_primitive_opt::<Int64Type>()
        .map(|values| Some(values.value(row)))
        .ok_or_else(|| report_unexpected_type("an Int64", array.data_type()))
}

/// The microsecond timestamp at `row`, or `None` when it is NULL.
fn read_micros(array: &ArrayRef, row: usize) -> Result<Option<i64>, OpenSearchError> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_primitive_opt::<TimestampMicrosecondType>()
            .map(|values| Some(values.value(row)))
            .ok_or_else(|| report_unexpected_type("a microsecond timestamp", array.data_type())),
        other => Err(report_unexpected_type("a microsecond timestamp", other)),
    }
}

/// The fixed-width id at `row` as lowercase hex, or `None` when it is NULL.
fn read_hex(array: &ArrayRef, row: usize) -> Result<Option<String>, OpenSearchError> {
    if array.is_null(row) {
        return Ok(None);
    }
    array
        .as_fixed_size_binary_opt()
        .map(|ids| Some(hex::encode(ids.value(row))))
        .ok_or_else(|| report_unexpected_type("a fixed-size binary", array.data_type()))
}

/// The member `name` of `members`.
fn read_member<'s>(members: &'s StructArray, name: &str) -> Result<&'s ArrayRef, OpenSearchError> {
    members
        .column_by_name(name)
        .ok_or_else(|| OpenSearchError::internal(format!("a struct lacks its member `{name}`")))
}

/// The struct elements of a list of structs at `row`; `None` when the list is NULL.
fn read_struct_list(array: &ArrayRef, row: usize) -> Result<Option<StructArray>, OpenSearchError> {
    if array.is_null(row) {
        return Ok(None);
    }
    let list = array
        .as_list_opt::<i32>()
        .ok_or_else(|| report_unexpected_type("a list", array.data_type()))?;
    let elements = list.value(row);
    elements
        .as_struct_opt()
        .cloned()
        .map(Some)
        .ok_or_else(|| report_unexpected_type("a list of structs", elements.data_type()))
}

/// An attribute MAP at `row` as one object keyed by the `keys` spelling of each stored key; a
/// NULL map renders `{}`.
///
/// # Errors
///
/// Returns the 400 of [`escape_attribute_key`] for a stored key holding `@` under `Escaped`.
fn render_attribute_map(array: &ArrayRef, row: usize, keys: AttributeKeys) -> Result<Value, OpenSearchError> {
    let mut object = Map::new();
    if array.is_null(row) {
        return Ok(Value::Object(object));
    }
    let map = array
        .as_map_opt()
        .ok_or_else(|| report_unexpected_type("a map", array.data_type()))?;
    let entries = map.value(row);
    for entry in 0..entries.len() {
        let key = read_text(entries.column(0), entry)?
            .ok_or_else(|| OpenSearchError::internal("an attribute map holds a NULL key"))?;
        let value = read_text(entries.column(1), entry)?.map_or(Value::Null, |text| Value::String(text.to_string()));
        object.insert(keys.spell_key(key)?, value);
    }
    Ok(Value::Object(object))
}

/// The value of the stored `key` of the MAP at `row`; `Value::Null` when absent.
fn render_map_key(array: &ArrayRef, row: usize, key: &str) -> Result<Value, OpenSearchError> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    let map = array
        .as_map_opt()
        .ok_or_else(|| report_unexpected_type("a map", array.data_type()))?;
    let entries = map.value(row);
    for entry in 0..entries.len() {
        if read_text(entries.column(0), entry)? == Some(key) {
            return Ok(read_text(entries.column(1), entry)?.map_or(Value::Null, |text| Value::String(text.to_string())));
        }
    }
    Ok(Value::Null)
}

/// The events at `row`: `time`, `name` (always present), `attributes` (keys spelled per
/// `keys`), `droppedAttributesCount`; a NULL list renders `[]`.
fn render_events(array: &ArrayRef, row: usize, keys: AttributeKeys) -> Result<Value, OpenSearchError> {
    let Some(events) = read_struct_list(array, row)? else {
        return Ok(Value::Array(Vec::new()));
    };
    let times = read_member(&events, COL_TIMESTAMP)?;
    let names = read_member(&events, COL_NAME)?;
    let attributes = read_member(&events, COL_ATTRIBUTES)?;
    let dropped_counts = read_member(&events, COL_DROPPED_ATTRIBUTES_COUNT)?;
    let mut rendered_events = Vec::with_capacity(events.len());
    for element in 0..events.len() {
        let mut event = Map::new();
        if let Some(micros) = read_micros(times, element)? {
            event.insert("time".to_string(), Value::String(format_rfc3339_micros(micros)?));
        }
        event.insert(
            "name".to_string(),
            Value::String(read_text(names, element)?.unwrap_or_default().to_string()),
        );
        event.insert(
            "attributes".to_string(),
            render_attribute_map(attributes, element, keys)?,
        );
        if let Some(count) = read_i32(dropped_counts, element)? {
            event.insert("droppedAttributesCount".to_string(), Value::from(count));
        }
        rendered_events.push(Value::Object(event));
    }
    Ok(Value::Array(rendered_events))
}

/// The links at `row`: hex `traceId` and `spanId`, `traceState` (`""` for NULL), `attributes`
/// (keys spelled per `keys`), `droppedAttributesCount`; a NULL list renders `[]`.
fn render_links(array: &ArrayRef, row: usize, keys: AttributeKeys) -> Result<Value, OpenSearchError> {
    let Some(links) = read_struct_list(array, row)? else {
        return Ok(Value::Array(Vec::new()));
    };
    let trace_ids = read_member(&links, COL_TRACE_ID)?;
    let span_ids = read_member(&links, COL_SPAN_ID)?;
    let states = read_member(&links, COL_TRACE_STATE)?;
    let attributes = read_member(&links, COL_ATTRIBUTES)?;
    let dropped_counts = read_member(&links, COL_DROPPED_ATTRIBUTES_COUNT)?;
    let mut rendered_links = Vec::with_capacity(links.len());
    for element in 0..links.len() {
        let mut link = Map::new();
        if let Some(trace_id) = read_hex(trace_ids, element)? {
            link.insert("traceId".to_string(), Value::String(trace_id));
        }
        if let Some(span_id) = read_hex(span_ids, element)? {
            link.insert("spanId".to_string(), Value::String(span_id));
        }
        link.insert(
            "traceState".to_string(),
            Value::String(read_text(states, element)?.unwrap_or_default().to_string()),
        );
        link.insert(
            "attributes".to_string(),
            render_attribute_map(attributes, element, keys)?,
        );
        if let Some(count) = read_i32(dropped_counts, element)? {
            link.insert("droppedAttributesCount".to_string(), Value::from(count));
        }
        rendered_links.push(Value::Object(link));
    }
    Ok(Value::Array(rendered_links))
}

/// The client-visible value of an encoded field at `row` of its stored column `array`;
/// `Value::Null` when the field is absent from the row. Events and links render with the
/// escaped key spelling; a table with another spelling renders them through its own
/// [`AttributeKeys`].
///
/// # Errors
///
/// Returns `500` for a column of another type, a stored span kind outside the `OTel` kinds, a
/// stored metric type or aggregation temporality with no name, and exponential buckets, which
/// render from their whole row (see [`render_profile_document`]); and the 400 of
/// [`escape_attribute_key`] for an event or link attribute key holding `@`.
pub(crate) fn render_encoded_value(
    field: &EncodedField,
    array: &ArrayRef,
    row: usize,
) -> Result<Value, OpenSearchError> {
    match field {
        EncodedField::HexIdOrEmpty { .. } => Ok(Value::String(read_hex(array, row)?.unwrap_or_default())),
        EncodedField::EmptyWhenNull { .. } => Ok(Value::String(read_text(array, row)?.unwrap_or_default().to_string())),
        EncodedField::SpanKindName { .. } => {
            let kind = read_i32(array, row)?.unwrap_or(0);
            SPAN_KINDS
                .iter()
                .find(|(stored_kind, _)| *stored_kind == kind)
                .map(|(_, name)| Value::String((*name).to_string()))
                .ok_or_else(|| OpenSearchError::internal(format!("stored span kind {kind} is not an `OTel` span kind")))
        }
        EncodedField::Rfc3339Time { .. } => Ok(read_micros(array, row)?
            .map(format_rfc3339_micros)
            .transpose()?
            .map_or(Value::Null, Value::String)),
        EncodedField::StatusCodeOrZero { .. } => Ok(Value::from(read_i32(array, row)?.unwrap_or(0))),
        EncodedField::EscapedMapKey { key, .. } => render_map_key(array, row, key),
        EncodedField::Events => render_events(array, row, AttributeKeys::Escaped),
        EncodedField::Links => render_links(array, row, AttributeKeys::Escaped),
        EncodedField::MetricKindName => match read_text(array, row)? {
            None | Some(METRIC_TYPE_SUMMARY) => Ok(Value::Null),
            Some(stored_type) => find_enum_name(&METRIC_KINDS, stored_type)
                .map(|name| Value::String(name.to_string()))
                .ok_or_else(|| {
                    OpenSearchError::internal(format!("stored metric type [{stored_type}] has no kind name"))
                }),
        },
        EncodedField::TemporalityName => read_text(array, row)?.map_or(Ok(Value::Null), |stored_temporality| {
            find_enum_name(&TEMPORALITIES, stored_temporality)
                .map(|name| Value::String(name.to_string()))
                .ok_or_else(|| {
                    OpenSearchError::internal(format!(
                        "stored aggregation temporality [{stored_temporality}] has no name"
                    ))
                })
        }),
        EncodedField::QuantileValues => render_quantiles(array, row),
        EncodedField::Exemplars => render_exemplars(array, row),
        EncodedField::ExponentialBuckets { .. } => Err(OpenSearchError::internal(
            "exponential buckets render from their row: they read the scale and the offset too",
        )),
    }
}

/// The value of a derived field at `row` of `batch`.
///
/// # Errors
///
/// Returns `500` when a source column is missing or of another type, or the duration product
/// overflows.
pub(crate) fn render_derived_value(
    field: DerivedField,
    batch: &RecordBatch,
    row: usize,
) -> Result<Value, OpenSearchError> {
    match field {
        DerivedField::DurationInNanos => {
            let micros_column = read_batch_column(batch, COL_DURATION_MICROS)?;
            read_i64(micros_column, row)?
                .map(|micros| {
                    micros.checked_mul(NANOS_PER_MICRO).map(Value::from).ok_or_else(|| {
                        OpenSearchError::internal(format!("duration {micros} microseconds overflows nanoseconds"))
                    })
                })
                .transpose()
                .map(|value| value.unwrap_or(Value::Null))
        }
        DerivedField::ArrayLength { column, .. } => {
            let lists = read_batch_column(batch, column)?;
            if lists.is_null(row) {
                return Ok(Value::Null);
            }
            let length = lists
                .as_list_opt::<i32>()
                .ok_or_else(|| report_unexpected_type("a list", lists.data_type()))?
                .value_length(row);
            u64::try_from(length)
                .map(Value::from)
                .map_err(|_| OpenSearchError::internal(format!("the list `{column}` has length {length}")))
        }
        DerivedField::MetricValue => {
            let doubles = read_batch_column(batch, COL_VALUE_DOUBLE)?;
            if !doubles.is_null(row) {
                return render_array_value(doubles.as_ref(), row);
            }
            Ok(read_i64(read_batch_column(batch, COL_VALUE_INT)?, row)?
                .map_or(Value::Null, |integer| Value::from(convert_integer_to_double(integer))))
        }
    }
}

/// `value` as the nearest double, as `cast(value_int as double)` computes it.
#[expect(
    clippy::cast_precision_loss,
    reason = "the metrics value is the double nearest to an integer point, as its read expression casts it"
)]
const fn convert_integer_to_double(value: i64) -> f64 {
    value as f64
}

/// A finite double as a JSON number.
///
/// # Errors
///
/// Returns `500` for a `NaN` or an infinity, which no computed bucket bound reaches at a scale and
/// offset ingest stores.
fn render_finite_double(value: f64) -> Result<Value, OpenSearchError> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| OpenSearchError::internal(format!("the double {value} has no JSON rendering")))
}

/// The bounds `(min, max)` of exponential-histogram bucket `index` at `scale` and `offset`:
/// bucket `k` covers `(base^(offset+k), base^(offset+k+1)]` with `base = 2^(2^-scale)`, and a
/// negative bucket is its mirror image. `None` when `index` or the exponent `offset + index + 1`
/// does not fit `i32`.
pub(crate) fn compute_exponential_bucket_bounds(
    scale: i32,
    offset: i32,
    index: usize,
    is_negative: bool,
) -> Option<(f64, f64)> {
    let first_exponent = offset.checked_add(i32::try_from(index).ok()?)?;
    let last_exponent = first_exponent.checked_add(1)?;
    // base^i = 2^(i · 2^-scale).
    let exponent_per_index = f64::exp2(f64::from(scale.checked_neg()?));
    let lower_bound = f64::exp2(f64::from(first_exponent) * exponent_per_index);
    let upper_bound = f64::exp2(f64::from(last_exponent) * exponent_per_index);
    Some(if is_negative {
        (-upper_bound, -lower_bound)
    } else {
        (lower_bound, upper_bound)
    })
}

/// The exponential-histogram buckets at `row`: one `{count, min, max}` per stored count, in
/// stored order, a NULL count absent from its bucket as [`render_quantiles`] leaves out a NULL
/// member; `Value::Null` when the counts are NULL.
///
/// # Errors
///
/// Returns `500` when the row holds counts but no scale or offset, or the bounds overflow.
fn render_exponential_buckets(
    batch: &RecordBatch,
    row: usize,
    counts_column: &str,
    offset_column: &str,
    is_negative: bool,
) -> Result<Value, OpenSearchError> {
    let counts = read_batch_column(batch, counts_column)?;
    if counts.is_null(row) {
        return Ok(Value::Null);
    }
    let scale = read_i32(read_batch_column(batch, COL_SCALE)?, row)?
        .ok_or_else(|| OpenSearchError::internal("an exponential histogram row has no scale"))?;
    let offset = read_i32(read_batch_column(batch, offset_column)?, row)?
        .ok_or_else(|| OpenSearchError::internal(format!("an exponential histogram row has no `{offset_column}`")))?;
    let elements = counts
        .as_list_opt::<i32>()
        .ok_or_else(|| report_unexpected_type("a list", counts.data_type()))?
        .value(row);
    let mut rendered_buckets = Vec::with_capacity(elements.len());
    for bucket_index in 0..elements.len() {
        let (min, max) = compute_exponential_bucket_bounds(scale, offset, bucket_index, is_negative)
            .ok_or_else(|| OpenSearchError::internal("exponential bucket bounds overflow"))?;
        let mut bucket = Map::new();
        let count = render_array_value(elements.as_ref(), bucket_index)?;
        if !count.is_null() {
            bucket.insert("count".to_string(), count);
        }
        bucket.insert("min".to_string(), render_finite_double(min)?);
        bucket.insert("max".to_string(), render_finite_double(max)?);
        rendered_buckets.push(Value::Object(bucket));
    }
    Ok(Value::Array(rendered_buckets))
}

/// The summary quantiles at `row` as `{quantile, value}` objects, a NULL member absent;
/// `Value::Null` for a NULL list.
fn render_quantiles(array: &ArrayRef, row: usize) -> Result<Value, OpenSearchError> {
    let Some(entries) = read_struct_list(array, row)? else {
        return Ok(Value::Null);
    };
    let quantiles = read_member(&entries, QUANTILE_FIELD_QUANTILE)?;
    let values = read_member(&entries, QUANTILE_FIELD_VALUE)?;
    let mut rendered_quantiles = Vec::with_capacity(entries.len());
    for quantile_index in 0..entries.len() {
        let mut object = Map::new();
        for (name, members) in [("quantile", quantiles), ("value", values)] {
            let member_value = render_array_value(members.as_ref(), quantile_index)?;
            if !member_value.is_null() {
                object.insert(name.to_string(), member_value);
            }
        }
        rendered_quantiles.push(Value::Object(object));
    }
    Ok(Value::Array(rendered_quantiles))
}

/// The exemplars at `row` as `{time, traceId, spanId}` objects, each member absent when NULL;
/// `Value::Null` for a NULL list. The exemplar struct names its members like the columns they
/// mirror.
fn render_exemplars(array: &ArrayRef, row: usize) -> Result<Value, OpenSearchError> {
    let Some(exemplars) = read_struct_list(array, row)? else {
        return Ok(Value::Null);
    };
    let times = read_member(&exemplars, COL_TIMESTAMP)?;
    let trace_ids = read_member(&exemplars, COL_TRACE_ID)?;
    let span_ids = read_member(&exemplars, COL_SPAN_ID)?;
    let mut rendered_exemplars = Vec::with_capacity(exemplars.len());
    for exemplar_index in 0..exemplars.len() {
        let mut exemplar = Map::new();
        if let Some(micros) = read_micros(times, exemplar_index)? {
            exemplar.insert("time".to_string(), Value::String(format_rfc3339_micros(micros)?));
        }
        if let Some(trace_id) = read_hex(trace_ids, exemplar_index)? {
            exemplar.insert("traceId".to_string(), Value::String(trace_id));
        }
        if let Some(span_id) = read_hex(span_ids, exemplar_index)? {
            exemplar.insert("spanId".to_string(), Value::String(span_id));
        }
        rendered_exemplars.push(Value::Object(exemplar));
    }
    Ok(Value::Array(rendered_exemplars))
}

/// What one field of a non-native profile reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProfileFieldSource {
    /// A stored column under its own value.
    Column(&'static str),
    /// Another name of the declared field it names: it filters, sorts and aggregates like that
    /// field and is not repeated in `_source`.
    Alias(&'static str),
    /// A fixed-width id as hex, NULL as `""`.
    HexIdOrEmpty(&'static str),
    /// A fixed-width id as hex, absent when NULL.
    StoredHexId(&'static str),
    /// A string whose NULL renders `""`.
    EmptyWhenNull(&'static str),
    /// The span kind as its `OTel` name.
    SpanKind(&'static str),
    /// A microsecond timestamp as RFC 3339 text.
    Time(&'static str),
    /// `duration_micros × 1000`.
    DurationInNanos,
    /// A status code, NULL as `0`.
    StatusCode(&'static str),
    /// The metric type as its `kind` name.
    MetricKind,
    /// The aggregation temporality as its `aggregationTemporality` name.
    Temporality,
    /// A metric point's value, whether stored as an integer or a double.
    MetricValue,
    /// The length of a list column.
    ArrayLength(&'static str),
    /// Exponential-histogram buckets.
    ExponentialBuckets {
        /// The stored bucket counts.
        counts: &'static str,
        /// The stored bucket offset.
        offset: &'static str,
        /// Whether the buckets lie below zero.
        is_negative: bool,
    },
    /// Summary quantiles.
    Quantiles,
    /// Exemplars.
    Exemplars,
    /// A structural object grouping the fields below it.
    Object,
    /// An attribute MAP: each stored key is a `keyword` leaf below this object.
    Attributes(&'static str),
    /// The span's events.
    Events,
    /// The span's links.
    Links,
    /// A value read from the root span of the row's trace.
    CrossRow(CrossRowField),
}

/// The fields of one non-native profile index, in `_mapping` order, and how it spells attribute
/// keys. A dotted field name sits under the structural object its prefix names. The fields come
/// in segments so that two profiles of one table share the segment they have in common.
#[derive(Debug)]
pub(crate) struct ProfileTable {
    /// The table the profile index reads; its promoted resource keys resolve to their columns.
    pub(crate) table: &'static str,
    /// Every field, by its client name, segment after segment.
    pub(crate) fields: &'static [&'static [(&'static str, ProfileFieldSource)]],
    /// How the attribute objects spell stored keys.
    pub(crate) attribute_keys: AttributeKeys,
}

impl ProfileTable {
    /// Every declared field, in order.
    pub(crate) fn iter_fields(&self) -> impl Iterator<Item = &(&'static str, ProfileFieldSource)> {
        self.fields.iter().flat_map(|segment| segment.iter())
    }

    /// The declared field `name`, under its `'static` name; `None` for a name the table does not
    /// declare.
    fn find_field(&self, name: &str) -> Option<(&'static str, ProfileFieldSource)> {
        self.iter_fields().find(|(field, _)| *field == name).copied()
    }

    /// The source the declared field `name` reads through: an alias's target source, any other
    /// field's own. `None` for an undeclared name, and for an alias whose target is undeclared or
    /// another alias, so an alias never chains.
    fn find_read_source(&self, name: &str) -> Option<ProfileFieldSource> {
        match self.find_field(name)?.1 {
            ProfileFieldSource::Alias(target) => self
                .find_field(target)
                .map(|(_, target_source)| target_source)
                .filter(|target_source| !matches!(target_source, ProfileFieldSource::Alias(_))),
            source => Some(source),
        }
    }
}

/// The structural object a dotted field name sits under.
fn find_parent(name: &str) -> Option<String> {
    name.rsplit_once('.').map(|(parent, _)| parent.to_string())
}

/// The stored column a source that shows one column under a conversion reads (see
/// [`map_column_encoding`]); `None` for every other source.
const fn find_encoded_column(source: ProfileFieldSource) -> Option<&'static str> {
    match source {
        ProfileFieldSource::HexIdOrEmpty(column)
        | ProfileFieldSource::EmptyWhenNull(column)
        | ProfileFieldSource::SpanKind(column)
        | ProfileFieldSource::Time(column)
        | ProfileFieldSource::StatusCode(column)
        | ProfileFieldSource::ExponentialBuckets { counts: column, .. } => Some(column),
        ProfileFieldSource::MetricKind => Some(COL_METRIC_TYPE),
        ProfileFieldSource::Temporality => Some(COL_AGGREGATION_TEMPORALITY),
        ProfileFieldSource::Quantiles => Some(COL_QUANTILE_VALUES),
        ProfileFieldSource::Exemplars => Some(COL_EXEMPLARS),
        ProfileFieldSource::Column(_)
        | ProfileFieldSource::Alias(_)
        | ProfileFieldSource::StoredHexId(_)
        | ProfileFieldSource::DurationInNanos
        | ProfileFieldSource::MetricValue
        | ProfileFieldSource::ArrayLength(_)
        | ProfileFieldSource::Object
        | ProfileFieldSource::Attributes(_)
        | ProfileFieldSource::Events
        | ProfileFieldSource::Links
        | ProfileFieldSource::CrossRow(_) => None,
    }
}

/// The client type and encoding of a source that shows one stored column under a conversion,
/// given that column's stored type: the one mapping from such a source to its [`EncodedField`],
/// which resolution and rendering share. `None` for every other source, and for an id column
/// that is no fixed-size binary.
const fn map_column_encoding(source: ProfileFieldSource, column_type: &DataType) -> Option<(FieldType, EncodedField)> {
    match source {
        ProfileFieldSource::HexIdOrEmpty(column) => {
            let DataType::FixedSizeBinary(width) = column_type else {
                return None;
            };
            Some((FieldType::Keyword, EncodedField::HexIdOrEmpty { column, width: *width }))
        }
        ProfileFieldSource::EmptyWhenNull(column) => Some((FieldType::Keyword, EncodedField::EmptyWhenNull { column })),
        ProfileFieldSource::SpanKind(column) => Some((FieldType::Keyword, EncodedField::SpanKindName { column })),
        ProfileFieldSource::Time(column) => Some((FieldType::Date, EncodedField::Rfc3339Time { column })),
        ProfileFieldSource::StatusCode(column) => Some((FieldType::Integer, EncodedField::StatusCodeOrZero { column })),
        ProfileFieldSource::MetricKind => Some((FieldType::Keyword, EncodedField::MetricKindName)),
        ProfileFieldSource::Temporality => Some((FieldType::Keyword, EncodedField::TemporalityName)),
        ProfileFieldSource::ExponentialBuckets { counts, .. } => Some((
            FieldType::Object,
            EncodedField::ExponentialBuckets { counts_column: counts },
        )),
        ProfileFieldSource::Quantiles => Some((FieldType::Object, EncodedField::QuantileValues)),
        ProfileFieldSource::Exemplars => Some((FieldType::Object, EncodedField::Exemplars)),
        ProfileFieldSource::Column(_)
        | ProfileFieldSource::Alias(_)
        | ProfileFieldSource::StoredHexId(_)
        | ProfileFieldSource::DurationInNanos
        | ProfileFieldSource::MetricValue
        | ProfileFieldSource::ArrayLength(_)
        | ProfileFieldSource::Object
        | ProfileFieldSource::Attributes(_)
        | ProfileFieldSource::Events
        | ProfileFieldSource::Links
        | ProfileFieldSource::CrossRow(_) => None,
    }
}

/// The derived field a source computes, for the declared field `name`: the one mapping from a
/// source to its [`DerivedField`], which resolution and rendering share. `None` for every other
/// source.
const fn map_derived_field(name: &'static str, source: ProfileFieldSource) -> Option<(FieldType, DerivedField)> {
    match source {
        ProfileFieldSource::DurationInNanos => Some((FieldType::Long, DerivedField::DurationInNanos)),
        ProfileFieldSource::MetricValue => Some((FieldType::Double, DerivedField::MetricValue)),
        ProfileFieldSource::ArrayLength(column) => {
            Some((FieldType::Long, DerivedField::ArrayLength { column, field: name }))
        }
        ProfileFieldSource::Column(_)
        | ProfileFieldSource::Alias(_)
        | ProfileFieldSource::HexIdOrEmpty(_)
        | ProfileFieldSource::StoredHexId(_)
        | ProfileFieldSource::EmptyWhenNull(_)
        | ProfileFieldSource::SpanKind(_)
        | ProfileFieldSource::Time(_)
        | ProfileFieldSource::StatusCode(_)
        | ProfileFieldSource::MetricKind
        | ProfileFieldSource::Temporality
        | ProfileFieldSource::ExponentialBuckets { .. }
        | ProfileFieldSource::Quantiles
        | ProfileFieldSource::Exemplars
        | ProfileFieldSource::Object
        | ProfileFieldSource::Attributes(_)
        | ProfileFieldSource::Events
        | ProfileFieldSource::Links
        | ProfileFieldSource::CrossRow(_) => None,
    }
}

/// The resolution of the declared field `name`, which reads `source` (an alias's target source,
/// see [`ProfileTable::find_read_source`]), against its table's schema; `None` for a structural
/// object or an attribute object, which no leaf filters on, and for a column the schema lacks.
fn resolve_profile_source(schema: &Schema, name: &'static str, source: ProfileFieldSource) -> Option<ResolvedField> {
    let (field_type, value) = match source {
        ProfileFieldSource::Column(column) => (
            native_column_type(schema.field_with_name(column).ok()?).ok()?,
            FieldValue::Column {
                column: column.to_string(),
            },
        ),
        ProfileFieldSource::StoredHexId(column) => {
            let DataType::FixedSizeBinary(width) = schema.field_with_name(column).ok()?.data_type() else {
                return None;
            };
            (
                FieldType::Keyword,
                FieldValue::HexId {
                    column: column.to_string(),
                    width: *width,
                },
            )
        }
        ProfileFieldSource::HexIdOrEmpty(_)
        | ProfileFieldSource::EmptyWhenNull(_)
        | ProfileFieldSource::SpanKind(_)
        | ProfileFieldSource::Time(_)
        | ProfileFieldSource::StatusCode(_)
        | ProfileFieldSource::MetricKind
        | ProfileFieldSource::Temporality
        | ProfileFieldSource::ExponentialBuckets { .. }
        | ProfileFieldSource::Quantiles
        | ProfileFieldSource::Exemplars => {
            let column_type = schema.field_with_name(find_encoded_column(source)?).ok()?.data_type();
            let (field_type, encoded_field) = map_column_encoding(source, column_type)?;
            (field_type, FieldValue::Encoded(encoded_field))
        }
        ProfileFieldSource::DurationInNanos | ProfileFieldSource::MetricValue | ProfileFieldSource::ArrayLength(_) => {
            let (field_type, derived_field) = map_derived_field(name, source)?;
            (field_type, FieldValue::Derived(derived_field))
        }
        ProfileFieldSource::Events => (FieldType::Object, FieldValue::Encoded(EncodedField::Events)),
        ProfileFieldSource::Links => (FieldType::Object, FieldValue::Encoded(EncodedField::Links)),
        ProfileFieldSource::CrossRow(field) => (field.field_type(), FieldValue::CrossRow(field)),
        ProfileFieldSource::Alias(_) | ProfileFieldSource::Object | ProfileFieldSource::Attributes(_) => return None,
    };
    Some(ResolvedField {
        name: name.to_string(),
        field_type,
        value,
    })
}

/// The resolution of the declared field `name` of `table`, an alias as its target.
fn resolve_declared_field(table: &ProfileTable, schema: &Schema, name: &'static str) -> Option<ResolvedField> {
    resolve_profile_source(schema, name, table.find_read_source(name)?)
}

/// The resolution of the attribute path `name`, whose key part `spelled_key` names a stored key
/// of the MAP `column` in `table`'s spelling. A resource key `table` promotes to a column (dotted
/// or underscored, the way ingest moves it) reads that column.
fn resolve_attribute_key(
    table: &ProfileTable,
    schema: &Schema,
    column: &'static str,
    name: &str,
    spelled_key: &str,
) -> Option<ResolvedField> {
    let key = table.attribute_keys.read_stored_key(spelled_key);
    if column == COL_RESOURCE_ATTRIBUTES
        && let Some(promoted) = find_promoted_resource_key(table.table, &key)
    {
        return Some(ResolvedField {
            name: name.to_string(),
            field_type: native_column_type(schema.field_with_name(promoted.column).ok()?).ok()?,
            value: FieldValue::Column {
                column: promoted.column.to_string(),
            },
        });
    }
    let value = match table.attribute_keys {
        AttributeKeys::Escaped => FieldValue::Encoded(EncodedField::EscapedMapKey { column, key }),
        AttributeKeys::Stored => FieldValue::MapKey {
            column: column.to_string(),
            key,
        },
    };
    Some(ResolvedField {
        name: name.to_string(),
        field_type: FieldType::Keyword,
        value,
    })
}

/// Resolve `name` against `table` over its table's `schema`. `<attributes object>.<key>`
/// reads the stored key the spelling names, with no enumeration; a path below a render-only
/// field resolves to that field under the requested path, so a leaf on it is refused naming the
/// path. An alias resolves as its target under its own name. A structural object and an unknown
/// name resolve to nothing, so a leaf on either matches nothing.
pub(crate) fn resolve_profile_field(table: &ProfileTable, schema: &Schema, name: &str) -> Option<ResolvedField> {
    if let Some((declared_name, _)) = table.find_field(name) {
        return resolve_declared_field(table, schema, declared_name);
    }
    table.iter_fields().find_map(|(prefix, source)| {
        let sub_path = name.strip_prefix(prefix)?.strip_prefix('.')?;
        match source {
            ProfileFieldSource::Attributes(column) => {
                if sub_path.is_empty() {
                    None
                } else {
                    resolve_attribute_key(table, schema, column, name, sub_path)
                }
            }
            // The render-only fields: a leaf on a path below one is refused like a leaf on it.
            ProfileFieldSource::ExponentialBuckets { .. }
            | ProfileFieldSource::Quantiles
            | ProfileFieldSource::Exemplars => {
                resolve_profile_source(schema, prefix, *source).map(|resolved_field| ResolvedField {
                    name: name.to_string(),
                    ..resolved_field
                })
            }
            ProfileFieldSource::Column(_)
            | ProfileFieldSource::Alias(_)
            | ProfileFieldSource::HexIdOrEmpty(_)
            | ProfileFieldSource::StoredHexId(_)
            | ProfileFieldSource::EmptyWhenNull(_)
            | ProfileFieldSource::SpanKind(_)
            | ProfileFieldSource::Time(_)
            | ProfileFieldSource::DurationInNanos
            | ProfileFieldSource::StatusCode(_)
            | ProfileFieldSource::MetricKind
            | ProfileFieldSource::Temporality
            | ProfileFieldSource::MetricValue
            | ProfileFieldSource::ArrayLength(_)
            | ProfileFieldSource::Object
            | ProfileFieldSource::Events
            | ProfileFieldSource::Links
            | ProfileFieldSource::CrossRow(_) => None,
        }
    })
}

/// A field `_field_caps` reports but no query reaches.
fn build_unsearchable_leaf(name: String, field_type: FieldType) -> IndexField {
    IndexField {
        parent: find_parent(&name),
        name,
        field_type,
        is_searchable: false,
        is_aggregatable: false,
    }
}

/// Whether `value` is an encoded id whose stored column admits NULL in `schema`. Such a field
/// renders a missing id as `""`, which sorts before every id and is a bucket of its own; an order
/// or a bucket over the stored bytes places it elsewhere, so no sort or aggregation reads it.
pub(crate) fn is_nullable_encoded_id(schema: &Schema, value: &FieldValue) -> bool {
    let FieldValue::Encoded(EncodedField::HexIdOrEmpty { column, .. }) = value else {
        return false;
    };
    schema.field_with_name(column).is_ok_and(Field::is_nullable)
}

/// The fields of `table` over its table's `schema`, in declared order. Each attribute object
/// lists one `keyword` leaf per key of `map_keys` under its MAP column, in the table's
/// spelling. Events list `events.time` (date) and `events.name`, neither searchable nor
/// aggregatable. A nullable encoded id (see [`is_nullable_encoded_id`]) is searchable but not
/// aggregatable. An alias lists with its target's type; a render-only field is an `object`.
///
/// # Errors
///
/// Returns `500` naming a declared column the schema lacks, and the 400 of
/// [`escape_attribute_key`] for a listed key the spelling cannot write.
pub(crate) fn list_profile_fields(
    table: &ProfileTable,
    schema: &Schema,
    map_keys: &MapKeys,
) -> Result<Vec<IndexField>, OpenSearchError> {
    let mut fields = Vec::new();
    for (name, source) in table.iter_fields() {
        let parent = find_parent(name);
        match source {
            ProfileFieldSource::Object | ProfileFieldSource::Links => fields.push(IndexField {
                parent,
                ..IndexField::object((*name).to_string())
            }),
            ProfileFieldSource::Events => {
                fields.push(IndexField {
                    parent,
                    ..IndexField::object((*name).to_string())
                });
                fields.push(build_unsearchable_leaf(format!("{name}.time"), FieldType::Date));
                fields.push(build_unsearchable_leaf(format!("{name}.name"), FieldType::Keyword));
            }
            ProfileFieldSource::Attributes(column) => {
                fields.push(IndexField {
                    parent,
                    ..IndexField::object((*name).to_string())
                });
                for key in map_keys.get(*column).into_iter().flatten() {
                    fields.push(IndexField::leaf(
                        format!("{name}.{}", table.attribute_keys.spell_key(key)?),
                        FieldType::Keyword,
                        Some((*name).to_string()),
                    ));
                }
            }
            ProfileFieldSource::Column(_)
            | ProfileFieldSource::Alias(_)
            | ProfileFieldSource::HexIdOrEmpty(_)
            | ProfileFieldSource::StoredHexId(_)
            | ProfileFieldSource::EmptyWhenNull(_)
            | ProfileFieldSource::SpanKind(_)
            | ProfileFieldSource::Time(_)
            | ProfileFieldSource::DurationInNanos
            | ProfileFieldSource::StatusCode(_)
            | ProfileFieldSource::MetricKind
            | ProfileFieldSource::Temporality
            | ProfileFieldSource::MetricValue
            | ProfileFieldSource::ArrayLength(_)
            | ProfileFieldSource::ExponentialBuckets { .. }
            | ProfileFieldSource::Quantiles
            | ProfileFieldSource::Exemplars
            | ProfileFieldSource::CrossRow(_) => {
                let resolved_field = resolve_declared_field(table, schema, name).ok_or_else(|| {
                    OpenSearchError::internal(format!("the table lacks the column behind profile field [{name}]"))
                })?;
                if resolved_field.field_type == FieldType::Object {
                    fields.push(IndexField {
                        parent,
                        ..IndexField::object((*name).to_string())
                    });
                    continue;
                }
                let leaf = IndexField::leaf((*name).to_string(), resolved_field.field_type, parent);
                fields.push(IndexField {
                    is_aggregatable: leaf.is_aggregatable && !is_nullable_encoded_id(schema, &resolved_field.value),
                    ..leaf
                });
            }
        }
    }
    Ok(fields)
}

/// The fields a full-text pattern reaches on `table`: the leaves whose names it matches, and each
/// attribute MAP whose every key path it covers, selected whole, as a native MAP column is. A
/// pattern covering the resource attributes also reaches the columns `table` promotes resource
/// keys to, as a native index's pattern does. Objects, lists and render-only fields are never
/// reached, and neither is a cross-row field, which only a search of its own index reads.
///
/// # Errors
///
/// Returns `400 illegal_argument_exception` for a pattern that would select only some keys of an
/// attribute MAP: answering it needs key enumeration, which resolution never depends on.
pub(crate) fn expand_profile_pattern(
    table: &ProfileTable,
    schema: &Schema,
    pattern: &str,
) -> Result<Vec<SelectedField>, OpenSearchError> {
    let mut selected_fields = Vec::new();
    let mut promoted_fields = Vec::new();
    for (name, source) in table.iter_fields() {
        match source {
            ProfileFieldSource::Object
            | ProfileFieldSource::Events
            | ProfileFieldSource::Links
            | ProfileFieldSource::ExponentialBuckets { .. }
            | ProfileFieldSource::Quantiles
            | ProfileFieldSource::Exemplars
            | ProfileFieldSource::CrossRow(_) => {}
            ProfileFieldSource::Attributes(column) => {
                if covers_every_key(pattern, name) {
                    selected_fields.push(SelectedField::AllMapValues {
                        column: (*column).to_string(),
                    });
                    if *column == COL_RESOURCE_ATTRIBUTES {
                        promoted_fields.extend(list_promoted_resource_fields(table, schema, name)?);
                    }
                } else if glob_matches_some_extension(pattern, &format!("{name}.")) {
                    return Err(OpenSearchError::illegal_argument(format!(
                        "field pattern [{pattern}] selects some keys of [{name}]: name one key, or select every key \
                         with [{name}.*]"
                    )));
                }
            }
            ProfileFieldSource::Column(_)
            | ProfileFieldSource::Alias(_)
            | ProfileFieldSource::HexIdOrEmpty(_)
            | ProfileFieldSource::StoredHexId(_)
            | ProfileFieldSource::EmptyWhenNull(_)
            | ProfileFieldSource::SpanKind(_)
            | ProfileFieldSource::Time(_)
            | ProfileFieldSource::DurationInNanos
            | ProfileFieldSource::StatusCode(_)
            | ProfileFieldSource::MetricKind
            | ProfileFieldSource::Temporality
            | ProfileFieldSource::MetricValue
            | ProfileFieldSource::ArrayLength(_) => {
                if matches_glob(pattern, name)
                    && let Some(resolved_field) = resolve_declared_field(table, schema, name)
                {
                    selected_fields.push(SelectedField::Field(resolved_field));
                }
            }
        }
    }
    for promoted_field in promoted_fields {
        let is_selected = selected_fields
            .iter()
            .any(|candidate| matches!(candidate, SelectedField::Field(field) if field.value == promoted_field.value));
        if !is_selected {
            selected_fields.push(SelectedField::Field(promoted_field));
        }
    }
    Ok(selected_fields)
}

/// The columns `table` promotes resource keys to, each resolved under its attribute path below
/// the resource attribute object `object_name`.
///
/// # Errors
///
/// Returns the 400 of [`escape_attribute_key`] for a promoted key the spelling cannot write.
fn list_promoted_resource_fields(
    table: &ProfileTable,
    schema: &Schema,
    object_name: &str,
) -> Result<Vec<ResolvedField>, OpenSearchError> {
    let mut promoted_fields = Vec::new();
    for promoted_key in promoted_resource_keys(table.table) {
        let spelled_key = table.attribute_keys.spell_key(promoted_key.attribute_key)?;
        if let Some(resolved_field) = resolve_attribute_key(
            table,
            schema,
            COL_RESOURCE_ATTRIBUTES,
            &format!("{object_name}.{spelled_key}"),
            &spelled_key,
        ) {
            promoted_fields.push(resolved_field);
        }
    }
    Ok(promoted_fields)
}

/// The column `column` of `batch`.
fn read_batch_column<'b>(batch: &'b RecordBatch, column: &str) -> Result<&'b ArrayRef, OpenSearchError> {
    batch
        .column_by_name(column)
        .ok_or_else(|| OpenSearchError::internal(format!("a row lacks the column `{column}`")))
}

/// The row being rendered: the row `row` of `batch`, read under the fields of `table`.
#[derive(Debug, Clone, Copy)]
struct ProfileRow<'r> {
    /// The profile the row is rendered under.
    table: &'r ProfileTable,
    /// The batch holding the row.
    batch: &'r RecordBatch,
    /// The row's index in `batch`.
    row: usize,
}

/// The value in `profile_row` of the declared field `name`, which reads `source`; `Value::Null`
/// when absent, and for an alias, whose target renders instead. A cross-row field reads the root
/// of the row's trace from `trace_roots`.
fn render_source_field(
    profile_row: ProfileRow<'_>,
    name: &'static str,
    source: ProfileFieldSource,
    trace_roots: &TraceRoots,
) -> Result<Value, OpenSearchError> {
    let ProfileRow { table, batch, row } = profile_row;
    match source {
        ProfileFieldSource::Column(column) | ProfileFieldSource::StoredHexId(column) => {
            render_array_value(read_batch_column(batch, column)?.as_ref(), row)
        }
        ProfileFieldSource::HexIdOrEmpty(_)
        | ProfileFieldSource::EmptyWhenNull(_)
        | ProfileFieldSource::SpanKind(_)
        | ProfileFieldSource::Time(_)
        | ProfileFieldSource::StatusCode(_)
        | ProfileFieldSource::MetricKind
        | ProfileFieldSource::Temporality
        | ProfileFieldSource::Quantiles
        | ProfileFieldSource::Exemplars => {
            let column = find_encoded_column(source)
                .ok_or_else(|| OpenSearchError::internal(format!("profile field [{name}] reads no column")))?;
            let array = read_batch_column(batch, column)?;
            let (_, encoded_field) = map_column_encoding(source, array.data_type())
                .ok_or_else(|| report_unexpected_type("the type its encoding reads", array.data_type()))?;
            render_encoded_value(&encoded_field, array, row)
        }
        ProfileFieldSource::ExponentialBuckets {
            counts,
            offset,
            is_negative,
        } => render_exponential_buckets(batch, row, counts, offset, is_negative),
        ProfileFieldSource::DurationInNanos | ProfileFieldSource::MetricValue | ProfileFieldSource::ArrayLength(_) => {
            let (_, derived_field) = map_derived_field(name, source)
                .ok_or_else(|| OpenSearchError::internal(format!("profile field [{name}] derives no value")))?;
            render_derived_value(derived_field, batch, row)
        }
        ProfileFieldSource::Attributes(column) => {
            render_attribute_map(read_batch_column(batch, column)?, row, table.attribute_keys)
        }
        ProfileFieldSource::Events => render_events(read_batch_column(batch, COL_EVENTS)?, row, table.attribute_keys),
        ProfileFieldSource::Links => render_links(read_batch_column(batch, COL_LINKS)?, row, table.attribute_keys),
        ProfileFieldSource::CrossRow(field) => render_cross_row_field(field, batch, row, trace_roots),
        ProfileFieldSource::Alias(_) | ProfileFieldSource::Object => Ok(Value::Null),
    }
}

/// Insert `value` at the dotted `path` of `document`, creating the objects on the way.
fn insert_nested_value(document: &mut Map<String, Value>, path: &str, value: Value) {
    match path.split_once('.') {
        None => {
            document.insert(path.to_string(), value);
        }
        Some((head, rest)) => {
            let child = document.entry(head.to_string()).or_insert_with(|| Value::Object(Map::new()));
            if !child.is_object() {
                *child = Value::Object(Map::new());
            }
            if let Value::Object(object) = child {
                insert_nested_value(object, rest, value);
            }
        }
    }
}

/// Keep the members of `object` (at `prefix`) that `includes` and `excludes` select, matching
/// each pattern against the dotted path of a member. A member whose path an include matches is
/// kept whole; an excluded path is dropped with everything under it.
fn filter_source_paths(
    object: Map<String, Value>,
    prefix: &str,
    includes: &[String],
    excludes: &[String],
) -> Map<String, Value> {
    let mut kept_members = Map::new();
    for (key, value) in object {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        if excludes.iter().any(|pattern| matches_glob(pattern, &path)) {
            continue;
        }
        let is_included = includes.is_empty() || includes.iter().any(|pattern| matches_glob(pattern, &path));
        match value {
            Value::Object(inner) => {
                let inner_members =
                    filter_source_paths(inner, &path, if is_included { &[] } else { includes }, excludes);
                if is_included || !inner_members.is_empty() {
                    kept_members.insert(key, Value::Object(inner_members));
                }
            }
            other_value if is_included => {
                kept_members.insert(key, other_value);
            }
            _ => {}
        }
    }
    kept_members
}

/// The `_source` of `batch[row]` under `table`: a nested object of the table's fields, absent
/// fields omitted, under `filter`. Cross-row fields render from `trace_roots`, absent for a trace
/// not in it.
///
/// # Errors
///
/// Returns `500` for a row lacking a declared column or holding an invalid stored value, and
/// the 400 of [`escape_attribute_key`] for a stored attribute key the spelling cannot write.
pub(crate) fn render_profile_document(
    table: &ProfileTable,
    batch: &RecordBatch,
    row: usize,
    filter: &SourceFilter,
    trace_roots: &TraceRoots,
) -> Result<Value, OpenSearchError> {
    let profile_row = ProfileRow { table, batch, row };
    let mut document = Map::new();
    for (name, source) in table.iter_fields() {
        let value = render_source_field(profile_row, name, *source, trace_roots)?;
        if !value.is_null() {
            insert_nested_value(&mut document, name, value);
        }
    }
    Ok(Value::Object(match filter {
        SourceFilter::All => document,
        SourceFilter::Nothing => Map::new(),
        SourceFilter::Paths { includes, excludes } => filter_source_paths(document, "", includes, excludes),
    }))
}

/// The value of the field `name` of `table` at `row`, for `fields` and `docvalue_fields`;
/// `Value::Null` when absent or when `name` is no leaf of the table. An alias renders its
/// target's value. A cross-row field renders `Value::Null`: the search refuses to name one in
/// `fields` or `docvalue_fields` before any row is read.
///
/// # Errors
///
/// As [`render_profile_document`].
pub(crate) fn render_profile_field(
    table: &ProfileTable,
    batch: &RecordBatch,
    row: usize,
    name: &str,
) -> Result<Value, OpenSearchError> {
    if let Some((declared_name, _)) = table.find_field(name) {
        return table.find_read_source(declared_name).map_or(Ok(Value::Null), |source| {
            render_source_field(
                ProfileRow { table, batch, row },
                declared_name,
                source,
                &TraceRoots::new(),
            )
        });
    }
    match resolve_profile_field(table, &batch.schema(), name).map(|resolved_field| resolved_field.value) {
        Some(FieldValue::Encoded(EncodedField::EscapedMapKey { column, key })) => {
            render_map_key(read_batch_column(batch, column)?, row, &key)
        }
        Some(FieldValue::MapKey { column, key }) => render_map_key(read_batch_column(batch, &column)?, row, &key),
        Some(FieldValue::Column { column }) => render_array_value(read_batch_column(batch, &column)?.as_ref(), row),
        Some(FieldValue::HexId { .. } | FieldValue::Encoded(_) | FieldValue::Derived(_) | FieldValue::CrossRow(_))
        | None => Ok(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use chrono::{DateTime, Utc};
    use datafusion::arrow::array::{
        Array, ArrayRef, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
        TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion::logical_expr::{Expr, lit};
    use datafusion::prelude::{SessionContext, ident};
    use icegate_common::schema::{
        COL_ATTRIBUTES, COL_BUCKET_COUNTS, COL_DROPPED_ATTRIBUTES_COUNT, COL_DURATION_MICROS, COL_END_TIMESTAMP,
        COL_EVENTS, COL_EXPLICIT_BOUNDS, COL_INGESTED_TIMESTAMP, COL_METRIC_NAME, COL_METRIC_TYPE, COL_NAME,
        COL_NEGATIVE_BUCKET_COUNTS, COL_NEGATIVE_OFFSET, COL_POSITIVE_BUCKET_COUNTS, COL_POSITIVE_OFFSET,
        COL_RESOURCE_ATTRIBUTES, COL_SCALE, COL_SCOPE_ATTRIBUTES, COL_SPAN_ATTRIBUTES, COL_SPAN_ID, COL_STATUS_CODE,
        COL_TIMESTAMP, COL_TRACE_ID, COL_VALUE_DOUBLE, COL_VALUE_INT, spans_schema,
    };
    use icegate_common::{METRICS_TABLE, SPANS_TABLE};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use serde_json::{Value, json};

    use super::{
        DerivedField, EncodedField, IntegerBounds, IntegerWidth, build_integer_range,
        compute_exponential_bucket_bounds, escape_attribute_key, expand_profile_pattern, format_rfc3339_micros,
        integer_bounds, lower_derived_range, lower_derived_term, lower_encoded_range, lower_encoded_term,
        render_encoded_value, render_exponential_buckets, render_profile_field, unescape_attribute_key,
    };
    use crate::opensearch::catalog::SelectedField;
    use crate::opensearch::catalog::data_prepper::{DATA_PREPPER_SPAN_INDEX, DATA_PREPPER_TABLE};
    use crate::opensearch::catalog::ss4o::find_ss4o_table;
    use crate::opensearch::error::{OpenSearchError, render_envelope};
    use crate::query_dsl::ast::RangeBound;
    use crate::query_dsl::date_math::{BoundSide, DateFormat, resolve_date_bound};
    use crate::test_support::{SearchFixture, TestCell, TestRow, find_index_entry};

    const INDEX: &str = "otel-v1-apm-span-000001";
    /// The fixed seed of the round-trip property; a failure reproduces with it.
    const SEED: u64 = 0x7e57_0007;
    const ROW: &str = "row";
    const TENANT_A: &str = "tenant-a";
    const TENANT_B: &str = "tenant-b";
    /// 2025-10-09T08:53:20Z.
    const T0: i64 = 1_760_000_000_000_000;
    const TRACE_ID: [u8; 16] = [0x0a; 16];
    const EVENTFUL_SPAN: [u8; 8] = [0x01; 8];
    const QUIET_SPAN: [u8; 8] = [0x02; 8];
    const TENANT_B_SPAN: [u8; 8] = [0x03; 8];

    /// The rows of `values` (one column named `column`, plus a row number) that `predicate`
    /// selects, by row number.
    async fn select_rows(column: &str, values: ArrayRef, predicate: Expr) -> BTreeSet<i64> {
        let row_count = i64::try_from(values.len()).expect("row count");
        let schema = Arc::new(Schema::new(vec![
            Field::new(column, values.data_type().clone(), true),
            Field::new(ROW, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![values, Arc::new(Int64Array::from_iter_values(0..row_count))],
        )
        .expect("batch");
        let batches = SessionContext::new()
            .read_batch(batch)
            .expect("frame")
            .filter(predicate)
            .expect("filter")
            .select_columns(&[ROW])
            .expect("project")
            .collect()
            .await
            .expect("collect");
        batches
            .iter()
            .flat_map(|batch| {
                let row_numbers = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("row numbers")
                    .clone();
                row_numbers.values().to_vec()
            })
            .collect()
    }

    /// The round trip of one encoded column: for every row, rendering the stored value and
    /// lowering a `term` on the rendered value (the field named `field_name`) selects exactly the
    /// rows that render the same.
    async fn assert_terms_round_trip(field: &EncodedField, field_name: &str, column: &str, values: ArrayRef) {
        let rendered_values: Vec<Value> = (0..values.len())
            .map(|row| render_encoded_value(field, &values, row).expect("render"))
            .collect();
        for (row, value) in rendered_values.iter().enumerate() {
            let expected_rows: BTreeSet<i64> = rendered_values
                .iter()
                .enumerate()
                .filter(|(_, other_value)| *other_value == value)
                .map(|(other_row, _)| i64::try_from(other_row).expect("row"))
                .collect();
            let predicate = lower_encoded_term(field, field_name, value, INDEX).expect("lower");
            let selected_rows = select_rows(column, Arc::clone(&values), predicate).await;
            assert_eq!(selected_rows, expected_rows, "{field:?} row {row} renders {value}");
        }
    }

    #[tokio::test]
    async fn encoded_fields_round_trip_their_filter_inverses() {
        let mut random = StdRng::seed_from_u64(SEED);

        let mut ids: Vec<Option<Vec<u8>>> = (0..24)
            .map(|_| Some((0..16).map(|_| random.random::<u8>()).collect()))
            .collect();
        ids.push(None);
        let ids: ArrayRef =
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(ids.into_iter(), 16).expect("ids"));
        assert_terms_round_trip(
            &EncodedField::HexIdOrEmpty {
                column: "id",
                width: 16,
            },
            "traceId",
            "id",
            ids,
        )
        .await;

        let mut states: Vec<Option<String>> = (0..24)
            .map(|_| {
                Some(
                    (0..random.random_range(1..8))
                        .map(|_| char::from(random.random_range(b'a'..=b'z')))
                        .collect(),
                )
            })
            .collect();
        // A stored empty string renders like NULL, so a `term ""` selects both.
        states.extend([None, Some(String::new())]);
        let states: ArrayRef = Arc::new(StringArray::from(states));
        assert_terms_round_trip(
            &EncodedField::EmptyWhenNull { column: "state" },
            "traceState",
            "state",
            states,
        )
        .await;

        let mut kinds: Vec<Option<i32>> = (0..24).map(|_| Some(random.random_range(0..6))).collect();
        kinds.extend([None, Some(0)]);
        let kinds: ArrayRef = Arc::new(Int32Array::from(kinds));
        assert_terms_round_trip(&EncodedField::SpanKindName { column: "kind" }, "kind", "kind", kinds).await;

        let mut codes: Vec<Option<i32>> = (0..24).map(|_| Some(random.random_range(0..3))).collect();
        codes.push(None);
        let codes: ArrayRef = Arc::new(Int32Array::from(codes));
        assert_terms_round_trip(
            &EncodedField::StatusCodeOrZero { column: "code" },
            "status.code",
            "code",
            codes,
        )
        .await;

        // A time renders as RFC 3339 with `Z` and six fraction digits, parses back to the stored
        // microsecond, and its epoch millisecond, as a range bound, brackets it. RFC 3339 has
        // four-digit years only, so the samples end before 10000-01-01T00:00:00Z.
        assert_eq!(format_rfc3339_micros(0).expect("format"), "1970-01-01T00:00:00.000000Z");
        assert_eq!(
            format_rfc3339_micros(253_402_300_799_999_999).expect("format"),
            "9999-12-31T23:59:59.999999Z"
        );
        let now = DateTime::<Utc>::from_timestamp_micros(1_760_000_000_000_000).expect("instant");
        let times: Vec<i64> = (0..64).map(|_| random.random_range(0..253_402_300_800_000_000)).collect();
        let time_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(times.clone()));
        let time_field = EncodedField::Rfc3339Time { column: "time" };
        for (row, micros) in times.iter().enumerate() {
            let Value::String(text) = render_encoded_value(&time_field, &time_array, row).expect("render") else {
                panic!("a time renders as text");
            };
            assert!(text.ends_with('Z'), "{text}");
            let fraction = text.rsplit('.').next().expect("a fraction").trim_end_matches('Z');
            assert_eq!(fraction.len(), 6, "{text}");
            let parsed_micros = DateTime::parse_from_rfc3339(&text).expect("RFC 3339").timestamp_micros();
            assert_eq!(parsed_micros, *micros, "{text}");
            let millis = micros.div_euclid(1_000).to_string();
            let lower_micros = resolve_date_bound(
                &millis,
                Some(DateFormat::EpochMillis),
                None,
                now,
                BoundSide::LowerInclusive,
            )
            .expect("lower bound");
            let upper_micros = resolve_date_bound(
                &millis,
                Some(DateFormat::EpochMillis),
                None,
                now,
                BoundSide::UpperInclusive,
            )
            .expect("upper bound");
            assert!(
                lower_micros <= *micros && *micros < upper_micros,
                "{micros} within [{lower_micros}, {upper_micros})"
            );
        }

        let null_time: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>]));
        assert_eq!(
            render_encoded_value(&time_field, &null_time, 0).expect("render"),
            Value::Null
        );
    }

    #[tokio::test]
    async fn attribute_keys_escape_dots_as_at_signs_and_a_stored_at_sign_is_refused() {
        assert_eq!(escape_attribute_key("http.method").expect("escape"), "http@method");
        assert_eq!(escape_attribute_key("plain").expect("escape"), "plain");
        assert_eq!(escape_attribute_key("a.b.c").expect("escape"), "a@b@c");
        assert_eq!(unescape_attribute_key("http@method"), "http.method");
        assert_eq!(unescape_attribute_key("plain"), "plain");
        for key in ["http.method", "service.name", "x"] {
            assert_eq!(unescape_attribute_key(&escape_attribute_key(key).expect("escape")), key);
        }

        let error = escape_attribute_key("user@domain").expect_err("a stored @ is ambiguous");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status.as_u16(), 400);
        for cause in [&body["error"], &body["error"]["root_cause"][0]] {
            assert_eq!(cause["type"], "illegal_argument_exception");
            assert_eq!(cause["key"], "user@domain");
        }
    }

    /// A span of `tenant` in trace [`TRACE_ID`] at `T0` lasting 1 000 µs, with `cells`.
    fn build_span_row<'a>(tenant: &'a str, span: &'a [u8], mut cells: Vec<(&'a str, TestCell<'a>)>) -> TestRow<'a> {
        cells.extend([
            (COL_TRACE_ID, TestCell::Bytes(&TRACE_ID)),
            (COL_SPAN_ID, TestCell::Bytes(span)),
            (COL_TIMESTAMP, TestCell::Micros(T0)),
            (COL_END_TIMESTAMP, TestCell::Micros(T0 + 1_000)),
            (COL_INGESTED_TIMESTAMP, TestCell::Micros(T0 + 1_000)),
            (COL_DURATION_MICROS, TestCell::Long(1_000)),
        ]);
        TestRow { tenant, cells }
    }

    /// The events of a committed span render as the profile states: `time` as RFC 3339 with `Z`
    /// and six fraction digits, `name`, `attributes` with each stored `.` written `@`, and
    /// `droppedAttributesCount`; a span with a NULL events list renders `[]`. Tenant B's span sits
    /// in the same commit; tenant A's scan holds only its own two spans.
    #[tokio::test]
    async fn committed_events_render_their_time_name_escaped_attributes_and_dropped_count() {
        let fixture = SearchFixture::new().await;
        let retry_event: &[(&str, TestCell<'_>)] = &[
            (COL_TIMESTAMP, TestCell::Micros(T0 + 100_000)),
            (COL_NAME, TestCell::Text("retry")),
            (COL_ATTRIBUTES, TestCell::Map(&[("retry.count", "2")])),
            (COL_DROPPED_ATTRIBUTES_COUNT, TestCell::Int(3)),
        ];
        let tenant_b_event: &[(&str, TestCell<'_>)] = &[(COL_NAME, TestCell::Text("tenant-b-event"))];
        fixture
            .commit_rows(
                SPANS_TABLE,
                &[
                    build_span_row(
                        TENANT_A,
                        &EVENTFUL_SPAN,
                        vec![(COL_EVENTS, TestCell::Structs(&[retry_event]))],
                    ),
                    build_span_row(TENANT_A, &QUIET_SPAN, Vec::new()),
                    build_span_row(
                        TENANT_B,
                        &TENANT_B_SPAN,
                        vec![(COL_EVENTS, TestCell::Structs(&[tenant_b_event]))],
                    ),
                ],
            )
            .await;
        let session = fixture.open_session(TENANT_A).await;
        let entry = find_index_entry(&session, DATA_PREPPER_SPAN_INDEX);
        let plan = session.scan_index(entry).expect("scan").build().expect("plan");

        let batches = session.collect(plan).await.expect("rows");

        let mut events_by_span: Vec<(Value, Value)> = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                events_by_span.push((
                    render_profile_field(&DATA_PREPPER_TABLE, batch, row, "spanId").expect("spanId"),
                    render_profile_field(&DATA_PREPPER_TABLE, batch, row, "events").expect("events"),
                ));
            }
        }
        events_by_span.sort_by_key(|(span_id, _)| span_id.to_string());
        assert_eq!(
            events_by_span,
            [
                (
                    json!("0101010101010101"),
                    json!([{
                        "time": "2025-10-09T08:53:20.100000Z",
                        "name": "retry",
                        "attributes": {"retry@count": "2"},
                        "droppedAttributesCount": 3
                    }])
                ),
                (json!("0202020202020202"), json!([])),
            ]
        );
    }

    /// The selection of `pattern` on the span profile, read back as the names of the selected
    /// fields and the MAP columns selected whole, each in declared order.
    fn expand_span_pattern(pattern: &str) -> Result<(Vec<String>, Vec<String>), OpenSearchError> {
        let schema = iceberg::arrow::schema_to_arrow_schema(&spans_schema().expect("the spans schema"))
            .expect("the spans Arrow schema");
        let mut field_names = Vec::new();
        let mut map_columns = Vec::new();
        for selected_field in expand_profile_pattern(&DATA_PREPPER_TABLE, &schema, pattern)? {
            match selected_field {
                SelectedField::Field(field) => field_names.push(field.name),
                SelectedField::AllMapValues { column } => map_columns.push(column),
            }
        }
        Ok((field_names, map_columns))
    }

    /// A pattern selects an attribute MAP whole when it covers every key path under it, the rule
    /// of a native MAP column; one that covers only some key paths is refused, and one that covers
    /// none leaves the MAP alone.
    #[tokio::test]
    async fn a_field_pattern_selects_an_attribute_map_whole_or_is_refused_when_it_covers_some_keys() {
        let attribute_map_columns = [COL_SPAN_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES];
        let (every_field_names, every_map_columns) = expand_span_pattern("*").expect("a selection");
        assert_eq!(every_map_columns, attribute_map_columns);
        assert_eq!(
            every_field_names,
            [
                "traceId",
                "spanId",
                "parentSpanId",
                "traceState",
                "name",
                "kind",
                "serviceName",
                "startTime",
                "endTime",
                "durationInNanos",
                "status.code",
                "status.message",
                "droppedAttributesCount",
                "droppedEventsCount",
                "droppedLinksCount"
            ]
        );
        assert_eq!(
            expand_span_pattern("span.*").expect("a selection"),
            (Vec::new(), vec![COL_SPAN_ATTRIBUTES.to_string()])
        );
        assert_eq!(
            expand_span_pattern("s*").expect("a selection"),
            (
                ["spanId", "serviceName", "startTime", "status.code", "status.message"]
                    .map(str::to_string)
                    .to_vec(),
                vec![COL_SPAN_ATTRIBUTES.to_string()]
            )
        );
        assert_eq!(
            expand_span_pattern("*.attributes.*").expect("a selection"),
            (Vec::new(), attribute_map_columns.map(str::to_string).to_vec())
        );
        assert_eq!(
            expand_span_pattern("trace*").expect("a selection"),
            (["traceId", "traceState"].map(str::to_string).to_vec(), Vec::new())
        );

        for pattern in ["span.attributes.http*", "*.attributes.http*"] {
            let error = expand_span_pattern(pattern).expect_err(pattern);
            let (status, body) = render_envelope(error).await;
            assert_eq!(status.as_u16(), 400, "{pattern}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{pattern}");
        }
    }

    /// A range bound of `text`.
    fn build_bound(text: &str, is_inclusive: bool) -> RangeBound {
        RangeBound {
            value: Value::String(text.to_string()),
            is_inclusive,
        }
    }

    /// The integers a double range admits, under IEEE totalOrder (`-0.0` just below `+0.0`): a
    /// fractional bound rounds inward, an exclusive integral bound steps past itself, a bound
    /// beyond the `i64` range empties its side or drops it, and crossed bounds admit nothing.
    #[test]
    fn integer_bounds_admit_the_integers_inside_a_double_range() {
        let cases: [(Option<RangeBound>, Option<RangeBound>, Option<IntegerBounds>); 13] = [
            (
                Some(build_bound("1.5", false)),
                None,
                Some(IntegerBounds {
                    min: Some(2),
                    max: None,
                }),
            ),
            (
                Some(build_bound("2", true)),
                None,
                Some(IntegerBounds {
                    min: Some(2),
                    max: None,
                }),
            ),
            (
                Some(build_bound("2", false)),
                None,
                Some(IntegerBounds {
                    min: Some(3),
                    max: None,
                }),
            ),
            (
                None,
                Some(build_bound("-0.0", true)),
                Some(IntegerBounds {
                    min: None,
                    max: Some(-1),
                }),
            ),
            (
                Some(build_bound("-0.0", false)),
                None,
                Some(IntegerBounds {
                    min: Some(0),
                    max: None,
                }),
            ),
            (Some(build_bound("NaN", true)), None, None),
            (None, Some(build_bound("NaN", true)), None),
            (Some(build_bound("1e19", true)), None, None),
            (None, Some(build_bound("-1e19", true)), None),
            (
                Some(build_bound("-1e19", false)),
                Some(build_bound("1e19", true)),
                Some(IntegerBounds { min: None, max: None }),
            ),
            (Some(build_bound("3", true)), Some(build_bound("2", true)), None),
            (
                Some(build_bound("-9223372036854775808", true)),
                None,
                Some(IntegerBounds {
                    min: Some(i64::MIN),
                    max: None,
                }),
            ),
            // No integer lies below the smallest `i64`.
            (None, Some(build_bound("-9223372036854775808", false)), None),
        ];
        for (lower, upper, expected_bounds) in cases {
            let bounds = integer_bounds(lower.as_ref(), upper.as_ref(), "status.code", INDEX).expect("numeric bounds");

            assert_eq!(bounds, expected_bounds, "{lower:?} {upper:?}");
        }
    }

    /// A range keeps literals of the column's width: a bound past the width's range on the far
    /// side empties the range, a bound at or past the width's edge on its own side is no
    /// constraint, a bound at the far edge stays a constraint, and a range with no constraint left
    /// selects the non-NULL values.
    #[test]
    fn integer_ranges_clip_their_bounds_to_the_column_width() {
        let cases = [
            (
                IntegerBounds {
                    min: Some(2_147_483_648),
                    max: None,
                },
                IntegerWidth::I32,
                None,
            ),
            (
                IntegerBounds {
                    min: None,
                    max: Some(-2_147_483_649),
                },
                IntegerWidth::I32,
                None,
            ),
            (
                IntegerBounds {
                    min: Some(-2_147_483_648),
                    max: Some(5),
                },
                IntegerWidth::I32,
                Some(ident("c").lt_eq(lit(5_i32))),
            ),
            (
                IntegerBounds {
                    min: Some(-2_147_483_648),
                    max: Some(2_147_483_647),
                },
                IntegerWidth::I32,
                Some(ident("c").is_not_null()),
            ),
            (
                IntegerBounds {
                    min: Some(2),
                    max: Some(3),
                },
                IntegerWidth::I64,
                Some(ident("c").gt_eq(lit(2_i64)).and(ident("c").lt_eq(lit(3_i64)))),
            ),
            (
                IntegerBounds {
                    min: Some(i64::MIN),
                    max: None,
                },
                IntegerWidth::I64,
                Some(ident("c").is_not_null()),
            ),
            (
                IntegerBounds {
                    min: Some(2_147_483_647),
                    max: None,
                },
                IntegerWidth::I32,
                Some(ident("c").gt_eq(lit(2_147_483_647_i32))),
            ),
            (
                IntegerBounds {
                    min: None,
                    max: Some(-2_147_483_648),
                },
                IntegerWidth::I32,
                Some(ident("c").lt_eq(lit(-2_147_483_648_i32))),
            ),
            (
                IntegerBounds {
                    min: Some(-2_147_483_649),
                    max: Some(2_147_483_648),
                },
                IntegerWidth::I32,
                Some(ident("c").is_not_null()),
            ),
        ];
        for (bounds, width, expected_predicate) in cases {
            assert_eq!(
                build_integer_range(ident("c"), bounds, width),
                expected_predicate,
                "{bounds:?} {width:?}"
            );
        }
    }

    /// A range with a `""` bound on a string whose NULL renders `""` selects the NULL rows exactly
    /// when `""` lies inside it. Rows: NULL, `""`, `"a"`, each rendering `""`, `""`, `"a"`.
    #[tokio::test]
    async fn empty_string_bounds_select_null_rows_exactly_when_empty_lies_inside() {
        let field = EncodedField::EmptyWhenNull { column: "state" };
        let cases = [
            (Some(build_bound("", true)), None, vec![0, 1, 2]),
            (Some(build_bound("", false)), None, vec![2]),
            (None, Some(build_bound("", true)), vec![0, 1]),
            (None, Some(build_bound("", false)), vec![]),
        ];
        for (lower, upper, expected_rows) in cases {
            let states: ArrayRef = Arc::new(StringArray::from(vec![None, Some(""), Some("a")]));
            let predicate =
                lower_encoded_range(&field, "traceState", lower.as_ref(), upper.as_ref(), INDEX).expect("lower");

            let selected_rows = select_rows("state", states, predicate).await;

            assert_eq!(
                selected_rows,
                expected_rows.into_iter().collect::<BTreeSet<i64>>(),
                "{lower:?} {upper:?}"
            );
        }
    }

    /// A status-code term that is fractional or outside the `Int32` range names no stored code,
    /// so it matches nothing; `0` matches the NULL rows it renders for. Rows: NULL, 0, 1, 2.
    #[tokio::test]
    async fn a_status_code_term_outside_the_stored_codes_matches_nothing() {
        let field = EncodedField::StatusCodeOrZero { column: "code" };
        let cases = [
            ("1.5", vec![]),
            ("2147483648", vec![]),
            ("0", vec![0, 1]),
            ("2", vec![3]),
        ];
        for (text, expected_rows) in cases {
            let codes: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(0), Some(1), Some(2)]));
            let predicate =
                lower_encoded_term(&field, "status.code", &Value::String(text.to_string()), INDEX).expect("lower");

            let selected_rows = select_rows("code", codes, predicate).await;

            assert_eq!(
                selected_rows,
                expected_rows.into_iter().collect::<BTreeSet<i64>>(),
                "{text}"
            );
        }
    }

    /// A literal that cannot denote a value of an encoded or derived field is the query-shard 400
    /// whose root cause names the field as the request did, never the column behind it: a status
    /// code, a duration or a bound that is no number, and an id or an id bound that is no hex of
    /// its width. The encoded fields read the real stored columns, whose names differ from the
    /// requested ones.
    #[tokio::test]
    async fn a_malformed_literal_on_an_encoded_or_derived_field_is_a_query_shard_400_naming_the_requested_field() {
        let text = Value::String("x".to_string());
        let status_code = EncodedField::StatusCodeOrZero {
            column: COL_STATUS_CODE,
        };
        let refusals = [
            (
                lower_encoded_term(&status_code, "status.code", &text, INDEX).expect_err("status code x"),
                "status.code",
            ),
            (
                lower_encoded_range(&status_code, "status.code", Some(&build_bound("x", true)), None, INDEX)
                    .expect_err("status code bound x"),
                "status.code",
            ),
            (
                lower_encoded_term(
                    &EncodedField::HexIdOrEmpty {
                        column: COL_TRACE_ID,
                        width: 16,
                    },
                    "traceId",
                    &Value::String("zz".to_string()),
                    INDEX,
                )
                .expect_err("trace id zz"),
                "traceId",
            ),
            (
                lower_encoded_range(
                    &EncodedField::HexIdOrEmpty {
                        column: COL_SPAN_ID,
                        width: 8,
                    },
                    "spanId",
                    None,
                    Some(&build_bound("zz", false)),
                    INDEX,
                )
                .expect_err("span id bound zz"),
                "spanId",
            ),
            (
                lower_derived_term(DerivedField::DurationInNanos, &text, INDEX).expect_err("duration x"),
                "durationInNanos",
            ),
            (
                lower_derived_range(
                    DerivedField::DurationInNanos,
                    None,
                    Some(&build_bound("x", true)),
                    INDEX,
                )
                .expect_err("duration bound x"),
                "durationInNanos",
            ),
            (
                integer_bounds(Some(&build_bound("x", true)), None, "status.code", INDEX).expect_err("bound x"),
                "status.code",
            ),
        ];
        for (refusal, expected_field) in refusals {
            let (status, body) = render_envelope(refusal).await;

            assert_eq!(status.as_u16(), 400, "{body}");
            let cause = &body["error"]["root_cause"][0];
            assert_eq!(cause["type"], "query_shard_exception", "{body}");
            assert_eq!(cause["field"], expected_field, "{body}");
            assert_eq!(cause["index"], INDEX, "{body}");
        }
    }

    /// Bucket `k` covers `(base^(offset+k), base^(offset+k+1)]` with `base = 2^(2^-scale)`; a
    /// negative bucket mirrors it. Scales 0 and -1 give bases 2 and 4, whose bounds are exact
    /// powers of two, stated here by hand.
    #[test]
    fn exponential_bucket_bounds_follow_the_scale_and_the_offset() {
        let cases: [((i32, i32, usize, bool), (f64, f64)); 7] = [
            ((0, -1, 0, false), (0.5, 1.0)),
            ((0, -1, 1, false), (1.0, 2.0)),
            ((0, 2, 0, false), (4.0, 8.0)),
            ((-1, 1, 0, false), (4.0, 16.0)),
            ((-1, -1, 0, false), (0.25, 1.0)),
            ((0, 0, 0, true), (-2.0, -1.0)),
            ((0, 0, 2, true), (-8.0, -4.0)),
        ];
        for ((scale, offset, index, is_negative), (min, max)) in cases {
            let bounds = compute_exponential_bucket_bounds(scale, offset, index, is_negative).expect("in range");
            assert_eq!(
                bounds,
                (min, max),
                "scale {scale} offset {offset} bucket {index} negative {is_negative}"
            );
        }
    }

    /// The overflow boundary of the bucket exponent `offset + index + 1`: one below it is the last
    /// bucket that has bounds (infinite at that exponent, so only their presence is stated), at it
    /// the exponent overflows, and an index beyond `i32` has no exponent at all.
    #[test]
    fn exponential_bucket_bounds_end_where_the_bucket_exponent_overflows() {
        assert!(compute_exponential_bucket_bounds(0, i32::MAX - 1, 0, false).is_some());
        assert_eq!(compute_exponential_bucket_bounds(0, i32::MAX, 0, false), None);
        assert_eq!(compute_exponential_bucket_bounds(0, 0, usize::MAX, false), None);
    }

    /// A NULL bucket count is absent from its bucket, which keeps its bounds, as a NULL quantile
    /// member is absent. The stored count element is required, so the list here is built by hand
    /// with a nullable element: scale 0 and offset -1 give buckets `(2^-1, 2^0]`, `(2^0, 2^1]`
    /// and `(2^1, 2^2]`.
    #[test]
    fn a_null_exponential_bucket_count_is_absent_and_its_bounds_stay() {
        let counts: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some([
            Some(3),
            None,
            Some(5),
        ])]));
        let schema = Schema::new(vec![
            Field::new(COL_SCALE, DataType::Int32, true),
            Field::new(COL_POSITIVE_OFFSET, DataType::Int32, true),
            Field::new(COL_POSITIVE_BUCKET_COUNTS, counts.data_type().clone(), true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(Int32Array::from(vec![-1])),
                counts,
            ],
        )
        .expect("a batch");

        let buckets = render_exponential_buckets(&batch, 0, COL_POSITIVE_BUCKET_COUNTS, COL_POSITIVE_OFFSET, false)
            .expect("buckets");

        assert_eq!(
            buckets,
            json!([
                {"count": 3, "min": 0.5, "max": 1.0},
                {"min": 1.0, "max": 2.0},
                {"count": 5, "min": 2.0, "max": 4.0}
            ])
        );
    }

    /// A metrics row of `tenant` at `T0` named `name`, of `metric_type`, with `cells`.
    fn build_metric_row<'a>(
        tenant: &'a str,
        name: &'a str,
        metric_type: &'a str,
        mut cells: Vec<(&'a str, TestCell<'a>)>,
    ) -> TestRow<'a> {
        cells.extend([
            (COL_TIMESTAMP, TestCell::Micros(T0)),
            (COL_INGESTED_TIMESTAMP, TestCell::Micros(T0)),
            (COL_METRIC_NAME, TestCell::Text(name)),
            (COL_METRIC_TYPE, TestCell::Text(metric_type)),
        ]);
        TestRow { tenant, cells }
    }

    /// `fields` and `docvalue_fields` render the derived and bucket fields of the metrics
    /// profile from the committed row: exponential buckets with their bounds at scale 0 (base 2,
    /// positive offset -1 gives `(2^-1, 2^0]` and `(2^0, 2^1]`, negative offset 0 gives
    /// `[-2^1, -2^0)`), list lengths with `0` for an empty list and absent for a NULL one, and
    /// the metric value as a double from `value_int` and from `value_double`. Tenant B's row sits
    /// in the same commit; tenant A's scan holds only its own four.
    #[tokio::test]
    async fn metrics_profile_fields_render_buckets_lengths_and_values_from_the_committed_row() {
        let fixture = SearchFixture::new().await;
        fixture
            .commit_rows(
                METRICS_TABLE,
                &[
                    build_metric_row(
                        TENANT_A,
                        "payload",
                        "exponential_histogram",
                        vec![
                            (COL_SCALE, TestCell::Int(0)),
                            (COL_POSITIVE_OFFSET, TestCell::Int(-1)),
                            (COL_POSITIVE_BUCKET_COUNTS, TestCell::Longs(&[3, 5])),
                            (COL_NEGATIVE_OFFSET, TestCell::Int(0)),
                            (COL_NEGATIVE_BUCKET_COUNTS, TestCell::Longs(&[7])),
                            (COL_BUCKET_COUNTS, TestCell::Longs(&[])),
                        ],
                    ),
                    build_metric_row(
                        TENANT_A,
                        "latency",
                        "histogram",
                        vec![
                            (COL_BUCKET_COUNTS, TestCell::Longs(&[1, 2, 3])),
                            (COL_EXPLICIT_BOUNDS, TestCell::Doubles(&[0.5, 1.0])),
                        ],
                    ),
                    build_metric_row(TENANT_A, "requests", "sum", vec![(COL_VALUE_INT, TestCell::Long(20))]),
                    build_metric_row(
                        TENANT_A,
                        "ratio",
                        "gauge",
                        vec![(COL_VALUE_DOUBLE, TestCell::Double(0.5))],
                    ),
                    build_metric_row(
                        TENANT_B,
                        "latency",
                        "gauge",
                        vec![(COL_VALUE_DOUBLE, TestCell::Double(9.5))],
                    ),
                ],
            )
            .await;
        let session = fixture.open_session(TENANT_A).await;
        let entry = find_index_entry(&session, "ss4o_metrics-icegate-default");
        let table = find_ss4o_table(METRICS_TABLE).expect("the ss4o metrics table");
        let plan = session.scan_index(entry).expect("scan").build().expect("plan");

        let batches = session.collect(plan).await.expect("rows");

        let field_names = [
            "name",
            "positiveBuckets",
            "negativeBuckets",
            "bucketCount",
            "explicitBoundsCount",
            "value",
        ];
        let mut rendered_rows: Vec<Value> = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let mut rendered_fields = serde_json::Map::new();
                for field_name in field_names {
                    let value = render_profile_field(table, batch, row, field_name).expect(field_name);
                    if !value.is_null() {
                        rendered_fields.insert(field_name.to_string(), value);
                    }
                }
                rendered_rows.push(Value::Object(rendered_fields));
            }
        }
        rendered_rows.sort_by_key(|rendered_row| rendered_row["name"].to_string());
        assert_eq!(
            rendered_rows,
            [
                json!({"name": "latency", "bucketCount": 3, "explicitBoundsCount": 2}),
                json!({
                    "name": "payload",
                    "positiveBuckets": [{"count": 3, "min": 0.5, "max": 1.0}, {"count": 5, "min": 1.0, "max": 2.0}],
                    "negativeBuckets": [{"count": 7, "min": -2.0, "max": -1.0}],
                    "bucketCount": 0
                }),
                json!({"name": "ratio", "value": 0.5}),
                json!({"name": "requests", "value": 20.0}),
            ]
        );
    }

    /// A cross-row field is read from another row, the root span of its trace, which only a
    /// search of its own index looks up, so no full-text pattern ever selects one.
    #[test]
    fn a_field_pattern_never_selects_a_cross_row_field() {
        for pattern in [
            "traceGroup*",
            "traceGroupFields.*",
            "traceGroupFields.*Time",
            "trace?roup",
        ] {
            assert_eq!(
                expand_span_pattern(pattern).expect("a selection"),
                (Vec::new(), Vec::new()),
                "{pattern}"
            );
        }
        let (every_field_names, _) = expand_span_pattern("*").expect("a selection");
        for cross_row_name in [
            "traceGroup",
            "traceGroupFields.endTime",
            "traceGroupFields.durationInNanos",
            "traceGroupFields.statusCode",
        ] {
            assert!(
                !every_field_names.iter().any(|name| name == cross_row_name),
                "{cross_row_name}"
            );
        }
    }
}
