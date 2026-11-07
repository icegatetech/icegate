//! Typed literals of term-level leaves: number literals, numeric and boolean terms, integer
//! range bounds, date terms and exclusive double bounds.
//!
//! The rules here decide which rows a typed value matches. Each one is stated on its function
//! and pinned by a literal table.

use chrono::{DateTime, Utc};

use crate::opensearch::catalog::FieldType;
use crate::query_dsl::date_math::{BoundSide, TimeZoneSpec, resolve_date_bound, resolve_date_text_span};

/// Why a typed literal does not denote a value of its field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LiteralError {
    /// The text is not a value of the field's type.
    #[error("malformed value")]
    Malformed,
    /// The text is a number outside the field's range.
    #[error("value out of range")]
    OutOfRange,
}

/// What a numeric `term` matches.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NumericTerm {
    /// Rows holding this integer.
    Integer(i64),
    /// Rows holding this double (totalOrder equality: `-0.0` and `+0.0` differ).
    Double(f64),
    /// No row: an integer field never holds a value with a fractional part.
    NoMatch,
}

/// One side of an integer range, rounded toward the inside of the range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntegerBound {
    /// The side does not restrict the range.
    Unbounded,
    /// The side is this inclusive bound.
    Inclusive(i64),
    /// No value of the field satisfies the side.
    Empty,
}

/// `-2^63` as a double, the least `i64`.
const I64_MIN_AS_DOUBLE: f64 = -9_223_372_036_854_775_808.0;
/// `2^63` as a double, one above the greatest `i64`.
const I64_END_AS_DOUBLE: f64 = 9_223_372_036_854_775_808.0;

/// Parse `text` as a number literal of this grammar: characters up to U+0020 around the text
/// are trimmed; then come an optional sign, and `NaN` or `Infinity` (case-sensitive), or a
/// decimal significand (digits, an optional fraction, at least one digit) with an optional
/// exponent, or a hex significand (`0x`/`0X`) with a required binary exponent (`p`/`P`); then an
/// optional `d`/`D`/`f`/`F` suffix.
///
/// The value is the nearest double, ties to even. Overflow is infinite and underflow zero.
/// `None` for anything else, such as `0x10`, `1_000`, `3,14`, `inf` or `nan`.
pub(crate) fn parse_java_double(text: &str) -> Option<f64> {
    let trimmed_text = trim_java_whitespace(text);
    let (is_negative, unsigned_text) = match trimmed_text.as_bytes().first() {
        Some(b'-') => (true, &trimmed_text[1..]),
        Some(b'+') => (false, &trimmed_text[1..]),
        _ => (false, trimmed_text),
    };
    let magnitude = match unsigned_text {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        _ => {
            let body = unsigned_text.strip_suffix(['d', 'D', 'f', 'F']).unwrap_or(unsigned_text);
            match body.strip_prefix("0x").or_else(|| body.strip_prefix("0X")) {
                Some(hex_body) => parse_hex_magnitude(hex_body)?,
                None => parse_decimal_magnitude(body)?,
            }
        }
    };
    Some(if is_negative { -magnitude } else { magnitude })
}

/// `text` without the characters up to U+0020 around it, the whitespace this number grammar
/// trims. Wider Unicode whitespace (U+3000) is kept, so it makes the text malformed.
fn trim_java_whitespace(text: &str) -> &str {
    text.trim_matches(|character: char| character <= ' ')
}

/// The number of ASCII digits in `bytes` from `start_index` on, up to the first non-digit.
fn count_digits(bytes: &[u8], start_index: usize) -> usize {
    bytes
        .get(start_index..)
        .map_or(0, |rest| rest.iter().take_while(|byte| byte.is_ascii_digit()).count())
}

/// A decimal significand with an optional exponent, validated here and rounded by Rust's
/// correctly rounding parser (which would also accept `inf` and `nan`; the grammar check
/// refuses them first).
fn parse_decimal_magnitude(body: &str) -> Option<f64> {
    let bytes = body.as_bytes();
    let integer_digits = count_digits(bytes, 0);
    let mut position = integer_digits;
    let mut fraction_digits = 0;
    if bytes.get(position) == Some(&b'.') {
        position += 1;
        fraction_digits = count_digits(bytes, position);
        position += fraction_digits;
    }
    if integer_digits + fraction_digits == 0 {
        return None;
    }
    if matches!(bytes.get(position), Some(b'e' | b'E')) {
        position += 1;
        if matches!(bytes.get(position), Some(b'+' | b'-')) {
            position += 1;
        }
        let exponent_digits = count_digits(bytes, position);
        if exponent_digits == 0 {
            return None;
        }
        position += exponent_digits;
    }
    if position != bytes.len() {
        return None;
    }
    body.parse::<f64>().ok()
}

/// A binary exponent: an optional sign and decimal digits, clamped far outside the double
/// range (a larger magnitude rounds to the same infinity or zero).
fn parse_binary_exponent(text: &str) -> Option<i64> {
    let (is_negative, digits) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let magnitude = digits.bytes().fold(0_i64, |value, digit| {
        (value * 10 + i64::from(digit - b'0')).min(1_000_000)
    });
    Some(if is_negative { -magnitude } else { magnitude })
}

/// A hex significand with a required binary exponent, rounded to nearest-even exactly.
fn parse_hex_magnitude(body: &str) -> Option<f64> {
    let (significand, exponent_text) = body.split_once(['p', 'P'])?;
    let exponent = parse_binary_exponent(exponent_text)?;
    let (integer_part, fraction_part) = significand.split_once('.').unwrap_or((significand, ""));
    if integer_part.is_empty() && fraction_part.is_empty() {
        return None;
    }
    let mut mantissa: u128 = 0;
    let mut is_inexact = false;
    let mut scale: i64 = 0;
    for (index, character) in integer_part.chars().chain(fraction_part.chars()).enumerate() {
        let digit = character.to_digit(16)?;
        let is_fraction_digit = index >= integer_part.len();
        // Up to 124 bits the next digit still fits; past that, digits only decide whether the
        // value is inexact (a sticky bit for the rounding below) and shift the scale.
        if mantissa >> 124 == 0 {
            mantissa = (mantissa << 4) | u128::from(digit);
            if is_fraction_digit {
                scale -= 4;
            }
        } else {
            is_inexact |= digit != 0;
            if !is_fraction_digit {
                scale += 4;
            }
        }
    }
    compose_binary_double(mantissa, is_inexact, exponent + scale)
}

/// The double nearest to `mantissa · 2^exponent` (plus less than one unit of `mantissa` when
/// `is_inexact`), ties to even, built from its IEEE fields so no rounding happens twice.
///
/// `None` only if a field fails to convert to its width, which the bounds checked here rule out;
/// the caller then answers malformed rather than a made-up value.
fn compose_binary_double(mantissa: u128, is_inexact: bool, exponent: i64) -> Option<f64> {
    if mantissa == 0 {
        return Some(0.0);
    }
    let bit_length = i64::from(128 - mantissa.leading_zeros());
    let leading_exponent = exponent + bit_length - 1;
    if leading_exponent > 1023 {
        return Some(f64::INFINITY);
    }
    // A normal result keeps 53 bits; a subnormal one keeps the bits down to 2^-1074.
    let kept_bit_count = if leading_exponent >= -1022 {
        53
    } else {
        leading_exponent + 1075
    };
    let dropped_bit_count = bit_length - kept_bit_count;
    let (kept_mantissa, kept_exponent) = if dropped_bit_count <= 0 {
        (mantissa, exponent)
    } else if dropped_bit_count > 128 {
        (0, exponent + dropped_bit_count)
    } else {
        let dropped_shift = u32::try_from(dropped_bit_count).ok()?;
        let (truncated_mantissa, remainder) = if dropped_shift == 128 {
            (0, mantissa)
        } else {
            (mantissa >> dropped_shift, mantissa & ((1_u128 << dropped_shift) - 1))
        };
        let half = 1_u128 << (dropped_shift - 1);
        let is_round_up = remainder > half || (remainder == half && (is_inexact || truncated_mantissa & 1 == 1));
        (
            truncated_mantissa + u128::from(is_round_up),
            exponent + dropped_bit_count,
        )
    };
    if kept_mantissa == 0 {
        return Some(0.0);
    }
    let kept_length = i64::from(128 - kept_mantissa.leading_zeros());
    let kept_leading_exponent = kept_exponent + kept_length - 1;
    if kept_leading_exponent > 1023 {
        return Some(f64::INFINITY);
    }
    let bits = if kept_leading_exponent >= -1022 {
        // Normal: the leading bit is implicit; the next 52 bits are the fraction field.
        let shift = 53 - kept_length;
        let normalized_mantissa = if shift >= 0 {
            kept_mantissa << shift
        } else {
            kept_mantissa >> -shift
        };
        let fraction_field = u64::try_from(normalized_mantissa & ((1_u128 << 52) - 1)).ok()?;
        let biased_exponent = u64::try_from(kept_leading_exponent + 1023).ok()?;
        (biased_exponent << 52) | fraction_field
    } else {
        // Subnormal: the field counts units of 2^-1074 (a carry into 2^52 is the least normal).
        let shift = kept_exponent + 1074;
        let units = if shift >= 0 {
            kept_mantissa << shift
        } else {
            kept_mantissa >> -shift
        };
        u64::try_from(units).ok()?
    };
    Some(f64::from_bits(bits))
}

/// The inclusive value range of an integer field type; `None` for any other type.
fn find_integer_range(field_type: FieldType) -> Option<(i64, i64)> {
    match field_type {
        FieldType::Integer => Some((i64::from(i32::MIN), i64::from(i32::MAX))),
        FieldType::Long => Some((i64::MIN, i64::MAX)),
        FieldType::Text
        | FieldType::Keyword
        | FieldType::Date
        | FieldType::Double
        | FieldType::Boolean
        | FieldType::Object => None,
    }
}

/// `value` (integral, inside the `i64` range) as an `i64`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "callers pass an integral double inside [-2^63, 2^63), which converts exactly"
)]
const fn convert_integral_double(value: f64) -> i64 {
    value as i64
}

/// What a numeric `term` with `text` matches on a field of `field_type`.
///
/// An exact integer is read without passing through a double, so every `long` value is
/// reachable. An integer field term with a fractional part matches nothing.
///
/// # Errors
///
/// Returns [`LiteralError::Malformed`] for a non-number, a non-finite value or a non-numeric
/// field type, and [`LiteralError::OutOfRange`] for an integer outside the field's range.
pub(crate) fn parse_numeric_term(text: &str, field_type: FieldType) -> Result<NumericTerm, LiteralError> {
    if field_type == FieldType::Double {
        let value = parse_java_double(text).ok_or(LiteralError::Malformed)?;
        return if value.is_finite() {
            Ok(NumericTerm::Double(value))
        } else {
            Err(LiteralError::Malformed)
        };
    }
    let (minimum, maximum) = find_integer_range(field_type).ok_or(LiteralError::Malformed)?;
    if let Ok(integer) = trim_java_whitespace(text).parse::<i64>() {
        return if (minimum..=maximum).contains(&integer) {
            Ok(NumericTerm::Integer(integer))
        } else {
            Err(LiteralError::OutOfRange)
        };
    }
    let value = parse_java_double(text).ok_or(LiteralError::Malformed)?;
    if !value.is_finite() {
        return Err(LiteralError::Malformed);
    }
    if value.fract() != 0.0 {
        return Ok(NumericTerm::NoMatch);
    }
    if !(I64_MIN_AS_DOUBLE..I64_END_AS_DOUBLE).contains(&value) {
        return Err(LiteralError::OutOfRange);
    }
    let integer = convert_integral_double(value);
    if (minimum..=maximum).contains(&integer) {
        Ok(NumericTerm::Integer(integer))
    } else {
        Err(LiteralError::OutOfRange)
    }
}

/// One side of an integer range on a field of `field_type`, rounded toward the inside of the
/// range: a lower side takes the least integer it admits, an upper side the greatest. A side
/// past the field's range either does not restrict it or admits no value; `±Infinity` is such a
/// side, not an error.
///
/// # Errors
///
/// Returns [`LiteralError::Malformed`] for a non-number, `NaN` or a non-integer field type, and
/// [`LiteralError::OutOfRange`] only if a bound inside the field's range failed to convert to
/// `i64`, which the range checks rule out.
pub(crate) fn resolve_integer_bound(
    text: &str,
    field_type: FieldType,
    side: BoundSide,
) -> Result<IntegerBound, LiteralError> {
    let (minimum, maximum) = find_integer_range(field_type).ok_or(LiteralError::Malformed)?;
    let is_lower = matches!(side, BoundSide::LowerInclusive | BoundSide::LowerExclusive);
    // A candidate is the inclusive bound before the field's range applies; `i128` holds it one
    // step past either end of `i64`. The last arm holds a value inside the field's range, so its
    // conversion cannot fail; if it did, the bound is refused rather than guessed.
    let bound_from_candidate = |candidate: i128| -> Result<IntegerBound, LiteralError> {
        match (is_lower, candidate) {
            (true, value) if value <= i128::from(minimum) => Ok(IntegerBound::Unbounded),
            (true, value) if value > i128::from(maximum) => Ok(IntegerBound::Empty),
            (false, value) if value >= i128::from(maximum) => Ok(IntegerBound::Unbounded),
            (false, value) if value < i128::from(minimum) => Ok(IntegerBound::Empty),
            (_, value) => i64::try_from(value)
                .map(IntegerBound::Inclusive)
                .map_err(|_| LiteralError::OutOfRange),
        }
    };
    let exclusive_step: i128 = match side {
        BoundSide::LowerExclusive => 1,
        BoundSide::UpperExclusive => -1,
        BoundSide::LowerInclusive | BoundSide::UpperInclusive => 0,
    };
    if let Ok(integer) = trim_java_whitespace(text).parse::<i64>() {
        return bound_from_candidate(i128::from(integer) + exclusive_step);
    }
    let value = parse_java_double(text).ok_or(LiteralError::Malformed)?;
    if value.is_nan() {
        return Err(LiteralError::Malformed);
    }
    if value.is_infinite() {
        let is_toward_outside = is_lower == value.is_sign_negative();
        return Ok(if is_toward_outside {
            IntegerBound::Unbounded
        } else {
            IntegerBound::Empty
        });
    }
    // Rounding to an integer is exact in doubles; the exclusive step is not (past 2^53 the
    // neighbouring integers are no doubles), so it is taken in `i128` below.
    let rounded_value = match side {
        BoundSide::LowerInclusive | BoundSide::UpperExclusive => value.ceil(),
        BoundSide::LowerExclusive | BoundSide::UpperInclusive => value.floor(),
    };
    if rounded_value < I64_MIN_AS_DOUBLE {
        return Ok(if is_lower {
            IntegerBound::Unbounded
        } else {
            IntegerBound::Empty
        });
    }
    if rounded_value >= I64_END_AS_DOUBLE {
        return Ok(if is_lower {
            IntegerBound::Empty
        } else {
            IntegerBound::Unbounded
        });
    }
    bound_from_candidate(i128::from(convert_integral_double(rounded_value)) + exclusive_step)
}

/// `true` or `false`; any other text, `TRUE` and `1` included, is no boolean.
pub(crate) fn parse_boolean_literal(text: &str) -> Option<bool> {
    match text {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// The adjacent double past `value` on an exclusive `side`, in IEEE totalOrder; `value` itself
/// on an inclusive side.
///
/// totalOrder puts `-0.0` directly below `+0.0`, so `gt -0.0` starts at `+0.0` and `lt +0.0`
/// ends at `-0.0`. An infinity has no neighbour past it and stays.
pub(crate) const fn step_exclusive_double(value: f64, side: BoundSide) -> f64 {
    const NEGATIVE_ZERO_BITS: u64 = 0x8000_0000_0000_0000;
    match side {
        BoundSide::LowerInclusive | BoundSide::UpperInclusive => value,
        BoundSide::LowerExclusive if value.to_bits() == NEGATIVE_ZERO_BITS => 0.0,
        BoundSide::LowerExclusive => value.next_up(),
        BoundSide::UpperExclusive if value.to_bits() == 0 => -0.0,
        BoundSide::UpperExclusive => value.next_down(),
    }
}

/// `-?digits[.digits]` epoch milliseconds, the fraction truncated.
fn parse_decimal_epoch_millis(text: &str) -> Option<i64> {
    let (integer_text, fraction_text) = text.split_once('.').unwrap_or((text, "0"));
    let digits = integer_text.strip_prefix('-').unwrap_or(integer_text);
    let is_well_formed = !digits.is_empty()
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && !fraction_text.is_empty()
        && fraction_text.bytes().all(|byte| byte.is_ascii_digit());
    if !is_well_formed {
        return None;
    }
    integer_text.parse().ok()
}

/// The microsecond span `[start_micros, start_micros + 1000)` of one millisecond.
///
/// # Errors
///
/// Returns [`LiteralError::OutOfRange`] when the span leaves the microsecond range.
fn build_millisecond_span(start_micros: i64) -> Result<(i64, i64), LiteralError> {
    let end_micros = start_micros.checked_add(1_000).ok_or(LiteralError::OutOfRange)?;
    Ok((start_micros, end_micros))
}

/// The half-open microsecond range `[start, end)` a date `term` with `text` covers.
///
/// A `strict_date_optional_time` date covers itself rounded up over the time-of-day fields it
/// omits, at millisecond precision, as [`resolve_date_text_span`] states: a date alone covers its
/// day (`2026` is 2026-01-01), `T12` an hour, `T12:00` a minute, `T12:00:00` a second, and a
/// fraction one millisecond. An epoch-ms term, a decimal epoch (fraction truncated) and a
/// date-math term (`now…`, `…||…`) cover one millisecond. A date without an offset is read in
/// `time_zone`, else in UTC.
///
/// # Errors
///
/// Returns [`LiteralError::Malformed`] for text in none of these forms, and
/// [`LiteralError::OutOfRange`] for an instant outside the microsecond range.
pub(crate) fn resolve_date_term(
    text: &str,
    time_zone: Option<&TimeZoneSpec>,
    now: DateTime<Utc>,
) -> Result<(i64, i64), LiteralError> {
    if text.starts_with("now") || text.contains("||") {
        let start = resolve_date_bound(text, None, time_zone, now, BoundSide::LowerInclusive)
            .map_err(|_| LiteralError::Malformed)?;
        return build_millisecond_span(start);
    }
    if let Some((start, end)) = resolve_date_text_span(text, time_zone) {
        return Ok((start.timestamp_micros(), end.timestamp_micros()));
    }
    let millis = parse_decimal_epoch_millis(text).ok_or(LiteralError::Malformed)?;
    build_millisecond_span(millis.checked_mul(1_000).ok_or(LiteralError::OutOfRange)?)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::{
        IntegerBound, LiteralError, NumericTerm, parse_boolean_literal, parse_java_double, parse_numeric_term,
        resolve_date_term, resolve_integer_bound, step_exclusive_double,
    };
    use crate::opensearch::catalog::FieldType;
    use crate::query_dsl::date_math::{BoundSide, TimeZoneSpec, parse_time_zone};

    fn parse_request_instant() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-08T12:34:56.789Z")
            .expect("literal instant")
            .with_timezone(&Utc)
    }

    /// The hex rows were computed outside the code with a correctly rounded hex-float reader;
    /// the two decimal rows past 2^53 are ties, derived by hand: 2^53 + 1 rounds to the even
    /// neighbour 2^53, and 2^53 + 3 to the even neighbour 2^53 + 4.
    #[test]
    fn java_double_literals_parse_or_fail_as_the_stated_table() {
        let accepted: &[(&str, u64)] = &[
            ("0x1p3", 0x4020_0000_0000_0000),
            ("1e3", 0x408F_4000_0000_0000),
            ("1d", 0x3FF0_0000_0000_0000),
            (" 2 ", 0x4000_0000_0000_0000),
            ("\t+1.5f", 0x3FF8_0000_0000_0000),
            (".5", 0x3FE0_0000_0000_0000),
            ("5.", 0x4014_0000_0000_0000),
            ("-0", 0x8000_0000_0000_0000),
            ("1e309", 0x7FF0_0000_0000_0000),
            ("-1e309", 0xFFF0_0000_0000_0000),
            ("1e-400", 0x0000_0000_0000_0000),
            ("Infinity", 0x7FF0_0000_0000_0000),
            ("-Infinity", 0xFFF0_0000_0000_0000),
            ("0x.8p1", 0x3FF0_0000_0000_0000),
            ("0X1P-1074", 0x0000_0000_0000_0001),
            ("0x1p-1075", 0x0000_0000_0000_0000),
            ("0x1.8p-1075", 0x0000_0000_0000_0001),
            ("0x1.fffffffffffffp1023", 0x7FEF_FFFF_FFFF_FFFF),
            ("0x1p1024", 0x7FF0_0000_0000_0000),
            ("0x1.00000000000008p0", 0x3FF0_0000_0000_0000),
            ("0x1.00000000000018p0", 0x3FF0_0000_0000_0002),
            ("0x1.0000000000000801p0", 0x3FF0_0000_0000_0001),
            ("9007199254740993", 0x4340_0000_0000_0000),
            ("9007199254740995", 0x4340_0000_0000_0002),
        ];
        for (text, bits) in accepted {
            assert_eq!(parse_java_double(text).map(f64::to_bits), Some(*bits), "{text:?}");
        }
        assert!(parse_java_double("NaN").is_some_and(f64::is_nan));
        assert!(parse_java_double("-NaN").is_some_and(f64::is_nan));

        for text in [
            "0x10", "1_000", "3,14", "inf", "infinity", "nan", "INFINITY", "", " ", "e5", "1e", "0x1p", "0xp1", "--1",
            "1.2.3", "NaNd", "1 2", "0x1.g", "1e+", ".",
        ] {
            assert_eq!(parse_java_double(text), None, "{text:?}");
        }
    }

    /// Every literal of the shared decimal subset (an optional '-', digits, an optional fraction,
    /// an optional exponent) is a number of the grammar.
    #[test]
    fn decimal_literals_in_the_shared_subset_are_admitted() {
        let mut generator = StdRng::seed_from_u64(0x5EED_D0B1_E000);
        for _ in 0..20_000 {
            let mut text = String::new();
            if generator.random_bool(0.3) {
                text.push('-');
            }
            for _ in 0..generator.random_range(1..=20) {
                text.push(char::from(b'0' + generator.random_range(0..10_u8)));
            }
            if generator.random_bool(0.5) {
                text.push('.');
                for _ in 0..generator.random_range(0..=20) {
                    text.push(char::from(b'0' + generator.random_range(0..10_u8)));
                }
            }
            if generator.random_bool(0.4) {
                text.push(if generator.random_bool(0.5) { 'e' } else { 'E' });
                match generator.random_range(0..3_u8) {
                    0 => text.push('-'),
                    1 => text.push('+'),
                    _ => {}
                }
                text.push_str(&generator.random_range(0..400_u16).to_string());
            }
            assert!(parse_java_double(&text).is_some(), "{text}");
        }
    }

    #[test]
    fn an_integer_term_with_a_fraction_matches_nothing_and_integral_scientific_is_accepted() {
        let cases: &[(&str, FieldType, Result<NumericTerm, LiteralError>)] = &[
            ("1.5", FieldType::Long, Ok(NumericTerm::NoMatch)),
            ("-0.25", FieldType::Integer, Ok(NumericTerm::NoMatch)),
            ("1e3", FieldType::Long, Ok(NumericTerm::Integer(1_000))),
            ("1.0", FieldType::Integer, Ok(NumericTerm::Integer(1))),
            (" -7 ", FieldType::Long, Ok(NumericTerm::Integer(-7))),
            ("\u{1}5\t", FieldType::Long, Ok(NumericTerm::Integer(5))),
            ("\u{3000}5", FieldType::Long, Err(LiteralError::Malformed)),
            (
                "9223372036854775807",
                FieldType::Long,
                Ok(NumericTerm::Integer(i64::MAX)),
            ),
            ("9223372036854775808", FieldType::Long, Err(LiteralError::OutOfRange)),
            ("1e19", FieldType::Long, Err(LiteralError::OutOfRange)),
            (
                "2147483647",
                FieldType::Integer,
                Ok(NumericTerm::Integer(2_147_483_647)),
            ),
            ("2147483648", FieldType::Integer, Err(LiteralError::OutOfRange)),
            ("abc", FieldType::Long, Err(LiteralError::Malformed)),
            ("NaN", FieldType::Long, Err(LiteralError::Malformed)),
            ("Infinity", FieldType::Integer, Err(LiteralError::Malformed)),
            ("1.25", FieldType::Double, Ok(NumericTerm::Double(1.25))),
            ("NaN", FieldType::Double, Err(LiteralError::Malformed)),
            ("-Infinity", FieldType::Double, Err(LiteralError::Malformed)),
            ("x", FieldType::Double, Err(LiteralError::Malformed)),
            ("1", FieldType::Keyword, Err(LiteralError::Malformed)),
        ];
        for (text, field_type, expected) in cases {
            assert_eq!(
                parse_numeric_term(text, *field_type),
                *expected,
                "{text:?} as {field_type:?}"
            );
        }
        let Ok(NumericTerm::Double(negative_zero)) = parse_numeric_term("-0.0", FieldType::Double) else {
            panic!("-0.0 is a double term");
        };
        assert_eq!(negative_zero.to_bits(), 0x8000_0000_0000_0000);
    }

    /// The last row is 2^60 written in scientific notation: every double that large is an
    /// integer, so the step past it happens in integers, not in doubles (2^60 + 1 is no double).
    #[test]
    fn integer_range_bounds_round_toward_the_inside_of_the_range() {
        let cases: &[(&str, FieldType, BoundSide, Result<IntegerBound, LiteralError>)] = &[
            (
                "1.5",
                FieldType::Long,
                BoundSide::LowerInclusive,
                Ok(IntegerBound::Inclusive(2)),
            ),
            (
                "1.5",
                FieldType::Long,
                BoundSide::LowerExclusive,
                Ok(IntegerBound::Inclusive(2)),
            ),
            (
                "1.5",
                FieldType::Long,
                BoundSide::UpperInclusive,
                Ok(IntegerBound::Inclusive(1)),
            ),
            (
                "1.5",
                FieldType::Long,
                BoundSide::UpperExclusive,
                Ok(IntegerBound::Inclusive(1)),
            ),
            (
                "2",
                FieldType::Long,
                BoundSide::LowerExclusive,
                Ok(IntegerBound::Inclusive(3)),
            ),
            (
                "2",
                FieldType::Long,
                BoundSide::UpperExclusive,
                Ok(IntegerBound::Inclusive(1)),
            ),
            (
                "-Infinity",
                FieldType::Long,
                BoundSide::LowerInclusive,
                Ok(IntegerBound::Unbounded),
            ),
            (
                "Infinity",
                FieldType::Long,
                BoundSide::LowerInclusive,
                Ok(IntegerBound::Empty),
            ),
            (
                "Infinity",
                FieldType::Integer,
                BoundSide::UpperInclusive,
                Ok(IntegerBound::Unbounded),
            ),
            (
                "3000000000",
                FieldType::Integer,
                BoundSide::LowerInclusive,
                Ok(IntegerBound::Empty),
            ),
            (
                "3000000000",
                FieldType::Integer,
                BoundSide::UpperInclusive,
                Ok(IntegerBound::Unbounded),
            ),
            (
                "-3000000000",
                FieldType::Integer,
                BoundSide::LowerInclusive,
                Ok(IntegerBound::Unbounded),
            ),
            (
                "9223372036854775807",
                FieldType::Long,
                BoundSide::LowerExclusive,
                Ok(IntegerBound::Empty),
            ),
            (
                "NaN",
                FieldType::Long,
                BoundSide::LowerInclusive,
                Err(LiteralError::Malformed),
            ),
            (
                "ten",
                FieldType::Integer,
                BoundSide::UpperInclusive,
                Err(LiteralError::Malformed),
            ),
            (
                "\u{1}5\t",
                FieldType::Long,
                BoundSide::LowerExclusive,
                Ok(IntegerBound::Inclusive(6)),
            ),
            (
                "\u{3000}5",
                FieldType::Long,
                BoundSide::LowerInclusive,
                Err(LiteralError::Malformed),
            ),
            (
                "1.152921504606846976e18",
                FieldType::Long,
                BoundSide::LowerExclusive,
                Ok(IntegerBound::Inclusive(1_152_921_504_606_846_977)),
            ),
        ];
        for (text, field_type, side, expected) in cases {
            assert_eq!(
                resolve_integer_bound(text, *field_type, *side),
                *expected,
                "{text:?} {side:?}"
            );
        }
    }

    #[test]
    fn only_true_and_false_parse_as_booleans() {
        assert_eq!(parse_boolean_literal("true"), Some(true));
        assert_eq!(parse_boolean_literal("false"), Some(false));
        for text in ["TRUE", "True", "False", "1", "0", "yes", "", " true", "true "] {
            assert_eq!(parse_boolean_literal(text), None, "{text:?}");
        }
    }

    /// The zone rows were derived by hand from the tz rules: `Asia/Dubai` is +04:00;
    /// `America/New_York` changes to daylight time at 2026-03-08T07:00Z (02:00 EST) and back at
    /// 2026-11-01T06:00Z (02:00 EDT); `Australia/Lord_Howe` moves from +10:30 to +11:00 at
    /// 2026-10-04 02:00 local, a 30-minute gap.
    #[test]
    fn a_date_term_covers_its_omitted_time_of_day_at_millisecond_precision() {
        let dubai = parse_time_zone("Asia/Dubai").expect("zone");
        let new_york = parse_time_zone("America/New_York").expect("zone");
        let lord_howe = parse_time_zone("Australia/Lord_Howe").expect("zone");
        let cases: &[(&str, Option<&TimeZoneSpec>, (i64, i64))] = &[
            ("2026", None, (1_767_225_600_000_000, 1_767_312_000_000_000)),
            ("2026-10-08", None, (1_791_417_600_000_000, 1_791_504_000_000_000)),
            ("2026-10-08T12", None, (1_791_460_800_000_000, 1_791_464_400_000_000)),
            ("2026-10-08T12:30", None, (1_791_462_600_000_000, 1_791_462_660_000_000)),
            (
                "2026-10-08T12:00:00Z",
                None,
                (1_791_460_800_000_000, 1_791_460_801_000_000),
            ),
            (
                "2026-10-08T12:00:00.123456Z",
                None,
                (1_791_460_800_123_000, 1_791_460_800_124_000),
            ),
            (
                "2026-10-08T17:30:00+05:30",
                None,
                (1_791_460_800_000_000, 1_791_460_801_000_000),
            ),
            ("1760000000123", None, (1_760_000_000_123_000, 1_760_000_000_124_000)),
            (
                "1760000000123.789",
                None,
                (1_760_000_000_123_000, 1_760_000_000_124_000),
            ),
            ("now-1d", None, (1_791_376_496_789_000, 1_791_376_496_790_000)),
            (
                "2026-10-08",
                Some(&dubai),
                (1_791_403_200_000_000, 1_791_489_600_000_000),
            ),
            (
                "2026-10-08T12:00:00Z",
                Some(&dubai),
                (1_791_460_800_000_000, 1_791_460_801_000_000),
            ),
            // The day of the change to daylight time has 23 hours.
            (
                "2026-03-08",
                Some(&new_york),
                (1_772_946_000_000_000, 1_773_028_800_000_000),
            ),
            // 02:30 lies in the hour the change at 02:00 skips: it moves forward by the gap at
            // the EST offset in force before it, to 2026-03-08T07:30:00Z, for one minute.
            (
                "2026-03-08T02:30",
                Some(&new_york),
                (1_772_955_000_000_000, 1_772_955_060_000_000),
            ),
            // The day of the change back has 25 hours.
            (
                "2026-11-01",
                Some(&new_york),
                (1_793_505_600_000_000, 1_793_595_600_000_000),
            ),
            // 01:30 happens twice; the earlier instant is EDT, 2026-11-01T05:30:00Z.
            (
                "2026-11-01T01:30",
                Some(&new_york),
                (1_793_511_000_000_000, 1_793_511_060_000_000),
            ),
            // 02:15 lies in the 30-minute gap: at the +10:30 offset before it,
            // 2026-10-03T15:45:00Z.
            (
                "2026-10-04T02:15",
                Some(&lord_howe),
                (1_791_042_300_000_000, 1_791_042_360_000_000),
            ),
        ];
        for (text, time_zone, expected) in cases {
            assert_eq!(
                resolve_date_term(text, *time_zone, parse_request_instant()),
                Ok(*expected),
                "{text:?}"
            );
        }
        for text in [
            "2026-13-01",
            "2026-1-01",
            "2026-10-08T25",
            "yesterday",
            "2026-10-08 12:00",
            "26-10-08",
            "2026T12",
            "2026-10-08T12:00+05:99",
        ] {
            assert_eq!(
                resolve_date_term(text, None, parse_request_instant()),
                Err(LiteralError::Malformed),
                "{text:?}"
            );
        }
    }

    #[test]
    fn exclusive_double_bounds_step_to_the_adjacent_double() {
        let cases: &[(f64, BoundSide, u64)] = &[
            (0.0, BoundSide::LowerExclusive, 0x0000_0000_0000_0001),
            (-0.0, BoundSide::LowerExclusive, 0x0000_0000_0000_0000),
            (0.0, BoundSide::UpperExclusive, 0x8000_0000_0000_0000),
            (-0.0, BoundSide::UpperExclusive, 0x8000_0000_0000_0001),
            (1.0, BoundSide::LowerExclusive, 0x3FF0_0000_0000_0001),
            (1.0, BoundSide::UpperExclusive, 0x3FEF_FFFF_FFFF_FFFF),
            (1.0, BoundSide::LowerInclusive, 0x3FF0_0000_0000_0000),
            (-0.0, BoundSide::UpperInclusive, 0x8000_0000_0000_0000),
            (f64::INFINITY, BoundSide::LowerExclusive, 0x7FF0_0000_0000_0000),
        ];
        for (value, side, bits) in cases {
            assert_eq!(
                step_exclusive_double(*value, *side).to_bits(),
                *bits,
                "{value:?} {side:?}"
            );
        }
    }
}
