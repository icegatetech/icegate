//! Duration parsing for LogQL queries.
//!
//! Supports both simple durations (`5m`, `1h`) and compound durations (`1h30m`,
//! `5y2w3d4h5m6s`). Units must appear in descending order: y, w, d, h, m, s,
//! ms, us/µs, ns.

use chrono::TimeDelta;

use super::common::parse_error;
use crate::error::Result;

/// Unit priority for ordering validation (higher = larger unit).
const UNIT_PRIORITY: &[(&str, u8)] = &[
    ("y", 9),
    ("w", 8),
    ("d", 7),
    ("h", 6),
    ("m", 5),
    ("s", 4),
    ("ms", 3),
    ("us", 2),
    ("µs", 2),
    ("ns", 1),
];

/// Nanoseconds per unit.
const NANOS_PER_NS: i64 = 1;
const NANOS_PER_US: i64 = 1_000;
const NANOS_PER_MS: i64 = 1_000_000;
const NANOS_PER_S: i64 = 1_000_000_000;
const NANOS_PER_M: i64 = 60 * NANOS_PER_S;
const NANOS_PER_H: i64 = 3600 * NANOS_PER_S;
const NANOS_PER_D: i64 = 86400 * NANOS_PER_S;
const NANOS_PER_W: i64 = 7 * NANOS_PER_D;
const NANOS_PER_Y: i64 = 365 * NANOS_PER_D;

/// Get the priority for a unit (higher = larger unit).
fn unit_priority(unit: &str) -> Option<u8> {
    UNIT_PRIORITY.iter().find(|(u, _)| *u == unit).map(|(_, p)| *p)
}

/// Get nanoseconds multiplier for a unit.
fn unit_to_nanos(unit: &str) -> Option<i64> {
    match unit {
        "ns" => Some(NANOS_PER_NS),
        "us" | "µs" => Some(NANOS_PER_US),
        "ms" => Some(NANOS_PER_MS),
        "s" => Some(NANOS_PER_S),
        "m" => Some(NANOS_PER_M),
        "h" => Some(NANOS_PER_H),
        "d" => Some(NANOS_PER_D),
        "w" => Some(NANOS_PER_W),
        "y" => Some(NANOS_PER_Y),
        _ => None,
    }
}

/// Convert f64 to i64 with overflow checking.
#[allow(clippy::cast_precision_loss)]
fn f64_to_i64_checked(value: f64) -> Result<i64> {
    if value.is_nan() || value.is_infinite() {
        return Err(parse_error("Duration value is not finite"));
    }
    if value > i64::MAX as f64 || value < i64::MIN as f64 {
        return Err(parse_error("Duration value out of range"));
    }
    #[allow(clippy::cast_possible_truncation)]
    Ok(value.trunc() as i64)
}

/// Extract the next number from a string, returning `(number_str, remaining)`.
fn extract_number(text: &str) -> Option<(&str, &str)> {
    let mut num_end = 0;
    for (i, c) in text.char_indices() {
        if c.is_ascii_digit() || c == '.' {
            num_end = i + c.len_utf8();
        } else {
            break;
        }
    }
    if num_end == 0 {
        return None;
    }
    Some((&text[..num_end], &text[num_end..]))
}

/// Extract the next unit from a string, returning `(unit, remaining)`.
fn extract_unit(text: &str) -> Option<(&str, &str)> {
    // Try multi-char units first (ms, us, µs, ns)
    for unit in &["ms", "us", "µs", "ns"] {
        if let Some(remaining) = text.strip_prefix(unit) {
            return Some((unit, remaining));
        }
    }
    // Try single-char units (y, w, d, h, m, s)
    for unit in &["y", "w", "d", "h", "m", "s"] {
        if let Some(remaining) = text.strip_prefix(unit) {
            return Some((unit, remaining));
        }
    }
    None
}

/// Check if a number string contains a decimal point.
fn is_float(num_str: &str) -> bool {
    num_str.contains('.')
}

/// Parse a duration string like "5m", "1h", "1h30m", "5y2w3d4h5m6s" into
/// Duration.
///
/// # Supported formats
///
/// - Simple: `5m`, `1h`, `30s`, `2.5h`
/// - Compound: `1h30m`, `2d5h30m`, `5y2w3d4h5m6s`
///
/// # Unit ordering
///
/// Units must appear in descending order: y → w → d → h → m → s → ms → us/µs →
/// ns. For example, `2w3y` is invalid because weeks come after years.
///
/// # Float values
///
/// Float values are only allowed for single-unit durations (e.g., `2.5h`).
/// Compound durations must use integers (e.g., `1h30m` not `1.5h30m`).
///
/// # Errors
///
/// Returns an error if:
/// - The duration string is empty or invalid
/// - Units are out of order
/// - Float values are used in compound durations
/// - The duration value overflows
pub fn parse_duration(text: &str) -> Result<TimeDelta> {
    let text = text.trim();
    if text.is_empty() {
        return Err(parse_error("Empty duration string"));
    }

    let is_negative = text.starts_with('-');
    let text = text.trim_start_matches('-').trim_start_matches('+');

    if text.is_empty() {
        return Err(parse_error("Duration has no value after sign"));
    }

    let mut remaining = text;
    let mut total_nanos: i64 = 0;
    let mut last_priority: Option<u8> = None;
    let mut component_count = 0;
    let mut has_float = false;

    while !remaining.is_empty() {
        // Extract number
        let (num_str, after_num) = extract_number(remaining)
            .ok_or_else(|| parse_error(format!("Expected number in duration: {remaining}")))?;

        // Check for float in compound duration
        if is_float(num_str) {
            has_float = true;
        }

        // Extract unit
        let (unit, after_unit) =
            extract_unit(after_num).ok_or_else(|| parse_error(format!("Unknown duration unit: {after_num}")))?;

        // Validate unit ordering (strict: must be descending)
        let priority = unit_priority(unit).ok_or_else(|| parse_error(format!("Unknown duration unit: {unit}")))?;

        if let Some(last) = last_priority {
            if priority >= last {
                return Err(parse_error(format!(
                    "Duration units must be in descending order, got '{unit}' after a smaller or equal unit"
                )));
            }
        }
        last_priority = Some(priority);

        // Parse number and calculate nanos
        let num: f64 = num_str
            .parse()
            .map_err(|_| parse_error(format!("Invalid number in duration: {num_str}")))?;

        let nanos_per_unit =
            unit_to_nanos(unit).ok_or_else(|| parse_error(format!("Unknown duration unit: {unit}")))?;

        #[allow(clippy::cast_precision_loss)]
        let component_nanos = f64_to_i64_checked(num * nanos_per_unit as f64)?;

        total_nanos = total_nanos
            .checked_add(component_nanos)
            .ok_or_else(|| parse_error("Duration overflow"))?;

        component_count += 1;
        remaining = after_unit;
    }

    // Reject float in compound durations
    if has_float && component_count > 1 {
        return Err(parse_error("Float values are only allowed for single-unit durations"));
    }

    let total_nanos = if is_negative { -total_nanos } else { total_nanos };
    Ok(TimeDelta::nanoseconds(total_nanos))
}

/// Parse a duration string and return nanoseconds directly.
///
/// This is a convenience wrapper for HTTP handlers that need raw nanoseconds.
/// Returns `None` on parse errors instead of `Result`.
pub fn parse_duration_nanos(text: &str) -> Option<i64> {
    parse_duration(text).ok().and_then(|d| d.num_nanoseconds())
}

/// Parse a duration string and return a `TimeDelta`.
///
/// This is a convenience wrapper that returns `Option<TimeDelta>`.
pub fn parse_duration_opt(text: &str) -> Option<TimeDelta> {
    parse_duration(text).ok()
}

#[cfg(test)]
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
mod tests {
    use super::*;

    /// Helper to get nanos from `TimeDelta`
    fn nanos(d: TimeDelta) -> i64 {
        d.num_nanoseconds().unwrap()
    }

    // =========================================================================
    // Simple durations
    // =========================================================================

    #[test]
    fn test_parse_duration_simple_ns() {
        let d = parse_duration("100ns").unwrap();
        assert_eq!(nanos(d), 100);
    }

    #[test]
    fn test_parse_duration_simple_us() {
        let d = parse_duration("50us").unwrap();
        assert_eq!(nanos(d), 50_000);
    }

    #[test]
    fn test_parse_duration_simple_us_micro_symbol() {
        let d = parse_duration("50µs").unwrap();
        assert_eq!(nanos(d), 50_000);
    }

    #[test]
    fn test_parse_duration_simple_ms() {
        let d = parse_duration("20ms").unwrap();
        assert_eq!(nanos(d), 20_000_000);
    }

    #[test]
    fn test_parse_duration_simple_s() {
        let d = parse_duration("30s").unwrap();
        assert_eq!(nanos(d), 30 * NANOS_PER_S);
    }

    #[test]
    fn test_parse_duration_simple_m() {
        let d = parse_duration("5m").unwrap();
        assert_eq!(nanos(d), 5 * NANOS_PER_M);
    }

    #[test]
    fn test_parse_duration_simple_h() {
        let d = parse_duration("1h").unwrap();
        assert_eq!(nanos(d), NANOS_PER_H);
    }

    #[test]
    fn test_parse_duration_simple_d() {
        let d = parse_duration("2d").unwrap();
        assert_eq!(nanos(d), 2 * NANOS_PER_D);
    }

    #[test]
    fn test_parse_duration_simple_w() {
        let d = parse_duration("1w").unwrap();
        assert_eq!(nanos(d), NANOS_PER_W);
    }

    #[test]
    fn test_parse_duration_simple_y() {
        let d = parse_duration("1y").unwrap();
        assert_eq!(nanos(d), NANOS_PER_Y);
    }

    // =========================================================================
    // Compound durations
    // =========================================================================

    #[test]
    fn test_parse_duration_compound_hm() {
        let d = parse_duration("1h30m").unwrap();
        assert_eq!(nanos(d), NANOS_PER_H + 30 * NANOS_PER_M);
    }

    #[test]
    fn test_parse_duration_compound_dhm() {
        let d = parse_duration("2d5h30m").unwrap();
        assert_eq!(nanos(d), 2 * NANOS_PER_D + 5 * NANOS_PER_H + 30 * NANOS_PER_M);
    }

    #[test]
    fn test_parse_duration_compound_full() {
        // 5y2w3d4h5m6s
        let d = parse_duration("5y2w3d4h5m6s").unwrap();
        let expected =
            5 * NANOS_PER_Y + 2 * NANOS_PER_W + 3 * NANOS_PER_D + 4 * NANOS_PER_H + 5 * NANOS_PER_M + 6 * NANOS_PER_S;
        assert_eq!(nanos(d), expected);
    }

    #[test]
    fn test_parse_duration_compound_with_ms() {
        let d = parse_duration("1s500ms").unwrap();
        assert_eq!(nanos(d), NANOS_PER_S + 500 * NANOS_PER_MS);
    }

    #[test]
    fn test_parse_duration_compound_ms_us_ns() {
        let d = parse_duration("1ms500us100ns").unwrap();
        assert_eq!(nanos(d), NANOS_PER_MS + 500 * NANOS_PER_US + 100);
    }

    // =========================================================================
    // Float single-unit durations
    // =========================================================================

    #[test]
    fn test_parse_duration_float_h() {
        let d = parse_duration("2.5h").unwrap();
        assert_eq!(nanos(d), (2.5 * NANOS_PER_H as f64) as i64);
    }

    #[test]
    fn test_parse_duration_float_m() {
        let d = parse_duration("1.5m").unwrap();
        assert_eq!(nanos(d), (1.5 * NANOS_PER_M as f64) as i64);
    }

    // =========================================================================
    // Negative durations
    // =========================================================================

    #[test]
    fn test_parse_duration_negative() {
        let d = parse_duration("-5m").unwrap();
        assert_eq!(nanos(d), -5 * NANOS_PER_M);
    }

    #[test]
    fn test_parse_duration_negative_compound() {
        let d = parse_duration("-1h30m").unwrap();
        assert_eq!(nanos(d), -(NANOS_PER_H + 30 * NANOS_PER_M));
    }

    #[test]
    fn test_parse_duration_negative_float() {
        let d = parse_duration("-2.5h").unwrap();
        assert_eq!(nanos(d), -(2.5 * NANOS_PER_H as f64) as i64);
    }

    #[test]
    fn test_parse_duration_positive_sign() {
        let d = parse_duration("+5m").unwrap();
        assert_eq!(nanos(d), 5 * NANOS_PER_M);
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    #[test]
    fn test_parse_duration_with_whitespace() {
        let d = parse_duration("  5m  ").unwrap();
        assert_eq!(nanos(d), 5 * NANOS_PER_M);
    }

    #[test]
    fn test_parse_duration_zero() {
        let d = parse_duration("0s").unwrap();
        assert_eq!(nanos(d), 0);
    }

    // =========================================================================
    // Error cases
    // =========================================================================

    #[test]
    fn test_parse_duration_invalid_unit() {
        let result = parse_duration("5x");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duration_empty() {
        let result = parse_duration("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duration_only_sign() {
        let result = parse_duration("-");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duration_out_of_order() {
        // weeks after years is invalid
        let result = parse_duration("2w3y");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duration_out_of_order_hm() {
        // minutes after hours is valid, but hours after minutes is invalid
        let result = parse_duration("30m1h");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duration_float_in_compound() {
        let result = parse_duration("1.5h30m");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duration_duplicate_units() {
        // Same unit twice should fail due to ordering (s is not > s)
        let result = parse_duration("5s5s");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duration_no_number() {
        let result = parse_duration("h");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duration_no_unit() {
        let result = parse_duration("123");
        assert!(result.is_err());
    }

    // =========================================================================
    // parse_duration_nanos convenience function
    // =========================================================================

    #[test]
    fn test_parse_duration_nanos_success() {
        let nanos = parse_duration_nanos("5m");
        assert_eq!(nanos, Some(5 * NANOS_PER_M));
    }

    #[test]
    fn test_parse_duration_nanos_compound() {
        let nanos = parse_duration_nanos("1h30m");
        assert_eq!(nanos, Some(NANOS_PER_H + 30 * NANOS_PER_M));
    }

    #[test]
    fn test_parse_duration_nanos_error() {
        let nanos = parse_duration_nanos("invalid");
        assert_eq!(nanos, None);
    }
}
