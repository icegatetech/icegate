//! Parsing UDFs for converting strings to numbers (numeric, bytes, duration).

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{Float64Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, Result, exec_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};

/// UDF: `parse_numeric(value)` - parses string to Float64.
///
/// # Arguments
/// - `value`: A `String` column
///
/// # Returns
/// Parsed Float64 value, or NULL if parsing fails.
///
/// # Example
/// ```sql
/// SELECT parse_numeric('42.5') -- 42.5
/// SELECT parse_numeric('invalid') -- NULL
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseNumeric {
    signature: Signature,
}

impl Default for ParseNumeric {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseNumeric {
    /// Creates a new `ParseNumeric` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ParseNumeric {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_numeric"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("parse_numeric requires one argument");
        }

        let value = &args.args[0];

        match value {
            ColumnarValue::Scalar(scalar) => {
                let result = match scalar {
                    datafusion::scalar::ScalarValue::Utf8(Some(s)) => s.trim().parse::<f64>().ok(),
                    _ => None,
                };

                Ok(ColumnarValue::Scalar(result.map_or_else(
                    || datafusion::scalar::ScalarValue::Float64(None),
                    |v| datafusion::scalar::ScalarValue::Float64(Some(v)),
                )))
            }
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?;

                let result: Float64Array = string_array
                    .iter()
                    .map(|opt_str| opt_str.and_then(|s| s.trim().parse::<f64>().ok()))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

/// UDF: `parse_bytes(value)` - parses humanized byte string to Float64.
///
/// # Arguments
/// - `value`: A `String` column containing byte values like "10KB", "5.5MB"
///
/// # Returns
/// Parsed byte count as Float64, or NULL if parsing fails.
///
/// # Supported Units
/// B, KB, MB, GB, TB, PB (1024-based), also `KiB`, `MiB`, `GiB`, etc.
///
/// # Example
/// ```sql
/// SELECT parse_bytes('10KB') -- 10240.0
/// SELECT parse_bytes('5.5MB') -- 5767168.0
/// SELECT parse_bytes('invalid') -- NULL
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseBytes {
    signature: Signature,
}

impl Default for ParseBytes {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseBytes {
    /// Creates a new `ParseBytes` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    /// Parse a byte string like "10KB" to float.
    ///
    /// Handles both unit-suffixed strings (e.g., "10KB") and unitless numeric strings (e.g., "1024").
    /// Unitless strings are treated as raw bytes.
    fn parse_byte_string(s: &str) -> Option<f64> {
        let s = s.trim();

        // Find where the unit starts (first non-digit, non-dot character)
        let unit_start = s.find(|c: char| !c.is_ascii_digit() && c != '.');

        if let Some(unit_start_idx) = unit_start {
            // Has a unit suffix
            let (num_str, unit_str) = s.split_at(unit_start_idx);
            let num: f64 = num_str.trim().parse().ok()?;

            let multiplier = match unit_str.trim().to_uppercase().as_str() {
                "B" => 1.0,
                "KB" | "K" | "KIB" => 1024.0,
                "MB" | "M" | "MIB" => 1024.0 * 1024.0,
                "GB" | "G" | "GIB" => 1024.0 * 1024.0 * 1024.0,
                "TB" | "T" | "TIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
                "PB" | "P" | "PIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0,
                _ => return None,
            };

            Some(num * multiplier)
        } else {
            // No unit suffix - treat as raw bytes
            s.parse::<f64>().ok()
        }
    }
}

impl ScalarUDFImpl for ParseBytes {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_bytes"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("parse_bytes requires one argument");
        }

        let value = &args.args[0];

        match value {
            ColumnarValue::Scalar(scalar) => {
                let result = match scalar {
                    datafusion::scalar::ScalarValue::Utf8(Some(s)) => Self::parse_byte_string(s.trim()),
                    _ => None,
                };

                Ok(ColumnarValue::Scalar(result.map_or_else(
                    || datafusion::scalar::ScalarValue::Float64(None),
                    |v| datafusion::scalar::ScalarValue::Float64(Some(v)),
                )))
            }
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?;

                let result: Float64Array = string_array
                    .iter()
                    .map(|opt_str| opt_str.and_then(Self::parse_byte_string))
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

/// UDF: `parse_duration(value, as_seconds)` - parses Go-style duration to Float64.
///
/// # Arguments
/// - `value`: A `String` column containing durations like "5s", "1h30m", "500ms"
/// - `as_seconds`: Boolean - true to return seconds, false to return nanoseconds
///
/// # Returns
/// Parsed duration as Float64, or NULL if parsing fails.
///
/// # Supported Units
/// ns, us (µs), ms, s, m, h (can be combined like "1h30m45s")
///
/// # Example
/// ```sql
/// SELECT parse_duration('5s', true) -- 5.0 seconds
/// SELECT parse_duration('1h30m', true) -- 5400.0 seconds
/// SELECT parse_duration('500ms', false) -- 500000000.0 nanoseconds
/// SELECT parse_duration('invalid', true) -- NULL
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ParseDuration {
    signature: Signature,
}

impl Default for ParseDuration {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseDuration {
    /// Creates a new `ParseDuration` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    /// Parse a Go-style duration string like "1h30m45s" to nanoseconds
    fn parse_duration_string(s: &str) -> Option<f64> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }

        let mut total_nanos = 0.0;
        let mut current_num = String::new();

        let chars: Vec<char> = s.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            let c = chars[i];

            if c.is_ascii_digit() || c == '.' {
                current_num.push(c);
                i += 1;
            } else {
                // Found a unit character
                if current_num.is_empty() {
                    return None;
                }

                let num: f64 = current_num.parse().ok()?;
                current_num.clear();

                // Determine the unit
                let unit = if c == 'h' {
                    i += 1;
                    3_600_000_000_000.0 // hours to nanoseconds
                } else if c == 'm' {
                    // Check if next char is 's' (milliseconds)
                    if i + 1 < chars.len() && chars[i + 1] == 's' {
                        i += 2;
                        1_000_000.0 // milliseconds to nanoseconds
                    } else {
                        i += 1;
                        60_000_000_000.0 // minutes to nanoseconds
                    }
                } else if c == 's' {
                    i += 1;
                    1_000_000_000.0 // seconds to nanoseconds
                } else if c == 'n' && i + 1 < chars.len() && chars[i + 1] == 's' {
                    i += 2;
                    1.0 // nanoseconds
                } else if c == 'u' || c == 'µ' || c == 'μ' {
                    // microseconds - must be followed by 's' (us, µs, or μs)
                    if i + 1 < chars.len() && chars[i + 1] == 's' {
                        i += 2;
                        1_000.0 // microseconds to nanoseconds
                    } else {
                        return None; // Invalid: bare 'u'/'µ'/'μ' without 's'
                    }
                } else {
                    return None; // Unknown unit
                };

                total_nanos += num * unit;
            }
        }

        if !current_num.is_empty() {
            // Trailing number without unit
            return None;
        }

        Some(total_nanos)
    }
}

impl ScalarUDFImpl for ParseDuration {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "parse_duration"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("parse_duration requires two arguments");
        }

        let value = &args.args[0];
        let as_seconds = &args.args[1];

        // Extract as_seconds boolean value
        let as_secs = match as_seconds {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Boolean(Some(b))) => *b,
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Boolean(None)) => false,
            _ => return exec_err!("Second argument must be a boolean"),
        };

        match value {
            ColumnarValue::Scalar(scalar) => {
                let result = match scalar {
                    datafusion::scalar::ScalarValue::Utf8(Some(s)) => Self::parse_duration_string(s.trim()).map(
                        |nanos| {
                            if as_secs { nanos / 1_000_000_000.0 } else { nanos }
                        },
                    ),
                    _ => None,
                };

                Ok(ColumnarValue::Scalar(result.map_or_else(
                    || datafusion::scalar::ScalarValue::Float64(None),
                    |v| datafusion::scalar::ScalarValue::Float64(Some(v)),
                )))
            }
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))?;

                let result: Float64Array = string_array
                    .iter()
                    .map(|opt_str| {
                        opt_str
                            .and_then(Self::parse_duration_string)
                            .map(|nanos| if as_secs { nanos / 1_000_000_000.0 } else { nanos })
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_numeric_valid() {
        assert_eq!(ParseNumeric::new().name(), "parse_numeric");
    }

    #[test]
    fn test_parse_bytes_basic_units() {
        assert_eq!(ParseBytes::parse_byte_string("10B"), Some(10.0));
        assert_eq!(ParseBytes::parse_byte_string("10KB"), Some(10240.0));
        assert_eq!(ParseBytes::parse_byte_string("10MB"), Some(10_485_760.0));
    }

    #[test]
    fn test_parse_bytes_case_insensitive() {
        assert_eq!(ParseBytes::parse_byte_string("10kb"), Some(10240.0));
        assert_eq!(ParseBytes::parse_byte_string("10Kb"), Some(10240.0));
        assert_eq!(ParseBytes::parse_byte_string("10KB"), Some(10240.0));
    }

    #[test]
    fn test_parse_duration_seconds() {
        let result = ParseDuration::parse_duration_string("5s");
        assert_eq!(result, Some(5_000_000_000.0));
    }

    #[test]
    fn test_parse_duration_compound() {
        let result = ParseDuration::parse_duration_string("1h30m45s");
        assert_eq!(result, Some(5_445_000_000_000.0)); // 1h + 30m + 45s in nanoseconds
    }

    #[test]
    fn test_parse_duration_milliseconds() {
        let result = ParseDuration::parse_duration_string("500ms");
        assert_eq!(result, Some(500_000_000.0));
    }
}
