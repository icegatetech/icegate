//! Fixed-point quantization for rate columns.
//!
//! `icegate.prices` stores every rate as an Iceberg `Decimal(38,10)` so money is
//! exact — binary `f64` cannot represent values like `0.075`, and a float `SUM`
//! over a cost query accumulates error. The crawler's internals stay `f64`, so
//! this module is the one boundary that maps between the two:
//!
//! - [`rate_to_unscaled`] / [`unscaled_to_rate`] encode/decode the Arrow
//!   `Decimal128` unscaled `i128`.
//! - [`quantize_rate`] snaps an `f64` onto the same decimal grid *before* the
//!   diff runs.
//!
//! The last point is load-bearing, not cosmetic. Live rates read back from
//! storage are already on the grid; a freshly fetched `3.0` arrives as
//! `3.0000000000000004`. Without quantizing the fresh value first, `rates_differ`
//! would see them as different and append a spurious revision on *every* crawl,
//! destroying the "steady state commits nothing" property. Quantizing here and
//! encoding in the writer both derive from the same rounding, so a stored value
//! and its re-fetched twin compare equal (see the round-trip test).

use icegate_common::schema::{PRICE_DECIMAL_PRECISION, PRICE_DECIMAL_SCALE};

use crate::error::{MaintainError, Result};
use crate::pricing::source::RateObservation;

/// Arrow decimal precision for the rate columns, from the Iceberg schema.
/// The cast is exact: 38 fits `u8`, and `Decimal128` caps precision at 38.
#[allow(clippy::cast_possible_truncation)]
pub const RATE_PRECISION: u8 = PRICE_DECIMAL_PRECISION as u8;
/// Arrow decimal scale for the rate columns, from the Iceberg schema.
/// The cast is exact: 10 fits `i8`.
#[allow(clippy::cast_possible_truncation)]
pub const RATE_SCALE: i8 = PRICE_DECIMAL_SCALE as i8;

/// `10^scale` as an `f64`, the factor between a rate and its unscaled integer.
fn scale_factor() -> f64 {
    10f64.powi(i32::from(RATE_SCALE))
}

/// How far a rate may sit from the nearest grid point and still count as "on the
/// grid, modulo `f64` noise" rather than carrying real sub-grid precision.
///
/// A tenth of one grid step (`10^-scale`). The `f64` representation error of a
/// scaled rate at the guard ceiling is ~0.026 of a step, so a tenth of a step
/// clears the noise with margin while still being far below the half-step that
/// separates two grid points. Derived from the scale so it tracks a scale change.
fn grid_tolerance() -> f64 {
    0.1 / scale_factor()
}

/// Snap a rate onto the `Decimal(_, scale)` grid it will be stored on.
///
/// Idempotent: `quantize_rate(quantize_rate(v)) == quantize_rate(v)`. Only
/// removes `f64` noise for a grid-representable value; a value with real
/// sub-grid precision is rejected upstream by [`carries_subgrid_precision`]
/// before it reaches here, so this never silently discards meaningful digits.
#[must_use]
pub fn quantize_rate(value: f64) -> f64 {
    (value * scale_factor()).round() / scale_factor()
}

/// Whether `value` carries more precision than the storage grid can hold.
///
/// True when snapping to the grid would change the value by more than `f64`
/// noise — i.e. the rate genuinely has finer-than-`scale` digits, which storing
/// it would silently drop. The pricing guard rejects such rows rather than
/// rounding them silently. Non-finite values are *not* this function's concern
/// (the row guards reject them first); it only distinguishes noise from real
/// sub-grid content, so it treats a non-finite input as "not sub-grid".
#[must_use]
pub fn carries_subgrid_precision(value: f64) -> bool {
    value.is_finite() && (value - quantize_rate(value)).abs() > grid_tolerance()
}

/// Encode a rate as the unscaled `i128` an Arrow `Decimal128` column holds.
///
/// # Errors
///
/// Returns [`MaintainError::InvariantViolation`] if `value` is not finite or
/// scales beyond the `i128` range. Both are unreachable in the crawl — the row
/// guards reject non-finite and above-ceiling rates, and a real rate scaled by
/// `10^10` is nowhere near `i128::MAX` — so this is defence in depth against the
/// silent saturation an `as i128` cast would otherwise do (`NaN` → 0, ∞ →
/// `i128::MAX`), which on a money path would turn a broken rate into a plausible
/// number.
pub fn rate_to_unscaled(value: f64) -> Result<i128> {
    if !value.is_finite() {
        return Err(MaintainError::InvariantViolation(format!(
            "rate {value} is not finite; refusing to encode it as a decimal"
        )));
    }
    let scaled = (value * scale_factor()).round();
    if !scaled.is_finite() || scaled >= i128_max_as_f64() || scaled <= i128_min_as_f64() {
        return Err(MaintainError::InvariantViolation(format!(
            "rate {value} scales beyond the representable decimal range"
        )));
    }
    #[allow(clippy::cast_possible_truncation)] // bounds checked above; deliberate f64 -> fixed-point boundary
    Ok(scaled as i128)
}

/// `i128::MAX` as `f64` (rounds to `2^127`), the upper cast bound.
#[allow(clippy::cast_precision_loss)]
const fn i128_max_as_f64() -> f64 {
    i128::MAX as f64
}

/// `i128::MIN` as `f64` (rounds to `-2^127`), the lower cast bound.
#[allow(clippy::cast_precision_loss)]
const fn i128_min_as_f64() -> f64 {
    i128::MIN as f64
}

/// Decode an unscaled `i128` from a `Decimal128` column back to a rate.
#[must_use]
#[allow(clippy::cast_precision_loss)] // deliberate fixed-point -> f64 boundary; real rates use far fewer than 52 bits
pub fn unscaled_to_rate(unscaled: i128) -> f64 {
    unscaled as f64 / scale_factor()
}

/// Quantize every rate column of `rate` in place, before it reaches the diff.
pub fn quantize_observation(rate: &mut RateObservation) {
    for column in [
        &mut rate.input_usd_per_1m,
        &mut rate.output_usd_per_1m,
        &mut rate.cache_read_usd_per_1m,
        &mut rate.cache_write_usd_per_1m,
        &mut rate.reasoning_usd_per_1m,
        &mut rate.request_usd,
        &mut rate.image_input_usd_per_unit,
        &mut rate.image_output_usd_per_unit,
        &mut rate.audio_input_usd_per_second,
        &mut rate.audio_output_usd_per_second,
    ] {
        if let Some(value) = column {
            *column = Some(quantize_rate(*value));
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]

    use super::{carries_subgrid_precision, quantize_rate, rate_to_unscaled, unscaled_to_rate};

    #[test]
    fn quantize_removes_the_float_multiplication_artifact() {
        // The exact symptom the user reported: a rate that should be 3.0 arrives
        // carrying a mantissa artifact one ULP above it. Quantization snaps it
        // back onto the grid.
        let artifact = 3.000_000_000_000_000_4_f64;
        assert_ne!(artifact, 3.0, "the artifact is a distinct f64 before quantization");
        assert_eq!(quantize_rate(artifact), 3.0);
    }

    #[test]
    fn quantize_is_exact_for_a_value_f64_cannot_represent() {
        // 0.075 has no exact binary representation; the grid value must still be
        // the intended 0.075, i.e. decode(encode(0.075)) == 0.075.
        assert_eq!(unscaled_to_rate(rate_to_unscaled(0.075).expect("in range")), 0.075);
    }

    #[test]
    fn quantize_is_idempotent() {
        for v in [0.0, 3.0, 0.075, 749.999_999_999_9, 2.5e-5] {
            assert_eq!(quantize_rate(quantize_rate(v)), quantize_rate(v));
        }
    }

    #[test]
    fn quantize_matches_the_writer_reader_round_trip() {
        // The diff-stability guarantee: the value quantize_rate produces must
        // equal what the writer stores and the reader reads back. If these ever
        // diverge, every crawl appends a spurious revision.
        for v in [0.0, 3.0, 15.0, 0.075, 0.000_3, 3.000_000_000_000_000_4] {
            let stored_then_read = unscaled_to_rate(rate_to_unscaled(v).expect("in range"));
            assert_eq!(
                quantize_rate(v),
                stored_then_read,
                "quantize and store/read must agree for {v}"
            );
        }
    }

    #[test]
    fn zero_stays_zero() {
        // A free model is priced, not unpriced; 0.0 must survive the grid.
        assert_eq!(quantize_rate(0.0), 0.0);
        assert_eq!(unscaled_to_rate(rate_to_unscaled(0.0).expect("in range")), 0.0);
    }

    #[test]
    fn encode_rejects_non_finite_rather_than_silently_zeroing() {
        // `f64::NAN as i128` is 0 and `INFINITY as i128` saturates to i128::MAX —
        // both silent on the money path. The fallible encode must refuse them.
        assert!(rate_to_unscaled(f64::NAN).is_err());
        assert!(rate_to_unscaled(f64::INFINITY).is_err());
        assert!(rate_to_unscaled(f64::NEG_INFINITY).is_err());
    }

    #[test]
    fn encode_rejects_a_value_that_scales_beyond_i128() {
        // Far above the guard ceiling, but proves the range check fires rather
        // than the cast saturating silently.
        assert!(rate_to_unscaled(1e30).is_err());
    }

    #[test]
    fn grid_representable_values_do_not_count_as_subgrid() {
        // Clean rates and pure f64 artifacts are on the grid modulo noise.
        for v in [
            0.0,
            3.0,
            0.075,
            15.0,
            0.000_3,
            3.000_000_000_000_000_4,
            9_999.123_456_789,
        ] {
            assert!(!carries_subgrid_precision(v), "{v} is grid-representable, not sub-grid");
        }
    }

    #[test]
    fn genuine_eleventh_digit_precision_is_flagged_as_subgrid() {
        // A rate carrying an 11th fractional digit cannot be stored at scale 10;
        // it must be flagged so the guard rejects the row rather than rounding.
        assert!(carries_subgrid_precision(0.000_000_000_05));
        assert!(carries_subgrid_precision(3.000_000_000_05));
    }
}
