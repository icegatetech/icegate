//! Hex literals for fixed-width binary columns.
//!
//! Every front end (Query DSL, Lucene, PPL) turns a hex id a client typed into the column's own
//! `FixedSizeBinary(n)` scalar here. The width comes from the column's resolved Arrow type,
//! never from a column-name table. A `Binary` literal would compare through a cast, and neither
//! the WAL Parquet scan nor the Iceberg scan can prune with a cast. The Iceberg predicate
//! converter maps exactly this scalar to a `Fixed` datum
//! (`engine/provider/expr_to_predicate.rs`).

use datafusion::arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;

/// Why a hex literal does not denote a value of a fixed-width binary column.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum FixedBinaryLiteralError {
    /// The text is not an even-length run of hex digits.
    #[error("not an even-length hex string")]
    InvalidHex,
    /// The text decodes to a byte count other than the column's width.
    #[error("expected {expected} bytes, got {actual}")]
    WrongLength {
        /// The column's width in bytes.
        expected: i32,
        /// The number of bytes the text decodes to.
        actual: usize,
    },
    /// The column is not a `FixedSizeBinary` column.
    #[error("the column is not a fixed-width binary column")]
    NotFixedSizeBinary,
}

/// Decode `text`, hex digits in either case, to a `FixedSizeBinary` scalar of `data_type`'s width.
///
/// # Errors
///
/// Returns [`FixedBinaryLiteralError::NotFixedSizeBinary`] when `data_type` is not
/// `FixedSizeBinary`, [`FixedBinaryLiteralError::InvalidHex`] when `text` is not an even-length
/// hex string, and [`FixedBinaryLiteralError::WrongLength`] when it decodes to another number of
/// bytes than the width.
pub(crate) fn decode_fixed_binary_literal(
    text: &str,
    data_type: &DataType,
) -> Result<ScalarValue, FixedBinaryLiteralError> {
    let DataType::FixedSizeBinary(width) = data_type else {
        return Err(FixedBinaryLiteralError::NotFixedSizeBinary);
    };
    let bytes = hex::decode(text).map_err(|_| FixedBinaryLiteralError::InvalidHex)?;
    if usize::try_from(*width).ok() != Some(bytes.len()) {
        return Err(FixedBinaryLiteralError::WrongLength {
            expected: *width,
            actual: bytes.len(),
        });
    }
    Ok(ScalarValue::FixedSizeBinary(*width, Some(bytes)))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::scalar::ScalarValue;

    use super::{FixedBinaryLiteralError, decode_fixed_binary_literal};

    #[test]
    fn hex_literals_decode_to_the_columns_fixed_width_in_either_case() {
        let trace_id_type = DataType::FixedSizeBinary(16);
        let trace_id = ScalarValue::FixedSizeBinary(
            16,
            Some(vec![
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
            ]),
        );
        assert_eq!(
            decode_fixed_binary_literal("0123456789abcdeffedcba9876543210", &trace_id_type),
            Ok(trace_id.clone())
        );
        assert_eq!(
            decode_fixed_binary_literal("0123456789ABCDEFFEDCBA9876543210", &trace_id_type),
            Ok(trace_id)
        );
        assert_eq!(
            decode_fixed_binary_literal("00000000000000fF", &DataType::FixedSizeBinary(8)),
            Ok(ScalarValue::FixedSizeBinary(8, Some(vec![0, 0, 0, 0, 0, 0, 0, 0xff])))
        );
    }

    #[test]
    fn invalid_hex_or_a_wrong_length_is_refused() {
        let span_id_type = DataType::FixedSizeBinary(8);
        assert_eq!(
            decode_fixed_binary_literal("zz00000000000000", &span_id_type),
            Err(FixedBinaryLiteralError::InvalidHex)
        );
        assert_eq!(
            decode_fixed_binary_literal("abc", &span_id_type),
            Err(FixedBinaryLiteralError::InvalidHex)
        );
        assert_eq!(
            decode_fixed_binary_literal("0123", &span_id_type),
            Err(FixedBinaryLiteralError::WrongLength { expected: 8, actual: 2 })
        );
        assert_eq!(
            decode_fixed_binary_literal("", &span_id_type),
            Err(FixedBinaryLiteralError::WrongLength { expected: 8, actual: 0 })
        );
        assert_eq!(
            decode_fixed_binary_literal("0123456789abcdeffedcba9876543210", &span_id_type),
            Err(FixedBinaryLiteralError::WrongLength {
                expected: 8,
                actual: 16
            })
        );
        assert_eq!(
            decode_fixed_binary_literal("0123456789abcdef", &DataType::Utf8),
            Err(FixedBinaryLiteralError::NotFixedSizeBinary)
        );
    }
}
