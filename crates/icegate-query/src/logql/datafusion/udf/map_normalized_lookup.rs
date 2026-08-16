//! Normalized-key lookup into a `MAP<Utf8, Utf8>` attribute column.

use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, MapArray, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{Result, exec_err, plan_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
    scalar::ScalarValue,
};
use icegate_common::attribute_key::matches_wire_name;

/// UDF: `map_get_by_normalized_key(map, name)`.
///
/// Returns the value of the first entry whose key, with every `.` replaced by
/// `_`, equals `name`; NULL when no entry matches.
///
/// # Why normalization happens here
///
/// See [`icegate_common::attribute_key`] for the stored-key/wire-name mapping
/// and why every path resolving it shares one definition.
///
/// The mapping is one-way, so two stored keys can normalize alike. Entries are
/// scanned in stored order, which ingest guarantees is ascending lexicographic
/// (every map is built through a `BTreeMap`), making the winner deterministic.
///
/// An entry present with an empty-string value is *present* and wins over a
/// later match — callers rely on this for coalesce-based level precedence. An
/// entry with a NULL key or NULL value is *absent*: it does not claim the wire
/// name, so a later colliding key still wins it.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapGetByNormalizedKey {
    signature: Signature,
}

impl Default for MapGetByNormalizedKey {
    fn default() -> Self {
        Self::new()
    }
}

impl MapGetByNormalizedKey {
    /// Creates a new `MapGetByNormalizedKey` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapGetByNormalizedKey {
    fn name(&self) -> &'static str {
        "map_get_by_normalized_key"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("map_get_by_normalized_key requires two arguments: map, name");
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("map_get_by_normalized_key requires two arguments: map, name");
        }

        let target = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                return exec_err!("map_get_by_normalized_key name cannot be NULL");
            }
            _ => return exec_err!("map_get_by_normalized_key name must be a Utf8 scalar"),
        };

        let map_array: ArrayRef = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        let Some(map) = map_array.as_any().downcast_ref::<MapArray>() else {
            return exec_err!("map_get_by_normalized_key first argument must be a MAP column");
        };

        // The entries array and its two children are properties of the batch,
        // not of a row, so they are resolved once here. Row boundaries come
        // from `value_offsets`, which index into the (unsliced) children even
        // when the MapArray itself is a slice.
        let entries = map.entries();
        let Some(keys) = entries.column(0).as_any().downcast_ref::<StringArray>() else {
            return exec_err!("map_get_by_normalized_key expects Utf8 map keys");
        };
        let Some(values) = entries.column(1).as_any().downcast_ref::<StringArray>() else {
            return exec_err!("map_get_by_normalized_key expects Utf8 map values");
        };
        let offsets = map.value_offsets();

        let mut out = StringBuilder::with_capacity(map.len(), map.len() * 32);
        for row in 0..map.len() {
            if map.is_null(row) {
                out.append_null();
                continue;
            }
            #[allow(clippy::cast_sign_loss)]
            let start = offsets[row] as usize;
            #[allow(clippy::cast_sign_loss)]
            let end = offsets[row + 1] as usize;

            let mut hit = None;
            for i in start..end {
                // A NULL key or a NULL value makes the entry invisible, not
                // merely valueless: it does not claim the wire name, so a later
                // colliding key can still win it. See the module docs on
                // `icegate_common::attribute_key` for why every path resolving
                // a wire name has to agree on that.
                if keys.is_null(i) || values.is_null(i) {
                    continue;
                }
                if matches_wire_name(keys.value(i), &target) {
                    hit = Some(i);
                    break;
                }
            }
            match hit {
                Some(i) => out.append_value(values.value(i)),
                None => out.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::array::{Array, ArrayRef, MapArray, StringArray},
        logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl},
        scalar::ScalarValue,
    };

    use super::MapGetByNormalizedKey;

    /// One-row MAP<Utf8,Utf8> from sorted pairs, mirroring what ingest writes.
    ///
    /// Values are `Option` so the NULL-value contract can be exercised even
    /// though the Iceberg schema declares the value field `required` — the UDF
    /// defends against NULL regardless, and that defence has to be pinned.
    fn map_of(pairs: &[(&str, Option<&str>)]) -> ArrayRef {
        use datafusion::arrow::{
            array::StructArray,
            buffer::{OffsetBuffer, ScalarBuffer},
            datatypes::{DataType, Field, Fields},
        };
        let keys = StringArray::from(pairs.iter().map(|(k, _)| *k).collect::<Vec<_>>());
        let values = StringArray::from(pairs.iter().map(|(_, v)| *v).collect::<Vec<_>>());
        let fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ]
        .into();
        let entries = StructArray::new(
            fields.clone(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
            None,
        );
        let entry_field = Arc::new(Field::new("key_value", DataType::Struct(fields), false));
        let pair_count = i32::try_from(pairs.len()).expect("test fixture pair count fits in i32");
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, pair_count]));
        Arc::new(MapArray::new(entry_field, offsets, entries, None, false))
    }

    fn lookup(pairs: &[(&str, &str)], name: &str) -> Option<String> {
        let nullable: Vec<(&str, Option<&str>)> = pairs.iter().map(|(k, v)| (*k, Some(*v))).collect();
        lookup_nullable(&nullable, name)
    }

    fn lookup_nullable(pairs: &[(&str, Option<&str>)], name: &str) -> Option<String> {
        let udf = MapGetByNormalizedKey::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(map_of(pairs)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(name.to_string()))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                datafusion::arrow::datatypes::DataType::Utf8,
                true,
            )),
            config_options: Arc::new(datafusion::common::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).expect("invoke");
        let ColumnarValue::Array(arr) = out else {
            panic!("expected array output");
        };
        let arr = arr.as_any().downcast_ref::<StringArray>().expect("utf8 out");
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_string())
        }
    }

    #[test]
    fn matches_a_dotted_key_from_an_underscored_query_name() {
        assert_eq!(
            lookup(&[("k8s.pod.name", "web-1")], "k8s_pod_name"),
            Some("web-1".into())
        );
    }

    #[test]
    fn matches_an_already_underscored_key() {
        assert_eq!(lookup(&[("pod", "web-1")], "pod"), Some("web-1".into()));
    }

    #[test]
    fn returns_null_when_no_key_matches() {
        assert_eq!(lookup(&[("k8s.pod.name", "web-1")], "namespace"), None);
    }

    #[test]
    fn on_a_normalization_collision_the_first_key_in_order_wins() {
        // Ingest writes keys in ascending lexicographic order, so "k8s.pod.name"
        // precedes "k8s_pod_name" ('.' < '_'). The rule is first-in-order.
        assert_eq!(
            lookup(
                &[("k8s.pod.name", "dotted"), ("k8s_pod_name", "underscored")],
                "k8s_pod_name"
            ),
            Some("dotted".into())
        );
    }

    #[test]
    fn an_empty_string_value_counts_as_present() {
        assert_eq!(lookup(&[("http.method", "")], "http_method"), Some(String::new()));
    }

    #[test]
    fn a_null_valued_entry_is_absent_rather_than_a_null_result() {
        assert_eq!(lookup_nullable(&[("http.method", None)], "http_method"), None);
    }

    #[test]
    fn a_null_valued_entry_does_not_claim_the_wire_name_from_a_later_key() {
        // The counterpart of the collision test above: because the first
        // entry is invisible, the second wins instead of the lookup
        // short-circuiting to NULL. `merge_attribute_levels` and
        // `loki::formatters::extract_attributes_map` resolve the same row and
        // must reach the same value.
        assert_eq!(
            lookup_nullable(&[("http.method", None), ("http_method", Some("GET"))], "http_method"),
            Some("GET".into())
        );
    }
}
