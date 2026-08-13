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

/// UDF: `map_get_by_normalized_key(map, name)`.
///
/// Returns the value of the first entry whose key, with every `.` replaced by
/// `_`, equals `name`; NULL when no entry matches.
///
/// # Why normalization happens here
///
/// Attribute keys are stored in OTel-native dotted form, but Loki label names
/// are restricted to `[a-zA-Z_][a-zA-Z0-9_]*` — a dotted or colon-bearing label
/// name is rejected by upstream Loki. Queries therefore address `k8s.pod.name`
/// as `k8s_pod_name`, and the mapping is resolved at read time.
///
/// The mapping is one-way, so two stored keys can normalize alike. Entries are
/// scanned in stored order, which ingest guarantees is ascending lexicographic
/// (every map is built through a `BTreeMap`), making the winner deterministic.
///
/// An entry present with an empty-string value is *present* and wins over a
/// later match — callers rely on this for coalesce-based level precedence.
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

        let mut out = StringBuilder::with_capacity(map.len(), map.len() * 32);
        for row in 0..map.len() {
            if map.is_null(row) {
                out.append_null();
                continue;
            }
            let entries = map.value(row);
            let Some(keys) = entries.column(0).as_any().downcast_ref::<StringArray>() else {
                return exec_err!("map_get_by_normalized_key expects Utf8 map keys");
            };
            let Some(values) = entries.column(1).as_any().downcast_ref::<StringArray>() else {
                return exec_err!("map_get_by_normalized_key expects Utf8 map values");
            };

            let mut hit = None;
            for i in 0..keys.len() {
                if keys.is_null(i) {
                    continue;
                }
                if normalize_key(keys.value(i)) == target {
                    hit = Some(i);
                    break;
                }
            }
            match hit {
                Some(i) if !values.is_null(i) => out.append_value(values.value(i)),
                _ => out.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Render a stored attribute key as the Loki label name that addresses it.
///
/// Allocation-free for the common already-underscored key.
fn normalize_key(key: &str) -> std::borrow::Cow<'_, str> {
    if key.contains('.') {
        std::borrow::Cow::Owned(key.replace('.', "_"))
    } else {
        std::borrow::Cow::Borrowed(key)
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
    fn map_of(pairs: &[(&str, &str)]) -> ArrayRef {
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
}
