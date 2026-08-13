//! Merge the per-level attribute maps into one wire-shaped map.

use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, MapArray, StringArray, StringBuilder, StructArray},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, Field, Fields},
    },
    common::{Result, exec_err, plan_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};

/// UDF: `map_merge_normalized(resource, scope, log)`.
///
/// Collapses the three per-level attribute maps into the single flat map the
/// Loki wire format exposes, keys normalized to `[a-zA-Z_][a-zA-Z0-9_]*` by
/// replacing `.` with `_`. Two precedence rules apply, and they point in
/// opposite directions:
///
/// - **Across levels**: applied resource -> scope -> log, so the most
///   specific level wins — the same precedence the matcher path resolves
///   through `coalesce`. A level is more or less specific *context*, so the
///   narrower one shadows the broader one.
/// - **Within a level**: when two distinct raw (dotted) keys normalize to the
///   same wire name — e.g. `k8s.pod.name` and `k8s_pod_name` both present in
///   `resource_attributes` — the FIRST raw key in that level's stored order
///   wins. This is not precedence but ambiguity: ingest dedupes by the raw
///   dotted-key string, never by normalized form, so both keys can
///   legitimately coexist in one level's map. The tie-break must match
///   `map_get_by_normalized_key`'s own scan-and-break rule exactly, because
///   that UDF resolves the matcher path over the same data — if the two
///   disagreed on a collision, a row could match a query through one value
///   while its series identity and displayed labels showed a different value
///   for the same label.
///
/// A single `BTreeMap` pass cannot express both rules at once: the
/// within-level rule needs first-insert-wins, the cross-level rule needs
/// last-insert-wins. Each level is therefore resolved against its own
/// collisions first, and only the per-level results are merged across levels.
///
/// Series identity, `by`/`without` grouping, and key serialization all operate
/// on one map, so merging here keeps that logic identical to the pre-split
/// version instead of triplicating it.
///
/// Output keys are sorted: grouping serializes them into a string, so a stable
/// order is required for two identical label sets to group together.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapMergeNormalized {
    signature: Signature,
}

impl Default for MapMergeNormalized {
    fn default() -> Self {
        Self::new()
    }
}

impl MapMergeNormalized {
    /// Creates a new `MapMergeNormalized` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapMergeNormalized {
    fn name(&self) -> &'static str {
        "map_merge_normalized"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("map_merge_normalized requires three map arguments");
        }
        Ok(map_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return exec_err!("map_merge_normalized requires three map arguments");
        }

        let mut levels: Vec<ArrayRef> = Vec::with_capacity(3);
        for arg in &args.args {
            levels.push(match arg {
                ColumnarValue::Array(a) => Arc::clone(a),
                ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
            });
        }

        let mut keys_out = StringBuilder::new();
        let mut values_out = StringBuilder::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(args.number_rows + 1);
        offsets.push(0);
        let mut total: i32 = 0;

        for row in 0..args.number_rows {
            // Cross-level pass: each level's own collisions are resolved
            // first (below), then later levels overwrite earlier ones here —
            // resource -> scope -> log, so BTreeMap::extend's
            // overwrite-on-collision semantics give log the win. BTreeMap
            // also gives the sorted output order.
            let mut merged: BTreeMap<String, String> = BTreeMap::new();
            for level in &levels {
                let Some(map) = level.as_any().downcast_ref::<MapArray>() else {
                    return exec_err!("map_merge_normalized arguments must be MAP columns");
                };
                if map.is_null(row) {
                    continue;
                }
                let entries = map.value(row);
                let Some(keys) = entries.column(0).as_any().downcast_ref::<StringArray>() else {
                    return exec_err!("map_merge_normalized expects Utf8 map keys");
                };
                let Some(values) = entries.column(1).as_any().downcast_ref::<StringArray>() else {
                    return exec_err!("map_merge_normalized expects Utf8 map values");
                };

                // Intra-level pass: entry(...).or_insert_with(...) only
                // writes on the first sight of a normalized key, so within
                // THIS level the first raw key in stored order wins on a
                // collision — matching map_get_by_normalized_key's
                // scan-and-break tie-break (see the struct doc for why the
                // two rules must agree).
                let mut level_resolved: BTreeMap<String, String> = BTreeMap::new();
                for i in 0..keys.len() {
                    if keys.is_null(i) {
                        continue;
                    }
                    let value = if values.is_null(i) { "" } else { values.value(i) };
                    level_resolved
                        .entry(keys.value(i).replace('.', "_"))
                        .or_insert_with(|| value.to_string());
                }
                merged.extend(level_resolved);
            }

            for (key, value) in &merged {
                keys_out.append_value(key);
                values_out.append_value(value);
            }
            total += i32::try_from(merged.len()).map_err(|_| {
                datafusion::error::DataFusionError::Execution("attribute map too large for i32 offsets".into())
            })?;
            offsets.push(total);
        }

        let entries = StructArray::new(
            entry_fields(),
            vec![
                Arc::new(keys_out.finish()) as ArrayRef,
                Arc::new(values_out.finish()) as ArrayRef,
            ],
            None,
        );
        let map = MapArray::new(
            Arc::new(Field::new("key_value", DataType::Struct(entry_fields()), false)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            entries,
            None,
            false,
        );
        Ok(ColumnarValue::Array(Arc::new(map)))
    }
}

/// Entry struct of the merged map, matching the ingest-side field names.
fn entry_fields() -> Fields {
    vec![
        Arc::new(Field::new("key", DataType::Utf8, false)),
        Arc::new(Field::new("value", DataType::Utf8, true)),
    ]
    .into()
}

/// Declared return type of the merged map.
fn map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new("key_value", DataType::Struct(entry_fields()), false)),
        false,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::array::{Array, ArrayRef, MapArray, StringArray},
        logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl},
    };

    use super::MapMergeNormalized;

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

    fn merge(
        resource: &[(&str, &str)],
        scope: &[(&str, &str)],
        log: &[(&str, &str)],
    ) -> std::collections::BTreeMap<String, String> {
        let udf = MapMergeNormalized::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(map_of(resource)),
                ColumnarValue::Array(map_of(scope)),
                ColumnarValue::Array(map_of(log)),
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
        let ColumnarValue::Array(arr) = udf.invoke_with_args(args).expect("invoke") else {
            panic!("expected array output");
        };
        let map = arr.as_any().downcast_ref::<MapArray>().expect("map out");
        let entries = map.value(0);
        let keys = entries.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
        let values = entries.column(1).as_any().downcast_ref::<StringArray>().expect("values");
        (0..keys.len())
            .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
            .collect()
    }

    #[test]
    fn normalizes_keys_to_wire_names() {
        let out = merge(&[("k8s.pod.name", "web-1")], &[], &[]);
        assert_eq!(out.get("k8s_pod_name").map(String::as_str), Some("web-1"));
        assert!(!out.contains_key("k8s.pod.name"));
    }

    #[test]
    fn on_an_intra_level_normalization_collision_the_first_key_in_order_wins() {
        // Same fixture shape as map_get_by_normalized_key's identical test:
        // ingest dedupes by the raw dotted-key string, never by normalized
        // form, so "k8s.pod.name" and "k8s_pod_name" can legitimately
        // coexist in one level's stored map ('.' < '_', so ingest's
        // ascending order places the dotted key first). The two UDFs MUST
        // agree on the winner, or a row could match a query through one
        // value while displaying a different value for the same label.
        let out = merge(&[("k8s.pod.name", "dotted"), ("k8s_pod_name", "underscored")], &[], &[]);
        assert_eq!(out.get("k8s_pod_name").map(String::as_str), Some("dotted"));
        assert_eq!(out.len(), 1, "one wire name yields one entry");
    }

    #[test]
    fn log_wins_over_scope_which_wins_over_resource() {
        let out = merge(
            &[("shared.key", "resource")],
            &[("shared.key", "scope")],
            &[("shared.key", "log")],
        );
        assert_eq!(out.get("shared_key").map(String::as_str), Some("log"));
        assert_eq!(out.len(), 1, "one wire name yields one entry");
    }

    #[test]
    fn scope_wins_when_the_log_level_is_absent() {
        let out = merge(&[("shared.key", "resource")], &[("shared.key", "scope")], &[]);
        assert_eq!(out.get("shared_key").map(String::as_str), Some("scope"));
    }

    #[test]
    fn keys_from_all_levels_are_unioned() {
        let out = merge(&[("a.one", "1")], &[("b.two", "2")], &[("c.three", "3")]);
        assert_eq!(out.len(), 3);
        assert!(out.contains_key("a_one") && out.contains_key("b_two") && out.contains_key("c_three"));
    }

    #[test]
    fn output_keys_are_sorted_so_grouping_is_deterministic() {
        let out = merge(&[("z.last", "z")], &[("a.first", "a")], &[]);
        let keys: Vec<&str> = out.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["a_first", "z_last"]);
    }
}
