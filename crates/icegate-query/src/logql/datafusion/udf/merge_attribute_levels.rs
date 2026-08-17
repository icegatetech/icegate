//! Merge the per-level attribute maps into one wire-shaped map.

use std::{
    borrow::Cow,
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, MapArray, StringArray, StringBuilder, StructArray},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, Field, Fields},
    },
    common::{Result, exec_err, plan_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};
use icegate_common::attribute_key::normalize_attribute_key;

/// UDF: `merge_attribute_levels(resource, scope, log)`.
///
/// Collapses the three per-level attribute maps into the single flat map the
/// Loki wire format exposes, keys rendered as wire names by
/// [`icegate_common::attribute_key`]. Two precedence rules apply, and they
/// point in opposite directions:
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
/// One `BTreeMap` expresses both rules by storing the index of the level that
/// wrote each entry: an entry owned by an earlier level is overwritten, one
/// owned by the current level is kept.
///
/// An entry whose key or value is NULL is *absent* — it does not claim the
/// wire name, so a later colliding key still wins it. This is the same rule
/// `map_get_by_normalized_key` and `loki::formatters::extract_attributes_map`
/// apply, and it has to stay that way for the reason given above.
///
/// Series identity, `by`/`without` grouping, and key serialization all operate
/// on one map, so merging here keeps that logic identical to the pre-split
/// version instead of triplicating it.
///
/// Output keys are sorted: grouping serializes them into a string, so a stable
/// order is required for two identical label sets to group together.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MergeAttributeLevels {
    signature: Signature,
}

impl Default for MergeAttributeLevels {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeAttributeLevels {
    /// Creates a new `MergeAttributeLevels` UDF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MergeAttributeLevels {
    fn name(&self) -> &'static str {
        "merge_attribute_levels"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("merge_attribute_levels requires three map arguments");
        }
        Ok(map_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return exec_err!("merge_attribute_levels requires three map arguments");
        }

        let mut levels: Vec<ArrayRef> = Vec::with_capacity(3);
        for arg in &args.args {
            levels.push(match arg {
                ColumnarValue::Array(a) => Arc::clone(a),
                ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
            });
        }

        // The entries arrays and their children are properties of the batch,
        // not of a row, so each level is resolved once here rather than on
        // every iteration. Row boundaries come from `value_offsets`, which
        // index into the (unsliced) children even for a sliced MapArray.
        let mut resolved_levels: Vec<(&MapArray, &StringArray, &StringArray)> = Vec::with_capacity(levels.len());
        for level in &levels {
            let Some(map) = level.as_any().downcast_ref::<MapArray>() else {
                return exec_err!("merge_attribute_levels arguments must be MAP columns");
            };
            let entries = map.entries();
            let Some(keys) = entries.column(0).as_any().downcast_ref::<StringArray>() else {
                return exec_err!("merge_attribute_levels expects Utf8 map keys");
            };
            let Some(values) = entries.column(1).as_any().downcast_ref::<StringArray>() else {
                return exec_err!("merge_attribute_levels expects Utf8 map values");
            };
            resolved_levels.push((map, keys, values));
        }

        // One entry per level per row is a deliberate over-estimate of the
        // distinct merged keys; it costs one allocation instead of growing the
        // buffers repeatedly across a whole batch.
        let entry_estimate = args.number_rows * resolved_levels.len();
        let mut keys_out = StringBuilder::with_capacity(entry_estimate, entry_estimate * 32);
        let mut values_out = StringBuilder::with_capacity(entry_estimate, entry_estimate * 32);
        let mut offsets: Vec<i32> = Vec::with_capacity(args.number_rows + 1);
        offsets.push(0);
        let mut total: i32 = 0;

        // Reused across rows: `clear` keeps the allocated nodes, so a batch
        // pays for one map rather than one per row. Borrowed keys and values
        // point straight into the Arrow buffers, so a merged row allocates
        // only for a key that actually carries a `.`.
        //
        // The stored level index is what lets ONE map express both rules at
        // once: an entry written by an EARLIER level is overwritten (cross
        // level, most specific wins), while one written by the CURRENT level
        // is kept (intra level, first raw key in stored order wins). A second
        // per-level map would need its keys cloned into this one.
        let mut merged: BTreeMap<Cow<'_, str>, (usize, &str)> = BTreeMap::new();

        for row in 0..args.number_rows {
            merged.clear();
            for (level_idx, &(map, keys, values)) in resolved_levels.iter().enumerate() {
                if map.is_null(row) {
                    continue;
                }
                #[allow(clippy::cast_sign_loss)]
                let start = map.value_offsets()[row] as usize;
                #[allow(clippy::cast_sign_loss)]
                let end = map.value_offsets()[row + 1] as usize;

                for i in start..end {
                    // A NULL key or value makes the entry invisible, exactly
                    // as in the lookup UDF and the formatter: it does not
                    // claim the wire name, so a later colliding key still
                    // wins it.
                    if keys.is_null(i) || values.is_null(i) {
                        continue;
                    }
                    match merged.entry(normalize_attribute_key(keys.value(i))) {
                        Entry::Vacant(slot) => {
                            slot.insert((level_idx, values.value(i)));
                        }
                        Entry::Occupied(mut slot) if slot.get().0 != level_idx => {
                            slot.insert((level_idx, values.value(i)));
                        }
                        Entry::Occupied(_) => {}
                    }
                }
            }

            for (key, (_, value)) in &merged {
                keys_out.append_value(key);
                values_out.append_value(value);
            }
            let row_entries = i32::try_from(merged.len()).map_err(|_| {
                datafusion::error::DataFusionError::Execution("attribute map too large for i32 offsets".into())
            })?;
            // The accumulator, not the per-row count, is what can overflow —
            // and a wrapped `total` would make the offsets non-monotonic and
            // trip `OffsetBuffer::new`'s assertion, i.e. panic where this
            // error exists to return cleanly instead.
            total = total.checked_add(row_entries).ok_or_else(|| {
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

    use super::MergeAttributeLevels;

    /// Values are `Option` so the NULL-value contract can be exercised even
    /// though the Iceberg schema declares the value field `required` — the UDF
    /// defends against NULL regardless, and that defence must agree with
    /// `map_get_by_normalized_key`'s.
    fn map_of_nullable(pairs: &[(&str, Option<&str>)]) -> ArrayRef {
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
        fn to_nullable<'a>(pairs: &[(&'a str, &'a str)]) -> Vec<(&'a str, Option<&'a str>)> {
            pairs.iter().map(|(k, v)| (*k, Some(*v))).collect()
        }
        merge_arrays(&to_nullable(resource), &to_nullable(scope), &to_nullable(log))
    }

    fn merge_arrays(
        resource: &[(&str, Option<&str>)],
        scope: &[(&str, Option<&str>)],
        log: &[(&str, Option<&str>)],
    ) -> std::collections::BTreeMap<String, String> {
        let udf = MergeAttributeLevels::new();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(map_of_nullable(resource)),
                ColumnarValue::Array(map_of_nullable(scope)),
                ColumnarValue::Array(map_of_nullable(log)),
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

    #[test]
    fn a_null_valued_entry_is_absent_and_does_not_shadow_a_broader_level() {
        // The agreement with `map_get_by_normalized_key`: there, a NULL-valued
        // `pod` in log_attributes lets the coalesce fall through to resource,
        // so the row MATCHES {pod="web-1"}. If the merge stored `pod -> ""`
        // instead, the matched row would then be displayed and grouped as
        // `pod=""` — matching on one value while showing another.
        let merged = merge_arrays(&[("pod", Some("web-1"))], &[], &[("pod", None)]);
        assert_eq!(merged.get("pod").map(String::as_str), Some("web-1"));
    }

    #[test]
    fn a_null_valued_entry_does_not_claim_the_wire_name_from_a_later_key() {
        // Intra-level counterpart, mirroring the lookup UDF's test of the same
        // name: the invisible entry must not consume the first-wins slot.
        let merged = merge_arrays(&[], &[], &[("http.method", None), ("http_method", Some("GET"))]);
        assert_eq!(merged.get("http_method").map(String::as_str), Some("GET"));
    }
}
