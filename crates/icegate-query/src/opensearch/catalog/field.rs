//! The fields an index exposes, as `_field_caps` and `_mapping` report them, and a field name
//! resolved against one index.

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::Expr;
use datafusion::prelude::ident;
use icegate_common::schema::{COL_RESOURCE_ATTRIBUTES, promoted_resource_keys};

use super::encoding::{DerivedField, EncodedField, expand_profile_pattern};
use super::{IndexEntry, TenantSession, find_profile_table};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::glob::matches_glob;

/// An `OpenSearch` field type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldType {
    /// Analyzed full text: searchable, not aggregatable.
    Text,
    /// An exact string.
    Keyword,
    /// A point in time.
    Date,
    /// A 32-bit integer.
    Integer,
    /// A 64-bit integer.
    Long,
    /// A floating-point or decimal number.
    Double,
    /// `true` or `false`.
    Boolean,
    /// A structured value: present in `_source`, not itself searchable or aggregatable.
    Object,
}

impl FieldType {
    /// The type's `OpenSearch` name.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Keyword => "keyword",
            Self::Date => "date",
            Self::Integer => "integer",
            Self::Long => "long",
            Self::Double => "double",
            Self::Boolean => "boolean",
            Self::Object => "object",
        }
    }
}

/// One field of an index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexField {
    /// The name clients use. For a MAP key it is `<column>.<stored key>`.
    pub(crate) name: String,
    /// The `OpenSearch` type.
    pub(crate) field_type: FieldType,
    /// Queries may filter on the field.
    pub(crate) is_searchable: bool,
    /// Aggregations and sorts may use the field.
    pub(crate) is_aggregatable: bool,
    /// The structural field this one sits under: the MAP column of a key. Dots inside a stored
    /// key are never structure.
    pub(crate) parent: Option<String>,
}

impl IndexField {
    /// A searchable scalar field, aggregatable unless it is `text`.
    pub(crate) fn leaf(name: String, field_type: FieldType, parent: Option<String>) -> Self {
        Self {
            name,
            is_searchable: true,
            is_aggregatable: field_type != FieldType::Text,
            field_type,
            parent,
        }
    }

    /// An `object` field: present in `_source`, neither searchable nor aggregatable itself.
    pub(crate) const fn object(name: String) -> Self {
        Self {
            name,
            field_type: FieldType::Object,
            is_searchable: false,
            is_aggregatable: false,
            parent: None,
        }
    }
}

/// A field name resolved against one index: what the name means there and how to read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedField {
    /// The name the request used, kept for error reasons.
    pub(crate) name: String,
    /// The `OpenSearch` type the name has in that index.
    pub(crate) field_type: FieldType,
    /// Where the value is stored.
    pub(crate) value: FieldValue,
}

/// Where a resolved field's value is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FieldValue {
    /// A column, read as stored.
    Column {
        /// The column name.
        column: String,
    },
    /// One key of a `MAP<string, string>` column, looked up by its exact stored key. Dots in
    /// the key are part of the key.
    MapKey {
        /// The MAP column.
        column: String,
        /// The stored key.
        key: String,
    },
    /// A fixed-width binary id column, matched and rendered as lowercase hex of `width` bytes.
    HexId {
        /// The column name.
        column: String,
        /// The column's width in bytes.
        width: i32,
    },
    /// A stored value under a reversible conversion; filters lower through its inverse.
    Encoded(EncodedField),
    /// A per-row expression over stored columns.
    Derived(DerivedField),
}

impl FieldValue {
    /// The expression reading this value from a scan of its table.
    ///
    /// Column names go through `ident`, never through `col`: `col` parses a dotted name as a
    /// qualified reference, and no column name may pass through DataFusion's identifier
    /// normalization (names match case-sensitively).
    pub(crate) fn build_read_expr(&self) -> Expr {
        match self {
            Self::Column { column } | Self::HexId { column, .. } => ident(column),
            Self::MapKey { column, key } => get_field(ident(column), key.as_str()),
            Self::Encoded(encoded_field) => encoded_field.build_read_expr(),
            Self::Derived(derived_field) => derived_field.build_read_expr(),
        }
    }
}

impl ResolvedField {
    /// The field as a filter or a sort reads it. An RFC 3339 time reads as its stored date column
    /// (date math, epoch-millisecond bounds and formats apply unchanged) and an escaped attribute
    /// key as its stored MAP key; every other value is unchanged.
    pub(crate) fn into_lowering_view(self) -> Self {
        let value = match self.value {
            FieldValue::Encoded(EncodedField::Rfc3339Time { column }) => FieldValue::Column {
                column: column.to_string(),
            },
            FieldValue::Encoded(EncodedField::EscapedMapKey { column, key }) => FieldValue::MapKey {
                column: column.to_string(),
                key,
            },
            other_value => other_value,
        };
        Self { value, ..self }
    }
}

/// What a field name or pattern of a full-text query selects in one index.
#[derive(Debug, Clone)]
pub(crate) enum SelectedField {
    /// One field, resolved like any field name.
    Field(ResolvedField),
    /// Every value of a MAP column: the pattern covers all of its key paths, so
    /// the column is matched as a whole, without enumerating keys, and a key that
    /// is only in the WAL is covered too.
    AllMapValues {
        /// The MAP column.
        column: String,
    },
}

impl TenantSession {
    /// The field `name` selects in `entry`, read literally (`*` and `?` are plain
    /// characters): the field [`TenantSession::resolve_field`] resolves, or nothing
    /// when unmapped.
    pub(crate) fn select_named_field(&self, entry: &IndexEntry, name: &str) -> Vec<SelectedField> {
        self.resolve_field(entry, name).map(SelectedField::Field).into_iter().collect()
    }

    /// The searchable fields `pattern` selects in `entry`.
    ///
    /// A name without `*` or `?` selects like [`TenantSession::select_named_field`].
    /// A pattern selects, by glob: every column except the tenant column and list
    /// or struct columns; one [`SelectedField::AllMapValues`] per MAP column whose
    /// every key path it covers (`*`, `log_attributes.*`, `log_*`); and the column
    /// of each promoted resource key whose attribute path it matches, so
    /// `resource_attributes.*` reaches `service_name` where ingest moved that key
    /// out of the map. On an index of a non-native profile the pattern selects among
    /// the profile's fields instead, under the same MAP rule (see
    /// `expand_profile_pattern`).
    ///
    /// # Errors
    ///
    /// `illegal_argument` (400) when the pattern matches some but not every key
    /// path of a MAP column: which keys exist is not known without enumerating
    /// them, and a silently partial selection would return wrong rows. Any error of
    /// [`TenantSession::scan_index`] and [`TenantSession::table_schema`].
    pub(crate) fn expand_field_pattern(
        &self,
        entry: &IndexEntry,
        pattern: &str,
    ) -> Result<Vec<SelectedField>, OpenSearchError> {
        if !pattern.contains(['*', '?']) {
            return Ok(self.select_named_field(entry, pattern));
        }
        if let Some(table) = find_profile_table(entry) {
            return expand_profile_pattern(table, &*self.table_schema(entry)?, pattern);
        }
        let scan = self.scan_index(entry)?;
        let mut selected_fields = Vec::new();
        for field in scan.schema().fields() {
            let column = field.name();
            match field.data_type() {
                DataType::Map(..) => {
                    if covers_every_key(pattern, column) {
                        selected_fields.push(SelectedField::AllMapValues { column: column.clone() });
                    } else if glob_matches_some_extension(pattern, &format!("{column}.")) {
                        return Err(OpenSearchError::illegal_argument(format!(
                            "field pattern [{pattern}] selects some keys of [{column}]: name one key, or select \
                             every key with [{column}.*]"
                        )));
                    }
                }
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) | DataType::Struct(_) => {}
                _ => {
                    if matches_glob(pattern, column)
                        && let Some(resolved_field) = self.resolve_field(entry, column)
                    {
                        selected_fields.push(SelectedField::Field(resolved_field));
                    }
                }
            }
        }
        for promoted_key in promoted_resource_keys(entry.table()) {
            let is_selected = selected_fields
                .iter()
                .any(|candidate| matches!(candidate, SelectedField::Field(field) if field.name == promoted_key.column));
            if !is_selected
                && matches_glob(
                    pattern,
                    &format!("{COL_RESOURCE_ATTRIBUTES}.{}", promoted_key.attribute_key),
                )
                && let Some(resolved_field) = self.resolve_field(entry, promoted_key.column)
            {
                selected_fields.push(SelectedField::Field(resolved_field));
            }
        }
        Ok(selected_fields)
    }
}

/// Whether `pattern` matches every key path `<column>.<key>`: it ends with `*`
/// and matches `<column>.` itself, so its final `*` absorbs any key.
pub(super) fn covers_every_key(pattern: &str, column: &str) -> bool {
    pattern.ends_with('*') && matches_glob(pattern, &format!("{column}."))
}

/// Whether some text starting with `prefix` matches the glob `pattern` (`*` any
/// run, `?` one character). After `prefix` is consumed the pattern can still
/// match exactly when some pattern position is reachable, because any remaining
/// glob suffix matches some text.
pub(super) fn glob_matches_some_extension(pattern: &str, prefix: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let mut reachable_positions = vec![false; pattern.len() + 1];
    reachable_positions[0] = true;
    skip_stars(&pattern, &mut reachable_positions);
    for character in prefix.chars() {
        let mut next_positions = vec![false; pattern.len() + 1];
        for (position, glob_character) in pattern.iter().enumerate() {
            if !reachable_positions[position] {
                continue;
            }
            match glob_character {
                '*' => next_positions[position] = true,
                '?' => next_positions[position + 1] = true,
                literal if *literal == character => next_positions[position + 1] = true,
                _ => {}
            }
        }
        skip_stars(&pattern, &mut next_positions);
        reachable_positions = next_positions;
    }
    reachable_positions.contains(&true)
}

/// A `*` may match nothing: every position reaching a `*` also reaches the one after it.
fn skip_stars(pattern: &[char], reachable_positions: &mut [bool]) {
    for position in 0..pattern.len() {
        if reachable_positions[position] && pattern[position] == '*' {
            reachable_positions[position + 1] = true;
        }
    }
}

#[cfg(test)]
mod resolution_tests {
    use datafusion::arrow::datatypes::Schema;
    use icegate_common::schema::{
        COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SERVICE_INSTANCE_ID, COL_SERVICE_NAME, COL_TRACE_ID,
        logs_schema, metrics_schema, spans_schema,
    };
    use icegate_common::{LOGS_TABLE, METRICS_TABLE, SPANS_TABLE};
    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng as _};

    use super::{FieldType, FieldValue, ResolvedField, covers_every_key, glob_matches_some_extension};
    use crate::opensearch::catalog::native::resolve_native_field;

    fn convert_arrow_schema(iceberg_schema: icegate_common::Result<iceberg::spec::Schema>) -> Schema {
        iceberg::arrow::schema_to_arrow_schema(&iceberg_schema.expect("iceberg schema")).expect("arrow schema")
    }

    fn build_column_field(name: &str, field_type: FieldType, column: &str) -> ResolvedField {
        ResolvedField {
            name: name.to_string(),
            field_type,
            value: FieldValue::Column {
                column: column.to_string(),
            },
        }
    }

    fn build_map_key_field(name: &str, column: &str, key: &str) -> ResolvedField {
        ResolvedField {
            name: name.to_string(),
            field_type: FieldType::Keyword,
            value: FieldValue::MapKey {
                column: column.to_string(),
                key: key.to_string(),
            },
        }
    }

    #[test]
    fn promoted_resource_keys_resolve_to_their_column_on_logs_and_metrics_and_to_the_map_on_spans() {
        let logs = convert_arrow_schema(logs_schema());
        for name in ["resource_attributes.service.name", "resource_attributes.service_name"] {
            assert_eq!(
                resolve_native_field(&logs, LOGS_TABLE, name),
                Some(build_column_field(name, FieldType::Keyword, COL_SERVICE_NAME)),
                "{name} on logs"
            );
        }

        let metrics = convert_arrow_schema(metrics_schema());
        for name in ["resource_attributes.service.name", "resource_attributes.service_name"] {
            assert_eq!(
                resolve_native_field(&metrics, METRICS_TABLE, name),
                Some(build_column_field(name, FieldType::Keyword, COL_SERVICE_NAME)),
                "{name} on metrics"
            );
        }
        for name in [
            "resource_attributes.service.instance.id",
            "resource_attributes.service_instance_id",
        ] {
            assert_eq!(
                resolve_native_field(&metrics, METRICS_TABLE, name),
                Some(build_column_field(name, FieldType::Keyword, COL_SERVICE_INSTANCE_ID)),
                "{name} on metrics"
            );
        }

        let spans = convert_arrow_schema(spans_schema());
        assert_eq!(
            resolve_native_field(&spans, SPANS_TABLE, "resource_attributes.service.name"),
            Some(build_map_key_field(
                "resource_attributes.service.name",
                COL_RESOURCE_ATTRIBUTES,
                "service.name"
            ))
        );
    }

    #[test]
    fn an_unmapped_field_resolves_to_nothing() {
        let logs = convert_arrow_schema(logs_schema());
        for name in [
            "no_such_field",
            "tenant_id",
            "body.inner",
            "resource_attributes.",
            "Severity_Text",
            "",
        ] {
            assert_eq!(resolve_native_field(&logs, LOGS_TABLE, name), None, "{name:?}");
        }
    }

    #[test]
    fn a_map_key_path_resolves_without_enumeration() {
        let logs = convert_arrow_schema(logs_schema());
        assert_eq!(
            resolve_native_field(&logs, LOGS_TABLE, "log_attributes.http.method"),
            Some(build_map_key_field(
                "log_attributes.http.method",
                COL_LOG_ATTRIBUTES,
                "http.method"
            ))
        );
        assert_eq!(
            resolve_native_field(&logs, LOGS_TABLE, "log_attributes.http"),
            Some(build_map_key_field("log_attributes.http", COL_LOG_ATTRIBUTES, "http"))
        );
        assert_eq!(
            resolve_native_field(&logs, LOGS_TABLE, "log_attributes.Never.Stored"),
            Some(build_map_key_field(
                "log_attributes.Never.Stored",
                COL_LOG_ATTRIBUTES,
                "Never.Stored"
            ))
        );
        assert_eq!(
            resolve_native_field(&logs, LOGS_TABLE, COL_TRACE_ID),
            Some(ResolvedField {
                name: COL_TRACE_ID.to_string(),
                field_type: FieldType::Keyword,
                value: FieldValue::HexId {
                    column: COL_TRACE_ID.to_string(),
                    width: 16,
                },
            })
        );
    }

    #[test]
    fn a_pattern_covering_every_key_of_a_map_is_told_apart_from_one_covering_some() {
        for pattern in ["*", "log_*", "log_attributes.*", "*.*", "l?g_attributes.*"] {
            assert!(
                covers_every_key(pattern, "log_attributes"),
                "[{pattern}] covers every key"
            );
        }
        // A leading `*` absorbs any prefix, so `*_attributes` matches the key path
        // `log_attributes.x_attributes`: some keys, not all.
        for pattern in [
            "log_attributes.http*",
            "*method",
            "log_attributes.?",
            "*_name",
            "*_attributes",
        ] {
            assert!(
                !covers_every_key(pattern, "log_attributes"),
                "[{pattern}] does not cover every key"
            );
            assert!(
                glob_matches_some_extension(pattern, "log_attributes."),
                "[{pattern}] covers some key"
            );
        }
        for pattern in ["service_*", "scope_attributes.*", "log_attributes", "resource_*"] {
            assert!(
                !glob_matches_some_extension(pattern, "log_attributes."),
                "[{pattern}] covers no key"
            );
        }
    }

    /// The characters patterns and texts are drawn from: two letters, the separator `.`, and
    /// the glob metacharacters, which only patterns hold.
    const EXTENSION_ALPHABET: [char; 3] = ['a', 'b', '.'];
    const GLOB_ALPHABET: [char; 5] = ['a', 'b', '.', '*', '?'];
    const EXTENSION_CASE_COUNT: usize = 2_000;

    /// The glob rule stated directly: `*` tries every split, `?` takes one character, any other
    /// character matches itself.
    fn matches_glob_naively(pattern: &[char], text: &[char]) -> bool {
        match pattern.split_first() {
            None => text.is_empty(),
            Some(('*', pattern_rest)) => {
                (0..=text.len()).any(|skipped_count| matches_glob_naively(pattern_rest, &text[skipped_count..]))
            }
            Some((glob_character, pattern_rest)) => text.split_first().is_some_and(|(character, text_rest)| {
                (*glob_character == '?' || glob_character == character) && matches_glob_naively(pattern_rest, text_rest)
            }),
        }
    }

    /// Whether some extension of `prefix` of at most `max_length` characters over
    /// [`EXTENSION_ALPHABET`] matches `pattern`. A pattern consumes at most one text character
    /// per non-`*` character, so when any extension matches, one of at most the pattern's length
    /// over the pattern's own literals does: with the pattern drawn from that alphabet, this
    /// enumeration decides the question exactly.
    fn enumerate_matching_extension(pattern: &[char], prefix: &mut Vec<char>, max_length: usize) -> bool {
        if matches_glob_naively(pattern, prefix) {
            return true;
        }
        if max_length == 0 {
            return false;
        }
        EXTENSION_ALPHABET.iter().any(|character| {
            prefix.push(*character);
            let is_found = enumerate_matching_extension(pattern, prefix, max_length - 1);
            prefix.pop();
            is_found
        })
    }

    #[test]
    fn some_extension_agrees_with_enumerating_extensions_on_seeded_patterns() {
        let mut generator = StdRng::seed_from_u64(0x00E5_7E0D);
        let mut found_count = 0;
        for _ in 0..EXTENSION_CASE_COUNT {
            let pattern: Vec<char> = (0..generator.random_range(0..=5))
                .map(|_| GLOB_ALPHABET[generator.random_range(0..GLOB_ALPHABET.len())])
                .collect();
            let prefix: Vec<char> = (0..generator.random_range(0..=4))
                .map(|_| EXTENSION_ALPHABET[generator.random_range(0..EXTENSION_ALPHABET.len())])
                .collect();
            let pattern_text: String = pattern.iter().collect();
            let prefix_text: String = prefix.iter().collect();
            let is_expected = enumerate_matching_extension(&pattern, &mut prefix.clone(), pattern.len());

            assert_eq!(
                glob_matches_some_extension(&pattern_text, &prefix_text),
                is_expected,
                "{pattern_text:?} after {prefix_text:?}"
            );
            found_count += usize::from(is_expected);
        }
        // Both answers occur often enough for the agreement to mean something.
        assert!(
            found_count >= EXTENSION_CASE_COUNT / 4,
            "{found_count} extensible cases"
        );
        assert!(
            EXTENSION_CASE_COUNT - found_count >= EXTENSION_CASE_COUNT / 4,
            "{found_count} extensible cases"
        );
    }
}
