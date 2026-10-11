//! The system-index state of one tenant, and the transitions writes apply to it.
//!
//! Transitions are pure and deterministic: the same operation applied to the same snapshot yields
//! the same outcome. The store relies on it when it re-applies an operation after a compare-and-swap
//! that did not land.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::opensearch::error::OpenSearchError;

/// Characters a system-index name must not contain.
const INVALID_INDEX_NAME_CHARACTERS: [char; 12] = ['\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':'];
/// Longest system-index name, in bytes.
const MAX_INDEX_NAME_BYTES: usize = 255;
/// Mapping types a field can be searched and aggregated on.
const AGGREGATABLE_TYPES: [&str; 14] = [
    "keyword",
    "date",
    "date_nanos",
    "long",
    "integer",
    "short",
    "byte",
    "double",
    "float",
    "half_float",
    "scaled_float",
    "unsigned_long",
    "boolean",
    "ip",
];
/// Mapping types a field can be searched on but not aggregated on.
const SEARCH_ONLY_TYPES: [&str; 2] = ["text", "match_only_text"];

/// Everything one tenant keeps in its system indices: the state one snapshot object holds.
///
/// Which operations produced a snapshot is recorded in the lineage of its snapshot object, never
/// here, so the snapshot is exactly the state a reader sees.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct SystemSnapshot {
    /// The system indices, by name.
    indices: BTreeMap<String, SystemIndex>,
    /// Each alias and the one concrete index it names. An alias and an index never share a name.
    #[serde(default)]
    aliases: BTreeMap<String, String>,
}

/// One system index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SystemIndex {
    /// The mappings the index was created with, stored and returned verbatim: `dynamic`, `_meta`,
    /// unknown keys and key order included.
    pub(crate) mappings: Value,
    /// The settings the index was created with, stored and returned verbatim.
    pub(crate) settings: Value,
}

impl SystemIndex {
    /// The fields the stored mapping declares, depth first in mapping order. A property without
    /// a `type` is an `object`; object and nested properties list their children after them, and
    /// multi-fields follow their field.
    pub(crate) fn list_mapped_fields(&self) -> Vec<MappedField> {
        let mut fields = Vec::new();
        if let Some(properties) = self.mappings.get("properties").and_then(Value::as_object) {
            collect_mapped_fields(properties, "", &mut fields);
        }
        fields
    }
}

/// Append the fields of `properties`, named under `prefix`, to `fields`.
fn collect_mapped_fields(properties: &Map<String, Value>, prefix: &str, fields: &mut Vec<MappedField>) {
    for (key, mapping) in properties {
        let name = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        let type_name = mapping.get("type").and_then(Value::as_str).unwrap_or("object");
        let is_aggregatable = AGGREGATABLE_TYPES.contains(&type_name);
        fields.push(MappedField {
            name: name.clone(),
            type_name: type_name.to_string(),
            is_searchable: is_aggregatable || SEARCH_ONLY_TYPES.contains(&type_name),
            is_aggregatable,
        });
        if let Some(multi_fields) = mapping.get("fields").and_then(Value::as_object) {
            collect_mapped_fields(multi_fields, &name, fields);
        }
        if let Some(children) = mapping.get("properties").and_then(Value::as_object) {
            collect_mapped_fields(children, &name, fields);
        }
    }
}

/// A change a write asks the store to apply to a tenant's snapshot.
#[derive(Debug, Clone)]
pub(crate) enum SystemOperation {
    /// Create the system index `name`, refused when it exists.
    CreateIndex {
        /// A name [`validate_system_index_name`] accepts.
        name: String,
        /// The mappings to store verbatim.
        mappings: Value,
        /// The settings to store verbatim.
        settings: Value,
    },
    /// Apply alias actions in order, all of them or none.
    ApplyAliasActions {
        /// The actions, in request order.
        actions: Vec<AliasAction>,
    },
}

/// What applying an operation did.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SystemOutcome {
    /// The index `name` was created.
    IndexCreated {
        /// The created index.
        name: String,
    },
    /// Every alias action of a request was applied.
    AliasesApplied,
}

impl SystemSnapshot {
    /// Apply `operation`, or refuse it and leave the snapshot unchanged.
    ///
    /// # Errors
    ///
    /// Returns `resource_already_exists_exception` when creating an index whose name an index or
    /// an alias carries. For alias actions: `index_not_found_exception` for an action naming an
    /// index that does not exist (or that an earlier action of the request removed), and
    /// `illegal_argument_exception` for an alias name no system index may carry, an alias named
    /// like an index, an alias that already names another index, or a removal of an alias that
    /// does not name the given index.
    pub(crate) fn apply(&mut self, operation: &SystemOperation) -> Result<SystemOutcome, OpenSearchError> {
        match operation {
            SystemOperation::CreateIndex {
                name,
                mappings,
                settings,
            } => self.create_index(name, mappings, settings),
            SystemOperation::ApplyAliasActions { actions } => self.apply_alias_actions(actions),
        }
    }

    /// The system index named `name`, if it exists. An alias name finds nothing.
    pub(crate) fn find_index(&self, name: &str) -> Option<&SystemIndex> {
        self.indices.get(name)
    }

    /// The concrete index `name` addresses: an index name addresses itself, an alias its single
    /// index.
    pub(crate) fn resolve_name(&self, name: &str) -> Option<&str> {
        if let Some((index, _)) = self.indices.get_key_value(name) {
            return Some(index.as_str());
        }
        self.aliases.get(name).map(String::as_str)
    }

    /// Every index name, in name order.
    pub(crate) fn index_names(&self) -> impl Iterator<Item = &str> {
        self.indices.keys().map(String::as_str)
    }

    /// Every alias name, in name order.
    pub(crate) fn alias_names(&self) -> impl Iterator<Item = &str> {
        self.aliases.keys().map(String::as_str)
    }

    /// The aliases naming `index`, in name order.
    pub(crate) fn list_aliases_of<'a>(&'a self, index: &'a str) -> impl Iterator<Item = &'a str> {
        self.aliases
            .iter()
            .filter(move |(_, aliased_index)| aliased_index.as_str() == index)
            .map(|(alias, _)| alias.as_str())
    }

    /// Create `name` with `mappings` and `settings`, refused when an index or an alias carries the
    /// name.
    fn create_index(
        &mut self,
        name: &str,
        mappings: &Value,
        settings: &Value,
    ) -> Result<SystemOutcome, OpenSearchError> {
        if self.indices.contains_key(name) || self.aliases.contains_key(name) {
            return Err(OpenSearchError::resource_already_exists(name));
        }
        self.indices.insert(
            name.to_string(),
            SystemIndex {
                mappings: mappings.clone(),
                settings: settings.clone(),
            },
        );
        Ok(SystemOutcome::IndexCreated { name: name.to_string() })
    }

    /// Apply `actions` in order against a working copy of the alias map and of the removed
    /// indices; commit only when every action applied. The indices themselves are never copied.
    fn apply_alias_actions(&mut self, actions: &[AliasAction]) -> Result<SystemOutcome, OpenSearchError> {
        let mut aliases = self.aliases.clone();
        let mut removed_indices: BTreeSet<&str> = BTreeSet::new();
        let has_index = |name: &str, removed_indices: &BTreeSet<&str>| {
            self.indices.contains_key(name) && !removed_indices.contains(name)
        };
        for action in actions {
            match action {
                AliasAction::Add { index, alias } => {
                    if !has_index(index, &removed_indices) {
                        return Err(OpenSearchError::index_not_found(index));
                    }
                    validate_system_index_name(alias)?;
                    if has_index(alias, &removed_indices) {
                        return Err(OpenSearchError::illegal_argument(format!(
                            "an index exists with the same name as the alias [{alias}]"
                        )));
                    }
                    if let Some(aliased_index) = aliases.get(alias)
                        && aliased_index != index
                    {
                        return Err(OpenSearchError::illegal_argument(format!(
                            "alias [{alias}] already names index [{aliased_index}]; an alias names one index"
                        )));
                    }
                    aliases.insert(alias.clone(), index.clone());
                }
                AliasAction::Remove { index, alias } => {
                    if !has_index(index, &removed_indices) {
                        return Err(OpenSearchError::index_not_found(index));
                    }
                    if aliases.get(alias) != Some(index) {
                        return Err(OpenSearchError::illegal_argument(format!(
                            "alias [{alias}] is not set on index [{index}]"
                        )));
                    }
                    aliases.remove(alias);
                }
                AliasAction::RemoveIndex { index } => {
                    if !has_index(index, &removed_indices) {
                        return Err(OpenSearchError::index_not_found(index));
                    }
                    removed_indices.insert(index.as_str());
                    aliases.retain(|_, aliased_index| aliased_index != index);
                }
            }
        }
        for index in removed_indices {
            self.indices.remove(index);
        }
        self.aliases = aliases;
        Ok(SystemOutcome::AliasesApplied)
    }
}

/// One action of an `_aliases` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AliasAction {
    /// Make `alias` name `index`.
    Add {
        /// An existing system index.
        index: String,
        /// A name [`validate_system_index_name`] accepts and no index carries.
        alias: String,
    },
    /// Remove `alias`, which must name `index`.
    Remove {
        /// The index the alias names.
        index: String,
        /// The alias to remove.
        alias: String,
    },
    /// Delete `index` and every alias naming it.
    RemoveIndex {
        /// An existing system index.
        index: String,
    },
}

/// One field a stored mapping declares, as `_field_caps` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MappedField {
    /// The dotted path of the field: object and nested properties and multi-fields joined with `.`.
    pub(crate) name: String,
    /// The mapping type; `object` for a property without one.
    pub(crate) type_name: String,
    /// Whether a query can match on the field.
    pub(crate) is_searchable: bool,
    /// Whether an aggregation can bucket or measure the field.
    pub(crate) is_aggregatable: bool,
}

/// Refuse a name no system index may carry: it must start with `.`, must not be `.` or `..`, must
/// be lower case, hold none of the characters an index name may not hold, and fit the length
/// bound.
///
/// # Errors
///
/// Returns `illegal_argument_exception` naming `name`.
pub(crate) fn validate_system_index_name(name: &str) -> Result<(), OpenSearchError> {
    let is_valid = name.starts_with('.')
        && name != "."
        && name != ".."
        && name.len() <= MAX_INDEX_NAME_BYTES
        && !name
            .chars()
            .any(|character| character.is_uppercase() || INVALID_INDEX_NAME_CHARACTERS.contains(&character));
    if is_valid {
        Ok(())
    } else {
        Err(OpenSearchError::illegal_argument(format!(
            "invalid system index name [{name}]: it must start with '.', be lower case, be at most \
             {MAX_INDEX_NAME_BYTES} bytes, not be '.' or '..', and contain none of {INVALID_INDEX_NAME_CHARACTERS:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::*;
    use crate::opensearch::error::render_envelope;

    fn build_index_creation(name: &str, mappings: Value, settings: Value) -> SystemOperation {
        SystemOperation::CreateIndex {
            name: name.to_string(),
            mappings,
            settings,
        }
    }

    #[tokio::test]
    async fn creating_an_existing_index_is_resource_already_exists() {
        let mut snapshot = SystemSnapshot::default();
        snapshot
            .apply(&build_index_creation(
                ".kibana_1",
                json!({"dynamic": "strict"}),
                json!({}),
            ))
            .expect("the first create");
        let snapshot_before_refusal = snapshot.clone();

        let error = snapshot
            .apply(&build_index_creation(".kibana_1", json!({"dynamic": false}), json!({})))
            .expect_err("the second create is refused");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "resource_already_exists_exception");
        assert_eq!(body["error"]["index"], ".kibana_1");
        assert_eq!(snapshot, snapshot_before_refusal, "a refused operation changes nothing");
    }

    /// Stored mappings are compared byte for byte (`serde_json` keeps key order in this crate):
    /// the store returns them exactly as they were created, key order included.
    #[test]
    fn mappings_and_settings_are_returned_exactly_as_stored() {
        let mappings_text = r#"{"dynamic":"strict","properties":{"type":{"type":"keyword"},"migrationVersion":{"dynamic":"true","type":"object"},"references":{"type":"nested","properties":{"id":{"type":"keyword"}}}},"_meta":{"migrationMappingPropertyHashes":{"type":"2f4316de49999235636386fe51dc06c1","references":"7997cf5a56cc02bdc9c93361bde732b0"}},"unknown_key":[1,"two",null]}"#;
        let settings_text = r#"{"number_of_shards":1,"auto_expand_replicas":"0-1"}"#;
        let mut snapshot = SystemSnapshot::default();

        let outcome = snapshot
            .apply(&build_index_creation(
                ".kibana_1",
                serde_json::from_str(mappings_text).expect("mappings JSON"),
                serde_json::from_str(settings_text).expect("settings JSON"),
            ))
            .expect("create");

        assert_eq!(
            outcome,
            SystemOutcome::IndexCreated {
                name: ".kibana_1".to_string()
            }
        );
        let index = snapshot.find_index(".kibana_1").expect("the created index");
        assert_eq!(serde_json::to_string(&index.mappings).expect("mappings"), mappings_text);
        assert_eq!(serde_json::to_string(&index.settings).expect("settings"), settings_text);
        assert!(snapshot.find_index(".kibana").is_none());
    }

    #[tokio::test]
    async fn an_invalid_system_index_name_is_refused() {
        let too_long_name = format!(".{}", "a".repeat(255));
        for name in [
            "kibana",
            ".",
            "..",
            ".Kibana",
            ".kib*",
            ".a,b",
            ".a b",
            ".a:b",
            ".a/b",
            ".a#b",
            too_long_name.as_str(),
        ] {
            let error = validate_system_index_name(name).expect_err(name);

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{name}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{name}");
        }
        let longest_name = format!(".{}", "a".repeat(254));
        for name in [
            ".kibana",
            ".kibana_1",
            ".opensearch_dashboards_config",
            longest_name.as_str(),
        ] {
            assert!(validate_system_index_name(name).is_ok(), "{name}");
        }
    }

    fn build_two_indices() -> SystemSnapshot {
        let mut snapshot = SystemSnapshot::default();
        for name in [".kibana_1", ".kibana_2"] {
            snapshot
                .apply(&build_index_creation(name, json!({"dynamic": "strict"}), json!({})))
                .expect("create");
        }
        snapshot
    }

    fn apply_actions(
        snapshot: &mut SystemSnapshot,
        actions: Vec<AliasAction>,
    ) -> Result<SystemOutcome, OpenSearchError> {
        snapshot.apply(&SystemOperation::ApplyAliasActions { actions })
    }

    fn build_add_action(index: &str, alias: &str) -> AliasAction {
        AliasAction::Add {
            index: index.to_string(),
            alias: alias.to_string(),
        }
    }

    fn build_remove_action(index: &str, alias: &str) -> AliasAction {
        AliasAction::Remove {
            index: index.to_string(),
            alias: alias.to_string(),
        }
    }

    fn build_remove_index_action(index: &str) -> AliasAction {
        AliasAction::RemoveIndex {
            index: index.to_string(),
        }
    }

    /// A swap is one request of a removal and an addition: both apply, or neither does.
    #[tokio::test]
    async fn alias_actions_apply_atomically_or_not_at_all() {
        let mut snapshot = build_two_indices();
        apply_actions(&mut snapshot, vec![build_add_action(".kibana_1", ".kibana")]).expect("add");

        let swap_outcome = apply_actions(
            &mut snapshot,
            vec![
                build_remove_action(".kibana_1", ".kibana"),
                build_add_action(".kibana_2", ".kibana"),
            ],
        );

        assert_eq!(swap_outcome.expect("swap"), SystemOutcome::AliasesApplied);
        assert_eq!(snapshot.resolve_name(".kibana"), Some(".kibana_2"));
        let snapshot_before_refusal = snapshot.clone();
        let refused_outcome = apply_actions(
            &mut snapshot,
            vec![
                build_add_action(".kibana_2", ".kibana_extra"),
                build_add_action(".missing", ".other"),
            ],
        );
        let (status, body) =
            render_envelope(refused_outcome.expect_err("the second action names a missing index")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["index"], ".missing");
        assert_eq!(
            snapshot, snapshot_before_refusal,
            "the first action of a refused request is not applied"
        );
        assert_eq!(snapshot.resolve_name(".kibana_extra"), None);
        let unset_alias_outcome = apply_actions(&mut snapshot, vec![build_remove_action(".kibana_1", ".kibana")]);
        let (status, _) = render_envelope(unset_alias_outcome.expect_err("the alias names another index")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn remove_index_drops_the_index_and_its_aliases() {
        let mut snapshot = build_two_indices();
        apply_actions(
            &mut snapshot,
            vec![
                build_add_action(".kibana_1", ".kibana"),
                build_add_action(".kibana_1", ".kibana_old"),
                build_add_action(".kibana_2", ".kibana_new"),
            ],
        )
        .expect("add");

        apply_actions(&mut snapshot, vec![build_remove_index_action(".kibana_1")]).expect("remove_index");

        assert!(snapshot.find_index(".kibana_1").is_none());
        assert_eq!(snapshot.index_names().collect::<Vec<_>>(), [".kibana_2"]);
        assert_eq!(snapshot.alias_names().collect::<Vec<_>>(), [".kibana_new"]);
    }

    /// An alias names exactly one index, and a name is an index or an alias, never both. Removing
    /// an index frees its name for an alias in the same request.
    #[tokio::test]
    async fn an_alias_resolves_to_its_single_concrete_index() {
        let mut snapshot = build_two_indices();
        apply_actions(&mut snapshot, vec![build_add_action(".kibana_1", ".kibana")]).expect("add");

        assert_eq!(snapshot.resolve_name(".kibana"), Some(".kibana_1"));
        assert_eq!(snapshot.resolve_name(".kibana_1"), Some(".kibana_1"));
        assert_eq!(snapshot.resolve_name(".missing"), None);
        assert_eq!(snapshot.list_aliases_of(".kibana_1").collect::<Vec<_>>(), [".kibana"]);
        let second_index_outcome = apply_actions(&mut snapshot, vec![build_add_action(".kibana_2", ".kibana")]);
        let (status, _) = render_envelope(second_index_outcome.expect_err("one index per alias")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let alias_over_index_outcome = apply_actions(&mut snapshot, vec![build_add_action(".kibana_1", ".kibana_2")]);
        let (status, _) = render_envelope(alias_over_index_outcome.expect_err("an index has the name")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let index_over_alias_outcome = snapshot.apply(&build_index_creation(".kibana", json!({}), json!({})));
        let (status, body) = render_envelope(index_over_alias_outcome.expect_err("an alias has the name")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "resource_already_exists_exception");
        apply_actions(
            &mut snapshot,
            vec![
                build_remove_index_action(".kibana_2"),
                build_add_action(".kibana_1", ".kibana_2"),
            ],
        )
        .expect("a removed index's name is free in the same request");
        assert_eq!(snapshot.resolve_name(".kibana_2"), Some(".kibana_1"));
    }

    /// Every action that names a missing index is the missing-index `404`, a removed index
    /// included, and an alias name no system index may carry is refused; a refused request leaves
    /// the snapshot unchanged.
    #[tokio::test]
    async fn an_action_naming_a_missing_index_or_an_invalid_alias_name_is_refused() {
        let mut snapshot = build_two_indices();
        apply_actions(&mut snapshot, vec![build_add_action(".kibana_1", ".kibana")]).expect("add");
        let snapshot_before_refusals = snapshot.clone();
        let missing_index_cases = [
            vec![build_remove_action(".missing", ".kibana")],
            vec![build_remove_index_action(".missing")],
            vec![
                build_remove_index_action(".kibana_2"),
                build_remove_index_action(".kibana_2"),
            ],
            vec![
                build_remove_index_action(".kibana_2"),
                build_add_action(".kibana_2", ".kibana_new"),
            ],
        ];

        for actions in missing_index_cases {
            let refusal = apply_actions(&mut snapshot, actions.clone()).expect_err("a missing index");

            let (status, body) = render_envelope(refusal).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{actions:?}");
            assert_eq!(body["error"]["type"], "index_not_found_exception", "{actions:?}");
        }
        for alias in ["kibana", ".Kibana", ".a,b"] {
            let refusal = apply_actions(&mut snapshot, vec![build_add_action(".kibana_1", alias)])
                .expect_err("an invalid alias name");

            let (status, body) = render_envelope(refusal).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{alias}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{alias}");
        }
        assert_eq!(snapshot, snapshot_before_refusals);
    }

    #[test]
    fn stored_mappings_list_their_fields_with_capabilities() {
        let mappings = json!({
            "dynamic": "strict",
            "properties": {
                "type": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "references": {"type": "nested", "properties": {"id": {"type": "keyword"}}},
                "migrationVersion": {"dynamic": "true", "type": "object"},
                "config": {"dynamic": false, "properties": {}}
            }
        });
        let mut snapshot = SystemSnapshot::default();
        snapshot
            .apply(&build_index_creation(".kibana_1", mappings, json!({})))
            .expect("create");

        let fields = snapshot.find_index(".kibana_1").expect("index").list_mapped_fields();

        let build_field = |name: &str, type_name: &str, is_searchable: bool, is_aggregatable: bool| MappedField {
            name: name.to_string(),
            type_name: type_name.to_string(),
            is_searchable,
            is_aggregatable,
        };
        assert_eq!(
            fields,
            [
                build_field("type", "keyword", true, true),
                build_field("title", "text", true, false),
                build_field("title.keyword", "keyword", true, true),
                build_field("references", "nested", false, false),
                build_field("references.id", "keyword", true, true),
                build_field("migrationVersion", "object", false, false),
                build_field("config", "object", false, false),
            ]
        );
    }
}
