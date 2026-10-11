//! The system-index state of one tenant, and the transitions writes apply to it.
//!
//! Transitions are pure and deterministic: the same operation applied to the same snapshot yields
//! the same outcome. The store relies on it when it re-applies an operation after a compare-and-swap
//! that did not land.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::opensearch::error::OpenSearchError;

/// Characters a system-index name must not contain.
const INVALID_INDEX_NAME_CHARACTERS: [char; 12] = ['\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':'];
/// Longest system-index name, in bytes.
const MAX_INDEX_NAME_BYTES: usize = 255;

/// Everything one tenant keeps in its system indices: the state one snapshot object holds.
///
/// Which operations produced a snapshot is recorded in the lineage of its snapshot object, never
/// here, so the snapshot is exactly the state a reader sees.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct SystemSnapshot {
    /// The system indices, by name.
    indices: BTreeMap<String, SystemIndex>,
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
}

/// What applying an operation did.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SystemOutcome {
    /// The index `name` was created.
    IndexCreated {
        /// The created index.
        name: String,
    },
}

impl SystemSnapshot {
    /// Apply `operation`, or refuse it and leave the snapshot unchanged.
    ///
    /// # Errors
    ///
    /// Returns `resource_already_exists_exception` when creating an index that exists.
    pub(crate) fn apply(&mut self, operation: &SystemOperation) -> Result<SystemOutcome, OpenSearchError> {
        match operation {
            SystemOperation::CreateIndex {
                name,
                mappings,
                settings,
            } => self.create_index(name, mappings, settings),
        }
    }

    /// The system index named `name`, if it exists.
    pub(crate) fn find_index(&self, name: &str) -> Option<&SystemIndex> {
        self.indices.get(name)
    }

    /// Create `name` with `mappings` and `settings`, refused when it exists.
    fn create_index(
        &mut self,
        name: &str,
        mappings: &Value,
        settings: &Value,
    ) -> Result<SystemOutcome, OpenSearchError> {
        if self.indices.contains_key(name) {
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
}
