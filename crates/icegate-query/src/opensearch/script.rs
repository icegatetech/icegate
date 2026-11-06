//! Script recognition. IceGate runs no Painless: a script is accepted only when it is one of the
//! forms recognized here, and the recognized form is executed natively. Any other script is a
//! `400 illegal_argument_exception` naming it, because running a different computation than the
//! client asked for would return wrong data.

use serde_json::{Map, Value};

use super::error::OpenSearchError;

/// The saved-object counter script, with every whitespace run collapsed to one space.
const INCREMENT_COUNTER_SOURCE: &str = "if (ctx._source[params.type][params.counterFieldName] == null) { ctx._source[params.type][params.counterFieldName] = params.count; } else { ctx._source[params.type][params.counterFieldName] += params.count; } ctx._source.updated_at = params.time;";

/// The parameter names the counter script reads; any other key is refused.
const INCREMENT_COUNTER_PARAMETERS: [&str; 4] = ["count", "time", "type", "counterFieldName"];

/// A recognized document-update script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DocumentScript {
    /// Adds `count` to one counter of a saved object and stamps `updated_at`.
    IncrementCounter(CounterIncrement),
}

/// The parameters of the counter script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CounterIncrement {
    /// `params.type`: the top-level `_source` key holding the saved object's attributes.
    pub(crate) object_type: String,
    /// `params.counterFieldName`: the counter inside that object.
    pub(crate) counter_field_name: String,
    /// `params.count`: the amount added.
    pub(crate) count: i64,
    /// `params.time`: the value written to `updated_at`.
    pub(crate) time: String,
}

impl CounterIncrement {
    /// Run the script on an existing document's `_source`.
    ///
    /// An absent or `null` counter is set to `count`; an integer counter gets `count` added;
    /// `updated_at` is set to `time`.
    ///
    /// # Errors
    ///
    /// `illegal_argument_exception` when `_source[object_type]` is not an object, when the
    /// counter holds anything but an integer, or when the sum overflows. `source` is then left
    /// unchanged, so the script can run on the stored document itself.
    pub(crate) fn apply_to(&self, source: &mut Value) -> Result<(), OpenSearchError> {
        let document = source
            .as_object_mut()
            .ok_or_else(|| OpenSearchError::illegal_argument("a document source must be a JSON object"))?;
        let attributes = document
            .get_mut(&self.object_type)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                OpenSearchError::illegal_argument(format!(
                    "the counter script needs an object at [{}]",
                    self.object_type
                ))
            })?;
        let counter = match attributes.get(&self.counter_field_name) {
            None | Some(Value::Null) => self.count,
            Some(Value::Number(stored_count)) => stored_count
                .as_i64()
                .and_then(|stored_integer| stored_integer.checked_add(self.count))
                .ok_or_else(|| self.refuse_counter_value())?,
            Some(_) => return Err(self.refuse_counter_value()),
        };
        attributes.insert(self.counter_field_name.clone(), Value::from(counter));
        document.insert("updated_at".to_string(), Value::String(self.time.clone()));
        Ok(())
    }

    /// The refusal of a counter that does not hold an integer, or whose sum with `count` overflows.
    fn refuse_counter_value(&self) -> OpenSearchError {
        OpenSearchError::illegal_argument(format!(
            "the counter [{}.{}] does not hold an integer that can take [{}]",
            self.object_type, self.counter_field_name, self.count
        ))
    }

    /// Read the script's `params`, which hold exactly `count` (a JSON integer) and the strings
    /// `time`, `type` and `counterFieldName`. `script` is what a refusal names.
    ///
    /// # Errors
    ///
    /// `illegal_argument_exception` naming `script` for another key, a missing key or a value of
    /// the wrong kind.
    fn from_parameters(parameters: &Map<String, Value>, script: &Value) -> Result<Self, OpenSearchError> {
        if let Some(key) = parameters
            .keys()
            .find(|key| !INCREMENT_COUNTER_PARAMETERS.contains(&key.as_str()))
        {
            return Err(refuse_script(
                script,
                &format!("the counter script does not take the parameter [{key}]"),
            ));
        }
        let read_text = |name: &str| {
            parameters.get(name).and_then(Value::as_str).map(str::to_string).ok_or_else(|| {
                refuse_script(
                    script,
                    &format!("the counter script needs the string parameter [{name}]"),
                )
            })
        };
        let count = parameters
            .get("count")
            .and_then(Value::as_i64)
            .ok_or_else(|| refuse_script(script, "the counter script needs the integer parameter [count]"))?;
        Ok(Self {
            object_type: read_text("type")?,
            counter_field_name: read_text("counterFieldName")?,
            count,
            time: read_text("time")?,
        })
    }
}

/// Recognize the `script` of a document update.
///
/// The source matches when it equals a recognized form after every whitespace run is collapsed
/// to one space and the ends are trimmed; `lang` must be absent or `painless`. The script does not
/// run on an upsert: a missing document is stored from the request's `upsert` as given, and an
/// update body has no `scripted_upsert`.
///
/// # Errors
///
/// `illegal_argument_exception` naming the whole script when it is not an object with a `source`,
/// carries another key, names another language, is not a recognized form, or its `params` are not
/// the ones the form reads.
pub(crate) fn recognize_document_script(script: &Value) -> Result<DocumentScript, OpenSearchError> {
    let object = script
        .as_object()
        .ok_or_else(|| refuse_script(script, "a script is an object with a source"))?;
    if let Some(key) = object.keys().find(|key| !matches!(key.as_str(), "source" | "lang" | "params")) {
        return Err(refuse_script(
            script,
            &format!("a script does not take the key [{key}]"),
        ));
    }
    match object.get("lang") {
        None => {}
        Some(Value::String(lang)) if lang == "painless" => {}
        Some(lang) => return Err(refuse_script(script, &format!("the language [{lang}] is not served"))),
    }
    let source = object
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| refuse_script(script, "a script needs a string [source]"))?;
    if !source.split_whitespace().eq(INCREMENT_COUNTER_SOURCE.split(' ')) {
        return Err(refuse_script(script, "it is not a recognized form"));
    }
    let parameters = object
        .get("params")
        .and_then(Value::as_object)
        .ok_or_else(|| refuse_script(script, "the counter script needs [params]"))?;
    CounterIncrement::from_parameters(parameters, script).map(DocumentScript::IncrementCounter)
}

/// The refusal of `script`: every refusal names the whole script, as a client sent it, with what
/// is wrong with it.
fn refuse_script(script: &Value, cause: &str) -> OpenSearchError {
    OpenSearchError::illegal_argument(format!("unsupported script [{script}]: {cause}"))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::{Value, json};

    use super::*;
    use crate::opensearch::error::render_envelope;

    /// A counter-script-style source: a line break after the opening backtick, 14- and 16-space
    /// indents, 12 spaces before the closing backtick.
    const TRANSCRIBED_SOURCE: &str = "\n              if (ctx._source[params.type][params.counterFieldName] == null) {\n                ctx._source[params.type][params.counterFieldName] = params.count;\n              }\n              else {\n                ctx._source[params.type][params.counterFieldName] += params.count;\n              }\n              ctx._source.updated_at = params.time;\n            ";

    fn build_counter_script(source: &str, parameters: &Value) -> Value {
        json!({"source": source, "lang": "painless", "params": parameters})
    }

    fn build_counter_parameters() -> Value {
        json!({"count": 1, "time": "2026-10-08T12:00:00.000Z", "type": "ui-metric", "counterFieldName": "count"})
    }

    #[test]
    fn the_increment_counter_script_is_recognized_with_its_params() {
        let expected = DocumentScript::IncrementCounter(CounterIncrement {
            object_type: "ui-metric".to_string(),
            counter_field_name: "count".to_string(),
            count: 1,
            time: "2026-10-08T12:00:00.000Z".to_string(),
        });

        let transcribed_script =
            recognize_document_script(&build_counter_script(TRANSCRIBED_SOURCE, &build_counter_parameters()));
        // Whitespace runs collapse, so the same statements on one line are the same script.
        let one_line_script = recognize_document_script(&json!({
            "source": "if (ctx._source[params.type][params.counterFieldName] == null) { ctx._source[params.type][params.counterFieldName] = params.count; } else { ctx._source[params.type][params.counterFieldName] += params.count; } ctx._source.updated_at = params.time;",
            "params": build_counter_parameters()
        }));

        assert_eq!(
            transcribed_script.expect("the transcribed script is recognized"),
            expected
        );
        assert_eq!(one_line_script.expect("an absent lang means painless"), expected);
    }

    /// The refusal names the whole script, whichever part of it is at fault, so a client can tell
    /// which of its scripts is not served.
    #[tokio::test]
    async fn any_other_document_script_is_refused_naming_it() {
        let refused_scripts = [
            build_counter_script("ctx._source.count += params.count", &build_counter_parameters()),
            // The comparison is token by token: one changed token, a missing last statement and
            // an extra one are each a different script.
            build_counter_script(&TRANSCRIBED_SOURCE.replace("+=", "-="), &build_counter_parameters()),
            build_counter_script(
                &TRANSCRIBED_SOURCE.replace("ctx._source.updated_at = params.time;", ""),
                &build_counter_parameters(),
            ),
            build_counter_script(
                &format!("{TRANSCRIBED_SOURCE} ctx._source.extra = 1;"),
                &build_counter_parameters(),
            ),
            json!({"source": TRANSCRIBED_SOURCE, "lang": "expression", "params": build_counter_parameters()}),
            json!({"source": TRANSCRIBED_SOURCE, "lang": 1, "params": build_counter_parameters()}),
            json!({"source": TRANSCRIBED_SOURCE, "id": "stored", "params": build_counter_parameters()}),
            json!({"lang": "painless", "params": build_counter_parameters()}),
            json!({"source": 1, "params": build_counter_parameters()}),
            json!({"source": TRANSCRIBED_SOURCE}),
            json!({"source": TRANSCRIBED_SOURCE, "params": [1]}),
            build_counter_script(TRANSCRIBED_SOURCE, &json!({"count": 1, "type": "ui-metric"})),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"time": "t", "type": "ui-metric", "counterFieldName": "count"}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": 1.5, "time": "t", "type": "ui-metric", "counterFieldName": "count"}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": "1", "time": "t", "type": "ui-metric", "counterFieldName": "count"}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": 1, "time": 1, "type": "ui-metric", "counterFieldName": "count"}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": 1, "time": "t", "type": 1, "counterFieldName": "count"}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": 1, "time": "t", "type": "ui-metric", "counterFieldName": 1}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": 1, "time": "t", "type": "ui-metric", "counterFieldName": "count", "extra": 1}),
            ),
            json!("ctx._source.count += 1"),
        ];
        for script in refused_scripts {
            let error = recognize_document_script(&script).expect_err("the script is refused");

            let (status, body) = render_envelope(error).await;

            assert_eq!(status, StatusCode::BAD_REQUEST, "{script}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{script}");
            let reason = body["error"]["reason"].as_str().expect("reason is a string");
            assert!(
                reason.contains(&script.to_string()),
                "the refusal names the script: {reason}"
            );
        }
    }

    fn build_increment(count: i64) -> CounterIncrement {
        CounterIncrement {
            object_type: "ui-metric".to_string(),
            counter_field_name: "count".to_string(),
            count,
            time: "2026-10-08T12:00:00.000Z".to_string(),
        }
    }

    /// An absent or `null` counter is set, an integer counter takes the count, up to and exactly
    /// at the largest integer a counter holds.
    #[test]
    fn a_counter_is_set_or_added_to_up_to_the_largest_integer() {
        let cases = [
            ("absent", json!({"ui-metric": {}}), 2, json!(2)),
            ("null", json!({"ui-metric": {"count": null}}), 2, json!(2)),
            ("integer", json!({"ui-metric": {"count": 4}}), 3, json!(7)),
            ("negative", json!({"ui-metric": {"count": -5}}), 2, json!(-3)),
            (
                "just below the largest integer",
                json!({"ui-metric": {"count": i64::MAX - 1}}),
                1,
                json!(i64::MAX),
            ),
        ];
        for (label, mut source, count, expected_counter) in cases {
            build_increment(count).apply_to(&mut source).expect(label);

            assert_eq!(source["ui-metric"]["count"], expected_counter, "{label}");
            assert_eq!(source["updated_at"], "2026-10-08T12:00:00.000Z", "{label}");
        }
    }

    /// A refusal leaves the source as it was: the script runs on the stored document, and the
    /// operations of one batch share a snapshot, so a refused count must not stamp `updated_at`.
    #[tokio::test]
    async fn a_counter_that_cannot_take_the_count_is_refused_and_leaves_the_source_unchanged() {
        let cases = [
            ("a string counter", json!({"ui-metric": {"count": "5"}})),
            ("a fractional counter", json!({"ui-metric": {"count": 1.5}})),
            ("a boolean counter", json!({"ui-metric": {"count": true}})),
            ("the largest integer", json!({"ui-metric": {"count": i64::MAX}})),
            (
                "a counter above the largest integer",
                json!({"ui-metric": {"count": 9_223_372_036_854_775_808_u64}}),
            ),
            ("attributes that are not an object", json!({"ui-metric": 1})),
            ("no attributes", json!({"type": "ui-metric"})),
            ("a source that is not an object", json!("text")),
        ];
        for (label, source) in cases {
            let mut applied_source = source.clone();

            let error = build_increment(1).apply_to(&mut applied_source).expect_err(label);

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{label}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{label}");
            assert_eq!(applied_source, source, "{label}");
        }
    }
}
