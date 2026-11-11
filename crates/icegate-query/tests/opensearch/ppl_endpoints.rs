//! The endpoints beside PPL: cluster settings, `SHOW TABLES`, the grammar bundle, the observability
//! object list and the ML config lookup. Requests are literal; expected bodies are stated here, and the
//! grammar bundle is compared with an independent reader of the committed `.interp` files.
#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::error::Error;

use icegate_query_grammar::ppl::{PPL_LEXER_INTERP, PPL_PARSER_INTERP};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::harness::TestServer;
use super::requests::{send_raw_request, send_request};

const TENANT_A: &str = "tenant-a";

/// `POST /_plugins/_sql` with `{"query": query}` as [`TENANT_A`]; the status and the JSON body.
async fn post_sql(server: &TestServer, query: &str) -> Result<(StatusCode, Value), Box<dyn Error>> {
    send_request(
        server,
        Method::POST,
        TENANT_A,
        "/_plugins/_sql",
        Some(&json!({ "query": query })),
    )
    .await
}

fn build_show_tables_row(index: &str) -> Value {
    json!(["icegate", null, index, "BASE TABLE", null, null, null, null, null, null])
}

fn assert_envelope(status: StatusCode, body: &Value, expected_status: StatusCode, expected_type: &str) {
    assert_eq!(status, expected_status, "{body}");
    assert_eq!(body["status"], expected_status.as_u16());
    assert_eq!(body["error"]["type"], expected_type, "{body}");
    assert_eq!(body["error"]["root_cause"][0]["type"], expected_type, "{body}");
}

/// Asserts that `GET path` answers `status_without_parameter` and that the same request with the
/// unknown query parameter `bogus` is the 400 `illegal_argument_exception` envelope. The refusal
/// carries no key naming the parameter; the answer without it shows that the route is served and
/// the parameter alone is refused, which tells the refusal from the listener's no-handler 400.
async fn assert_unknown_parameter_refused(server: &TestServer, path: &str, status_without_parameter: StatusCode) {
    let (status, body) = send_request(server, Method::GET, TENANT_A, path, None)
        .await
        .expect("GET request");
    assert_eq!(status, status_without_parameter, "{path}: {body}");
    let (status, body) = send_request(server, Method::GET, TENANT_A, &format!("{path}?bogus=1"), None)
        .await
        .expect("GET request");
    assert_envelope(status, &body, StatusCode::BAD_REQUEST, "illegal_argument_exception");
}

#[tokio::test]
async fn cluster_settings_report_the_calcite_flags_through_an_escaped_filter_path() {
    let (server, _catalog) = TestServer::start().await;
    // A query-enhancements-style request: flat names, defaults included, filtered by escaped dots.
    let (status, body) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/_cluster/settings?flat_settings=true&include_defaults=true&filter_path=*.plugins%5C.calcite%5C.enabled,*.plugins%5C.calcite%5C.all_join_types%5C.allowed",
        None,
    )
    .await
    .expect("GET request");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        json!({"persistent": {"plugins.calcite.enabled": "true", "plugins.calcite.all_join_types.allowed": "true"}})
    );

    let (status, body) = send_request(&server, Method::GET, TENANT_A, "/_cluster/settings", None)
        .await
        .expect("GET request");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        json!({
            "persistent": {"plugins": {"calcite": {"enabled": "true", "all_join_types": {"allowed": "true"}}}},
            "transient": {}
        })
    );

    let (status, body) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/_cluster/settings?include_defaults=true&flat_settings=true",
        None,
    )
    .await
    .expect("GET request");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        json!({
            "persistent": {"plugins.calcite.enabled": "true", "plugins.calcite.all_join_types.allowed": "true"},
            "transient": {},
            "defaults": {}
        })
    );

    // An unknown parameter is refused, not ignored.
    assert_unknown_parameter_refused(&server, "/_cluster/settings", StatusCode::OK).await;
    server.shutdown().await;
}

#[tokio::test]
async fn show_tables_like_lists_matching_data_indices_without_dot_indices_in_the_ten_column_schema() {
    let (server, _catalog) = TestServer::start().await;
    let (status, body) = send_request(&server, Method::PUT, TENANT_A, "/.kibanas", Some(&json!({})))
        .await
        .expect("create a dot index ending in s");
    assert_eq!(status, 200, "{body}");

    let (status, body) = post_sql(&server, "SHOW TABLES LIKE '%s'").await.expect("POST request");
    assert_eq!(status, 200, "{body}");
    let columns = [
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "TABLE_TYPE",
        "REMARKS",
        "TYPE_CAT",
        "TYPE_SCHEM",
        "TYPE_NAME",
        "SELF_REFERENCING_COL_NAME",
        "REF_GENERATION",
    ];
    let schema: Vec<Value> = columns.iter().map(|name| json!({"name": name, "type": "keyword"})).collect();
    // `%s` matches every name ending in `s`; `.kibanas` matches too and must still be omitted.
    let names = ["events", "logs", "metrics", "operations", "prices", "spans"];
    assert_eq!(
        body,
        json!({
            "schema": schema,
            "datarows": names.iter().map(|name| build_show_tables_row(name)).collect::<Vec<_>>(),
            "total": names.len(),
            "size": names.len(),
            "status": 200
        })
    );

    // `_` is one character and `\_` a literal underscore.
    let (_, body) = post_sql(&server, "SHOW TABLES LIKE '_ogs'").await.expect("POST request");
    assert_eq!(body["datarows"], json!([build_show_tables_row("logs")]));
    let (_, body) = post_sql(&server, "SHOW TABLES LIKE 'prices\\_%'").await.expect("POST request");
    assert_eq!(
        body["datarows"],
        json!([
            build_show_tables_row("prices_effective"),
            build_show_tables_row("prices_effective_by_time_of_day")
        ])
    );
    // Index names match case-sensitively.
    let (_, body) = post_sql(&server, "SHOW TABLES LIKE 'Logs'").await.expect("POST request");
    assert_eq!(body["datarows"], json!([]));
    server.shutdown().await;
}

#[tokio::test]
async fn show_tables_keywords_match_case_insensitively() {
    let (server, _catalog) = TestServer::start().await;
    for query in [
        "show Tables like 'logs'",
        "SHOW tables LIKE 'logs';",
        "  Show  TABLES\tLike 'logs'  ",
    ] {
        let (status, body) = post_sql(&server, query).await.expect("POST request");
        assert_eq!(status, 200, "{query}: {body}");
        assert_eq!(body["datarows"], json!([build_show_tables_row("logs")]), "{query}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn any_other_sql_is_a_400_naming_ppl() {
    let (server, _catalog) = TestServer::start().await;
    for query in [
        "SELECT * FROM logs",
        "SHOW TABLES",
        "SHOW TABLES LIKE logs",
        "SHOW TABLES LIKE 'a' extra",
        "DESCRIBE TABLES LIKE '%'",
    ] {
        let (status, body) = post_sql(&server, query).await.expect("POST request");
        assert_envelope(status, &body, StatusCode::BAD_REQUEST, "illegal_argument_exception");
        assert_eq!(body["error"]["key"], "query", "{query}: {body}");
        assert_eq!(body["error"]["value"], query, "{query}: {body}");
    }
    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/_plugins/_sql",
        Some(&json!({ "sql": "SHOW TABLES LIKE '%'" })),
    )
    .await
    .expect("POST request");
    assert_envelope(status, &body, StatusCode::BAD_REQUEST, "illegal_argument_exception");
    assert_eq!(body["error"]["key"], "query", "{body}");
    assert_eq!(body["error"].get("value"), None, "{body}");

    // A body that is not JSON is refused before any statement is read, so its refusal carries no
    // key; the answers to a valid body below show that the route is served.
    let (status, body) = send_raw_request(&server, Method::POST, TENANT_A, "/_plugins/_sql", "not json")
        .await
        .expect("POST request");
    assert_envelope(status, &body, StatusCode::BAD_REQUEST, "illegal_argument_exception");
    assert_eq!(body["error"].get("key"), None, "{body}");

    // An unknown query parameter is refused, not ignored; `format` is accepted and ignored. The
    // same request with `format` in its place answers 200, so the parameter alone is refused.
    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/_plugins/_sql?bogus=1",
        Some(&json!({ "query": "SHOW TABLES LIKE '%'" })),
    )
    .await
    .expect("POST request");
    assert_envelope(status, &body, StatusCode::BAD_REQUEST, "illegal_argument_exception");
    assert_eq!(body["error"].get("key"), None, "{body}");
    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/_plugins/_sql?format=jdbc",
        Some(&json!({ "query": "SHOW TABLES LIKE '%'" })),
    )
    .await
    .expect("POST request");
    assert_eq!(status, 200, "{body}");
    server.shutdown().await;
}

/// Sections of an ANTLR `.interp` file, keyed by header, read independently of the production
/// reader: sections are blank-line-separated blocks whose first line is `<header>:`.
fn read_interp(text: &str) -> HashMap<String, Vec<String>> {
    text.split("\n\n")
        .filter_map(|block| {
            let mut lines = block.lines().filter(|line| !line.is_empty());
            let header = lines.next()?.strip_suffix(':')?.to_owned();
            Some((header, lines.map(str::to_owned).collect()))
        })
        .collect()
}

/// The section under `header`, or an error naming the header.
fn read_section<'a>(sections: &'a HashMap<String, Vec<String>>, header: &str) -> Result<&'a [String], Box<dyn Error>> {
    sections
        .get(header)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("no section [{header}]").into())
}

fn read_atn(sections: &HashMap<String, Vec<String>>) -> Result<Vec<i32>, Box<dyn Error>> {
    let line = read_section(sections, "atn")?.first().ok_or("an empty ATN section")?;
    let values = line
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|value| value.trim().parse::<i32>())
        .collect::<Result<Vec<_>, _>>()?;
    Ok(values)
}

fn read_names(sections: &HashMap<String, Vec<String>>, header: &str) -> Result<Vec<Option<String>>, Box<dyn Error>> {
    Ok(read_section(sections, header)?
        .iter()
        .map(|line| (line != "null").then(|| line.clone()))
        .collect())
}

#[tokio::test]
async fn the_grammar_bundle_carries_every_key_and_the_committed_atns() {
    let (server, _catalog) = TestServer::start().await;
    let (status, bundle) = send_request(&server, Method::GET, TENANT_A, "/_plugins/_ppl/_grammar", None)
        .await
        .expect("GET request");
    assert_eq!(status, 200, "{bundle}");
    let Value::Object(keys) = &bundle else {
        panic!("bundle is not an object: {bundle}")
    };
    assert_eq!(
        keys.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "bundleVersion",
            "antlrVersion",
            "grammarHash",
            "startRuleIndex",
            "lexerSerializedATN",
            "lexerRuleNames",
            "channelNames",
            "modeNames",
            "parserSerializedATN",
            "parserRuleNames",
            "literalNames",
            "symbolicNames",
            "tokenDictionary",
            "ignoredTokens",
            "rulesToVisit"
        ]
    );
    let lexer = read_interp(PPL_LEXER_INTERP);
    let parser = read_interp(PPL_PARSER_INTERP);
    assert_eq!(bundle["bundleVersion"], "1.0");
    assert_eq!(bundle["antlrVersion"], "4.13.2");
    // `root` is the first rule of the committed grammar: 0 is its index, not the fallback.
    assert_eq!(bundle["parserRuleNames"][0], "root");
    assert_eq!(bundle["startRuleIndex"], 0);
    assert_eq!(
        bundle["lexerSerializedATN"],
        json!(read_atn(&lexer).expect("lexer ATN"))
    );
    assert_eq!(
        bundle["parserSerializedATN"],
        json!(read_atn(&parser).expect("parser ATN"))
    );
    assert_eq!(bundle["lexerRuleNames"], json!(lexer["rule names"]));
    assert_eq!(bundle["parserRuleNames"], json!(parser["rule names"]));
    assert_eq!(bundle["modeNames"], json!(lexer["mode names"]));
    // The lexer grammar declares `channels { WHITESPACE, ERRORCHANNEL }` after the two built-in ones.
    assert_eq!(
        bundle["channelNames"],
        json!(["DEFAULT_TOKEN_CHANNEL", "HIDDEN", "WHITESPACE", "ERRORCHANNEL"])
    );
    let literal_names = read_names(&parser, "token literal names").expect("literal names");
    let symbolic_names = read_names(&parser, "token symbolic names").expect("symbolic names");
    assert_eq!(bundle["literalNames"], json!(literal_names));
    assert_eq!(bundle["symbolicNames"], json!(symbolic_names));

    let token_type = |name: &str| -> usize {
        symbolic_names
            .iter()
            .position(|symbolic| symbolic.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("no token {name}"))
    };
    let mut dictionary = serde_json::Map::new();
    dictionary.insert("WHITESPACE".to_owned(), json!(2));
    for (key, token) in [
        ("FROM", "FROM"),
        ("OPENING_BRACKET", "LT_PRTHS"),
        ("CLOSING_BRACKET", "RT_PRTHS"),
        ("SEARCH", "SEARCH"),
        ("SOURCE", "SOURCE"),
        ("PIPE", "PIPE"),
        ("ID", "ID"),
        ("EQUAL", "EQUAL"),
        ("IN", "IN"),
        ("COMMA", "COMMA"),
        ("BACKTICK_QUOTE", "BQUOTA_STRING"),
        ("DOT", "DOT"),
    ] {
        dictionary.insert(key.to_owned(), json!(token_type(token)));
    }
    assert_eq!(bundle["tokenDictionary"], Value::Object(dictionary.clone()));
    assert_eq!(
        bundle["tokenDictionary"]
            .as_object()
            .expect("dictionary")
            .keys()
            .collect::<Vec<_>>(),
        dictionary.keys().collect::<Vec<_>>(),
        "dictionary key order"
    );

    let internal_token_names = [
        "ID",
        "NUMERIC_ID",
        "ID_DATE_SUFFIX",
        "CLUSTER",
        "TIME_SNAP",
        "SPANLENGTH",
        "DECIMAL_SPANLENGTH",
        "DQUOTA_STRING",
        "SQUOTA_STRING",
        "BQUOTA_STRING",
        "LINE_COMMENT",
        "BLOCK_COMMENT",
        "ERROR_RECOGNITION",
    ];
    let ignored_tokens: Vec<usize> = symbolic_names
        .iter()
        .enumerate()
        .filter(|(_, name)| {
            name.as_deref()
                .is_some_and(|name| name.ends_with("_LITERAL") || internal_token_names.contains(&name))
        })
        .map(|(token, _)| token)
        .collect();
    assert!(!ignored_tokens.is_empty());
    assert_eq!(bundle["ignoredTokens"], json!(ignored_tokens));

    let parser_rules = &parser["rule names"];
    let rules: Vec<usize> = [
        "statsFunctionName",
        "takeAggFunction",
        "integerLiteral",
        "decimalLiteral",
        "keywordsCanBeId",
        "renameClause",
        "qualifiedName",
        "tableQualifiedName",
        "wcQualifiedName",
        "positionFunctionName",
        "searchableKeyWord",
        "stringLiteral",
        "searchCommand",
        "searchComparisonOperator",
        "comparisonOperator",
        "sqlLikeJoinType",
    ]
    .iter()
    .map(|rule| {
        parser_rules
            .iter()
            .position(|name| name == rule)
            .unwrap_or_else(|| panic!("no rule {rule}"))
    })
    .collect();
    assert_eq!(bundle["rulesToVisit"], json!(rules));
    server.shutdown().await;
}

/// The bundle's hash, computed here from the committed `.interp` files by the stated algorithm.
fn compute_expected_grammar_hash() -> Result<String, Box<dyn Error>> {
    let lexer = read_interp(PPL_LEXER_INTERP);
    let parser = read_interp(PPL_PARSER_INTERP);
    let mut digest = Sha256::new();
    for value in read_atn(&lexer)?.into_iter().chain(read_atn(&parser)?) {
        digest.update(value.to_be_bytes());
    }
    let mut update_name = |name: Option<&str>| {
        match name {
            Some(name) => {
                digest.update([1_u8]);
                digest.update(name.as_bytes());
            }
            None => digest.update([0_u8]),
        }
        digest.update([0xFF_u8]);
    };
    for name in read_section(&lexer, "rule names")?
        .iter()
        .chain(read_section(&parser, "rule names")?)
    {
        update_name(Some(name));
    }
    for name in read_names(&parser, "token literal names")?
        .iter()
        .chain(&read_names(&parser, "token symbolic names")?)
    {
        update_name(name.as_deref());
    }
    digest.update(b"4.13.2");
    Ok(format!("sha256:{}", hex::encode(digest.finalize())))
}

#[tokio::test]
async fn the_grammar_hash_equals_an_independent_implementation_of_the_stated_algorithm() {
    let (server, _catalog) = TestServer::start().await;
    let (status, bundle) = send_request(&server, Method::GET, TENANT_A, "/_plugins/_ppl/_grammar", None)
        .await
        .expect("GET request");
    assert_eq!(status, 200, "{bundle}");
    let hash = bundle["grammarHash"].as_str().expect("hash text");
    assert_eq!(hash.len(), 71, "sha256: plus 64 hex digits");
    assert_eq!(hash, compute_expected_grammar_hash().expect("expected hash"));
    server.shutdown().await;
}

#[tokio::test]
async fn the_observability_object_list_is_empty_and_refuses_an_unknown_parameter() {
    let (server, _catalog) = TestServer::start().await;
    let (status, body) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/_plugins/_observability/object?objectType=savedQuery&sortOrder=desc&fromIndex=0",
        None,
    )
    .await
    .expect("GET request");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        json!({"observabilityObjectList": [], "startIndex": 0, "totalHitRelation": "eq", "totalHits": 0})
    );
    let (status, body) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/_plugins/_observability/object?maxItems=10",
        None,
    )
    .await
    .expect("GET request");
    assert_envelope(status, &body, StatusCode::BAD_REQUEST, "illegal_argument_exception");
    server.shutdown().await;
}

#[tokio::test]
async fn ml_config_is_a_404_status_exception() {
    let (server, _catalog) = TestServer::start().await;
    let (status, body) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/_plugins/_ml/config/agent-config",
        None,
    )
    .await
    .expect("GET request");
    assert_envelope(status, &body, StatusCode::NOT_FOUND, "status_exception");
    assert_eq!(body["error"]["id"], "agent-config", "{body}");
    server.shutdown().await;
}

#[tokio::test]
async fn the_grammar_and_ml_config_routes_refuse_an_unknown_parameter() {
    let (server, _catalog) = TestServer::start().await;
    assert_unknown_parameter_refused(&server, "/_plugins/_ppl/_grammar", StatusCode::OK).await;
    // The query parameters are read before the id is looked up, so an unknown parameter is a 400,
    // not the 404 of an unknown id.
    assert_unknown_parameter_refused(&server, "/_plugins/_ml/config/agent-config", StatusCode::NOT_FOUND).await;
    server.shutdown().await;
}

#[tokio::test]
async fn an_unmatched_plugin_route_stays_a_400_envelope() {
    let (server, _catalog) = TestServer::start().await;
    let (status, body) = send_request(&server, Method::GET, TENANT_A, "/_plugins/_unknown_plugin/thing", None)
        .await
        .expect("GET request");
    assert_envelope(status, &body, StatusCode::BAD_REQUEST, "illegal_argument_exception");
    let (status, body) = send_request(
        &server,
        Method::DELETE,
        TENANT_A,
        "/_plugins/_ml/config/agent-config",
        None,
    )
    .await
    .expect("DELETE request");
    assert_envelope(status, &body, StatusCode::BAD_REQUEST, "illegal_argument_exception");
    server.shutdown().await;
}
