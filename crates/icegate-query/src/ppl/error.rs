//! Why a PPL query cannot be answered, and how the errors of the shared helpers it calls map
//! onto those reasons.

use datafusion::error::DataFusionError;
use icegate_query_grammar::SyntaxError;

use crate::engine::fixed_binary::FixedBinaryLiteralError;
use crate::opensearch::error::{OpenSearchError, OpenSearchErrorKind};

/// Why a PPL query cannot be answered.
///
/// Every variant but [`PplError::Unavailable`] is answered in the PPL error envelope; a variant
/// is added only for a new class of error, never to carry a new message. A field that names what
/// a refusal is about is answered in the envelope's `context`, so a client reads it without
/// parsing the reason.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub(crate) enum PplError {
    /// The query text does not parse. Names the first offending token and its position (1-based
    /// line, 0-based column).
    #[error("unexpected '{offending_text}' at line {line}, column {column}")]
    Syntax {
        /// The offending token as written, `<EOF>` for the end of the query.
        offending_text: String,
        /// The 1-based line of the token.
        line: isize,
        /// The 0-based column of the token.
        column: isize,
    },
    /// The source names no index the tenant can read.
    #[error("no such index [{index}]")]
    IndexNotFound {
        /// The index name or expression as the request wrote it.
        index: String,
    },
    /// A field reference matches no column of the pipeline at that point (names are
    /// case-sensitive).
    #[error("Field [{field}] not found.")]
    FieldNotFound {
        /// The field reference as written.
        field: String,
    },
    /// The query uses a command IceGate does not plan.
    #[error("Command [{command}] is not supported.")]
    UnsupportedCommand {
        /// The command name.
        command: String,
    },
    /// The query uses a function or operator IceGate does not plan.
    #[error("Function [{function}] is not supported.")]
    UnsupportedFunction {
        /// The function or operator name.
        function: String,
    },
    /// The query is well-formed but asks for something invalid (wrong types, a missing argument,
    /// a source on system indices, conflicting field types across indices).
    #[error("{reason}")]
    Semantic {
        /// What is wrong, in IceGate's wording.
        reason: String,
        /// What the refusal is about: the field, alias, function, option or index it names, the
        /// operator, keyword or wildcard it refuses (`in`, `span`, `*`), or the argument it refuses
        /// (a literal or a pattern, without its quotes), spelled as the reason spells it. A
        /// refusal about a source names its index expression as the query wrote it, which may
        /// name several indices. It identifies the subject, not the refusal: two refusals about
        /// the same subject share it.
        /// Answered as `context.name`; a refusal without one answers no `context`. Built by
        /// [`PplError::semantic_naming`].
        name: Option<String>,
    },
    /// A `timechart` aggregation whose value over several series is not a function of the
    /// per-series values (an average, a distinct count, a percentile), with the series beyond the
    /// limit merged into the other-series row: that row could only show an invalid value.
    #[error(
        "timechart cannot merge [{function}] of the series beyond its limit into the other-series row exactly; \
         set useother=false to drop those series or limit=0 to keep every series"
    )]
    InexactOtherSeries {
        /// The aggregation function as planned: `avg`, `dc`, or `percentile` for every percentile
        /// spelling.
        function: String,
    },
    /// The output has more rows than `opensearch.ppl.max_result_rows` allows.
    #[error(
        "The query returns more than {limit} rows, the configured opensearch.ppl.max_result_rows; add | head {limit} to read the first rows."
    )]
    RowLimitExceeded {
        /// The configured limit.
        limit: usize,
    },
    /// The query nests parentheses and square brackets deeper than a PPL query may: the parser
    /// and the planner recurse once per level, so a deeper query is refused before it is parsed.
    #[error("The query nests brackets deeper than {limit_depth} levels, the limit of a PPL query.")]
    NestingLimitExceeded {
        /// The most levels a query may nest.
        limit_depth: usize,
    },
    /// The query nests subsearches (square brackets) deeper than a PPL query may: nested
    /// subsearches are planned on the async worker, at a cost that grows with their depth, so a
    /// deeper query is refused before it is parsed.
    #[error("The query nests subsearches deeper than {limit_subsearch_depth} levels, the limit of a PPL query.")]
    SubsearchNestingLimitExceeded {
        /// The most levels of subsearches a query may nest.
        limit_subsearch_depth: usize,
    },
    /// The query holds more operators than a PPL query may: a chain of operators nests one level
    /// per operator, so a query with more is refused before it is parsed.
    #[error("The query holds more than {limit_operators} operators, the limit of a PPL query.")]
    OperatorLimitExceeded {
        /// The most operators a query may hold.
        limit_operators: usize,
    },
    /// The query needs more memory than the per-query limit allows.
    #[error("The query needs more than {limit_bytes} bytes of memory, the configured per-query limit.")]
    MemoryLimitExceeded {
        /// The configured limit in bytes.
        limit_bytes: usize,
    },
    /// The listener shed the request, its deadline passed, or a store write contended. Answered
    /// in the `OpenSearch` 503 envelope of its cause: the listener has one retry contract for
    /// every route.
    #[error("{}", .0.reason())]
    Unavailable(OpenSearchError),
    /// A failure inside IceGate the request could not have avoided.
    #[error("{reason}")]
    Internal {
        /// What failed.
        reason: String,
    },
}

impl From<SyntaxError> for PplError {
    /// The grammar's entry point names the offending token on every error, `<EOF>` at the end of
    /// the query; an error naming none would report its message in the token's place.
    fn from(error: SyntaxError) -> Self {
        Self::Syntax {
            offending_text: error.offending_text.unwrap_or(error.message),
            line: error.line,
            column: error.column,
        }
    }
}

/// Maps every error kind of the shared `OpenSearch` helpers PPL calls (index resolution, scans,
/// execution under the memory limit). No reason string is parsed: the kind decides. A kind that
/// no helper PPL calls can raise maps to [`PplError::Internal`], because answering it as a client
/// error would mislabel a defect.
impl From<OpenSearchError> for PplError {
    fn from(error: OpenSearchError) -> Self {
        match error.kind() {
            OpenSearchErrorKind::IndexNotFound { index } => Self::IndexNotFound { index: index.clone() },
            OpenSearchErrorKind::MemoryLimitExceeded { limit_bytes } => Self::MemoryLimitExceeded {
                limit_bytes: *limit_bytes,
            },
            OpenSearchErrorKind::IllegalArgument | OpenSearchErrorKind::AggregationExecution => Self::Semantic {
                reason: error.reason().to_string(),
                name: None,
            },
            OpenSearchErrorKind::MemoryPressure
            | OpenSearchErrorKind::DeadlineExceeded
            | OpenSearchErrorKind::WriteContention => Self::Unavailable(error),
            OpenSearchErrorKind::Internal
            | OpenSearchErrorKind::Parsing
            | OpenSearchErrorKind::NoHandler
            | OpenSearchErrorKind::PayloadTooLarge
            | OpenSearchErrorKind::ResourceAlreadyExists
            | OpenSearchErrorKind::ClusterBlock
            | OpenSearchErrorKind::SnapshotTooLarge
            | OpenSearchErrorKind::VersionConflict
            | OpenSearchErrorKind::DocumentMissing
            | OpenSearchErrorKind::QueryShard
            | OpenSearchErrorKind::SearchContextMissing
            | OpenSearchErrorKind::ResultWindowTooLarge
            | OpenSearchErrorKind::TooManyBuckets
            // No PPL source reaches a cross-row field (a PPL source on the Data Prepper profile,
            // the one that declares them, is refused), so a PPL query never raises it.
            | OpenSearchErrorKind::TooManyCrossRowTraces
            | OpenSearchErrorKind::StatusNotFound => Self::Internal {
                reason: error.reason().to_string(),
            },
        }
    }
}

/// Maps a DataFusion error raised while planning a query. A plan or schema error comes from the
/// query itself (a type the expression cannot take, a name used twice), so it is a client error;
/// anything else is IceGate's.
impl From<DataFusionError> for PplError {
    fn from(error: DataFusionError) -> Self {
        match error.find_root() {
            DataFusionError::Plan(_) | DataFusionError::SchemaError(..) | DataFusionError::NotImplemented(_) => {
                Self::Semantic {
                    reason: error.strip_backtrace(),
                    name: None,
                }
            }
            _ => Self::Internal {
                reason: error.strip_backtrace(),
            },
        }
    }
}

impl PplError {
    /// The semantic refusal `reason` that names `name`, which the envelope answers as
    /// `context.name`.
    pub(crate) fn semantic_naming(name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Semantic {
            reason: reason.into(),
            name: Some(name.into()),
        }
    }

    /// The 400 for a string literal compared with a fixed-width id column that is not a value of that
    /// column: not hex, or another number of bytes. Its reason names the column and the literal as
    /// written; it names the literal.
    pub(crate) fn invalid_id_literal(column: &str, literal: &str, error: &FixedBinaryLiteralError) -> Self {
        Self::semantic_naming(
            literal,
            format!("'{literal}' is not a valid {column}: {error}; write the id as hex, two digits per byte"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_index_not_found_error_becomes_the_ppl_index_not_found_error() {
        let error = PplError::from(OpenSearchError::index_not_found("nope"));
        assert_eq!(
            error,
            PplError::IndexNotFound {
                index: "nope".to_string()
            }
        );
    }

    #[test]
    fn a_memory_limit_error_becomes_the_ppl_memory_limit_error() {
        let error = PplError::from(OpenSearchError::memory_limit_exceeded(1_048_576));
        assert_eq!(error, PplError::MemoryLimitExceeded { limit_bytes: 1_048_576 });
    }

    #[test]
    fn a_system_or_mixed_target_refusal_becomes_a_ppl_semantic_error() {
        let refusal = OpenSearchError::illegal_argument("[.kibana] are system indices");
        let reason = refusal.reason().to_string();
        assert_eq!(PplError::from(refusal), PplError::Semantic { reason, name: None });
    }

    #[test]
    fn an_aggregation_type_conflict_becomes_a_ppl_semantic_error() {
        let conflict = OpenSearchError::aggregation_execution("code");
        let reason = conflict.reason().to_string();
        assert_eq!(PplError::from(conflict), PplError::Semantic { reason, name: None });
    }

    #[test]
    fn shed_deadline_and_write_contention_keep_the_opensearch_503_envelope() {
        for error in [
            OpenSearchError::memory_pressure(),
            OpenSearchError::deadline_exceeded(),
            OpenSearchError::write_contention(),
        ] {
            assert_eq!(PplError::from(error.clone()), PplError::Unavailable(error));
        }
    }

    #[test]
    fn every_other_opensearch_error_kind_becomes_a_ppl_internal_error() {
        // One constructor per kind of the `Internal` arm, thirteen in all.
        let errors = [
            OpenSearchError::internal("disk on fire"),
            OpenSearchError::parsing("bad body"),
            OpenSearchError::no_handler(&axum::http::Method::GET, "/nope"),
            OpenSearchError::payload_too_large(),
            OpenSearchError::resource_already_exists(".kibana_1"),
            OpenSearchError::cluster_block("logs"),
            OpenSearchError::snapshot_too_large(1024),
            OpenSearchError::version_conflict(".kibana_1", "doc-1"),
            OpenSearchError::document_missing(".kibana_1", "doc-1"),
            OpenSearchError::query_shard("logs", "x"),
            OpenSearchError::search_context_missing("scroll-1"),
            OpenSearchError::result_window_too_large(9_000, 2_000, 10_000),
            OpenSearchError::too_many_buckets(10, 11),
        ];
        for error in errors {
            let reason = error.reason().to_string();
            assert_eq!(PplError::from(error), PplError::Internal { reason });
        }
    }

    #[test]
    fn a_status_not_found_error_becomes_a_ppl_internal_error() {
        let error = PplError::from(OpenSearchError::status_not_found("no ML config [abc]"));
        assert!(matches!(error, PplError::Internal { .. }), "{error:?}");
    }

    #[test]
    fn a_too_many_cross_row_traces_error_becomes_a_ppl_internal_error() {
        let error = PplError::from(OpenSearchError::too_many_cross_row_traces("otel-v1-apm-span-000001", 2));
        assert!(matches!(error, PplError::Internal { .. }), "{error:?}");
    }
}
