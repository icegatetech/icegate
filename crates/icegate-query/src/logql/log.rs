//! Log query expression types for LogQL.

use chrono::TimeDelta;

use super::common::{ComparisonOp, LabelExtraction, LabelFormatOp, MatchOp};

/// Log query expression: selector with optional pipeline stages.
#[derive(Debug, Clone, PartialEq)]
pub struct LogExpr {
    /// The selector to filter logs.
    pub selector: Selector,
    /// The pipeline stages to process logs.
    pub pipeline: Vec<PipelineStage>,
}

impl LogExpr {
    /// Creates a new `LogExpr` with the given selector and an empty pipeline.
    #[must_use]
    pub const fn new(selector: Selector) -> Self {
        Self {
            selector,
            pipeline: Vec::new(),
        }
    }

    /// Creates a new `LogExpr` with the given selector and pipeline.
    #[must_use]
    pub const fn with_pipeline(selector: Selector, pipeline: Vec<PipelineStage>) -> Self {
        Self {
            selector,
            pipeline,
        }
    }
}

/// Stream selector: `{label="value", ...}`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Selector {
    /// The list of label matchers.
    pub matchers: Vec<LabelMatcher>,
}

impl Selector {
    /// Creates a new `Selector` with the given matchers.
    #[must_use]
    pub const fn new(matchers: Vec<LabelMatcher>) -> Self {
        Self {
            matchers,
        }
    }

    /// Creates a new empty `Selector`.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            matchers: Vec::new(),
        }
    }
}

/// Label matcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    /// The label name.
    pub label: String,
    /// The matching operator.
    pub op: MatchOp,
    /// The value to match against.
    pub value: String,
}

impl LabelMatcher {
    /// Creates a new `LabelMatcher`.
    pub fn new(label: impl Into<String>, op: MatchOp, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            op,
            value: value.into(),
        }
    }

    /// Creates a new equality matcher (`=`).
    pub fn eq(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(label, MatchOp::Eq, value)
    }

    /// Creates a new inequality matcher (`!=`).
    pub fn neq(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(label, MatchOp::Neq, value)
    }

    /// Creates a new regex matcher (`=~`).
    pub fn re(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(label, MatchOp::Re, value)
    }

    /// Creates a new regex non-match matcher (`!~`).
    pub fn nre(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(label, MatchOp::Nre, value)
    }
}

/// Pipeline stage in a log query.
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStage {
    /// Line filter: `|= "text"`, `|~ "regex"`, etc.
    LineFilter(LineFilter),
    /// Parser: `| json`, `| logfmt`, `| regexp`, `| pattern`, `| unpack`
    LogParser(LogParser),
    /// Label format: `| label_format dst=src, dst="{{.template}}"`
    LabelFormat(Vec<LabelFormatOp>),
    /// Line format: `| line_format "{{.template}}"`
    LineFormat(String),
    /// Decolorize: `| decolorize`
    Decolorize,
    /// Drop labels: `| drop label1, label2`
    Drop(Vec<LabelExtraction>),
    /// Keep labels: `| keep label1, label2`
    Keep(Vec<LabelExtraction>),
    /// Label filter: `| label > 10`, `| label = "value"`
    LabelFilter(LabelFilterExpr),
}

/// Line filter expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineFilter {
    /// The filter operator.
    pub op: LineFilterOp,
    /// The filter values.
    pub filters: Vec<LineFilterValue>,
}

impl LineFilter {
    /// Creates a new `LineFilter`.
    #[must_use]
    pub const fn new(op: LineFilterOp, filters: Vec<LineFilterValue>) -> Self {
        Self {
            op,
            filters,
        }
    }

    /// Creates a new contains filter (`|=`).
    pub fn contains(value: impl Into<String>) -> Self {
        Self {
            op: LineFilterOp::Contains,
            filters: vec![LineFilterValue::String(value.into())],
        }
    }

    /// Creates a new not contains filter (`!=`).
    pub fn not_contains(value: impl Into<String>) -> Self {
        Self {
            op: LineFilterOp::NotContains,
            filters: vec![LineFilterValue::String(value.into())],
        }
    }

    /// Creates a new regex match filter (`|~`).
    pub fn matches(pattern: impl Into<String>) -> Self {
        Self {
            op: LineFilterOp::Match,
            filters: vec![LineFilterValue::String(pattern.into())],
        }
    }

    /// Creates a new regex not match filter (`!~`).
    pub fn not_matches(pattern: impl Into<String>) -> Self {
        Self {
            op: LineFilterOp::NotMatch,
            filters: vec![LineFilterValue::String(pattern.into())],
        }
    }
}

/// Line filter operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LineFilterOp {
    /// `|=` contains
    Contains,
    /// `!=` does not contain
    NotContains,
    /// `|~` regex match
    Match,
    /// `!~` regex not match
    NotMatch,
    /// `!>` not pattern (negated pattern match)
    NotPattern,
}

impl LineFilterOp {
    /// Returns the string representation of the operator.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Contains => "|=",
            Self::NotContains => "!=",
            Self::Match => "|~",
            Self::NotMatch => "!~",
            Self::NotPattern => "!>",
        }
    }
}

/// Value in a line filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineFilterValue {
    /// String literal
    String(String),
    /// IP CIDR filter: `ip("192.168.1.0/24")`
    Ip(String),
}

/// Log parser expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogParser {
    /// JSON parser: `| json`, `| json field1, field2="path"`
    Json(Option<Vec<LabelExtraction>>),
    /// Logfmt parser: `| logfmt`, `| logfmt --strict --keep-empty field1,
    /// field2`
    Logfmt {
        /// Whether to enforce strict parsing.
        strict: bool,
        /// Whether to keep empty values.
        keep_empty: bool,
        /// Optional list of fields to extract.
        fields: Option<Vec<LabelExtraction>>,
    },
    /// Regexp parser: `| regexp "<regex with named groups>"`
    Regexp(String),
    /// Pattern parser: `| pattern "<pattern>"`
    Pattern(String),
    /// Unpack parser: `| unpack`
    Unpack,
}

impl LogParser {
    /// Creates a new JSON parser.
    pub const fn json() -> Self {
        Self::Json(None)
    }

    /// Creates a new JSON parser with field extraction.
    pub const fn json_with_fields(fields: Vec<LabelExtraction>) -> Self {
        Self::Json(Some(fields))
    }

    /// Creates a new Logfmt parser.
    pub const fn logfmt() -> Self {
        Self::Logfmt {
            strict: false,
            keep_empty: false,
            fields: None,
        }
    }

    /// Creates a new strict Logfmt parser.
    pub const fn logfmt_strict() -> Self {
        Self::Logfmt {
            strict: true,
            keep_empty: false,
            fields: None,
        }
    }

    /// Creates a new regexp parser.
    pub fn regexp(pattern: impl Into<String>) -> Self {
        Self::Regexp(pattern.into())
    }

    /// Creates a new pattern parser.
    pub fn pattern(pattern: impl Into<String>) -> Self {
        Self::Pattern(pattern.into())
    }

    /// Creates a new unpack parser.
    pub const fn unpack() -> Self {
        Self::Unpack
    }
}

/// Label filter expression (post-pipeline filtering).
#[derive(Debug, Clone, PartialEq)]
pub enum LabelFilterExpr {
    /// Logical AND: `filter1, filter2` or `filter1 and filter2`
    And(Box<Self>, Box<Self>),
    /// Logical OR: `filter1 or filter2`
    Or(Box<Self>, Box<Self>),
    /// Parenthesized expression: `(filter)`
    Parens(Box<Self>),
    /// Label matcher: `label = "value"`, `label =~ "regex"`
    Matcher(LabelMatcher),
    /// Numeric comparison: `label > 10`, `label <= 100`
    Number {
        /// The label name.
        label: String,
        /// The comparison operator.
        op: ComparisonOp,
        /// The value to compare against.
        value: f64,
    },
    /// Duration comparison: `label > 1s`, `label <= 5m`
    Duration {
        /// The label name.
        label: String,
        /// The comparison operator.
        op: ComparisonOp,
        /// The duration value.
        value: TimeDelta,
    },
    /// Bytes comparison: `label > 1KB`, `label <= 1MB`
    Bytes {
        /// The label name.
        label: String,
        /// The comparison operator.
        op: ComparisonOp,
        /// The bytes value.
        value: u64,
    },
    /// IP filter: `label = ip("192.168.0.0/16")`
    Ip {
        /// The label name.
        label: String,
        /// Whether the filter is negated.
        negated: bool,
        /// The CIDR string.
        cidr: String,
    },
}

impl LabelFilterExpr {
    /// Creates a new logical AND filter.
    pub fn and(left: Self, right: Self) -> Self {
        Self::And(Box::new(left), Box::new(right))
    }

    /// Creates a new logical OR filter.
    pub fn or(left: Self, right: Self) -> Self {
        Self::Or(Box::new(left), Box::new(right))
    }

    /// Creates a new parenthesized filter.
    pub fn parens(inner: Self) -> Self {
        Self::Parens(Box::new(inner))
    }

    /// Creates a new matcher filter.
    pub const fn matcher(matcher: LabelMatcher) -> Self {
        Self::Matcher(matcher)
    }

    /// Creates a new numeric comparison filter.
    pub fn number(label: impl Into<String>, op: ComparisonOp, value: f64) -> Self {
        Self::Number {
            label: label.into(),
            op,
            value,
        }
    }

    /// Creates a new duration comparison filter.
    pub fn duration(label: impl Into<String>, op: ComparisonOp, value: TimeDelta) -> Self {
        Self::Duration {
            label: label.into(),
            op,
            value,
        }
    }

    /// Creates a new bytes comparison filter.
    pub fn bytes(label: impl Into<String>, op: ComparisonOp, value: u64) -> Self {
        Self::Bytes {
            label: label.into(),
            op,
            value,
        }
    }

    /// Creates a new IP filter.
    pub fn ip(label: impl Into<String>, negated: bool, cidr: impl Into<String>) -> Self {
        Self::Ip {
            label: label.into(),
            negated,
            cidr: cidr.into(),
        }
    }
}

/// Unwrap expression for extracting numeric values from labels.
#[derive(Debug, Clone, PartialEq)]
pub struct UnwrapExpr {
    /// The label to unwrap
    pub label: String,
    /// Optional conversion function
    pub conversion: Option<UnwrapConversion>,
    /// Optional post-unwrap filter
    pub post_filter: Option<Box<LabelFilterExpr>>,
}

impl UnwrapExpr {
    /// Creates a new `UnwrapExpr` for the given label.
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            conversion: None,
            post_filter: None,
        }
    }

    /// Creates a new `UnwrapExpr` with a conversion function.
    pub fn with_conversion(label: impl Into<String>, conversion: UnwrapConversion) -> Self {
        Self {
            label: label.into(),
            conversion: Some(conversion),
            post_filter: None,
        }
    }
}

/// Conversion function for unwrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnwrapConversion {
    /// `duration(label)` - parse as Go duration
    Duration,
    /// `duration_seconds(label)` - parse as seconds
    DurationSeconds,
    /// `bytes(label)` - parse as byte size
    Bytes,
}

impl UnwrapConversion {
    /// Returns the string representation of the conversion function.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Duration => "duration",
            Self::DurationSeconds => "duration_seconds",
            Self::Bytes => "bytes",
        }
    }
}
