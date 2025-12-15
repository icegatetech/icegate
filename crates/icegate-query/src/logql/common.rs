//! Common types shared across LogQL expression modules.

use icegate_common::errors::{IceGateError, ParseError};

/// Create a parse error with the given message.
pub fn parse_error(message: impl Into<String>) -> IceGateError {
    IceGateError::Parse(vec![ParseError {
        line: 0,
        column: 0,
        message: message.into(),
        antlr_error: None,
    }])
}

/// Match operator for label matchers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchOp {
    /// `=` exact equality
    Eq,
    /// `!=` not equal
    Neq,
    /// `=~` regex match
    Re,
    /// `!~` regex not match
    Nre,
}

impl MatchOp {
    /// Get the string representation of the operator.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Neq => "!=",
            Self::Re => "=~",
            Self::Nre => "!~",
        }
    }
}

/// Comparison operator for numeric, duration, and bytes filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComparisonOp {
    /// `>` greater than
    Gt,
    /// `>=` greater than or equal
    Ge,
    /// `<` less than
    Lt,
    /// `<=` less than or equal
    Le,
    /// `=` or `==` equal
    Eq,
    /// `!=` not equal
    Neq,
}

impl ComparisonOp {
    /// Get the string representation of the operator.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Eq => "==",
            Self::Neq => "!=",
        }
    }
}

/// Grouping clause for aggregations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grouping {
    /// `by (label1, label2, ...)`
    By(Vec<GroupingLabel>),
    /// `without (label1, label2, ...)`
    Without(Vec<GroupingLabel>),
}

/// A label used in grouping clauses.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupingLabel {
    /// The name of the label.
    pub name: String,
}

impl GroupingLabel {
    /// Create a new `GroupingLabel`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
        }
    }
}

/// Label extraction specification for parsers and drop/keep operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelExtraction {
    /// The label name to extract to
    pub name: String,
    /// Optional path (`JSONPath` for json parser, or source label for rename)
    pub path: Option<String>,
}

impl LabelExtraction {
    /// Create a new `LabelExtraction` with just a name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            path: None,
        }
    }

    /// Create a new `LabelExtraction` with a name and a path.
    pub fn with_path(name: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            path: Some(path.into()),
        }
    }
}

/// Label format operation for `label_format` pipeline stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelFormatOp {
    /// Rename a label: `dst=src`
    Rename {
        /// The destination label name.
        dst: String,
        /// The source label name.
        src: String,
    },
    /// Apply a template: `dst="{{.field}}"`
    Template {
        /// The destination label name.
        dst: String,
        /// The template string.
        template: String,
    },
}

impl LabelFormatOp {
    /// Create a new `Rename` operation.
    pub fn rename(dst: impl Into<String>, src: impl Into<String>) -> Self {
        Self::Rename {
            dst: dst.into(),
            src: src.into(),
        }
    }

    /// Create a new `Template` operation.
    pub fn template(dst: impl Into<String>, template: impl Into<String>) -> Self {
        Self::Template {
            dst: dst.into(),
            template: template.into(),
        }
    }
}
