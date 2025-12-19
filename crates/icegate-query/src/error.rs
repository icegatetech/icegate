//! Error types for query operations.

use core::fmt;

use antlr4rust::errors::ANTLRError;
use datafusion::error::DataFusionError;

/// Result type alias for query operations.
pub type Result<T> = std::result::Result<T, QueryError>;

/// Errors that can occur during query operations.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// Parse error with syntax details.
    #[error("{}", format_parse_errors(.0))]
    Parse(Vec<ParseError>),

    /// Query planning error.
    #[error("plan error: {0}")]
    Plan(String),

    /// DataFusion query execution error.
    #[error("datafusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    /// Feature not yet implemented.
    #[error("not implemented: {0}")]
    NotImplemented(String),

    /// Invalid query parameters.
    #[error("{0}")]
    Validation(String),

    /// Configuration error (step, range parameters).
    #[error("configuration error: {0}")]
    Config(String),

    /// Underlying Iceberg error (from catalog operations).
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
}

impl From<icegate_common::error::CommonError> for QueryError {
    fn from(err: icegate_common::error::CommonError) -> Self {
        use icegate_common::error::CommonError;
        match err {
            CommonError::Config(msg) => Self::Config(msg),
            CommonError::Toml(e) => Self::Config(e.to_string()),
            CommonError::Yaml(e) => Self::Config(e.to_string()),
            CommonError::Iceberg(e) => Self::Iceberg(e),
            CommonError::Io(e) => Self::Config(e.to_string()),
        }
    }
}

/// Detailed parse error information.
#[derive(Debug, Clone)]
pub struct ParseError {
    /// Line number where the error occurred.
    pub line: isize,
    /// Column number where the error occurred.
    pub column: isize,
    /// Human-readable error message.
    pub message: String,
    /// Optional ANTLR-specific error details.
    pub antlr_error: Option<ANTLRError>,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "line {}:{} - {}", self.line, self.column, self.message)?;
        if let Some(ref err) = self.antlr_error {
            write!(f, " ({err:?})")?;
        }
        Ok(())
    }
}

fn format_parse_errors(errors: &[ParseError]) -> String {
    use std::fmt::Write;

    if errors.is_empty() {
        "Parse error with no details".to_string()
    } else if errors.len() == 1 {
        format!("Parse error: {}", errors[0])
    } else {
        let mut result = format!("Parse errors ({} total):", errors.len());
        for (i, err) in errors.iter().enumerate() {
            let _ = write!(result, "\n  {}. {}", i + 1, err);
        }
        result
    }
}

/// Helper to create a parse error with a single message.
pub fn parse_error(message: impl Into<String>) -> QueryError {
    QueryError::Parse(vec![ParseError {
        line: 0,
        column: 0,
        message: message.into(),
        antlr_error: None,
    }])
}
