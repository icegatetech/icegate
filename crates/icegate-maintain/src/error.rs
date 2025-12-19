//! Error types for maintenance operations.

use core::fmt;

/// Result type alias for maintain operations.
pub type Result<T> = std::result::Result<T, MaintainError>;

/// Errors that can occur during maintenance operations.
#[derive(Debug, thiserror::Error)]
pub enum MaintainError {
    /// Configuration loading/validation error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Iceberg catalog/table operations.
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// Schema construction error.
    #[error("{0}")]
    Schema(#[from] SchemaError),
}

impl From<icegate_common::error::CommonError> for MaintainError {
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

/// Kind of schema error that occurred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaErrorKind {
    /// A required field was not found in the schema.
    FieldNotFound,
}

impl fmt::Display for SchemaErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::FieldNotFound => write!(f, "field not found"),
        }
    }
}

/// Schema-related error with context about the table and error kind.
#[derive(Debug, Clone, thiserror::Error)]
#[error("Schema error in table '{table}': {kind} - {message}")]
pub struct SchemaError {
    /// The table name where the error occurred.
    pub table: String,
    /// The kind of schema error.
    pub kind: SchemaErrorKind,
    /// Additional error message details.
    pub message: String,
}

impl SchemaError {
    /// Creates a new schema error.
    pub fn new(table: impl Into<String>, kind: SchemaErrorKind, message: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            kind,
            message: message.into(),
        }
    }

    /// Creates a field not found error.
    pub fn field_not_found(table: impl Into<String>, field_name: impl Into<String>) -> Self {
        let field_name = field_name.into();
        Self::new(
            table,
            SchemaErrorKind::FieldNotFound,
            format!("field '{field_name}' not found in schema"),
        )
    }
}
