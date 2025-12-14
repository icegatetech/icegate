//! Error types for IceGate operations.

use core::fmt;

use antlr4rust::errors::ANTLRError;
use datafusion::error::DataFusionError;

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
#[derive(Debug, Clone)]
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

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Schema error in table '{}': {} - {}",
            self.table, self.kind, self.message
        )
    }
}

/// The main error type for `IceGate` operations.
#[derive(Debug)]
pub enum IceGateError {
    /// Parse error with syntax errors.
    ///
    /// This error happens when the parser encounters syntax errors.
    Parse(Vec<ParseError>),
    /// Planner error
    ///
    /// This error happens when syntax of expression is valid, but usage of
    /// expression's part are not valid
    Plan(String),
    /// `DataFusion` query planning or execution error.
    ///
    /// This error happens when `DataFusion` operations fail during query
    /// planning or execution.
    DataFusion(DataFusionError),
    /// Schema error.
    ///
    /// This error happens when building or validating table schemas.
    Schema(SchemaError),
    /// Iceberg library error.
    ///
    /// This error happens when Iceberg operations fail.
    Iceberg(iceberg::Error),
    /// A collection of one or more [`IceGateError`]. Useful in cases where
    /// `IceGate` can recover from an erroneous state, and produce more errors
    /// before terminating. e.g. when transpiling a query.
    Collection(Vec<Self>),
    /// Not implemented error.
    NotImplemented(String),
    /// Configuration error.
    ///
    /// This error happens when loading or validating configuration fails.
    Config(String),
    /// I/O error.
    ///
    /// This error happens when I/O operations fail.
    Io(std::io::Error),
    /// Validation error for invalid request parameters.
    ///
    /// This error happens when request parameters fail validation.
    Validation(String),
}

impl fmt::Display for IceGateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NotImplemented(msg) => {
                write!(f, "Not implemented: {msg}")
            },
            Self::Parse(errors) => {
                if errors.is_empty() {
                    write!(f, "Parse error with no details")
                } else if errors.len() == 1 {
                    write!(f, "Parse error: {}", errors[0])
                } else {
                    writeln!(f, "Parse errors ({} total):", errors.len())?;
                    for (i, err) in errors.iter().enumerate() {
                        write!(f, "  {}. {}", i + 1, err)?;
                        if i < errors.len() - 1 {
                            writeln!(f)?;
                        }
                    }
                    Ok(())
                }
            },
            Self::DataFusion(err) => {
                write!(f, "DataFusion error: {err}")
            },
            Self::Schema(err) => {
                write!(f, "{err}")
            },
            Self::Iceberg(err) => {
                write!(f, "Iceberg error: {err}")
            },
            Self::Collection(errors) => {
                if errors.is_empty() {
                    write!(f, "Empty error collection")
                } else if errors.len() == 1 {
                    write!(f, "{}", errors[0])
                } else {
                    writeln!(f, "Multiple errors occurred ({} total):", errors.len())?;
                    for (i, err) in errors.iter().enumerate() {
                        write!(f, "  {}. {}", i + 1, err)?;
                        if i < errors.len() - 1 {
                            writeln!(f)?;
                        }
                    }
                    Ok(())
                }
            },
            Self::Config(msg) => {
                write!(f, "Configuration error: {msg}")
            },
            Self::Io(err) => {
                write!(f, "I/O error: {err}")
            },
            Self::Plan(msg) => {
                write!(f, "Plan error: {msg}")
            },
            Self::Validation(msg) => {
                write!(f, "{msg}")
            },
        }
    }
}

impl From<DataFusionError> for IceGateError {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusion(err)
    }
}

impl From<SchemaError> for IceGateError {
    fn from(err: SchemaError) -> Self {
        Self::Schema(err)
    }
}

impl From<iceberg::Error> for IceGateError {
    fn from(err: iceberg::Error) -> Self {
        Self::Iceberg(err)
    }
}

impl From<std::io::Error> for IceGateError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<Box<dyn std::error::Error>> for IceGateError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        Self::Config(err.to_string())
    }
}

impl std::error::Error for IceGateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DataFusion(err) => Some(err),
            Self::Iceberg(err) => Some(err),
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl std::error::Error for SchemaError {}

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
