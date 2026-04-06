//! Binary operation, unary operation, and subquery types for PromQL.

use chrono::TimeDelta;

use super::{common::AtModifier, expr::PromQLExpr};

/// `PromQL` binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    // Arithmetic operators
    /// `+` addition
    Add,
    /// `-` subtraction
    Sub,
    /// `*` multiplication
    Mul,
    /// `/` division
    Div,
    /// `%` modulo
    Mod,
    /// `^` power
    Pow,

    // Comparison operators
    /// `==` equal
    Eq,
    /// `!=` not equal
    Neq,
    /// `>` greater than
    Gt,
    /// `<` less than
    Lt,
    /// `>=` greater than or equal
    Ge,
    /// `<=` less than or equal
    Le,

    // Logical/set operators
    /// `and` intersection
    And,
    /// `or` union
    Or,
    /// `unless` complement
    Unless,
}

impl BinaryOperator {
    /// Returns the string representation of the operator.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Pow => "^",
            Self::Eq => "==",
            Self::Neq => "!=",
            Self::Gt => ">",
            Self::Lt => "<",
            Self::Ge => ">=",
            Self::Le => "<=",
            Self::And => "and",
            Self::Or => "or",
            Self::Unless => "unless",
        }
    }

    /// Returns true if the operator is arithmetic.
    pub const fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod | Self::Pow
        )
    }

    /// Returns true if the operator is a comparison.
    pub const fn is_comparison(&self) -> bool {
        matches!(self, Self::Eq | Self::Neq | Self::Gt | Self::Lt | Self::Ge | Self::Le)
    }

    /// Returns true if the operator is a logical/set operator.
    pub const fn is_set_operator(&self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Unless)
    }
}

/// A binary operation expression.
///
/// Represents expressions like `rate(a[5m]) + rate(b[5m])` or
/// `http_requests_total > bool 100`.
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryOp {
    /// Operator
    pub op: BinaryOperator,
    /// Left operand
    pub lhs: Box<PromQLExpr>,
    /// Right operand
    pub rhs: Box<PromQLExpr>,
    /// Optional modifier (bool return and/or vector matching)
    pub modifier: Option<BinaryOpModifier>,
}

impl BinaryOp {
    /// Creates a new binary operation.
    pub fn new(op: BinaryOperator, lhs: PromQLExpr, rhs: PromQLExpr) -> Self {
        Self {
            op,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
            modifier: None,
        }
    }

    /// Creates a binary operation with a modifier.
    pub fn with_modifier(op: BinaryOperator, lhs: PromQLExpr, rhs: PromQLExpr, modifier: BinaryOpModifier) -> Self {
        Self {
            op,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
            modifier: Some(modifier),
        }
    }
}

/// Modifier for binary operations controlling matching behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryOpModifier {
    /// `bool` modifier: return 0/1 instead of filtering for comparison ops
    pub return_bool: bool,
    /// Vector matching configuration
    pub matching: Option<VectorMatching>,
}

impl BinaryOpModifier {
    /// Creates a `bool`-only modifier.
    pub const fn bool_modifier() -> Self {
        Self {
            return_bool: true,
            matching: None,
        }
    }

    /// Creates a modifier with vector matching.
    pub const fn with_matching(matching: VectorMatching) -> Self {
        Self {
            return_bool: false,
            matching: Some(matching),
        }
    }

    /// Creates a modifier with both `bool` and vector matching.
    pub const fn bool_with_matching(matching: VectorMatching) -> Self {
        Self {
            return_bool: true,
            matching: Some(matching),
        }
    }
}

/// Vector matching configuration for binary operations between two vectors.
///
/// Controls which labels are used for matching and the cardinality
/// of the join (one-to-one, many-to-one, one-to-many).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorMatching {
    /// `on(labels)` - match only on specified labels.
    /// `None` when using `ignoring` instead.
    pub on_labels: Option<Vec<String>>,
    /// `ignoring(labels)` - match on all labels except specified.
    /// `None` when using `on` instead.
    pub ignoring_labels: Option<Vec<String>>,
    /// Group modifier for many-to-one/one-to-many matching.
    pub group: Option<GroupModifier>,
}

/// Group modifier for vector matching cardinality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupModifier {
    /// `group_left(labels)` - many-to-one, include extra labels from left
    Left(Vec<String>),
    /// `group_right(labels)` - one-to-many, include extra labels from right
    Right(Vec<String>),
}

/// Unary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
    /// `-` negation
    Neg,
    /// `+` (identity, no-op)
    Pos,
}

/// A unary operation expression.
///
/// Represents expressions like `-http_requests_total` or
/// `-rate(http_requests_total[5m])`.
#[derive(Debug, Clone, PartialEq)]
pub struct UnaryOp {
    /// Operator
    pub op: UnaryOperator,
    /// Operand
    pub expr: Box<PromQLExpr>,
}

impl UnaryOp {
    /// Creates a new unary operation.
    pub fn new(op: UnaryOperator, expr: PromQLExpr) -> Self {
        Self {
            op,
            expr: Box::new(expr),
        }
    }
}

/// A subquery expression.
///
/// Evaluates an instant query over a range, producing a range vector.
/// For example: `rate(http_requests_total[5m])[30m:1m]` evaluates
/// `rate(http_requests_total[5m])` every 1m over the last 30m.
#[derive(Debug, Clone, PartialEq)]
pub struct Subquery {
    /// Inner expression (must produce an instant vector)
    pub expr: Box<PromQLExpr>,
    /// Subquery range duration
    pub range: TimeDelta,
    /// Optional step (resolution). If omitted, uses the global evaluation
    /// interval.
    pub step: Option<TimeDelta>,
    /// Optional offset modifier
    pub offset: Option<TimeDelta>,
    /// Optional `@` modifier
    pub at: Option<AtModifier>,
}

impl Subquery {
    /// Creates a new subquery.
    pub fn new(expr: PromQLExpr, range: TimeDelta) -> Self {
        Self {
            expr: Box::new(expr),
            range,
            step: None,
            offset: None,
            at: None,
        }
    }

    /// Sets the step for the subquery.
    #[must_use]
    pub const fn with_step(mut self, step: TimeDelta) -> Self {
        self.step = Some(step);
        self
    }

    /// Sets the offset for the subquery.
    #[must_use]
    pub const fn with_offset(mut self, offset: TimeDelta) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Sets the `@` modifier for the subquery.
    #[must_use]
    pub const fn with_at(mut self, at: AtModifier) -> Self {
        self.at = Some(at);
        self
    }
}
