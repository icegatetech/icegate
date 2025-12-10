//! Metric query expression types for LogQL.

use chrono::TimeDelta;

use super::{
    common::Grouping,
    log::{LogExpr, UnwrapExpr},
};

/// Metric query expression.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricExpr {
    /// Binary operation: `left op right`
    /// Binary operation: `left op right`
    BinaryOp {
        /// Left operand
        left: Box<MetricExpr>,
        /// Operator
        op: BinaryOp,
        /// Optional modifier (bool or vector matching)
        modifier: Option<BinaryOpModifier>,
        /// Right operand
        right: Box<MetricExpr>,
    },
    /// Range aggregation: `rate({...}[5m])`, `count_over_time({...}[1h])`
    RangeAggregation(RangeAggregation),
    /// Vector aggregation: `sum by (label) (...)`, `avg(...))`
    VectorAggregation(VectorAggregation),
    /// Literal number: `42`, `3.14`
    Literal(f64),
    /// Label replace function: `label_replace(expr, dst, replacement, src,
    /// regex)`
    LabelReplace {
        /// Source expression
        expr: Box<MetricExpr>,
        /// Destination label name
        dst_label: String,
        /// Replacement string
        replacement: String,
        /// Source label name
        src_label: String,
        /// Regex pattern
        regex: String,
    },
    /// Vector literal: `vector(1.5)`
    Vector(f64),
    /// Variable reference: `$variable`
    Variable(String),
    /// Parenthesized expression: `(expr)`
    Parens(Box<MetricExpr>),
}

impl MetricExpr {
    /// Creates a literal metric expression.
    #[must_use]
    pub const fn literal(value: f64) -> Self {
        Self::Literal(value)
    }

    /// Creates a vector literal metric expression.
    #[must_use]
    pub const fn vector(value: f64) -> Self {
        Self::Vector(value)
    }

    /// Creates a variable reference metric expression.
    #[must_use]
    pub fn variable(name: impl Into<String>) -> Self {
        Self::Variable(name.into())
    }

    /// Creates a parenthesized metric expression.
    #[must_use]
    pub fn parens(inner: Self) -> Self {
        Self::Parens(Box::new(inner))
    }

    /// Creates a binary operation metric expression.
    #[must_use]
    pub fn binary(left: Self, op: BinaryOp, right: Self) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op,
            modifier: None,
            right: Box::new(right),
        }
    }

    /// Creates a binary operation metric expression with a modifier.
    pub fn binary_with_modifier(left: Self, op: BinaryOp, modifier: BinaryOpModifier, right: Self) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op,
            modifier: Some(modifier),
            right: Box::new(right),
        }
    }
}

/// Binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
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
    /// `>=` greater than or equal
    Ge,
    /// `<` less than
    Lt,
    /// `<=` less than or equal
    Le,

    // Logical operators
    /// `and` logical and
    And,
    /// `or` logical or
    Or,
    /// `unless` logical unless (set difference)
    Unless,
}

impl BinaryOp {
    /// Returns the string representation of the operator.
    #[must_use]
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
            Self::Ge => ">=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::And => "and",
            Self::Or => "or",
            Self::Unless => "unless",
        }
    }

    /// Returns true if the operator is arithmetic.
    #[must_use]
    pub const fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod | Self::Pow
        )
    }

    /// Returns true if the operator is a comparison.
    #[must_use]
    pub const fn is_comparison(&self) -> bool {
        matches!(self, Self::Eq | Self::Neq | Self::Gt | Self::Ge | Self::Lt | Self::Le)
    }

    /// Returns true if the operator is logical.
    #[must_use]
    pub const fn is_logical(&self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Unless)
    }
}

/// Binary operator modifier for vector matching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryOpModifier {
    /// Return 0/1 instead of filtering
    pub return_bool: bool,
    /// Vector matching configuration
    pub vector_matching: Option<VectorMatching>,
}

impl BinaryOpModifier {
    /// Creates a modifier that returns a boolean value (0/1).
    #[must_use]
    pub const fn bool_modifier() -> Self {
        Self {
            return_bool: true,
            vector_matching: None,
        }
    }

    /// Creates a modifier with vector matching configuration.
    #[must_use]
    pub const fn with_matching(matching: VectorMatching) -> Self {
        Self {
            return_bool: false,
            vector_matching: Some(matching),
        }
    }
}

/// Vector matching configuration for binary operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorMatching {
    /// Cardinality of the match
    pub card: VectorMatchCardinality,
    /// Labels to match on
    pub matching: MatchingLabels,
    /// Labels to include from the "one" side in `group_left`/`group_right`
    pub include: Vec<String>,
}

/// Vector match cardinality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorMatchCardinality {
    /// One-to-one matching (default)
    OneToOne,
    /// Many-to-one matching: `group_left`
    ManyToOne,
    /// One-to-many matching: `group_right`
    OneToMany,
}

/// Label matching mode for binary operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchingLabels {
    /// `on(label1, label2, ...)` - match only on these labels
    On(Vec<String>),
    /// `ignoring(label1, label2, ...)` - match on all labels except these
    Ignoring(Vec<String>),
}

/// Range aggregation expression.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeAggregation {
    /// Aggregation operator
    pub op: RangeAggregationOp,
    /// Range expression
    pub range_expr: RangeExpr,
    /// Optional grouping
    pub grouping: Option<Grouping>,
    /// Parameter for `quantile_over_time` (0.0-1.0)
    pub param: Option<f64>,
}

impl RangeAggregation {
    /// Creates a new range aggregation.
    pub const fn new(op: RangeAggregationOp, range_expr: RangeExpr) -> Self {
        Self {
            op,
            range_expr,
            grouping: None,
            param: None,
        }
    }

    /// Sets the grouping for the aggregation.
    #[must_use]
    pub fn with_grouping(mut self, grouping: Grouping) -> Self {
        self.grouping = Some(grouping);
        self
    }

    /// Sets the parameter for the aggregation (e.g., quantile).
    #[must_use]
    pub const fn with_param(mut self, param: f64) -> Self {
        self.param = Some(param);
        self
    }
}

/// Range aggregation operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RangeAggregationOp {
    // Log-based operations (no unwrap required)
    /// `count_over_time` - count of log lines
    CountOverTime,
    /// `rate` - rate of log lines per second
    Rate,
    /// `bytes_over_time` - sum of bytes
    BytesOverTime,
    /// `bytes_rate` - rate of bytes per second
    BytesRate,
    /// `absent_over_time` - returns 1 if no samples
    AbsentOverTime,

    // Unwrap-based operations
    /// `sum_over_time` - sum of unwrapped values
    SumOverTime,
    /// `avg_over_time` - average of unwrapped values
    AvgOverTime,
    /// `min_over_time` - minimum of unwrapped values
    MinOverTime,
    /// `max_over_time` - maximum of unwrapped values
    MaxOverTime,
    /// `stddev_over_time` - standard deviation
    StddevOverTime,
    /// `stdvar_over_time` - standard variance
    StdvarOverTime,
    /// `quantile_over_time` - quantile of unwrapped values
    QuantileOverTime,
    /// `first_over_time` - first value
    FirstOverTime,
    /// `last_over_time` - last value
    LastOverTime,
    /// `rate_counter` - rate for counter metrics
    RateCounter,
}

impl RangeAggregationOp {
    /// Returns the string representation of the operator.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CountOverTime => "count_over_time",
            Self::Rate => "rate",
            Self::BytesOverTime => "bytes_over_time",
            Self::BytesRate => "bytes_rate",
            Self::AbsentOverTime => "absent_over_time",
            Self::SumOverTime => "sum_over_time",
            Self::AvgOverTime => "avg_over_time",
            Self::MinOverTime => "min_over_time",
            Self::MaxOverTime => "max_over_time",
            Self::StddevOverTime => "stddev_over_time",
            Self::StdvarOverTime => "stdvar_over_time",
            Self::QuantileOverTime => "quantile_over_time",
            Self::FirstOverTime => "first_over_time",
            Self::LastOverTime => "last_over_time",
            Self::RateCounter => "rate_counter",
        }
    }

    /// Returns true if this operation requires an unwrap expression.
    pub const fn requires_unwrap(&self) -> bool {
        !matches!(
            self,
            Self::CountOverTime | Self::Rate | Self::BytesOverTime | Self::BytesRate | Self::AbsentOverTime
        )
    }
}

/// Range expression: log query with range, offset, and optional unwrap.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeExpr {
    /// Log expression
    pub log_expr: LogExpr,
    /// Range duration
    pub range: TimeDelta,
    /// Optional offset
    pub offset: Option<TimeDelta>,
    /// `@ <timestamp>` modifier
    pub at: Option<AtModifier>,
    /// Optional unwrap expression
    pub unwrap: Option<UnwrapExpr>,
}

impl RangeExpr {
    /// Creates a new range expression.
    pub const fn new(log_expr: LogExpr, range: TimeDelta) -> Self {
        Self {
            log_expr,
            range,
            offset: None,
            at: None,
            unwrap: None,
        }
    }

    /// Sets the offset for the range expression.
    #[must_use]
    pub const fn with_offset(mut self, offset: TimeDelta) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Sets the `@` modifier for the range expression.
    #[must_use]
    pub const fn with_at(mut self, at: AtModifier) -> Self {
        self.at = Some(at);
        self
    }

    /// Sets the unwrap expression for the range expression.
    #[must_use]
    pub fn with_unwrap(mut self, unwrap: UnwrapExpr) -> Self {
        self.unwrap = Some(unwrap);
        self
    }
}

/// `@` modifier for specifying evaluation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtModifier {
    /// `@ <unix_timestamp>` - evaluate at specific timestamp
    Timestamp(i64),
    /// `@ start()` - evaluate at query start time
    Start,
    /// `@ end()` - evaluate at query end time
    End,
}

/// Vector aggregation expression.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorAggregation {
    /// Aggregation operator
    pub op: VectorAggregationOp,
    /// Inner metric expression
    pub expr: Box<MetricExpr>,
    /// Optional grouping
    pub grouping: Option<Grouping>,
    /// Parameter for topk/bottomk (k value)
    pub param: Option<u32>,
}

impl VectorAggregation {
    /// Creates a new vector aggregation.
    #[must_use]
    pub fn new(op: VectorAggregationOp, expr: MetricExpr) -> Self {
        Self {
            op,
            expr: Box::new(expr),
            grouping: None,
            param: None,
        }
    }

    /// Sets the grouping for the aggregation.
    #[must_use]
    pub fn with_grouping(mut self, grouping: Grouping) -> Self {
        self.grouping = Some(grouping);
        self
    }

    /// Sets the parameter for the aggregation (e.g., k for topk).
    #[must_use]
    pub const fn with_param(mut self, param: u32) -> Self {
        self.param = Some(param);
        self
    }
}

/// Vector aggregation operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorAggregationOp {
    /// `sum` - sum of values
    Sum,
    /// `avg` - average of values
    Avg,
    /// `count` - count of elements
    Count,
    /// `max` - maximum value
    Max,
    /// `min` - minimum value
    Min,
    /// `stddev` - standard deviation
    Stddev,
    /// `stdvar` - standard variance
    Stdvar,
    /// `topk` - top k elements
    Topk,
    /// `bottomk` - bottom k elements
    Bottomk,
    /// `approx_topk` - approximate top k
    ApproxTopk,
    /// `sort` - sort ascending
    Sort,
    /// `sort_desc` - sort descending
    SortDesc,
}

impl VectorAggregationOp {
    /// Returns the string representation of the operator.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Count => "count",
            Self::Max => "max",
            Self::Min => "min",
            Self::Stddev => "stddev",
            Self::Stdvar => "stdvar",
            Self::Topk => "topk",
            Self::Bottomk => "bottomk",
            Self::ApproxTopk => "approx_topk",
            Self::Sort => "sort",
            Self::SortDesc => "sort_desc",
        }
    }

    /// Returns true if this operation requires a parameter (k value).
    pub const fn requires_param(&self) -> bool {
        matches!(self, Self::Topk | Self::Bottomk | Self::ApproxTopk)
    }
}
