//! The PPL abstract syntax tree: what a query asks for, independent of the parse tree that
//! produced it and of the tables it reads.
//!
//! A field reference is its dotted path with backticks removed: ``log_attributes.`http.method` ``
//! is `log_attributes.http.method`. The planner resolves it against the columns of the pipeline
//! at the point of use.

/// A parsed PPL query: one source and the commands piped after it, in order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PplQuery {
    /// Where the rows come from.
    pub(crate) source: PplSource,
    /// The commands applied to the rows, in pipe order.
    pub(crate) commands: Vec<PplCommand>,
}

/// What a query reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PplSource {
    /// `source=<expression>` (also `search source=…` and `index=…`): the indices an index
    /// expression names, comma lists and wildcards included, resolved case-sensitively.
    Index {
        /// The index expression, the listed sources joined with commas.
        expression: String,
    },
}

/// One command after the source.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PplCommand {
    /// `where <predicate>`: keeps the rows whose predicate is true; a NULL predicate drops the row.
    Where(PplExpr),
    /// `fields [+|-] <field>, …`: keeps, or with `-` drops, the listed fields. A `*` in a name is a
    /// wildcard over the current column names.
    Fields {
        /// Whether the listed fields are dropped instead of kept.
        is_exclusion: bool,
        /// The field names and wildcard patterns, in the order written.
        fields: Vec<String>,
    },
    /// `sort <key>, …`.
    Sort(Vec<PplSortKey>),
    /// `head [N] [from M]`: skips `offset` rows, then keeps `count`.
    Head {
        /// How many rows to keep; the builder's default when the command names no number.
        count: usize,
        /// How many rows to skip first.
        offset: usize,
    },
    /// `eval <field> = <expression>, …`: each assignment sees the fields of the ones before it.
    /// An existing field is overwritten in place; a new one is appended.
    Eval(Vec<(String, PplExpr)>),
    /// `rename <field> as <name>, …`: renames in place; a field already named `<name>` is
    /// replaced.
    Rename(Vec<(String, String)>),
}

/// One key of a `sort` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PplSortKey {
    /// The field the rows are ordered by.
    pub(crate) field: String,
    /// Whether the order is descending (`- field`, `field desc`). NULL sorts first in ascending
    /// order and last in descending order.
    pub(crate) is_descending: bool,
}

/// A PPL expression.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PplExpr {
    /// A field reference.
    Field(String),
    /// A literal value.
    Literal(PplLiteral),
    /// A comparison of two values.
    Compare {
        /// The comparison.
        operator: CompareOperator,
        /// The left operand.
        left: Box<Self>,
        /// The right operand.
        right: Box<Self>,
    },
    /// `not <predicate>`.
    Not(Box<Self>),
    /// `<predicate> and <predicate>`.
    And(Box<Self>, Box<Self>),
    /// `<predicate> or <predicate>`.
    Or(Box<Self>, Box<Self>),
    /// `<predicate> xor <predicate>`: true when exactly one side is true.
    Xor(Box<Self>, Box<Self>),
    /// `+`, `-`, `*`, `/` or `%` of two values.
    Arithmetic {
        /// The operation.
        operator: ArithmeticOperator,
        /// The left operand.
        left: Box<Self>,
        /// The right operand.
        right: Box<Self>,
    },
    /// A call of a named function; the name is lower-cased.
    FunctionCall {
        /// The function name.
        name: String,
        /// The arguments, in order.
        arguments: Vec<Self>,
    },
    /// `case(<condition>, <value>, … [else <value>])`: the value of the first true condition, else
    /// the `else` value, else NULL.
    Case {
        /// The conditions and their values, in order.
        branches: Vec<(Self, Self)>,
        /// The value when no condition is true.
        otherwise: Option<Box<Self>>,
    },
    /// `cast(<value> as <type>)`.
    Cast {
        /// The value converted.
        value: Box<Self>,
        /// The type it is converted to.
        target: CastTarget,
    },
    /// `<parameter> -> <body>` or `(<parameter>, …) -> <body>`, the argument of an array function.
    Lambda {
        /// The parameter names, bound to array elements.
        parameters: Vec<String>,
        /// The body, evaluated per element.
        body: Box<Self>,
    },
    /// `<value> [not] in (<literal>, …)`.
    In {
        /// The value tested.
        value: Box<Self>,
        /// The listed values.
        list: Vec<Self>,
        /// Whether the test is `not in`.
        is_negated: bool,
    },
    /// `<value> [not] between <low> and <high>`, both bounds inclusive.
    Between {
        /// The value tested.
        value: Box<Self>,
        /// The lower bound.
        low: Box<Self>,
        /// The upper bound.
        high: Box<Self>,
        /// Whether the test is `not between`.
        is_negated: bool,
    },
    /// `<value> is [not] null`.
    IsNull {
        /// The value tested.
        value: Box<Self>,
        /// Whether the test is `is not null`.
        is_negated: bool,
    },
}

/// A literal value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PplLiteral {
    /// A quoted string, unquoted.
    String(String),
    /// An integer.
    Integer(i64),
    /// A decimal (`1.5`), double (`1.5D`) or float (`1.5F`) literal, read as a double.
    Double(f64),
    /// `true` or `false`.
    Boolean(bool),
}

/// A comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompareOperator {
    /// `=` or `==`.
    Equal,
    /// `!=`.
    NotEqual,
    /// `<`.
    Less,
    /// `<=`.
    LessOrEqual,
    /// `>`.
    Greater,
    /// `>=`.
    GreaterOrEqual,
}

/// An arithmetic operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArithmeticOperator {
    /// `+`.
    Add,
    /// `-`.
    Subtract,
    /// `*`.
    Multiply,
    /// `/`: integer operands divide with truncation toward zero; dividing by zero is NULL.
    Divide,
    /// `%`: the remainder with the sign of the dividend; a zero divisor is NULL.
    Modulo,
}

/// The target type of a `cast`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastTarget {
    /// `double` (also `float`).
    Double,
    /// `long`.
    Long,
    /// `int` (also `integer`).
    Int,
    /// `string`.
    String,
    /// `boolean`.
    Boolean,
    /// `timestamp`.
    Timestamp,
}
