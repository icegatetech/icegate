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
    /// `describe <index>`: one row per field of the index's field model (JDBC `getColumns` shape).
    /// The index is kept as written; it names exactly one data index.
    Describe {
        /// The index expression as written in the query.
        index: String,
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
    /// `stats <aggregation>, … [by <key>, …]`.
    Stats(StatsCommand),
    /// `top [N] <field>, … [by <field>, …]`: the N most frequent value combinations per group.
    Top(RankCommand),
    /// `rare [N] <field>, … [by <field>, …]`: the N least frequent value combinations per group.
    Rare(RankCommand),
    /// `dedup [N] <field>, … [keepempty=<bool>]`.
    Dedup(DedupCommand),
    /// `timechart [span=…] [limit=…] [useother=…] [timefield=…] <aggregation> [by <field>]`.
    Timechart(TimechartCommand),
    /// `bin <field> span=<span>`.
    Bin(BinCommand),
    /// `parse <field> '<pattern>'`.
    Parse(ParseCommand),
    /// `fillnull` in any of its four forms.
    Fillnull(FillnullCommand),
}

/// `parse <field> '<pattern>'`: every named group of the pattern becomes a string field.
///
/// The pattern must match the whole value. A NULL source yields NULL in every group field; a value the
/// pattern does not match, or a group that did not take part in the match, yields `""`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ParseCommand {
    /// The parsed value: a field reference or any expression of a string type.
    pub(crate) source: PplExpr,
    /// The pattern as written, unanchored, with `(?<name>…)` named groups.
    pub(crate) pattern: String,
}

/// `fillnull`: which output columns take which value where they are NULL.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FillnullCommand {
    /// The columns to fill and their replacement values.
    pub(crate) replacements: FillnullReplacements,
}

/// The column selection of a `fillnull`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FillnullReplacements {
    /// `fillnull with <v>` or `fillnull value=<v>`: every output column takes `<v>`, so every column
    /// must accept its type.
    AllFields(PplExpr),
    /// Named columns, each with its own replacement, in the order written.
    Fields(Vec<(String, PplExpr)>),
}

/// A `stats` command.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StatsCommand {
    /// The aggregations in order, each with its output name: the alias, or the text of the call
    /// as written without whitespace (`count()`, `avg(duration)`).
    pub(crate) aggregations: Vec<(String, PplExpr)>,
    /// The group keys: the span key first, then the `by` fields in the order written.
    pub(crate) by: Vec<PplGroupKey>,
    /// Whether rows whose key is NULL form a group (`bucket_nullable`, true by default).
    pub(crate) is_bucket_nullable: bool,
}

/// A group key of `stats`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PplGroupKey {
    /// A field; the output column is named after it.
    Field(String),
    /// `span(<field>, <length><unit>) [as <alias>]`: the bucket a value falls in.
    Span {
        /// The bucketed field; the builder's implicit time field when the clause names none.
        field: String,
        /// The bucket length, a positive whole number for a time unit.
        length: f64,
        /// The unit of `length`.
        unit: SpanUnit,
        /// The output column name, when the clause names one.
        alias: Option<String>,
    },
}

/// The unit of a span length.
///
/// Fixed units bucket from the Unix epoch; `Week` buckets start on Mondays and `Month`,
/// `Quarter` and `Year` on the first day of their calendar period, all in UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpanUnit {
    /// No unit: the length is a number and the field numeric.
    Numeric,
    /// Microseconds (`bin` only).
    Microsecond,
    /// Milliseconds.
    Millisecond,
    /// Hundredths of a second (`bin` only).
    Centisecond,
    /// Tenths of a second (`bin` only).
    Decisecond,
    /// Seconds.
    Second,
    /// Minutes.
    Minute,
    /// Hours.
    Hour,
    /// Days.
    Day,
    /// Weeks starting on Monday.
    Week,
    /// Calendar months.
    Month,
    /// Calendar quarters.
    Quarter,
    /// Calendar years.
    Year,
}

/// A `top` or `rare` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RankCommand {
    /// How many value combinations to keep per group (the builder's default when unnamed).
    pub(crate) count: usize,
    /// The ranked fields.
    pub(crate) fields: Vec<String>,
    /// The group fields (`by`).
    pub(crate) by: Vec<String>,
    /// The name of the count column (`countfield`, the builder's default when unnamed).
    pub(crate) count_field: String,
    /// Whether the count column is returned (`showcount`, true by default).
    pub(crate) is_count_shown: bool,
    /// Whether NULL values form a combination (`usenull`, true by default).
    pub(crate) is_null_kept: bool,
}

/// A `dedup` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DedupCommand {
    /// How many rows to keep per combination of the fields (one when unnamed).
    pub(crate) count: usize,
    /// The fields whose combinations are deduplicated.
    pub(crate) fields: Vec<String>,
    /// Whether rows with a NULL field are kept, all of them (`keepempty`); otherwise they are
    /// dropped.
    pub(crate) keeps_empty: bool,
}

/// A `timechart` command.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TimechartCommand {
    /// The time field (`timefield`, the builder's implicit time field by default).
    pub(crate) time_field: String,
    /// The bucket length (`span`, the builder's default when unnamed).
    pub(crate) span_length: f64,
    /// The unit of `span_length`.
    pub(crate) span_unit: SpanUnit,
    /// How many series to keep before the rest become the other-series row (`limit`, the
    /// builder's default when unnamed; 0 keeps every series).
    pub(crate) limit: usize,
    /// Whether the series beyond `limit` are kept as the other-series row (`useother`, true by
    /// default) rather than dropped.
    pub(crate) uses_other: bool,
    /// The one aggregation, with its output name.
    pub(crate) aggregation: (String, PplExpr),
    /// The series field (`by`).
    pub(crate) by: Option<String>,
}

/// A `bin` command.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BinCommand {
    /// The binned field; the bin replaces its value in place.
    pub(crate) field: String,
    /// The bin width.
    pub(crate) span: BinSpan,
}

/// The width of a `bin`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum BinSpan {
    /// A numeric width: numbers bin to `<start>-<end>` ranges; on a timestamp field, hours.
    Numeric(f64),
    /// A time width.
    Time {
        /// The number of units.
        length: i64,
        /// The unit.
        unit: SpanUnit,
    },
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
