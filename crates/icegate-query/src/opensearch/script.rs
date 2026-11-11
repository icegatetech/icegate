//! Script recognition. IceGate runs no Painless: a script is accepted only when it is one of the
//! forms recognized here, and the recognized form is executed natively. Any other script is a
//! `400 illegal_argument_exception` naming it, because running a different computation than the
//! client asked for would return wrong data.
//!
//! Besides the saved-object counter script, the module recognizes arithmetic scripts, which
//! `bucket_script` and metric value scripts run, and the `MovingFunctions.*` invocations of
//! `moving_fn`. An arithmetic script holds decimal literals (`12`, `0.5`), `_value` (metric value
//! scripts only), `params.<name>` (`bucket_script` only, a name of its `buckets_path`),
//! `+ - * / %`, unary minus and parentheses, at most [`MAX_SCRIPT_DEPTH`] levels deep (nesting and
//! chained operators alike) and at most [`MAX_SCRIPT_LENGTH`] bytes long; it is evaluated in IEEE
//! double precision, so `_value / 1000` over `1500` is `1.5` and `x / 0` is infinite, which
//! renders `null`. A metric value script may also be one of the trace-list scripts, each
//! recognized verbatim (whitespace runs aside) and computed natively: the latency script, in its
//! form with a trailing `return 0` or without it, from the root span's duration, and the
//! last-updated script from the root span's end time, else the span's own. Every other script is
//! refused with a 400 whose `script` names it.

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn::{coalesce, floor};
use datafusion::logical_expr::{Expr, cast, lit};
use serde_json::{Map, Value};

use super::error::OpenSearchError;

/// The saved-object counter script, with every whitespace run collapsed to one space.
const INCREMENT_COUNTER_SOURCE: &str = "if (ctx._source[params.type][params.counterFieldName] == null) { ctx._source[params.type][params.counterFieldName] = params.count; } else { ctx._source[params.type][params.counterFieldName] += params.count; } ctx._source.updated_at = params.time;";

/// The parameter names the counter script reads; any other key is refused.
const INCREMENT_COUNTER_PARAMETERS: [&str; 4] = ["count", "time", "type", "counterFieldName"];

/// A recognized document-update script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DocumentScript {
    /// Adds `count` to one counter of a saved object and stamps `updated_at`.
    IncrementCounter(CounterIncrement),
}

/// The parameters of the counter script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CounterIncrement {
    /// `params.type`: the top-level `_source` key holding the saved object's attributes.
    pub(crate) object_type: String,
    /// `params.counterFieldName`: the counter inside that object.
    pub(crate) counter_field_name: String,
    /// `params.count`: the amount added.
    pub(crate) count: i64,
    /// `params.time`: the value written to `updated_at`.
    pub(crate) time: String,
}

impl CounterIncrement {
    /// Run the script on an existing document's `_source`.
    ///
    /// An absent or `null` counter is set to `count`; an integer counter gets `count` added;
    /// `updated_at` is set to `time`.
    ///
    /// # Errors
    ///
    /// `illegal_argument_exception` when `_source[object_type]` is not an object, when the
    /// counter holds anything but an integer, or when the sum overflows. `source` is then left
    /// unchanged, so the script can run on the stored document itself.
    pub(crate) fn apply_to(&self, source: &mut Value) -> Result<(), OpenSearchError> {
        let document = source
            .as_object_mut()
            .ok_or_else(|| OpenSearchError::illegal_argument("a document source must be a JSON object"))?;
        let attributes = document
            .get_mut(&self.object_type)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                OpenSearchError::illegal_argument(format!(
                    "the counter script needs an object at [{}]",
                    self.object_type
                ))
            })?;
        let counter = match attributes.get(&self.counter_field_name) {
            None | Some(Value::Null) => self.count,
            Some(Value::Number(stored_count)) => stored_count
                .as_i64()
                .and_then(|stored_integer| stored_integer.checked_add(self.count))
                .ok_or_else(|| self.refuse_counter_value())?,
            Some(_) => return Err(self.refuse_counter_value()),
        };
        attributes.insert(self.counter_field_name.clone(), Value::from(counter));
        document.insert("updated_at".to_string(), Value::String(self.time.clone()));
        Ok(())
    }

    /// The refusal of a counter that does not hold an integer, or whose sum with `count` overflows.
    fn refuse_counter_value(&self) -> OpenSearchError {
        OpenSearchError::illegal_argument(format!(
            "the counter [{}.{}] does not hold an integer that can take [{}]",
            self.object_type, self.counter_field_name, self.count
        ))
    }

    /// Read the script's `params`, which hold exactly `count` (a JSON integer) and the strings
    /// `time`, `type` and `counterFieldName`. `script` is what a refusal names.
    ///
    /// # Errors
    ///
    /// `illegal_argument_exception` naming `script` for another key, a missing key or a value of
    /// the wrong kind.
    fn from_parameters(parameters: &Map<String, Value>, script: &Value) -> Result<Self, OpenSearchError> {
        if let Some(key) = parameters
            .keys()
            .find(|key| !INCREMENT_COUNTER_PARAMETERS.contains(&key.as_str()))
        {
            return Err(refuse_script(
                script,
                &format!("the counter script does not take the parameter [{key}]"),
            ));
        }
        let read_text = |name: &str| {
            parameters.get(name).and_then(Value::as_str).map(str::to_string).ok_or_else(|| {
                refuse_script(
                    script,
                    &format!("the counter script needs the string parameter [{name}]"),
                )
            })
        };
        let count = parameters
            .get("count")
            .and_then(Value::as_i64)
            .ok_or_else(|| refuse_script(script, "the counter script needs the integer parameter [count]"))?;
        Ok(Self {
            object_type: read_text("type")?,
            counter_field_name: read_text("counterFieldName")?,
            count,
            time: read_text("time")?,
        })
    }
}

/// Recognize the `script` of a document update.
///
/// The source matches when it equals a recognized form after every whitespace run is collapsed
/// to one space and the ends are trimmed; `lang` must be absent or `painless`. The script does not
/// run on an upsert: a missing document is stored from the request's `upsert` as given, and an
/// update body has no `scripted_upsert`.
///
/// # Errors
///
/// `illegal_argument_exception` naming the whole script when it is not an object with a `source`,
/// carries another key, names another language, is not a recognized form, or its `params` are not
/// the ones the form reads.
pub(crate) fn recognize_document_script(script: &Value) -> Result<DocumentScript, OpenSearchError> {
    let object = script
        .as_object()
        .ok_or_else(|| refuse_script(script, "a script is an object with a source"))?;
    if let Some(key) = object.keys().find(|key| !matches!(key.as_str(), "source" | "lang" | "params")) {
        return Err(refuse_script(
            script,
            &format!("a script does not take the key [{key}]"),
        ));
    }
    match object.get("lang") {
        None => {}
        Some(Value::String(lang)) if lang == "painless" => {}
        Some(lang) => return Err(refuse_script(script, &format!("the language [{lang}] is not served"))),
    }
    let source = object
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| refuse_script(script, "a script needs a string [source]"))?;
    if !source.split_whitespace().eq(INCREMENT_COUNTER_SOURCE.split(' ')) {
        return Err(refuse_script(script, "it is not a recognized form"));
    }
    let parameters = object
        .get("params")
        .and_then(Value::as_object)
        .ok_or_else(|| refuse_script(script, "the counter script needs [params]"))?;
    CounterIncrement::from_parameters(parameters, script).map(DocumentScript::IncrementCounter)
}

/// The refusal of `script`: every refusal names the whole script, as a client sent it, with what
/// is wrong with it.
fn refuse_script(script: &Value, cause: &str) -> OpenSearchError {
    OpenSearchError::illegal_argument(format!("unsupported script [{script}]: {cause}"))
}

/// The variable a metric value script reads: the field's value of one row.
pub(crate) const VALUE_VARIABLE: &str = "_value";

/// Longest script source, in bytes, the recognizers read; a longer one is refused. A recognized
/// script is ASCII, so its bytes are its characters.
const MAX_SCRIPT_LENGTH: usize = 4096;

/// Deepest an arithmetic script may be, counted two ways that are both bounded: the parentheses
/// and unary minuses the parser descends, and the operators (binary and unary minus) on the
/// longest path of the parsed expression. The second bound matters because a chain such as
/// `1+1+...+1` nests no parentheses yet lowers to an expression as deep as the chain is long,
/// and DataFusion plans and evaluates expressions recursively: an unbounded chain overflows the
/// worker's stack and aborts the process.
const MAX_SCRIPT_DEPTH: usize = 64;

/// An arithmetic expression recognized from a script: the only computation IceGate runs for
/// `bucket_script` and for metric value scripts.
///
/// Evaluation is IEEE double arithmetic: `x / 0` is infinite and `0 / 0` is not a number. A
/// `bucket_script` renders a non-finite result as `null`, and a metric with a value script
/// renders each non-finite statistic as `null`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ArithmeticExpr {
    /// A decimal literal.
    Number(f64),
    /// `_value` (named [`VALUE_VARIABLE`]) or `params.<name>` (named `<name>`).
    Variable(String),
    /// Unary minus.
    Negate(Box<Self>),
    /// A binary operation.
    Binary {
        /// The operation.
        operator: ArithmeticOperator,
        /// The left operand.
        left: Box<Self>,
        /// The right operand.
        right: Box<Self>,
    },
}

/// A binary operator of [`ArithmeticExpr`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArithmeticOperator {
    /// `+`.
    Add,
    /// `-`.
    Subtract,
    /// `*`.
    Multiply,
    /// `/`.
    Divide,
    /// `%`, the remainder with the sign of the dividend.
    Remainder,
}

impl ArithmeticExpr {
    /// Evaluates the expression, reading each variable from `values`; `None` when a variable
    /// has no value.
    pub(crate) fn evaluate(&self, values: &dyn Fn(&str) -> Option<f64>) -> Option<f64> {
        match self {
            Self::Number(number) => Some(*number),
            Self::Variable(name) => values(name),
            Self::Negate(inner) => inner.evaluate(values).map(|value| -value),
            Self::Binary { operator, left, right } => {
                let left_value = left.evaluate(values)?;
                let right_value = right.evaluate(values)?;
                Some(match operator {
                    ArithmeticOperator::Add => left_value + right_value,
                    ArithmeticOperator::Subtract => left_value - right_value,
                    ArithmeticOperator::Multiply => left_value * right_value,
                    ArithmeticOperator::Divide => left_value / right_value,
                    ArithmeticOperator::Remainder => left_value % right_value,
                })
            }
        }
    }

    /// Lowers a metric value script over `value` (the row's coalesced field) to a `Float64`
    /// expression, so an integer field divides in double precision.
    ///
    /// A value script names no variable but [`VALUE_VARIABLE`], which the recognizer guarantees
    /// when it was given only that variable; every variable lowers to `value`.
    pub(crate) fn lower_value_script(&self, value: Expr) -> Expr {
        self.lower_expression_over(&cast(value, DataType::Float64))
    }

    /// [`Self::lower_value_script`] over an already cast value.
    fn lower_expression_over(&self, value: &Expr) -> Expr {
        match self {
            Self::Number(number) => lit(*number),
            Self::Variable(_) => value.clone(),
            Self::Negate(inner) => -inner.lower_expression_over(value),
            Self::Binary { operator, left, right } => {
                let left_operand = left.lower_expression_over(value);
                let right_operand = right.lower_expression_over(value);
                match operator {
                    ArithmeticOperator::Add => left_operand + right_operand,
                    ArithmeticOperator::Subtract => left_operand - right_operand,
                    ArithmeticOperator::Multiply => left_operand * right_operand,
                    ArithmeticOperator::Divide => left_operand / right_operand,
                    ArithmeticOperator::Remainder => left_operand % right_operand,
                }
            }
        }
    }
}

/// The trace-list latency script with its whitespace runs collapsed to one space. It reads the
/// root span's duration and truncates it to hundredths of a millisecond, answering 0 for a span
/// whose trace has no root.
const TRACE_LIST_LATENCY_SCRIPT_WITH_DEFAULT: &str = "if (doc.containsKey('traceGroupFields.durationInNanos') && !doc['traceGroupFields.durationInNanos'].empty) { return Math.round(doc['traceGroupFields.durationInNanos'].value / 10000) / 100.0 } return 0";

/// The same script without the trailing default, so a span whose trace has no root answers no
/// value. It is whitespace-collapsed like [`TRACE_LIST_LATENCY_SCRIPT_WITH_DEFAULT`].
const TRACE_LIST_LATENCY_SCRIPT_WITHOUT_DEFAULT: &str = "if (doc.containsKey('traceGroupFields.durationInNanos') && !doc['traceGroupFields.durationInNanos'].empty) { return Math.round(doc['traceGroupFields.durationInNanos'].value / 10000) / 100.0 }";

/// The trace-list last-updated script with its whitespace runs collapsed to one space. It reads
/// the root span's end time when the span's trace has a stored root, else the span's own end
/// time.
const TRACE_LIST_LAST_UPDATED_SCRIPT: &str = "if (doc.containsKey('traceGroupFields.endTime') && !doc['traceGroupFields.endTime'].empty) { return doc['traceGroupFields.endTime'].value; } if (doc.containsKey('endTime') && !doc['endTime'].empty) { return doc['endTime'].value; }";

/// The divisor of the latency script's integer division: nanoseconds to hundredths of a
/// millisecond.
const LATENCY_NANOS_PER_HUNDREDTH: f64 = 10_000.0;

/// The latency script's second divisor: hundredths of a millisecond to milliseconds.
const HUNDREDTHS_PER_MILLISECOND: f64 = 100.0;

/// A recognized metric value script.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MetricScript {
    /// Arithmetic over `_value` and numeric literals.
    Arithmetic(ArithmeticExpr),
    /// The trace-list latency of a span: its root's duration in milliseconds, truncated to
    /// hundredths. Only the Data Prepper span index answers it: a search that also reads another
    /// index is refused.
    TraceListLatency {
        /// Whether a span whose trace has no root answers `0` (the script ends in a default) or
        /// no value (it does not).
        is_zero_when_absent: bool,
    },
    /// The trace-list last-updated time of a span: its root's end time when its trace has a
    /// stored root, else its own end time. The metric reads both fields and answers the first
    /// present one as a date, so the script itself computes nothing more. Only the Data Prepper
    /// span index answers it: a search that also reads another index is refused.
    TraceLastUpdated,
}

impl MetricScript {
    /// The name a refusal gives the script.
    pub(crate) const fn describe_script(&self) -> &'static str {
        match self {
            Self::Arithmetic(_) => "arithmetic",
            Self::TraceListLatency { .. } => "latency",
            Self::TraceLastUpdated => "last-updated",
        }
    }

    /// The script applied to each row's `value` inside the aggregate.
    ///
    /// The latency script answers a `Float64` whether `value` is an integer or a double: a
    /// metric aggregation hands it a double, and `floor` truncates the quotient to hundredths as
    /// the script's integer division does (durations are never negative, so truncating is
    /// flooring). The form with a default answers 0 where the root duration is absent (NULL); the
    /// form without one answers NULL there, so a bucket holding only such spans has no value.
    ///
    /// The last-updated script answers `value` as it is: its metric reads the root's end time
    /// with the span's own end time standing in for an absent one, which is the script's whole
    /// computation.
    pub(crate) fn lower_value_script(&self, value: Expr) -> Expr {
        match self {
            Self::Arithmetic(arithmetic) => arithmetic.lower_value_script(value),
            Self::TraceLastUpdated => value,
            Self::TraceListLatency { is_zero_when_absent } => {
                let latency = floor(cast(value, DataType::Float64) / lit(LATENCY_NANOS_PER_HUNDREDTH))
                    / lit(HUNDREDTHS_PER_MILLISECOND);
                if *is_zero_when_absent {
                    coalesce(vec![latency, lit(0.0)])
                } else {
                    latency
                }
            }
        }
    }
}

/// Recognize a metric value `script`: the trace-list latency script in either form or the
/// trace-list last-updated script (each compared with its whitespace runs collapsed), or
/// arithmetic over `_value`.
///
/// # Errors
///
/// Returns `400 illegal_argument_exception` naming any other script.
pub(crate) fn recognize_metric_script(source: &str) -> Result<MetricScript, OpenSearchError> {
    let trace_list_scripts = [
        (
            TRACE_LIST_LATENCY_SCRIPT_WITH_DEFAULT,
            MetricScript::TraceListLatency {
                is_zero_when_absent: true,
            },
        ),
        (
            TRACE_LIST_LATENCY_SCRIPT_WITHOUT_DEFAULT,
            MetricScript::TraceListLatency {
                is_zero_when_absent: false,
            },
        ),
        (TRACE_LIST_LAST_UPDATED_SCRIPT, MetricScript::TraceLastUpdated),
    ];
    for (trace_list_script, recognized) in trace_list_scripts {
        if source.split_whitespace().eq(trace_list_script.split(' ')) {
            return Ok(recognized);
        }
    }
    recognize_arithmetic_script(source, &[VALUE_VARIABLE]).map(MetricScript::Arithmetic)
}

/// Recognizes an arithmetic script over `variables`: `_value` when `variables` holds
/// [`VALUE_VARIABLE`], `params.<name>` for every other name.
///
/// # Errors
///
/// `illegal_argument_exception` whose `script` names the source for anything outside the
/// grammar (decimal literals, the variables, `+ - * / %`, unary minus, parentheses), for a
/// variable not in `variables`, a source longer than [`MAX_SCRIPT_LENGTH`] bytes, or nesting or
/// operators deeper than [`MAX_SCRIPT_DEPTH`].
pub(crate) fn recognize_arithmetic_script(source: &str, variables: &[&str]) -> Result<ArithmeticExpr, OpenSearchError> {
    let build_script_refusal = || {
        OpenSearchError::unsupported_script(
            source,
            format!(
                "script [{source}] is not supported: IceGate runs only arithmetic (+ - * / %, parentheses) over {} \
                 and decimal literals, at most {MAX_SCRIPT_DEPTH} levels deep and {MAX_SCRIPT_LENGTH} bytes long",
                describe_variables(variables)
            ),
        )
    };
    if source.len() > MAX_SCRIPT_LENGTH {
        return Err(build_script_refusal());
    }
    let mut parser = ArithmeticParser {
        bytes: source.as_bytes(),
        position: 0,
        depth: 0,
        variables,
    };
    let expression = parser.parse_sum().ok_or_else(build_script_refusal)?;
    if parser.peek_byte().is_none() && measure_operator_depth(&expression) <= MAX_SCRIPT_DEPTH {
        Ok(expression)
    } else {
        Err(build_script_refusal())
    }
}

/// The operators (binary and unary minus) on the longest path of `expression`. Walks an explicit
/// stack, so measuring an expression that is too deep cannot itself exhaust the stack.
fn measure_operator_depth(expression: &ArithmeticExpr) -> usize {
    let mut deepest = 0;
    let mut pending = vec![(expression, 0)];
    while let Some((node, depth)) = pending.pop() {
        deepest = deepest.max(depth);
        match node {
            ArithmeticExpr::Number(_) | ArithmeticExpr::Variable(_) => {}
            ArithmeticExpr::Negate(inner) => pending.push((inner, depth + 1)),
            ArithmeticExpr::Binary { left, right, .. } => {
                pending.push((left, depth + 1));
                pending.push((right, depth + 1));
            }
        }
    }
    deepest
}

/// How a refusal names the variables a script may use.
fn describe_variables(variables: &[&str]) -> String {
    if variables.is_empty() {
        return "no variables".to_owned();
    }
    variables
        .iter()
        .map(|name| {
            if *name == VALUE_VARIABLE {
                VALUE_VARIABLE.to_owned()
            } else {
                format!("params.{name}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A recursive-descent parser over the ASCII arithmetic grammar; every method returns `None` on
/// the first token outside it. The recursion is bounded by [`MAX_SCRIPT_DEPTH`]: a chain of
/// binary operators is read in a loop, and only parentheses and unary minus descend (the depth
/// of the chain itself is bounded after parsing).
struct ArithmeticParser<'a> {
    /// The script source.
    bytes: &'a [u8],
    /// The next byte to read.
    position: usize,
    /// The parentheses and unary minuses open at `position`.
    depth: usize,
    /// The variables the script may name.
    variables: &'a [&'a str],
}

impl<'a> ArithmeticParser<'a> {
    /// Steps over ASCII whitespace.
    fn skip_whitespace(&mut self) {
        while self.bytes.get(self.position).is_some_and(u8::is_ascii_whitespace) {
            self.position += 1;
        }
    }

    /// The next byte that is not whitespace, without consuming it; `None` at the end.
    fn peek_byte(&mut self) -> Option<u8> {
        self.skip_whitespace();
        self.bytes.get(self.position).copied()
    }

    /// `sum := product (('+' | '-') product)*`
    fn parse_sum(&mut self) -> Option<ArithmeticExpr> {
        let mut left = self.parse_product()?;
        loop {
            let operator = match self.peek_byte() {
                Some(b'+') => ArithmeticOperator::Add,
                Some(b'-') => ArithmeticOperator::Subtract,
                _ => return Some(left),
            };
            self.position += 1;
            let right = self.parse_product()?;
            left = ArithmeticExpr::Binary {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    /// `product := unary (('*' | '/' | '%') unary)*`
    fn parse_product(&mut self) -> Option<ArithmeticExpr> {
        let mut left = self.parse_unary()?;
        loop {
            let operator = match self.peek_byte() {
                Some(b'*') => ArithmeticOperator::Multiply,
                Some(b'/') => ArithmeticOperator::Divide,
                Some(b'%') => ArithmeticOperator::Remainder,
                _ => return Some(left),
            };
            self.position += 1;
            let right = self.parse_unary()?;
            left = ArithmeticExpr::Binary {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    /// `unary := '-' unary | primary`
    fn parse_unary(&mut self) -> Option<ArithmeticExpr> {
        if self.peek_byte() != Some(b'-') {
            return self.parse_primary();
        }
        self.position += 1;
        self.enter_level()?;
        let inner = self.parse_unary()?;
        self.depth -= 1;
        Some(ArithmeticExpr::Negate(Box::new(inner)))
    }

    /// `primary := number | '_value' | 'params.' name | '(' sum ')'`
    fn parse_primary(&mut self) -> Option<ArithmeticExpr> {
        match self.peek_byte()? {
            b'(' => {
                self.position += 1;
                self.enter_level()?;
                let inner = self.parse_sum()?;
                if self.peek_byte() != Some(b')') {
                    return None;
                }
                self.position += 1;
                self.depth -= 1;
                Some(inner)
            }
            byte if byte.is_ascii_digit() || byte == b'.' => self.parse_number(),
            byte if byte.is_ascii_alphabetic() || byte == b'_' => self.parse_variable(),
            _ => None,
        }
    }

    /// Counts one more nesting level; `None` beyond [`MAX_SCRIPT_DEPTH`].
    fn enter_level(&mut self) -> Option<()> {
        self.depth += 1;
        (self.depth <= MAX_SCRIPT_DEPTH).then_some(())
    }

    /// `number := digits ('.' digits?)? | '.' digits`
    fn parse_number(&mut self) -> Option<ArithmeticExpr> {
        let start = self.position;
        let mut has_digit = false;
        let mut has_point = false;
        while let Some(byte) = self.bytes.get(self.position).copied() {
            if byte.is_ascii_digit() {
                has_digit = true;
            } else if byte == b'.' && !has_point {
                has_point = true;
            } else {
                break;
            }
            self.position += 1;
        }
        if !has_digit {
            return None;
        }
        // The slice is ASCII digits and at most one point: valid UTF-8 and a valid float.
        let text = std::str::from_utf8(self.bytes.get(start..self.position)?).ok()?;
        text.parse::<f64>().ok().map(ArithmeticExpr::Number)
    }

    /// `_value` or `params.<name>`, each allowed only when in `variables`.
    fn parse_variable(&mut self) -> Option<ArithmeticExpr> {
        let identifier = self.read_identifier()?;
        let name = if identifier == "params" {
            if self.bytes.get(self.position) != Some(&b'.') {
                return None;
            }
            self.position += 1;
            let name = self.read_identifier()?;
            // `params._value` is no spelling of the value variable.
            (name != VALUE_VARIABLE).then_some(name)?
        } else if identifier == VALUE_VARIABLE {
            identifier
        } else {
            return None;
        };
        self.variables
            .contains(&name)
            .then(|| ArithmeticExpr::Variable(name.to_owned()))
    }

    /// `[A-Za-z_][A-Za-z0-9_]*` at the current position, borrowed from the source (not from the
    /// parser) so the parser can read on.
    fn read_identifier(&mut self) -> Option<&'a str> {
        let bytes: &'a [u8] = self.bytes;
        let start = self.position;
        while bytes
            .get(self.position)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            self.position += 1;
        }
        let identifier = std::str::from_utf8(bytes.get(start..self.position)?).ok()?;
        identifier
            .bytes()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == b'_')
            .then_some(identifier)
    }
}

/// A recognized `MovingFunctions.*` invocation of `moving_fn`, and the model a legacy
/// `moving_avg` names.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum MovingFunction {
    /// `MovingFunctions.unweightedAvg(values)`: the mean.
    UnweightedAvg,
    /// `MovingFunctions.linearWeightedAvg(values)`: `Σ i·v_i / (1 + Σ i)`, the oldest value
    /// weighted 1 and the total weight starting at 1, not 0.
    LinearWeightedAvg,
    /// `MovingFunctions.ewma(values, alpha)`: seeded with the oldest value, then
    /// `s = alpha·v + (1 − alpha)·s`.
    Ewma {
        /// Smoothing of the level, in `[0, 1]`.
        alpha: f64,
    },
    /// `MovingFunctions.holt(values, alpha, beta)`: level seeded with the oldest value and trend
    /// 0, then `level = alpha·v + (1 − alpha)(level + trend)` and
    /// `trend = beta(level − previous level) + (1 − beta)·trend`; returns the last level.
    Holt {
        /// Smoothing of the level, in `[0, 1]`.
        alpha: f64,
        /// Smoothing of the trend, in `[0, 1]`.
        beta: f64,
    },
}

impl MovingFunction {
    /// Applies the function to `window`, oldest value first; `None` for an empty window.
    pub(crate) fn apply(&self, window: &[f64]) -> Option<f64> {
        let (first_value, later_values) = window.split_first()?;
        Some(match *self {
            Self::UnweightedAvg => {
                let (total, count) = window
                    .iter()
                    .fold((0.0, 0.0), |(total, count), value| (total + value, count + 1.0));
                total / count
            }
            Self::LinearWeightedAvg => {
                let (weighted_total, total_weight, _) =
                    window
                        .iter()
                        .fold((0.0, 1.0, 0.0), |(weighted_total, total_weight, weight), value| {
                            let weight = weight + 1.0;
                            (value.mul_add(weight, weighted_total), total_weight + weight, weight)
                        });
                weighted_total / total_weight
            }
            Self::Ewma { alpha } => later_values.iter().fold(*first_value, |average, value| {
                alpha.mul_add(*value, (1.0 - alpha) * average)
            }),
            Self::Holt { alpha, beta } => {
                let (level, _) = later_values.iter().fold((*first_value, 0.0), |(level, trend), value| {
                    let next_level = alpha.mul_add(*value, (1.0 - alpha) * (level + trend));
                    (next_level, beta.mul_add(next_level - level, (1.0 - beta) * trend))
                });
                level
            }
        })
    }
}

/// Recognizes a `moving_fn` script.
///
/// # Errors
///
/// `illegal_argument_exception` whose `script` names the source for anything but
/// `MovingFunctions.unweightedAvg(values)`, `MovingFunctions.linearWeightedAvg(values)`,
/// `MovingFunctions.ewma(values, <alpha>)` and `MovingFunctions.holt(values, <alpha>, <beta>)`
/// with smoothing factors in `[0, 1]` (whitespace around the call and its arguments is allowed).
pub(crate) fn recognize_moving_function(source: &str) -> Result<MovingFunction, OpenSearchError> {
    let build_script_refusal = || {
        OpenSearchError::unsupported_script(
            source,
            format!(
                "script [{source}] is not supported: moving_fn runs only MovingFunctions.unweightedAvg(values), \
                 MovingFunctions.linearWeightedAvg(values), MovingFunctions.ewma(values, alpha) and \
                 MovingFunctions.holt(values, alpha, beta)"
            ),
        )
    };
    let call = source
        .trim()
        .strip_prefix("MovingFunctions.")
        .ok_or_else(build_script_refusal)?;
    let (function_name, argument_text) = call.split_once('(').ok_or_else(build_script_refusal)?;
    let arguments: Vec<&str> = argument_text
        .strip_suffix(')')
        .ok_or_else(build_script_refusal)?
        .split(',')
        .map(str::trim)
        .collect();
    let Some((&"values", smoothing_texts)) = arguments.split_first() else {
        return Err(build_script_refusal());
    };
    let factors = smoothing_texts
        .iter()
        .map(|text| text.parse::<f64>().ok().filter(|factor| (0.0..=1.0).contains(factor)))
        .collect::<Option<Vec<f64>>>()
        .ok_or_else(build_script_refusal)?;
    match (function_name, factors.as_slice()) {
        ("unweightedAvg", []) => Ok(MovingFunction::UnweightedAvg),
        ("linearWeightedAvg", []) => Ok(MovingFunction::LinearWeightedAvg),
        ("ewma", [alpha]) => Ok(MovingFunction::Ewma { alpha: *alpha }),
        ("holt", [alpha, beta]) => Ok(MovingFunction::Holt {
            alpha: *alpha,
            beta: *beta,
        }),
        _ => Err(build_script_refusal()),
    }
}

/// The source of a `script` member of the aggregation `owner`, which a refusal names.
///
/// # Errors
///
/// `parsing_exception` unless `script` is a string or an object with exactly one of `source` and
/// `inline` (a string), an optional `lang` of `painless` or `expression`, and an optional empty
/// `params`: a stored script, another language or a parameter IceGate would not read is refused.
pub(crate) fn read_script_source<'a>(owner: &str, script: &'a Value) -> Result<&'a str, OpenSearchError> {
    let build_source_refusal = |reason: &str| OpenSearchError::parsing(format!("[script] of [{owner}] {reason}"));
    let object = match script {
        Value::String(source) => return Ok(source),
        Value::Object(object) => object,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) => {
            return Err(build_source_refusal("must be a string or an object"));
        }
    };
    for (key, value) in object {
        match key.as_str() {
            "source" | "inline" => {}
            "lang" => {
                if !matches!(value.as_str(), Some("painless" | "expression")) {
                    return Err(build_source_refusal("takes [lang] painless or expression"));
                }
            }
            "params" => {
                if !value.as_object().is_some_and(Map::is_empty) {
                    return Err(build_source_refusal(
                        "takes only empty [params]: IceGate scripts read only their own variables",
                    ));
                }
            }
            other => return Err(build_source_refusal(&format!("does not support [{other}]"))),
        }
    }
    match (object.get("source"), object.get("inline")) {
        (Some(Value::String(source)), None) | (None, Some(Value::String(source))) => Ok(source),
        _ => Err(build_source_refusal("needs exactly one string [source] or [inline]")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::StatusCode;
    use datafusion::arrow::array::{ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::col;
    use datafusion::prelude::SessionContext;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use serde_json::{Value, json};

    use super::*;
    use crate::opensearch::error::render_envelope;

    /// A counter-script-style source: a line break after the opening backtick, 14- and 16-space
    /// indents, 12 spaces before the closing backtick.
    const TRANSCRIBED_SOURCE: &str = "\n              if (ctx._source[params.type][params.counterFieldName] == null) {\n                ctx._source[params.type][params.counterFieldName] = params.count;\n              }\n              else {\n                ctx._source[params.type][params.counterFieldName] += params.count;\n              }\n              ctx._source.updated_at = params.time;\n            ";

    fn build_counter_script(source: &str, parameters: &Value) -> Value {
        json!({"source": source, "lang": "painless", "params": parameters})
    }

    fn build_counter_parameters() -> Value {
        json!({"count": 1, "time": "2026-10-08T12:00:00.000Z", "type": "ui-metric", "counterFieldName": "count"})
    }

    #[test]
    fn the_increment_counter_script_is_recognized_with_its_params() {
        let expected = DocumentScript::IncrementCounter(CounterIncrement {
            object_type: "ui-metric".to_string(),
            counter_field_name: "count".to_string(),
            count: 1,
            time: "2026-10-08T12:00:00.000Z".to_string(),
        });

        let transcribed_script =
            recognize_document_script(&build_counter_script(TRANSCRIBED_SOURCE, &build_counter_parameters()));
        // Whitespace runs collapse, so the same statements on one line are the same script.
        let one_line_script = recognize_document_script(&json!({
            "source": "if (ctx._source[params.type][params.counterFieldName] == null) { ctx._source[params.type][params.counterFieldName] = params.count; } else { ctx._source[params.type][params.counterFieldName] += params.count; } ctx._source.updated_at = params.time;",
            "params": build_counter_parameters()
        }));

        assert_eq!(
            transcribed_script.expect("the transcribed script is recognized"),
            expected
        );
        assert_eq!(one_line_script.expect("an absent lang means painless"), expected);
    }

    /// The refusal names the whole script, whichever part of it is at fault, so a client can tell
    /// which of its scripts is not served.
    #[tokio::test]
    async fn any_other_document_script_is_refused_naming_it() {
        let refused_scripts = [
            build_counter_script("ctx._source.count += params.count", &build_counter_parameters()),
            // The comparison is token by token: one changed token, a missing last statement and
            // an extra one are each a different script.
            build_counter_script(&TRANSCRIBED_SOURCE.replace("+=", "-="), &build_counter_parameters()),
            build_counter_script(
                &TRANSCRIBED_SOURCE.replace("ctx._source.updated_at = params.time;", ""),
                &build_counter_parameters(),
            ),
            build_counter_script(
                &format!("{TRANSCRIBED_SOURCE} ctx._source.extra = 1;"),
                &build_counter_parameters(),
            ),
            json!({"source": TRANSCRIBED_SOURCE, "lang": "expression", "params": build_counter_parameters()}),
            json!({"source": TRANSCRIBED_SOURCE, "lang": 1, "params": build_counter_parameters()}),
            json!({"source": TRANSCRIBED_SOURCE, "id": "stored", "params": build_counter_parameters()}),
            json!({"lang": "painless", "params": build_counter_parameters()}),
            json!({"source": 1, "params": build_counter_parameters()}),
            json!({"source": TRANSCRIBED_SOURCE}),
            json!({"source": TRANSCRIBED_SOURCE, "params": [1]}),
            build_counter_script(TRANSCRIBED_SOURCE, &json!({"count": 1, "type": "ui-metric"})),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"time": "t", "type": "ui-metric", "counterFieldName": "count"}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": 1.5, "time": "t", "type": "ui-metric", "counterFieldName": "count"}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": "1", "time": "t", "type": "ui-metric", "counterFieldName": "count"}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": 1, "time": 1, "type": "ui-metric", "counterFieldName": "count"}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": 1, "time": "t", "type": 1, "counterFieldName": "count"}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": 1, "time": "t", "type": "ui-metric", "counterFieldName": 1}),
            ),
            build_counter_script(
                TRANSCRIBED_SOURCE,
                &json!({"count": 1, "time": "t", "type": "ui-metric", "counterFieldName": "count", "extra": 1}),
            ),
            json!("ctx._source.count += 1"),
        ];
        for script in refused_scripts {
            let error = recognize_document_script(&script).expect_err("the script is refused");

            let (status, body) = render_envelope(error).await;

            assert_eq!(status, StatusCode::BAD_REQUEST, "{script}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{script}");
            let reason = body["error"]["reason"].as_str().expect("reason is a string");
            assert!(
                reason.contains(&script.to_string()),
                "the refusal names the script: {reason}"
            );
        }
    }

    fn build_increment(count: i64) -> CounterIncrement {
        CounterIncrement {
            object_type: "ui-metric".to_string(),
            counter_field_name: "count".to_string(),
            count,
            time: "2026-10-08T12:00:00.000Z".to_string(),
        }
    }

    /// An absent or `null` counter is set, an integer counter takes the count, up to and exactly
    /// at the largest integer a counter holds.
    #[test]
    fn a_counter_is_set_or_added_to_up_to_the_largest_integer() {
        let cases = [
            ("absent", json!({"ui-metric": {}}), 2, json!(2)),
            ("null", json!({"ui-metric": {"count": null}}), 2, json!(2)),
            ("integer", json!({"ui-metric": {"count": 4}}), 3, json!(7)),
            ("negative", json!({"ui-metric": {"count": -5}}), 2, json!(-3)),
            (
                "just below the largest integer",
                json!({"ui-metric": {"count": i64::MAX - 1}}),
                1,
                json!(i64::MAX),
            ),
        ];
        for (label, mut source, count, expected_counter) in cases {
            build_increment(count).apply_to(&mut source).expect(label);

            assert_eq!(source["ui-metric"]["count"], expected_counter, "{label}");
            assert_eq!(source["updated_at"], "2026-10-08T12:00:00.000Z", "{label}");
        }
    }

    /// A refusal leaves the source as it was: the script runs on the stored document, and the
    /// operations of one batch share a snapshot, so a refused count must not stamp `updated_at`.
    #[tokio::test]
    async fn a_counter_that_cannot_take_the_count_is_refused_and_leaves_the_source_unchanged() {
        let cases = [
            ("a string counter", json!({"ui-metric": {"count": "5"}})),
            ("a fractional counter", json!({"ui-metric": {"count": 1.5}})),
            ("a boolean counter", json!({"ui-metric": {"count": true}})),
            ("the largest integer", json!({"ui-metric": {"count": i64::MAX}})),
            (
                "a counter above the largest integer",
                json!({"ui-metric": {"count": 9_223_372_036_854_775_808_u64}}),
            ),
            ("attributes that are not an object", json!({"ui-metric": 1})),
            ("no attributes", json!({"type": "ui-metric"})),
            ("a source that is not an object", json!("text")),
        ];
        for (label, source) in cases {
            let mut applied_source = source.clone();

            let error = build_increment(1).apply_to(&mut applied_source).expect_err(label);

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{label}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{label}");
            assert_eq!(applied_source, source, "{label}");
        }
    }

    /// Evaluates `source` with the stated variable values; panics if refused.
    fn evaluate_script(source: &str, variables: &[(&str, f64)]) -> Option<f64> {
        let names: Vec<&str> = variables.iter().map(|(name, _)| *name).collect();
        let script = recognize_arithmetic_script(source, &names).expect("a recognized script");
        script.evaluate(&|name| {
            variables
                .iter()
                .find(|(candidate, _)| *candidate == name)
                .map(|(_, value)| *value)
        })
    }

    /// Renders the refusal of `source` over `variables` and checks it is a 400
    /// `illegal_argument_exception` carrying the script under `script`.
    async fn assert_script_refused(source: &str, variables: &[&str]) {
        let error = recognize_arithmetic_script(source, variables).expect_err("refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{source}");
        assert_eq!(body["error"]["type"], json!("illegal_argument_exception"), "{source}");
        assert_eq!(body["error"]["script"], json!(source), "{source}");
    }

    #[test]
    fn each_recognized_script_form_evaluates_to_the_stated_value() {
        let cases: [(&str, &[(&str, f64)], f64); 8] = [
            (
                "params.errors / params.total",
                &[("errors", 3.0), ("total", 12.0)],
                0.25,
            ),
            ("_value / 1000", &[("_value", 1500.0)], 1.5),
            ("(_value + 500) * 2", &[("_value", 2500.0)], 6000.0),
            (
                "(params.a + params.b) * 2 - params.c % 3",
                &[("a", 1.0), ("b", 2.0), ("c", 7.0)],
                5.0,
            ),
            ("-params.a + 10", &[("a", 1.0)], 9.0),
            ("- -params.a", &[("a", 4.0)], 4.0),
            ("params.var1*100/params.var2", &[("var1", 1.0), ("var2", 8.0)], 12.5),
            ("  12.5  ", &[], 12.5),
        ];
        for (source, variables, expected) in cases {
            assert_eq!(evaluate_script(source, variables), Some(expected), "{source}");
        }
        assert_eq!(evaluate_script("params.a / 0", &[("a", 1.0)]), Some(f64::INFINITY));
        let script = recognize_arithmetic_script("params.a + params.b", &["a", "b"]).expect("recognized");
        assert_eq!(
            script.evaluate(&|name| (name == "a").then_some(1.0)),
            None,
            "a missing variable has no value"
        );
    }

    #[tokio::test]
    async fn any_other_script_is_an_illegal_argument_400_naming_it() {
        let refused_cases: [(&str, &[&str]); 12] = [
            ("Math.max(params.a, 1)", &["a"]),
            ("params.a > 1 ? 1 : 0", &["a"]),
            ("doc['duration_micros'].value", &[]),
            ("params['a']", &["a"]),
            ("params.unknown + 1", &["a"]),
            ("_value + 1", &["a"]),
            ("params.a + 1", &["_value"]),
            ("params.a;", &["a"]),
            ("1 +", &[]),
            ("(1", &[]),
            ("", &[]),
            ("1e3", &[]),
        ];
        for (source, variables) in refused_cases {
            assert_script_refused(source, variables).await;
        }
        // Nesting: 64 levels of parentheses or of unary minus are read, 65 are refused.
        let deep_script = format!("{}1{}", "(".repeat(64), ")".repeat(64));
        assert_eq!(evaluate_script(&deep_script, &[]), Some(1.0));
        let deep_script = format!("{}1{}", "(".repeat(65), ")".repeat(65));
        assert_script_refused(&deep_script, &[]).await;
        let deep_script = format!("{}1", "-".repeat(64));
        assert_eq!(evaluate_script(&deep_script, &[]), Some(1.0));
        let deep_script = format!("{}1", "-".repeat(65));
        assert_script_refused(&deep_script, &[]).await;
        // A chain of operators counts toward the depth: 64 additions are read, 65 are refused.
        let chained_script = "1+".repeat(64) + "1";
        assert_eq!(evaluate_script(&chained_script, &[]), Some(65.0));
        let chained_script = "1+".repeat(65) + "1";
        assert_script_refused(&chained_script, &[]).await;
        // Length: a 4096-byte source is read (1, 4092 spaces, +11), one of 4097 bytes is refused.
        let long_script = format!("1{}+11", " ".repeat(4092));
        assert_eq!(long_script.len(), 4096);
        assert_eq!(evaluate_script(&long_script, &[]), Some(12.0));
        let long_script = format!("1{}+11", " ".repeat(4093));
        assert_eq!(long_script.len(), 4097);
        assert_script_refused(&long_script, &[]).await;
    }

    #[tokio::test]
    async fn script_sources_read_strings_and_source_objects() {
        assert_eq!(
            read_script_source("bucket_script", &json!("params.a")).expect("a string"),
            "params.a"
        );
        assert_eq!(
            read_script_source("bucket_script", &json!({"source": "params.a", "lang": "painless"})).expect("an object"),
            "params.a"
        );
        assert_eq!(
            read_script_source("bucket_script", &json!({"inline": "params.a", "params": {}})).expect("inline"),
            "params.a"
        );
        for refused_script in [
            json!({"source": "params.a", "lang": "mustache"}),
            json!({"source": "params.a", "params": {"k": 1}}),
            json!({"id": "stored"}),
            json!({"source": "params.a", "inline": "params.a"}),
            json!(7),
        ] {
            let error = read_script_source("bucket_script", &refused_script).expect_err("refused");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{refused_script}");
            assert_eq!(body["error"]["type"], json!("parsing_exception"), "{refused_script}");
        }
    }

    #[tokio::test]
    async fn a_value_script_lowers_to_float_arithmetic_over_the_value() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![Some(1500), Some(2500), None]))],
        )
        .expect("batch");
        let context = SessionContext::new();
        context
            .register_table(
                "rows",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("table")),
            )
            .expect("registers");
        let script = recognize_arithmetic_script("_value / 1000", &[VALUE_VARIABLE]).expect("recognized");
        let batches = context
            .table("rows")
            .await
            .expect("table")
            .select(vec![script.lower_value_script(col("value")).alias("scaled")])
            .expect("projection")
            .collect()
            .await
            .expect("runs");
        let scaled: Vec<Option<f64>> = batches[0].column(0).as_primitive::<Float64Type>().iter().collect();
        // Integer input, double arithmetic: 1500 / 1000 is 1.5, not 1.
        assert_eq!(scaled, vec![Some(1.5), Some(2.5), None]);
    }

    #[tokio::test]
    async fn a_value_script_at_the_depth_limit_plans_and_runs() {
        // `_value` followed by 64 `+1`: the deepest expression admitted (64 nested additions).
        let source = format!("_value{}", "+1".repeat(64));
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![Some(1500), None]))],
        )
        .expect("batch");
        let context = SessionContext::new();
        context
            .register_table(
                "rows",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("table")),
            )
            .expect("registers");
        let script = recognize_arithmetic_script(&source, &[VALUE_VARIABLE]).expect("recognized");
        let batches = context
            .table("rows")
            .await
            .expect("table")
            .select(vec![script.lower_value_script(col("value")).alias("summed")])
            .expect("projection")
            .collect()
            .await
            .expect("runs");
        let summed: Vec<Option<f64>> = batches[0].column(0).as_primitive::<Float64Type>().iter().collect();
        assert_eq!(summed, vec![Some(1564.0), None]);
    }

    /// The latency script as a multi-line text with indentation; recognition collapses
    /// whitespace runs, so the layout is not part of the contract.
    const LATENCY_SOURCE: &str = "\n                if (doc.containsKey('traceGroupFields.durationInNanos') && !doc['traceGroupFields.durationInNanos'].empty) {\n                  return Math.round(doc['traceGroupFields.durationInNanos'].value / 10000) / 100.0\n                }\n                return 0\n                ";

    /// OSD 3.9's form of the latency script: the same text without the trailing default, sent by
    /// its Traces table (end-to-end case
    /// `trace analytics traces table shows each trace with its latency, errors and group`).
    const LATENCY_SOURCE_WITHOUT_DEFAULT: &str = "\n                if (doc.containsKey('traceGroupFields.durationInNanos') && !doc['traceGroupFields.durationInNanos'].empty) {\n                  return Math.round(doc['traceGroupFields.durationInNanos'].value / 10000) / 100.0\n                }\n                ";

    #[tokio::test]
    async fn the_trace_list_latency_script_without_a_default_is_null_where_the_root_duration_is_absent() {
        let script = recognize_metric_script(LATENCY_SOURCE_WITHOUT_DEFAULT).expect("recognized");
        assert_eq!(
            script,
            MetricScript::TraceListLatency {
                is_zero_when_absent: false
            }
        );

        let schema = Arc::new(Schema::new(vec![Field::new("nanos", DataType::Int64, true)]));
        let nanos: ArrayRef = Arc::new(Int64Array::from(vec![Some(152_170_000), Some(735_519_999), None]));
        let batch = RecordBatch::try_new(schema, vec![nanos]).expect("batch");
        let batches = SessionContext::new()
            .read_batch(batch)
            .expect("frame")
            .select(vec![script.lower_value_script(col("nanos")).alias("latency")])
            .expect("projection")
            .collect()
            .await
            .expect("runs");
        let latencies: Vec<Option<f64>> = batches
            .iter()
            .flat_map(|batch| batch.column(0).as_primitive::<Float64Type>().iter().collect::<Vec<_>>())
            .collect();
        assert_eq!(latencies, vec![Some(152.17), Some(735.51), None]);
    }

    #[tokio::test]
    async fn the_trace_list_latency_script_with_one_token_changed_is_refused_naming_the_whole_script() {
        let edited = LATENCY_SOURCE_WITHOUT_DEFAULT.replace("/ 100.0", "/ 1000.0");
        let error = recognize_metric_script(&edited).expect_err("an edited latency script is no recognized form");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status.as_u16(), 400);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert!(
            body["error"]["reason"].as_str().is_some_and(|reason| reason.contains("1000.0")),
            "{body}"
        );
    }

    #[tokio::test]
    async fn the_trace_list_latency_script_truncates_to_hundredths_of_a_millisecond() {
        let script = recognize_metric_script(LATENCY_SOURCE).expect("the latency script is recognized");
        assert_eq!(
            script,
            MetricScript::TraceListLatency {
                is_zero_when_absent: true
            }
        );

        // A metric aggregation hands the script its input as a double; a projection over the
        // column hands it the stored integer. Both truncate alike.
        let nanos_columns: [(DataType, ArrayRef); 2] = [
            (
                DataType::Int64,
                Arc::new(Int64Array::from(vec![
                    Some(152_175_000),
                    Some(735_519_000),
                    Some(508_628_000),
                    Some(895_285_000),
                    None,
                ])),
            ),
            (
                DataType::Float64,
                Arc::new(Float64Array::from(vec![
                    Some(152_175_000.0),
                    Some(735_519_000.0),
                    Some(508_628_000.0),
                    Some(895_285_000.0),
                    None,
                ])),
            ),
        ];
        for (data_type, nanos) in nanos_columns {
            let schema = Arc::new(Schema::new(vec![Field::new("nanos", data_type.clone(), true)]));
            let batch = RecordBatch::try_new(schema, vec![nanos]).expect("batch");
            let batches = SessionContext::new()
                .read_batch(batch)
                .expect("frame")
                .select(vec![script.lower_value_script(col("nanos")).alias("latency")])
                .expect("projection")
                .collect()
                .await
                .expect("runs");
            let latencies: Vec<Option<f64>> = batches
                .iter()
                .flat_map(|batch| batch.column(0).as_primitive::<Float64Type>().iter().collect::<Vec<_>>())
                .collect();
            // floor(ns / 10^4) / 100: truncated, never rounded up; a span without a root reads 0.
            assert_eq!(
                latencies,
                [Some(152.17), Some(735.51), Some(508.62), Some(895.28), Some(0.0)],
                "{data_type}"
            );
        }

        let arithmetic = recognize_metric_script("_value / 1000").expect("arithmetic");
        assert!(matches!(arithmetic, MetricScript::Arithmetic(_)), "{arithmetic:?}");
        let error = recognize_metric_script("return doc['traceGroupFields.durationInNanos'].value")
            .expect_err("an edited latency script is no recognized form");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status.as_u16(), 400);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
    }

    /// OSD 3.9's last-updated script, with the whitespace it is sent with, sent by its Traces
    /// table (end-to-end case
    /// `trace analytics traces table shows each trace with its latency, errors and group`).
    const LAST_UPDATED_SOURCE: &str = "\n                  if (doc.containsKey('traceGroupFields.endTime') && !doc['traceGroupFields.endTime'].empty) {\n                    return doc['traceGroupFields.endTime'].value;\n                  }\n                  if (doc.containsKey('endTime') && !doc['endTime'].empty) {\n                    return doc['endTime'].value;\n                  }\n                ";

    #[tokio::test]
    async fn the_trace_last_updated_script_is_recognized_and_an_edited_one_refused() {
        assert_eq!(
            recognize_metric_script(LAST_UPDATED_SOURCE).expect("recognized"),
            MetricScript::TraceLastUpdated
        );

        let edited = LAST_UPDATED_SOURCE.replace("return doc['endTime']", "return doc['startTime']");
        let error = recognize_metric_script(&edited).expect_err("an edited last-updated script is no recognized form");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(body["error"]["script"], edited.as_str(), "{body}");
    }

    #[test]
    fn the_transcribed_moving_functions_apply_as_the_contract_states() {
        // Window 100, 300, 500 (oldest first), hand-computed:
        // mean 300; linear (100·1 + 300·2 + 500·3) / (1 + 1 + 2 + 3) = 2200 / 7, the
        // total weight starting at 1; ewma α 0.5: 100 → 200 → 350; holt α 0.5 β 0.5:
        // levels 100, 200, 375.
        let window = [100.0, 300.0, 500.0];
        let cases = [
            ("MovingFunctions.unweightedAvg(values)", 300.0),
            ("MovingFunctions.linearWeightedAvg(values)", 2200.0 / 7.0),
            ("MovingFunctions.ewma(values, 0.5)", 350.0),
            ("MovingFunctions.holt(values, 0.5, 0.5)", 375.0),
            (" MovingFunctions.ewma(values,0.5) ", 350.0),
        ];
        for (source, expected) in cases {
            let function = recognize_moving_function(source).expect("a transcribed form");
            assert_eq!(function.apply(&window), Some(expected), "{source}");
            assert_eq!(function.apply(&[]), None, "{source}: an empty window has no value");
        }
    }

    #[tokio::test]
    async fn any_other_moving_function_script_is_an_illegal_argument_400_naming_it() {
        for source in [
            "MovingFunctions.max(values)",
            "MovingFunctions.sum(values)",
            "if (values.length > 5*2) {MovingFunctions.holtWinters(values, 0.6, 0.3, 0.1, 5, false)}",
            "MovingFunctions.holtWinters(values, 0.6, 0.3, 0.1, 5, false)",
            "MovingFunctions.ewma(values)",
            "MovingFunctions.ewma(values, 1.5)",
            "MovingFunctions.unweightedAvg(values, 1)",
            "MovingFunctions.unweightedAvg(other)",
            "return MovingFunctions.unweightedAvg(values);",
            "values[0]",
        ] {
            let error = recognize_moving_function(source).expect_err("refused");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{source}");
            assert_eq!(body["error"]["type"], json!("illegal_argument_exception"), "{source}");
            assert_eq!(body["error"]["script"], json!(source), "{source}");
        }
    }

    /// A random arithmetic tree at most `depth` operator levels deep over `_value`,
    /// `params.a` and literals that print exactly (eighths below 10 000).
    fn build_random_tree(random: &mut StdRng, depth: usize) -> ArithmeticExpr {
        if depth == 0 || random.random_bool(0.3) {
            return match random.random_range(0..3) {
                0 => ArithmeticExpr::Number(f64::from(random.random_range(0_u32..80_000)) / 8.0),
                1 => ArithmeticExpr::Variable(VALUE_VARIABLE.to_owned()),
                _ => ArithmeticExpr::Variable("a".to_owned()),
            };
        }
        let operator = match random.random_range(0..6) {
            0 => return ArithmeticExpr::Negate(Box::new(build_random_tree(random, depth - 1))),
            1 => ArithmeticOperator::Add,
            2 => ArithmeticOperator::Subtract,
            3 => ArithmeticOperator::Multiply,
            4 => ArithmeticOperator::Divide,
            _ => ArithmeticOperator::Remainder,
        };
        ArithmeticExpr::Binary {
            operator,
            left: Box::new(build_random_tree(random, depth - 1)),
            right: Box::new(build_random_tree(random, depth - 1)),
        }
    }

    /// `expression` as script text with every operation in its own parentheses, so the
    /// recognizer's precedence plays no part in the tree it reads back.
    fn format_expression_source(expression: &ArithmeticExpr) -> String {
        match expression {
            ArithmeticExpr::Number(number) => format!("{number:?}"),
            ArithmeticExpr::Variable(name) if name == VALUE_VARIABLE => name.clone(),
            ArithmeticExpr::Variable(name) => format!("params.{name}"),
            ArithmeticExpr::Negate(inner) => format!("(-{})", format_expression_source(inner)),
            ArithmeticExpr::Binary { operator, left, right } => {
                let symbol = match operator {
                    ArithmeticOperator::Add => '+',
                    ArithmeticOperator::Subtract => '-',
                    ArithmeticOperator::Multiply => '*',
                    ArithmeticOperator::Divide => '/',
                    ArithmeticOperator::Remainder => '%',
                };
                format!(
                    "({} {symbol} {})",
                    format_expression_source(left),
                    format_expression_source(right)
                )
            }
        }
    }

    /// Operator levels of `expression`: 0 for a leaf.
    fn measure_depth(expression: &ArithmeticExpr) -> usize {
        match expression {
            ArithmeticExpr::Number(_) | ArithmeticExpr::Variable(_) => 0,
            ArithmeticExpr::Negate(inner) => 1 + measure_depth(inner),
            ArithmeticExpr::Binary { left, right, .. } => 1 + measure_depth(left).max(measure_depth(right)),
        }
    }

    /// Records in `seen_forms` (negation, then `+ - * / %`) every operation `expression` holds.
    fn record_operations(expression: &ArithmeticExpr, seen_forms: &mut [bool; 6]) {
        match expression {
            ArithmeticExpr::Number(_) | ArithmeticExpr::Variable(_) => {}
            ArithmeticExpr::Negate(inner) => {
                seen_forms[0] = true;
                record_operations(inner, seen_forms);
            }
            ArithmeticExpr::Binary { operator, left, right } => {
                let position = match operator {
                    ArithmeticOperator::Add => 1,
                    ArithmeticOperator::Subtract => 2,
                    ArithmeticOperator::Multiply => 3,
                    ArithmeticOperator::Divide => 4,
                    ArithmeticOperator::Remainder => 5,
                };
                seen_forms[position] = true;
                record_operations(left, seen_forms);
                record_operations(right, seen_forms);
            }
        }
    }

    /// Fully parenthesized random trees read back as the same tree and evaluate to the same
    /// bits. Each operator level prints at most two nesting levels (a negation's parenthesis
    /// and its minus), so twelve levels stay inside the nesting limit.
    #[test]
    fn parsing_round_trips_random_arithmetic_trees() {
        let mut random = StdRng::seed_from_u64(20_261_108);
        let values = |name: &str| match name {
            VALUE_VARIABLE => Some(1500.25),
            "a" => Some(-3.5),
            _ => None,
        };
        let mut seen_forms = [false; 6];
        let mut deepest_level = 0;
        let mut round_trips = 0;
        for _ in 0..500 {
            let tree = build_random_tree(&mut random, 12);
            let source = format_expression_source(&tree);
            // The length limit has its own boundary cases; a tree printing past it is not drawn
            // into this property.
            if source.len() > MAX_SCRIPT_LENGTH {
                continue;
            }
            let parsed = recognize_arithmetic_script(&source, &[VALUE_VARIABLE, "a"]).expect("a printed tree");
            assert_eq!(parsed, tree, "{source}");
            assert_eq!(
                parsed.evaluate(&values).map(f64::to_bits),
                tree.evaluate(&values).map(f64::to_bits),
                "{source}"
            );
            record_operations(&tree, &mut seen_forms);
            deepest_level = deepest_level.max(measure_depth(&tree));
            round_trips += 1;
        }
        // The seed reaches every operation and a tree of at least eight levels.
        assert_eq!(seen_forms, [true; 6]);
        assert!(deepest_level >= 8, "the deepest tree has {deepest_level} levels");
        assert!(round_trips >= 400, "{round_trips} trees round-tripped");
    }
}
