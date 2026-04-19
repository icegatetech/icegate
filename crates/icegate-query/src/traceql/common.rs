//! Common types shared across TraceQL expression modules.
//!
//! Defines: comparison operators, scope tags, intrinsic field names,
//! literal values, and field references. These appear across span
//! selectors, pipeline aggregations, and metrics-mode functions.

/// Comparison operator used in span filters and pipeline `count() > N`-style
/// stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComparisonOp {
    /// `=` equality (`TraceQL` uses `=` not `==`).
    Eq,
    /// `!=` inequality.
    Neq,
    /// `>` greater than.
    Gt,
    /// `>=` greater than or equal.
    Ge,
    /// `<` less than.
    Lt,
    /// `<=` less than or equal.
    Le,
    /// `=~` regex match.
    Re,
    /// `!~` regex not match.
    Nre,
}

impl ComparisonOp {
    /// Source-form spelling of the operator.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Neq => "!=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Re => "=~",
            Self::Nre => "!~",
        }
    }
}

/// Scope tag for an attribute reference in a `TraceQL` field expression.
///
/// `TraceQL` distinguishes attributes by the OTLP entity they belong to:
/// the span itself, its parent resource, an event, or a link. The
/// [`Scope::Any`] variant maps to `TraceQL`'s leading-dot shorthand
/// (e.g., `.http.status_code`) which lets the engine search any scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    /// `span.foo`
    Span,
    /// `resource.foo`
    Resource,
    /// `event.foo`
    Event,
    /// `link.foo`
    Link,
    /// `parent.span.foo` / `parent.resource.foo`
    Parent(ParentScope),
    /// `.foo` — any scope. Resolved at planning time.
    Any,
}

/// Inner scope for the `parent.X.foo` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParentScope {
    /// `parent.span.foo`
    Span,
    /// `parent.resource.foo`
    Resource,
}

/// `TraceQL` intrinsic fields — built-in span/trace properties that exist
/// outside the attributes map.
///
/// Source: <https://grafana.com/docs/tempo/latest/traceql/construct-traceql-queries/#intrinsic-fields>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntrinsicField {
    /// `name` — operation name.
    Name,
    /// `status` — span status (`error` / `ok` / `unset`).
    Status,
    /// `statusMessage` — span status description.
    StatusMessage,
    /// `kind` — span kind (`server` / `client` / `producer` / `consumer` / `internal`).
    Kind,
    /// `duration` — `endTime - startTime`.
    Duration,
    /// `traceDuration` — total trace duration (root span duration).
    TraceDuration,
    /// `rootName` — name of the trace's root span.
    RootName,
    /// `rootServiceName` — service of the trace's root span.
    RootServiceName,
    /// `traceID` — trace identifier.
    TraceID,
    /// `spanID` — span identifier.
    SpanID,
    /// `parent` — parent reference (rare; used in spanset operators).
    Parent,
    /// `event:name`
    EventName,
    /// `event:timeSinceStart`
    EventTimeSinceStart,
    /// `link:traceID`
    LinkTraceID,
    /// `link:spanID`
    LinkSpanID,
}

impl IntrinsicField {
    /// Source-form spelling of the intrinsic.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Status => "status",
            Self::StatusMessage => "statusMessage",
            Self::Kind => "kind",
            Self::Duration => "duration",
            Self::TraceDuration => "traceDuration",
            Self::RootName => "rootName",
            Self::RootServiceName => "rootServiceName",
            Self::TraceID => "traceID",
            Self::SpanID => "spanID",
            Self::Parent => "parent",
            Self::EventName => "event:name",
            Self::EventTimeSinceStart => "event:timeSinceStart",
            Self::LinkTraceID => "link:traceID",
            Self::LinkSpanID => "link:spanID",
        }
    }
}

/// A literal value in a `TraceQL` expression.
///
/// Numerics are stored in their widest form to avoid losing precision; the
/// planner narrows / coerces based on the target column type.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    /// Integer literal (e.g., `42`, `1024`).
    Int(i64),
    /// Floating-point literal (e.g., `3.14`).
    Float(f64),
    /// String literal (single or double quoted).
    String(String),
    /// Boolean literal (`true` / `false`).
    Bool(bool),
    /// Duration literal in nanoseconds (e.g., `1s` → `1_000_000_000`).
    Duration(i64),
    /// Data-size literal in bytes (e.g., `10MB` → `10_485_760`).
    Bytes(i64),
    /// Status literal (`ok`, `error`, `unset`).
    Status(StatusValue),
    /// Span kind literal (`server`, `client`, `producer`, `consumer`, `internal`).
    Kind(KindValue),
    /// `nil` — absent attribute.
    Nil,
}

/// Span status enum literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatusValue {
    /// `ok`
    Ok,
    /// `error`
    Error,
    /// `unset`
    Unset,
}

impl StatusValue {
    /// Numeric encoding matching the OTLP `Status.code` enum used in the
    /// spans schema (`status_code` column).
    #[must_use]
    pub const fn otlp_code(&self) -> i32 {
        match self {
            Self::Unset => 0,
            Self::Ok => 1,
            Self::Error => 2,
        }
    }
}

/// Span kind enum literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KindValue {
    /// Unspecified kind.
    Unspecified,
    /// `internal`
    Internal,
    /// `server`
    Server,
    /// `client`
    Client,
    /// `producer`
    Producer,
    /// `consumer`
    Consumer,
}

impl KindValue {
    /// Numeric encoding matching the OTLP `SpanKind` enum used in the
    /// spans schema (`kind` column).
    #[must_use]
    pub const fn otlp_code(&self) -> i32 {
        match self {
            Self::Unspecified => 0,
            Self::Internal => 1,
            Self::Server => 2,
            Self::Client => 3,
            Self::Producer => 4,
            Self::Consumer => 5,
        }
    }
}

/// A reference to a span field — either an intrinsic or an attribute under a scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldRef {
    /// Built-in intrinsic field (e.g., `duration`, `name`, `status`).
    Intrinsic(IntrinsicField),
    /// Scoped attribute (e.g., `span.http.method`, `resource.service.name`,
    /// `.cluster`).
    Attribute {
        /// The scope (Span / Resource / Event / Link / Parent / Any).
        scope: Scope,
        /// Dotted attribute key as written in the query (without the scope
        /// prefix, e.g., `http.method`).
        name: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparison_op_as_str_round_trips_known_ops() {
        assert_eq!(ComparisonOp::Eq.as_str(), "=");
        assert_eq!(ComparisonOp::Re.as_str(), "=~");
    }

    #[test]
    fn status_otlp_code_matches_otlp_spec() {
        assert_eq!(StatusValue::Unset.otlp_code(), 0);
        assert_eq!(StatusValue::Ok.otlp_code(), 1);
        assert_eq!(StatusValue::Error.otlp_code(), 2);
    }

    #[test]
    fn kind_otlp_code_matches_otlp_spec() {
        assert_eq!(KindValue::Server.otlp_code(), 2);
        assert_eq!(KindValue::Client.otlp_code(), 3);
    }
}
