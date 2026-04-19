//! Span selectors and spanset operators.
//!
//! The TraceQL grammar's central noun is a spanset — a set of spans
//! produced either by a `{ ... }` selector or by composing two spansets
//! with structural / boolean operators.

use super::common::{ComparisonOp, FieldRef, LiteralValue};

/// A composed spanset expression.
///
/// A bare `{ ... }` selector lifts to [`SpansetExpr::Selector`]. Composing
/// two spansets with `>>`, `>`, `<<`, `<`, `~`, `&&`, or `||` yields a
/// [`SpansetExpr::Op`] node.
///
/// v1 planning supports only [`SpansetExpr::Selector`]; [`SpansetExpr::Op`]
/// returns [`crate::error::QueryError::NotImplemented`] from the planner
/// regardless of [`SpansetOp`] variant.
#[derive(Debug, Clone, PartialEq)]
pub enum SpansetExpr {
    /// A primitive `{ ... }` selector.
    Selector(SpanSelector),
    /// Composition of two spansets with a structural or boolean operator.
    Op {
        /// Left-hand spanset.
        lhs: Box<Self>,
        /// The operator joining the two spansets.
        op: SpansetOp,
        /// Right-hand spanset.
        rhs: Box<Self>,
    },
}

/// Operator joining two spansets in a structural composition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpansetOp {
    /// `&&` — intersection of spansets (both must contain a matching span).
    And,
    /// `||` — union of spansets.
    Or,
    /// `>>` — descendant (transitive child).
    Descendant,
    /// `>` — direct child.
    Child,
    /// `<<` — ancestor (transitive parent).
    Ancestor,
    /// `<` — direct parent.
    Parent,
    /// `~` — sibling (shares parent).
    Sibling,
    /// `!>>` — not descendant.
    NotDescendant,
    /// `!>` — not child.
    NotChild,
    /// `!<<` — not ancestor.
    NotAncestor,
    /// `!<` — not parent.
    NotParent,
    /// `!~` — not sibling.
    NotSibling,
}

impl SpansetOp {
    /// Source-form spelling of the operator.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::And => "&&",
            Self::Or => "||",
            Self::Descendant => ">>",
            Self::Child => ">",
            Self::Ancestor => "<<",
            Self::Parent => "<",
            Self::Sibling => "~",
            Self::NotDescendant => "!>>",
            Self::NotChild => "!>",
            Self::NotAncestor => "!<<",
            Self::NotParent => "!<",
            Self::NotSibling => "!~",
        }
    }

    /// True if this is a structural (hierarchy) operator. Hierarchy ops are
    /// always rejected by the v1 planner; pure boolean ops (`&&`, `||`) are
    /// rejected only between distinct spansets (the planner never reaches
    /// this code path because boolean composition inside a single selector
    /// is encoded via [`SpanFilter`], not as a [`SpansetOp`]).
    #[must_use]
    pub const fn is_hierarchy(&self) -> bool {
        !matches!(self, Self::And | Self::Or)
    }
}

/// A primitive `{ ... }` span selector.
///
/// The optional [`SpanFilter`] inside the braces is built from comparisons
/// on [`FieldRef`]s combined with `&&`, `||`, `!`. An empty selector
/// (`{}`) matches every span in scope.
#[derive(Debug, Clone, PartialEq)]
pub struct SpanSelector {
    /// Filter expression inside the braces. `None` means "match all".
    pub filter: Option<SpanFilter>,
}

impl SpanSelector {
    /// Create a selector matching every span.
    #[must_use]
    pub const fn all() -> Self {
        Self { filter: None }
    }

    /// Create a selector with the given filter expression.
    #[must_use]
    pub const fn new(filter: SpanFilter) -> Self {
        Self { filter: Some(filter) }
    }
}

/// Filter expression inside a `{ ... }` selector.
///
/// Composed of leaf comparisons combined with logical operators.
/// Arithmetic (`span.bytes / span.jobs`) is NOT modelled here in v1 — the
/// grammar accepts it (Phase 2 task) but the parser rejects it before it
/// reaches the AST. Hierarchy operators are NOT modelled here — they live
/// at the [`SpansetExpr`] level.
#[derive(Debug, Clone, PartialEq)]
pub enum SpanFilter {
    /// `field op literal` — the workhorse leaf node.
    Compare {
        /// LHS field reference.
        field: FieldRef,
        /// Comparison operator.
        op: ComparisonOp,
        /// RHS literal.
        value: LiteralValue,
    },
    /// `(filter)` — preserves grouping for round-trip rendering.
    Paren(Box<Self>),
    /// `! filter`.
    Not(Box<Self>),
    /// `lhs && rhs`.
    And(Box<Self>, Box<Self>),
    /// `lhs || rhs`.
    Or(Box<Self>, Box<Self>),
}

impl SpanFilter {
    /// Build a leaf comparison.
    #[must_use]
    pub const fn cmp(field: FieldRef, op: ComparisonOp, value: LiteralValue) -> Self {
        Self::Compare { field, op, value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traceql::common::{IntrinsicField, Scope};

    #[test]
    fn selector_all_has_no_filter() {
        let s = SpanSelector::all();
        assert!(s.filter.is_none());
    }

    #[test]
    fn spanset_op_distinguishes_hierarchy_vs_boolean() {
        assert!(!SpansetOp::And.is_hierarchy());
        assert!(!SpansetOp::Or.is_hierarchy());
        assert!(SpansetOp::Descendant.is_hierarchy());
        assert!(SpansetOp::Sibling.is_hierarchy());
    }

    #[test]
    fn build_status_eq_error_filter() {
        let f = SpanFilter::cmp(
            FieldRef::Intrinsic(IntrinsicField::Status),
            ComparisonOp::Eq,
            LiteralValue::Status(crate::traceql::common::StatusValue::Error),
        );
        assert!(matches!(f, SpanFilter::Compare { .. }));
    }

    #[test]
    fn build_attribute_field_ref() {
        let r = FieldRef::Attribute {
            scope: Scope::Span,
            name: "http.method".to_string(),
        };
        assert!(matches!(r, FieldRef::Attribute { .. }));
    }
}
