parser grammar TraceQLParser;

options { tokenVocab=TraceQLLexer; }

// =====================================================================
// Entry
// =====================================================================
root
    : pipelineExpr EOF
    | spansetExpr EOF
    ;

// =====================================================================
// Pipeline (search and metrics modes)
// =====================================================================
pipelineExpr
    : spansetExpr (PIPE pipelineStage)+
    ;

pipelineStage
    : byClause
    | aggregateFilter
    | aggregate
    | metricsFunction byClause?
    ;

byClause
    : KW_BY LPAREN fieldRef (COMMA fieldRef)* RPAREN
    ;

aggregate
    : aggregateOp LPAREN fieldRef? (COMMA literal)? RPAREN
    ;

aggregateFilter
    : aggregateOp LPAREN fieldRef? (COMMA literal)? RPAREN comparisonOp literal
    ;

aggregateOp
    : FN_COUNT
    | FN_SUM
    | FN_AVG
    | FN_MIN
    | FN_MAX
    | FN_QUANTILE
    ;

metricsFunction
    : FN_RATE LPAREN RPAREN
    | FN_COUNT_OVER_TIME LPAREN RPAREN
    | FN_HISTOGRAM_OVER_TIME LPAREN fieldRef RPAREN
    ;

// =====================================================================
// Spansets
//
// Operator precedence from lowest to highest:
//   1. OR  (left-assoc)
//   2. AND (left-assoc)
//   3. structural relations: DESC `>>`, GT `>`, ANC `<<`, LT `<`,
//      SIBLING `~`, NOT_DESC `!>>`, NOT_CHILD `!>`, NOT_ANC `!<<`,
//      NOT_PARENT `!<`, and NEQ_RE `!~` (re-used as not-sibling).
//      Left-assoc, all at the same tier.
//   4. parenthesised group / bare span selector (atom).
//
// This mirrors the per-token precedence ladder used by `spanFilter`
// below: relationships bind tighter than booleans so a query like
// `{a} >> {b} && {c}` parses as `({a} >> {b}) && {c}`.
// =====================================================================
spansetExpr
    : spansetOr
    ;

spansetOr
    : spansetAnd (OR spansetAnd)*
    ;

spansetAnd
    : spansetRel (AND spansetRel)*
    ;

spansetRel
    : spansetPrimary (spansetRelOp spansetPrimary)*
    ;

spansetPrimary
    : LPAREN spansetExpr RPAREN
    | spanSelector
    ;

spansetRelOp
    : DESC | GT | ANC | LT | SIBLING
    | NOT_DESC | NOT_CHILD | NOT_ANC | NOT_PARENT | NEQ_RE
    ;

spanSelector
    : LBRACE spanFilter? RBRACE
    ;

// =====================================================================
// Span filter expressions
// =====================================================================
spanFilter
    : NOT spanFilter                                  # FilterNot
    | LPAREN spanFilter RPAREN                        # FilterParen
    | spanFilter AND spanFilter                       # FilterAnd
    | spanFilter OR spanFilter                        # FilterOr
    | fieldRef comparisonOp literal                   # FilterCompare
    ;

comparisonOp
    : EQ | NEQ | GT | GE | LT | LE | EQ_RE | NEQ_RE
    ;

// =====================================================================
// Field references
// =====================================================================
fieldRef
    : intrinsic                                       # FieldIntrinsic
    | scopedAttribute                                 # FieldScoped
    | DOT identChain                                  # FieldAnyScope
    ;

scopedAttribute
    : SCOPE_SPAN     DOT identChain
    | SCOPE_RESOURCE DOT identChain
    | SCOPE_EVENT    DOT identChain
    | SCOPE_LINK     DOT identChain
    | SCOPE_PARENT   DOT (SCOPE_SPAN | SCOPE_RESOURCE) DOT identChain
    ;

identChain
    : identPart (DOT identPart)*
    ;

// Identifier-like token. We accept IDENT plus all keyword-style tokens so
// that attribute paths can include words such as `name`, `status`, `kind`,
// `duration`, etc. that the lexer normally tokenizes as keywords. The
// grammar-level disambiguation lives elsewhere (e.g., the `intrinsic` rule
// only consumes INTR_* tokens at the top of a fieldRef).
identPart
    : IDENT
    | INTR_NAME | INTR_STATUS | INTR_STATUS_MESSAGE | INTR_KIND
    | INTR_DURATION | INTR_TRACE_DURATION
    | INTR_ROOT_NAME | INTR_ROOT_SVC
    | INTR_TRACE_ID  | INTR_SPAN_ID
    | SCOPE_SPAN | SCOPE_RESOURCE | SCOPE_EVENT | SCOPE_LINK
    | SCOPE_PARENT | SCOPE_TRACE
    | STATUS_OK | STATUS_ERROR | STATUS_UNSET
    | KIND_SERVER | KIND_CLIENT | KIND_PRODUCER | KIND_CONSUMER | KIND_INTERNAL
    | FN_COUNT | FN_SUM | FN_AVG | FN_MIN | FN_MAX | FN_QUANTILE
    | FN_RATE | FN_COUNT_OVER_TIME | FN_HISTOGRAM_OVER_TIME
    | KW_BY | KW_TRUE | KW_FALSE | KW_NIL
    ;

intrinsic
    : INTR_NAME | INTR_STATUS | INTR_STATUS_MESSAGE | INTR_KIND
    | INTR_DURATION | INTR_TRACE_DURATION
    | INTR_ROOT_NAME | INTR_ROOT_SVC
    | INTR_TRACE_ID  | INTR_SPAN_ID
    ;

// =====================================================================
// Literals
// =====================================================================
literal
    : DURATION
    | BYTES
    | FLOAT
    | INT
    | STRING
    | KW_TRUE
    | KW_FALSE
    | KW_NIL
    | STATUS_OK | STATUS_ERROR | STATUS_UNSET
    | KIND_SERVER | KIND_CLIENT | KIND_PRODUCER | KIND_CONSUMER | KIND_INTERNAL
    | IDENT    // bare-word string; matches Grafana Tempo's tolerance for unquoted values (e.g. `.svc = frontend`)
    ;
