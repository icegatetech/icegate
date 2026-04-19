lexer grammar TraceQLLexer;

// =====================================================================
// Punctuation
// =====================================================================
LBRACE       : '{' ;
RBRACE       : '}' ;
LPAREN       : '(' ;
RPAREN       : ')' ;
COMMA        : ',' ;
DOT          : '.' ;
COLON        : ':' ;
PIPE         : '|' ;

// =====================================================================
// Comparison operators (longest-match first)
// =====================================================================
EQ_RE        : '=~' ;
NEQ_RE       : '!~' ;
GE           : '>=' ;
LE           : '<=' ;
NEQ          : '!=' ;
EQ           : '=' ;
GT           : '>' ;
LT           : '<' ;

// =====================================================================
// Spanset operators
// =====================================================================
DESC         : '>>' ;       // descendant
ANC          : '<<' ;       // ancestor
SIBLING      : '~' ;
NOT_DESC     : '!>>' ;
NOT_ANC      : '!<<' ;
NOT_CHILD    : '!>' ;
NOT_PARENT   : '!<' ;
NOT_SIBLING  : '!~' ;       // overlaps NEQ_RE — disambiguated by parser context

// =====================================================================
// Boolean operators
// =====================================================================
AND          : '&&' ;
OR           : '||' ;
NOT          : '!' ;

// =====================================================================
// Arithmetic (parser-only; planner rejects)
// =====================================================================
PLUS         : '+' ;
MINUS        : '-' ;
STAR         : '*' ;
SLASH        : '/' ;
PERCENT      : '%' ;

// =====================================================================
// Scope keywords
// =====================================================================
SCOPE_SPAN     : 'span' ;
SCOPE_RESOURCE : 'resource' ;
SCOPE_EVENT    : 'event' ;
SCOPE_LINK     : 'link' ;
SCOPE_PARENT   : 'parent' ;
SCOPE_TRACE    : 'trace' ;

// =====================================================================
// Intrinsic field keywords
// =====================================================================
INTR_NAME              : 'name' ;
INTR_STATUS            : 'status' ;
INTR_STATUS_MESSAGE    : 'statusMessage' ;
INTR_KIND              : 'kind' ;
INTR_DURATION          : 'duration' ;
INTR_TRACE_DURATION    : 'traceDuration' ;
INTR_ROOT_NAME         : 'rootName' ;
INTR_ROOT_SVC          : 'rootServiceName' ;
INTR_TRACE_ID          : 'traceID' ;
INTR_SPAN_ID           : 'spanID' ;

// =====================================================================
// Status / kind enum literals
// =====================================================================
STATUS_OK              : 'ok' ;
STATUS_ERROR           : 'error' ;
STATUS_UNSET           : 'unset' ;

KIND_SERVER            : 'server' ;
KIND_CLIENT            : 'client' ;
KIND_PRODUCER          : 'producer' ;
KIND_CONSUMER          : 'consumer' ;
KIND_INTERNAL          : 'internal' ;

// =====================================================================
// Aggregation function keywords
// =====================================================================
FN_COUNT               : 'count' ;
FN_SUM                 : 'sum' ;
FN_AVG                 : 'avg' ;
FN_MIN                 : 'min' ;
FN_MAX                 : 'max' ;
FN_QUANTILE            : 'quantile_over_time' ;
FN_RATE                : 'rate' ;
FN_COUNT_OVER_TIME     : 'count_over_time' ;
FN_HISTOGRAM_OVER_TIME : 'histogram_over_time' ;

KW_BY                  : 'by' ;
KW_TRUE                : 'true' ;
KW_FALSE               : 'false' ;
KW_NIL                 : 'nil' ;

// =====================================================================
// Literals
// =====================================================================
DURATION
    : DIGIT+ ('.' DIGIT+)? ('ns' | 'us' | 'µs' | 'ms' | 's' | 'm' | 'h' | 'd' | 'w' | 'y')
    ;

BYTES
    : DIGIT+ ('.' DIGIT+)? ('KB' | 'MB' | 'GB' | 'TB' | 'PB' | 'KiB' | 'MiB' | 'GiB' | 'TiB' | 'PiB' | 'B')
    ;

FLOAT  : '-'? DIGIT+ '.' DIGIT+ ;
INT    : '-'? DIGIT+ ;
STRING : '"' (~["\\] | '\\' .)* '"' | '\'' (~['\\] | '\\' .)* '\'' ;

// IDENT allows internal hyphens to match common service / attribute names
// (e.g. `icegate-query`, `k8s.node-name`). The leading character is still a
// letter or underscore, so a literal `-` keeps tokenizing as `MINUS`.
IDENT  : (ALPHA | '_') (ALPHA | DIGIT | '_' | '-')* ;

// =====================================================================
// Whitespace & comments
// =====================================================================
WS      : [ \t\r\n]+ -> skip ;
COMMENT : '//' ~[\r\n]* -> skip ;

// =====================================================================
// Fragments
// =====================================================================
fragment ALPHA : [a-zA-Z] ;
fragment DIGIT : [0-9] ;
