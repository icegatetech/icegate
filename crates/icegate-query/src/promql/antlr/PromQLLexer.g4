// PromQL ANTLR Grammar v1.0.0 - Prometheus Query Language
// Adapted from grammars-v4/promql for antlr4rust compatibility

lexer grammar PromQLLexer;

channels {
    WHITESPACE,
    COMMENTS
}

// PromQL keywords are case-sensitive (no caseInsensitive option)

/*------------------------------------------------------------------
 *  LEXER TOKENS FOR OPERATORS & SYMBOLS
 *------------------------------------------------------------------*/
LPAREN     : '(' ;
RPAREN     : ')' ;
LBRACE     : '{' ;
RBRACE     : '}' ;
LBRACK     : '[' ;
RBRACK     : ']' ;
COLON      : ':' ;
COMMA      : ',' ;
ADD        : '+' ;
SUB        : '-' ;
MUL        : '*' ;
DIV        : '/' ;
POW        : '^' ;

// Comparison operators
EQ  : '=' ;
NE  : '!=' ;
RE  : '=~' ;
NRE : '!~' ;
GT  : '>'  ;
LT  : '<'  ;
GE  : '>=' ;
LE  : '<=' ;
EQL : '==' ;

// Logical/set operators
AND    : 'and'    ;
OR     : 'or'     ;
UNLESS : 'unless' ;
BOOL   : 'bool'   ;

// Grouping keywords
BY      : 'by' ;
WITHOUT : 'without' ;

// Vector matching keywords
ON          : 'on' ;
IGNORING    : 'ignoring' ;
GROUP_LEFT  : 'group_left' ;
GROUP_RIGHT : 'group_right' ;

// Modifiers
OFFSET : 'offset' ;
AT     : '@' ;
START  : 'start' ;
END    : 'end' ;

// Modulo operator
MOD : '%' ;

// Aggregation operators
AGG_SUM          : 'sum' ;
AGG_MIN          : 'min' ;
AGG_MAX          : 'max' ;
AGG_AVG          : 'avg' ;
AGG_GROUP        : 'group' ;
AGG_STDDEV       : 'stddev' ;
AGG_STDVAR       : 'stdvar' ;
AGG_COUNT        : 'count' ;
AGG_COUNT_VALUES : 'count_values' ;
AGG_BOTTOMK      : 'bottomk' ;
AGG_TOPK         : 'topk' ;
AGG_QUANTILE     : 'quantile' ;

// Range functions (operate on range vectors)
RATE                : 'rate' ;
IRATE               : 'irate' ;
INCREASE             : 'increase' ;
DELTA                : 'delta' ;
IDELTA               : 'idelta' ;
DERIV                : 'deriv' ;
CHANGES              : 'changes' ;
RESETS               : 'resets' ;
PREDICT_LINEAR       : 'predict_linear' ;
HOLT_WINTERS         : 'holt_winters' ;
AVG_OVER_TIME        : 'avg_over_time' ;
MIN_OVER_TIME        : 'min_over_time' ;
MAX_OVER_TIME        : 'max_over_time' ;
SUM_OVER_TIME        : 'sum_over_time' ;
COUNT_OVER_TIME      : 'count_over_time' ;
QUANTILE_OVER_TIME   : 'quantile_over_time' ;
STDDEV_OVER_TIME     : 'stddev_over_time' ;
STDVAR_OVER_TIME     : 'stdvar_over_time' ;
LAST_OVER_TIME       : 'last_over_time' ;
PRESENT_OVER_TIME    : 'present_over_time' ;
ABSENT_OVER_TIME     : 'absent_over_time' ;

// Math functions (operate on instant vectors or scalars)
ABS       : 'abs' ;
ABSENT    : 'absent' ;
CEIL      : 'ceil' ;
CLAMP     : 'clamp' ;
CLAMP_MAX : 'clamp_max' ;
CLAMP_MIN : 'clamp_min' ;
EXP       : 'exp' ;
FLOOR     : 'floor' ;
LN        : 'ln' ;
LOG2      : 'log2' ;
LOG10     : 'log10' ;
ROUND     : 'round' ;
SCALAR    : 'scalar' ;
SGN       : 'sgn' ;
SORT      : 'sort' ;
SORT_DESC : 'sort_desc' ;
SQRT      : 'sqrt' ;

// Trigonometric functions
ACOS  : 'acos' ;
ACOSH : 'acosh' ;
ASIN  : 'asin' ;
ASINH : 'asinh' ;
ATAN  : 'atan' ;
ATANH : 'atanh' ;
COS   : 'cos' ;
COSH  : 'cosh' ;
SIN   : 'sin' ;
SINH  : 'sinh' ;
TAN   : 'tan' ;
TANH  : 'tanh' ;
DEG   : 'deg' ;
RAD   : 'rad' ;
PI    : 'pi' ;

// Label manipulation functions
LABEL_JOIN    : 'label_join' ;
LABEL_REPLACE : 'label_replace' ;

// Time functions
TIME          : 'time' ;
TIMESTAMP     : 'timestamp' ;
DAY_OF_MONTH  : 'day_of_month' ;
DAY_OF_WEEK   : 'day_of_week' ;
DAY_OF_YEAR   : 'day_of_year' ;
DAYS_IN_MONTH : 'days_in_month' ;
HOUR          : 'hour' ;
MINUTE        : 'minute' ;
MONTH         : 'month' ;
YEAR          : 'year' ;

// Histogram functions
HISTOGRAM_COUNT    : 'histogram_count' ;
HISTOGRAM_SUM      : 'histogram_sum' ;
HISTOGRAM_FRACTION : 'histogram_fraction' ;
HISTOGRAM_QUANTILE : 'histogram_quantile' ;

// Vector function
VECTOR : 'vector' ;

// Duration type tokens - must come before IDENTIFIER to have precedence
DURATION_TYPE_MILLISECOND: 'ms';
DURATION_TYPE_YEAR: 'y';
DURATION_TYPE_WEEK: 'w';
DURATION_TYPE_DAY: 'd';
DURATION_TYPE_HOUR: 'h';
DURATION_TYPE_MINUTE: 'm';
DURATION_TYPE_SECOND: 's';

// Numeric tokens
NUMBER :
    ( [0-9]+ ( '.' [0-9]* )? | '.' [0-9]+ ) ( [eE] [+-]? [0-9]+ )?
    | '0x' [0-9a-fA-F]+
    ;

// Duration token - compound duration like 1h30m, 5m, 2d5h
DURATION :
    (
        (  NUMBER DURATION_TYPE_YEAR ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? NUMBER DURATION_TYPE_WEEK ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? NUMBER DURATION_TYPE_DAY ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? NUMBER DURATION_TYPE_HOUR ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? NUMBER DURATION_TYPE_MINUTE ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? NUMBER DURATION_TYPE_SECOND ( NUMBER DURATION_TYPE_MILLISECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? NUMBER DURATION_TYPE_MILLISECOND )
    )
    ;

// String literals
STRING
    : '\'' (~('\'' | '\\') | '\\' .)* '\''
    | '"' (~('"' | '\\') | '\\' .)* '"'
    | '`' (~('`'  | '\\') | '\\' .)* '`';

// Identifier - matches metric names (with colons) and label names
// Must come AFTER all keyword tokens for proper precedence
IDENTIFIER: [a-zA-Z_] [a-zA-Z0-9_:]*;

/*------------------------------------------------------------------
 *  WHITESPACE & COMMENTS
 *------------------------------------------------------------------*/
WS         : [\r\t\n ]+   -> channel(WHITESPACE);
SL_COMMENT : '#' ~[\r\n]* -> channel(COMMENTS);
