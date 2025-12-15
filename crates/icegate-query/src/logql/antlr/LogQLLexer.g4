// $antlr-format alignTrailingComments true, columnLimit 150, minEmptyLines 1, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine false, allowShortBlocksOnASingleLine true, alignSemicolons hanging, alignColons hanging
// LogQL ANTLR Grammar v1.1.0 - Updated for Grafana Loki compatibility

lexer grammar LogQLLexer;

channels {
    WHITESPACE,
    COMMENTS
}

// All keywords in LogQL are case insensitive, it is just function,
// label and metric names that are not.
//options {
//    caseInsensitive = true;
//}

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
SEMI       : ';' ;
PIPE       : '|' ;
DOT        : '.' ;
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
EQL : '==' ; // Double equals for metric expressions

// Pipe operators
PIPE_CONTAINS   : '|=' ;
PIPE_MATCH      : '|~' ;
PIPE_PATTERN    : '|>' ;
PIPE_NPATTERN   : '!>' ;
// Note: != and !~ are defined as NE and NRE above (lines 39, 41)
// They serve dual purpose: label matchers in selectors, line filters in pipelines

// Logical operators
AND        : 'and'    ;
OR         : 'or'     ;
UNLESS     : 'unless' ;
BOOL       : 'bool'   ;

// Named operators or keywords for grouping or vector range
BY         : 'by' ;
WITHOUT    : 'without' ;

// Vector or label transformations
KEEP          : 'keep' ;
DROP          : 'drop' ;
DECOLORIZE    : 'decolorize' ;
LABEL_REPLACE : 'label_replace' ;

// Aggregation operators
SUM          : 'sum' ;
AVG          : 'avg' ;
COUNT        : 'count' ;
MAX          : 'max' ;
MIN          : 'min' ;
STDDEV       : 'stddev' ;
STDVAR       : 'stdvar' ;
TOPK         : 'topk' ;
BOTTOMK      : 'bottomk' ;
APPROX_TOPK  : 'approx_topk' ;

// Modeled from typical LogQL pipeline usage
JSON         : 'json' ;
LOGFMT       : 'logfmt' ;
UNPACK       : 'unpack' ;
PATTERN      : 'pattern' ;
REGEXP       : 'regexp' ;
LINE_FORMAT  : 'line_format' ;
LABEL_FORMAT : 'label_format' ;
VECTOR       : 'vector' ;
OFFSET       : 'offset' ;
ON           : 'on' ;
IGNORING     : 'ignoring' ;
GROUP_LEFT   : 'group_left' ;
GROUP_RIGHT  : 'group_right' ;
UNWRAP       : 'unwrap' ;
SORT         : 'sort' ;
SORT_DESC    : 'sort_desc' ;
AT           : '@' ;

// Range operations for metric expressions
COUNT_OVER_TIME     : 'count_over_time' ;
RATE                : 'rate' ;
RATE_COUNTER        : 'rate_counter' ;
BYTES_OVER_TIME     : 'bytes_over_time' ;
BYTES_RATE          : 'bytes_rate' ;
AVG_OVER_TIME       : 'avg_over_time' ;
SUM_OVER_TIME       : 'sum_over_time' ;
MIN_OVER_TIME       : 'min_over_time' ;
MAX_OVER_TIME       : 'max_over_time' ;
STDDEV_OVER_TIME    : 'stddev_over_time' ;
STDVAR_OVER_TIME    : 'stdvar_over_time' ;
QUANTILE_OVER_TIME  : 'quantile_over_time' ;
FIRST_OVER_TIME     : 'first_over_time' ;
LAST_OVER_TIME      : 'last_over_time' ;
ABSENT_OVER_TIME    : 'absent_over_time' ;

// Duration and modulo operator
MOD         : '%' ;

LOGFMT_FLAG
    : '--strict'
    | '--keep-empty'
    ;

// Duration type tokens - must come before ATTRIBUTE to have precedence
DURATION_TYPE_MILLISECOND: 'ms';
DURATION_TYPE_MICROSECOND: 'µs' | 'us';
DURATION_TYPE_NANOSECOND: 'ns';
DURATION_TYPE_YEAR: 'y';
DURATION_TYPE_WEEK: 'w';
DURATION_TYPE_DAY: 'd';
DURATION_TYPE_HOUR: 'h';
DURATION_TYPE_MINUTE: 'm';
DURATION_TYPE_SECOND: 's';

// Numeric tokens - support scientific notation (defined before DURATION to avoid forward reference)
NUMBER :
    ( [0-9]+ ( '.' [0-9]* )? | '.' [0-9]+ ) ( [eE] [+-]? [0-9]+ )?
    | '0x' [0-9a-fA-F]+
    ;

DURATION :
    (
        (  NUMBER DURATION_TYPE_YEAR ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ( NUMBER DURATION_TYPE_MICROSECOND )? ( NUMBER DURATION_TYPE_NANOSECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? NUMBER DURATION_TYPE_WEEK ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ( NUMBER DURATION_TYPE_MICROSECOND )? ( NUMBER DURATION_TYPE_NANOSECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? NUMBER DURATION_TYPE_DAY ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ( NUMBER DURATION_TYPE_MICROSECOND )? ( NUMBER DURATION_TYPE_NANOSECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? NUMBER DURATION_TYPE_HOUR ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ( NUMBER DURATION_TYPE_MICROSECOND )? ( NUMBER DURATION_TYPE_NANOSECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? NUMBER DURATION_TYPE_MINUTE ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ( NUMBER DURATION_TYPE_MICROSECOND )? ( NUMBER DURATION_TYPE_NANOSECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? NUMBER DURATION_TYPE_SECOND ( NUMBER DURATION_TYPE_MILLISECOND )? ( NUMBER DURATION_TYPE_MICROSECOND )? ( NUMBER DURATION_TYPE_NANOSECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? NUMBER DURATION_TYPE_MILLISECOND ( NUMBER DURATION_TYPE_MICROSECOND )? ( NUMBER DURATION_TYPE_NANOSECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? NUMBER DURATION_TYPE_MICROSECOND ( NUMBER DURATION_TYPE_NANOSECOND )? ) |
        ( ( NUMBER DURATION_TYPE_YEAR )? ( NUMBER DURATION_TYPE_WEEK )? ( NUMBER DURATION_TYPE_DAY )? ( NUMBER DURATION_TYPE_HOUR )? ( NUMBER DURATION_TYPE_MINUTE )? ( NUMBER DURATION_TYPE_SECOND )? ( NUMBER DURATION_TYPE_MILLISECOND )? ( NUMBER DURATION_TYPE_MICROSECOND )? NUMBER DURATION_TYPE_NANOSECOND )
    )
    ;

// Bytes token for size units (uppercase SI and IEC units)
BYTES :
    NUMBER ( 'KB' | 'KiB' | 'MB' | 'MiB' | 'GB' | 'GiB' | 'TB' | 'TiB' | 'PB' | 'PiB' | 'EB' )
    ;

// Textual tokens
// STRING : '"' ( ~["\r\n] | '"' )* '"' ;
STRING
    : '\'' (~('\'' | '\\') | '\\' .)* '\''
    | '"' (~('"' | '\\') | '\\' .)* '"'
    | '`' (~('`'  | '\\') | '\\' .)* '`';

PREFIX: ('resource:'|'log:'|'scope:');

// Functions - must come before ATTRIBUTE to have precedence
IP : 'ip' ;

ATTRIBUTE: [a-zA-Z_] [a-zA-Z0-9_]*;
IPV4_ADDRESS : OCTET DOT OCTET DOT OCTET DOT OCTET ;
IPV6_ADDRESS : HEX_GROUP (COLON HEX_GROUP)+ (COLON COLON (HEX_GROUP (COLON HEX_GROUP)*)?)?
             | COLON COLON (HEX_GROUP (COLON HEX_GROUP)*)?
             ;

fragment OCTET : '25' [0-5]      // 250-255
              | '2' [0-4] DIGIT  // 200-249
              | '1' DIGIT DIGIT  // 100-199
              | [1-9] DIGIT      // 10-99
              | DIGIT;           // 0-9
fragment DIGIT: [0-9];
fragment HEX_GROUP: [0-9a-fA-F] [0-9a-fA-F]? [0-9a-fA-F]? [0-9a-fA-F]?;
/*------------------------------------------------------------------
 *  WHITESPACE & COMMENTS
 *------------------------------------------------------------------*/
WS         : [\r\t\n ]+   -> channel(WHITESPACE);
SL_COMMENT : '#' ~[\r\n]* -> channel(COMMENTS);
