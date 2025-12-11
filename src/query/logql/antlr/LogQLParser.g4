// $antlr-format alignTrailingComments true, columnLimit 150, minEmptyLines 1, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine false, allowShortBlocksOnASingleLine true, alignSemicolons hanging, alignColons hanging
// LogQL ANTLR Grammar v1.1.0 - Updated for Grafana Loki compatibility

parser grammar LogQLParser;

options {
    tokenVocab = LogQLLexer;
}

// Entry point: parse an entire LogQL expression.
root
    : expr EOF
    ;

/*------------------------------------------------------------------
 *  Combined expression that can be either the log expressions
 *  or the metric expressions
 *------------------------------------------------------------------*/
expr
    : logExpr
    | metricExpr
    ;

/*------------------------------------------------------------------
 *  LOG EXPRESSIONS
 *------------------------------------------------------------------*/
logExpr
    : selector              #logExprWithSelectorOnly
    | selector pipelineExpr #logExprWithPipeline
    ;

selector
    : LBRACE matchers RBRACE
    | LBRACE RBRACE
    ;

// List of matchers separated by commas.
matchers
    : matcher (COMMA matcher)*
    ;

// Single matcher, e.g., foo="bar", foo=~"val.*"
matcher
    : PREFIX? ATTRIBUTE EQ STRING  #matcherEq
    | PREFIX? ATTRIBUTE NE STRING  #matcherNeq
    | PREFIX? ATTRIBUTE RE STRING  #matcherRe
    | PREFIX? ATTRIBUTE NRE STRING #matcherNre
    ;

/*------------------------------------------------------------------
 * Pipeline expressions.
 *
 * For example:
 *   {foo="bar"} | json | logfmt ...
 *------------------------------------------------------------------*/
pipelineExpr
    : pipelineStage
    | pipelineExpr pipelineStage
    ;

/*------------------------------------------------------------------
 *  Pipeline Stage
 *  Expand as needed, e.g., line filters, label parsing, formatting.
 *------------------------------------------------------------------*/
pipelineStage
    : lineFilters
    | PIPE logfmtParser
    | PIPE regexpParser
    | PIPE patternParser
    | PIPE unpackParser
    | PIPE labelFormatExpr
    | PIPE lineFormatExpr
    | PIPE decolorizeExpr
    | PIPE dropExpr
    | PIPE keepExpr
    | PIPE jsonParser
    | PIPE labelFilter
    ;

/*------------------------------------------------------------------
 *  LINE FILTERS
 *  Supports: |= (contains), != (not contains), |~ (regex match), !~ (not regex match)
 *            |> (pattern match), !> (not pattern match)
 *  Note: != and !~ reuse NE/NRE tokens from label matchers
 *------------------------------------------------------------------*/
lineFilters
    : PIPE_CONTAINS lineFilter (OR lineFilter)*  #lineFiltersContains
    | NE lineFilter (OR lineFilter)*             #lineFiltersNotContains
    | PIPE_MATCH lineFilter (OR lineFilter)*     #lineFiltersMatch
    | NRE lineFilter (OR lineFilter)*            #lineFiltersNotMatch
    | PIPE_NPATTERN lineFilter (OR lineFilter)*  #lineFiltersNotPattern
    ;

lineFilter
    : STRING                    #lineFilterString
    | ipFn                      #lineFilterIp
    ;

// IP filter function - supports IPv4, IPv6, CIDR notation, and ranges (validated at runtime)
// Examples: ip("192.168.1.0/24"), ip("::1"), ip("192.168.0.1-192.168.0.255")
ipFn
    : IP LPAREN STRING RPAREN
    ;

regexpParser
    : REGEXP STRING
    ;

// Pattern parser - extracts fields using pattern syntax
// Example: | pattern "<ip> - <user>"
patternParser
    : PATTERN STRING
    ;

// Unpack parser - unpacks Promtail-packed logs
// Example: | unpack
unpackParser
    : UNPACK
    ;

/*------------------------------------------------------------------
 *  LOGFMT PARSER
 *------------------------------------------------------------------*/
logfmtParser
    : LOGFMT (LOGFMT_FLAG)* (labelExtractions)?
    ;

/*------------------------------------------------------------------
 * LINE & LABEL FORMAT
 * Templates use Go text/template syntax with runtime validation
 * Examples:
 *   | line_format "{{.field}} {{.other}}"
 *   | label_format dst=src
 *   | label_format dst="{{.status}} {{.query}}"
 *------------------------------------------------------------------*/
labelFormatExpr
    : LABEL_FORMAT labelFormatOps
    ;

labelFormatOps
    : labelFormatOp (COMMA labelFormatOp)*
    ;

labelFormatOp
    : ATTRIBUTE EQ ATTRIBUTE      #labelFormatRename
    | ATTRIBUTE EQ STRING         #labelFormatTemplate
    ;

lineFormatExpr
    : LINE_FORMAT STRING
    ;

decolorizeExpr
    : DECOLORIZE
    ;

/*------------------------------------------------------------------
 * DROP / KEEP expressions
 *------------------------------------------------------------------
*/
// https://clickhouse.com/docs/sql-reference/statements/select#dynamic-column-selection
// TODO: Loki supports all matchers (=, !=, =~, !~) for drop/keep expressions
// Example: drop level, method="GET", app=~"api.*", err!=""
// Current grammar only supports simple names and = matcher via labelExtractionExpr
dropExpr
    : DROP labelExtractions
    ;

keepExpr
    : KEEP labelExtractions
    ;

// JSON parser - extracts fields from JSON logs
// Examples: | json, | json field1, field2, | json nested="$.path.to.field"
jsonParser
    : JSON labelExtractions?
    ;

// Label extraction supports both simple field names and JSON path syntax
// Examples: field, field="value", field="$.nested.path"
labelExtractionExpr
    : ATTRIBUTE EQ STRING  #labelExtractionWithPath
    | ATTRIBUTE            #labelExtractionSimple
    ;

labelExtractions
    : labelExtractionExpr (COMMA labelExtractionExpr)*
    ;

// Label filters support logical combinations with and/or
labelFilter
    : labelFilter AND labelFilter    #labelFilterAnd
    | labelFilter OR labelFilter     #labelFilterOr
    | LPAREN labelFilter RPAREN      #labelFilterParens
    | matcher                        #labelFilterMatcher
    | numberFilter                   #labelFilterNumber
    | durationFilter                 #labelFilterDuration
    | bytesFilter                    #labelFilterBytes
    | ipLabelFilter                  #labelFilterIp
    ;

// Number filter - filters labels by numeric comparisons
// Example: | status_code > 500, | counter >= -1
numberFilter
    : ATTRIBUTE comparisonOp literalExpr
    ;

// Duration filter - filters labels by duration comparisons
// Example: | duration > 10s, | latency >= 100ms
durationFilter
    : ATTRIBUTE comparisonOp duration
    ;

// Bytes filter - filters labels by byte size comparisons
// Example: | bytes_consumed > 20MB, | size <= 1GB
bytesFilter
    : ATTRIBUTE comparisonOp BYTES
    ;

// IP label filter - filters labels by IP address patterns
// Example: | client_ip = ip("192.168.0.0/16")
ipLabelFilter
    : ATTRIBUTE EQ ipFn
    | ATTRIBUTE NE ipFn
    ;

// Comparison operators for numeric, duration, and bytes filters
comparisonOp
    : GT
    | GE
    | LT
    | LE
    | EQ
    | EQL
    | NE
    ;

/*------------------------------------------------------------------
 *  METRIC EXPRESSIONS
 *------------------------------------------------------------------*/
// Bin op is have to be inside metricExpr because of left-recurion issue.
metricExpr
    : metricExpr POW binOpModifier metricExpr               #binaryOpPow
    | metricExpr MUL binOpModifier metricExpr               #binaryOpMul
    | metricExpr DIV binOpModifier metricExpr               #binaryOpDiv
    | metricExpr MOD binOpModifier metricExpr               #binaryOpMod
    | metricExpr ADD binOpModifier metricExpr               #binaryOpAdd
    | metricExpr SUB binOpModifier metricExpr               #binaryOpSub
    | metricExpr EQL binOpModifier metricExpr               #binaryOpEql
    | metricExpr NE binOpModifier metricExpr                #binaryOpNeq
    | metricExpr GT binOpModifier metricExpr                #binaryOpGt
    | metricExpr GE binOpModifier metricExpr                #binaryOpGe
    | metricExpr LT binOpModifier metricExpr                #binaryOpLt
    | metricExpr LE binOpModifier metricExpr                #binaryOpLe
    | metricExpr AND binOpModifier metricExpr               #binaryOpAnd
    | metricExpr OR binOpModifier metricExpr                #binaryOpOr
    | metricExpr UNLESS binOpModifier metricExpr            #binaryOpUnless
    | rangeAggregationExpr                                  #metricExprRangeAgg
    | vectorAggregationExpr                                 #metricExprVectorAgg
    | literalExpr                                           #metricExprLiteral
    | labelReplaceExpr                                      #metricExprLabelReplace
    | vectorExpr                                            #metricExprVector
    | variableExpr                                          #metricExprVariable
    | LPAREN metricExpr RPAREN                              #metricExprParens
    ;

/*------------------------------------------------------------------
 *  RANGE AGGREGATION EXPRESSIONS
 *  Note: Grouping is only supported for certain unwrapped range aggregations
 *  - sum_over_time, absent_over_time, rate, rate_counter: NO grouping
 *  - avg_over_time, max_over_time, min_over_time, stddev_over_time,
 *    stdvar_over_time, quantile_over_time, first_over_time, last_over_time: grouping supported
 *------------------------------------------------------------------*/
rangeAggregationExpr
    : rangeLogOp LPAREN logRangeExpr RPAREN
    | rangeUnwrapOpNoGrouping LPAREN unwrappedRangeExpr RPAREN
    | rangeUnwrapOpWithGrouping LPAREN unwrappedRangeExpr RPAREN grouping
    | rangeUnwrapOpWithGrouping LPAREN unwrappedRangeExpr RPAREN
    | rangeUnwrapOpWithGrouping LPAREN NUMBER COMMA unwrappedRangeExpr RPAREN grouping
    | rangeUnwrapOpWithGrouping LPAREN NUMBER COMMA unwrappedRangeExpr RPAREN
    ;

rangeLogOp
    : COUNT_OVER_TIME    #rangeLogOpCount
    | RATE               #rangeLogOpRate
    | BYTES_OVER_TIME    #rangeLogOpBytes
    | BYTES_RATE         #rangeLogOpBytesRate
    | ABSENT_OVER_TIME   #rangeLogOpAbsent
    ;

// Unwrap operations that DO NOT support grouping
rangeUnwrapOpNoGrouping
    : SUM_OVER_TIME      #rangeUnwrapOpNoGroupSum
    | RATE               #rangeUnwrapOpNoGroupRate
    | RATE_COUNTER       #rangeUnwrapOpNoGroupRateCounter
    ;

// Unwrap operations that DO support grouping
rangeUnwrapOpWithGrouping
    : AVG_OVER_TIME      #rangeUnwrapOpAvg
    | MIN_OVER_TIME      #rangeUnwrapOpMin
    | MAX_OVER_TIME      #rangeUnwrapOpMax
    | STDDEV_OVER_TIME   #rangeUnwrapOpStddev
    | STDVAR_OVER_TIME   #rangeUnwrapOpStdvar
    | QUANTILE_OVER_TIME #rangeUnwrapOpQuantile
    | FIRST_OVER_TIME    #rangeUnwrapOpFirst
    | LAST_OVER_TIME     #rangeUnwrapOpLast
    ;

/*------------------------------------------------------------------
 *  VECTOR AGGREGATION EXPRESSIONS
 *------------------------------------------------------------------*/
vectorAggregationExpr
    : vectorOp LPAREN metricExpr RPAREN
    | vectorOp grouping LPAREN metricExpr RPAREN
    | vectorOp LPAREN metricExpr RPAREN grouping
    | vectorOp LPAREN NUMBER COMMA metricExpr RPAREN
    | vectorOp LPAREN NUMBER COMMA metricExpr RPAREN grouping
    | vectorOp grouping LPAREN NUMBER COMMA metricExpr RPAREN
    ;

vectorOp
    : SUM
    | AVG
    | COUNT
    | MAX
    | MIN
    | STDDEV
    | STDVAR
    | BOTTOMK
    | TOPK
    | APPROX_TOPK
    | SORT
    | SORT_DESC
    ;

// Binary operator modifiers
// Note: BOOL modifier only valid for comparison operators (==, !=, <, >, <=, >=)
// Arithmetic operators (+, -, *, /, %, ^) should not use BOOL modifier (validated at runtime)
binOpModifier
    : BOOL? (onOrIgnoringModifier ((GROUP_LEFT | GROUP_RIGHT) binOpGroupingLabels?)?)?
    ;

onOrIgnoringModifier
    : IGNORING binOpGroupingLabels
    | ON binOpGroupingLabels
    ;

/*------------------------------------------------------------------
 *  GROUPING EXPRESSIONS
 *------------------------------------------------------------------*/
grouping
    : BY LPAREN groupingLabels RPAREN                                              #groupingBy
    | WITHOUT LPAREN groupingLabels RPAREN                                         #groupingWithout
    | BY LPAREN RPAREN                                                     #groupingByEmpty
    | WITHOUT LPAREN RPAREN                                                #groupingWithoutEmpty
    ;

binOpGroupingLabels
    : LPAREN groupingLabelList RPAREN
    | LPAREN groupingLabelList COMMA RPAREN
    | LPAREN RPAREN
    ;

groupingLabelList
    : groupingLabelList COMMA groupingLabel
    | groupingLabel
    ;

groupingLabel
    : PREFIX? ATTRIBUTE
    ;

groupingLabels
    : groupingLabel (COMMA groupingLabel)*
    ;

/*------------------------------------------------------------------
 *  LOG RANGE EXPRESSIONS
 *  Supports optional @ modifier for instant-time evaluation
 *  Examples: {job="foo"}[5m], {job="foo"}[5m] offset 1h, {job="foo"}[5m] @ 1234567890
 *------------------------------------------------------------------*/
logRangeExpr
    : selector range atModifier?
    | selector range offsetExpr atModifier?
    | LPAREN selector RPAREN range atModifier?
    | LPAREN selector RPAREN range offsetExpr atModifier?
    | selector pipelineExpr range atModifier?
    | selector pipelineExpr range offsetExpr atModifier?
    | LPAREN selector pipelineExpr RPAREN range atModifier?
    | LPAREN selector pipelineExpr RPAREN range offsetExpr atModifier?
    | selector range pipelineExpr atModifier?
    | selector range offsetExpr pipelineExpr atModifier?
    | LPAREN logRangeExpr RPAREN
    ;

unwrappedRangeExpr
    : selector range unwrapExpr atModifier?
    | selector range offsetExpr unwrapExpr atModifier?
    | LPAREN selector RPAREN range unwrapExpr atModifier?
    | LPAREN selector RPAREN range offsetExpr unwrapExpr atModifier?
    | selector unwrapExpr range atModifier?
    | selector unwrapExpr range offsetExpr atModifier?
    | LPAREN selector unwrapExpr RPAREN range atModifier?
    | LPAREN selector unwrapExpr RPAREN range offsetExpr atModifier?
    | selector range pipelineExpr unwrapExpr atModifier?
    | selector pipelineExpr unwrapExpr range atModifier?
    | selector range offsetExpr pipelineExpr unwrapExpr atModifier?
    ;

/*------------------------------------------------------------------
 *  RANGE AND OFFSET
 *------------------------------------------------------------------*/
range
    : LBRACK duration RBRACK
    ;

offsetExpr
    : OFFSET duration
    ;

// @ modifier for instant-time evaluation (PromQL-compatible)
// Examples: @ 1234567890, @ start(), @ end()
atModifier
    : AT NUMBER
    | AT SUB NUMBER  // Negative timestamps
    ;

/*------------------------------------------------------------------
 *  UNWRAP EXPRESSIONS
 *  Note: Conversion function names (bytes, duration, duration_seconds) are parsed
 *  as ATTRIBUTE to avoid keyword conflicts with field names. Validation that the
 *  conversion function name is valid should be done at runtime/transpilation.
 *------------------------------------------------------------------*/
unwrapExpr
    : PIPE UNWRAP ATTRIBUTE                                                #unwrapBasic
    | PIPE UNWRAP ATTRIBUTE LPAREN ATTRIBUTE RPAREN                        #unwrapWithConversion
    | unwrapExpr PIPE labelFilter                                          #unwrapWithFilter
    ;

/*------------------------------------------------------------------
 *  LITERAL EXPRESSIONS
 *------------------------------------------------------------------*/
literalExpr
    : NUMBER                                                                #literalNumber
    | ADD NUMBER                                                            #literalPositiveNumber
    | SUB NUMBER                                                            #literalNegativeNumber
    ;

/*------------------------------------------------------------------
 *  LABEL REPLACE EXPRESSIONS
 *------------------------------------------------------------------*/
labelReplaceExpr
    : LABEL_REPLACE LPAREN metricExpr COMMA STRING COMMA STRING COMMA STRING COMMA STRING RPAREN
    ;

/*------------------------------------------------------------------
 *  VECTOR EXPRESSIONS
 *------------------------------------------------------------------*/
vectorExpr
    : VECTOR LPAREN NUMBER RPAREN
    ;

/*------------------------------------------------------------------
 *  VARIABLE EXPRESSIONS
 *------------------------------------------------------------------*/
variableExpr
    : ATTRIBUTE
    ;

/*------------------------------------------------------------------
 *  DURATION AND TIME EXPRESSIONS
 *  Note: Negative durations are allowed in grammar but may be rejected at runtime
 *------------------------------------------------------------------*/
duration: SUB? DURATION;
