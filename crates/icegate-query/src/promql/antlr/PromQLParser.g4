// PromQL ANTLR Grammar v1.0.0 - Prometheus Query Language
// Adapted from grammars-v4/promql for antlr4rust compatibility

parser grammar PromQLParser;

options {
    tokenVocab = PromQLLexer;
}

// Entry point: parse an entire PromQL expression.
root
    : expr EOF
    ;

/*------------------------------------------------------------------
 *  EXPRESSIONS
 *  Operator precedence (highest to lowest):
 *  1. ^ (right-associative)
 *  2. *, /, %
 *  3. +, -
 *  4. ==, !=, <=, <, >=, >
 *  5. and, unless
 *  6. or
 *------------------------------------------------------------------*/
expr
    : <assoc=right> expr POW binOpModifier expr                          #exprPow
    | expr mulDivModOp binOpModifier expr                                #exprMulDivMod
    | expr addSubOp binOpModifier expr                                   #exprAddSub
    | expr compareOp binOpModifier expr                                  #exprCompare
    | expr andUnlessOp binOpModifier expr                                #exprAndUnless
    | expr OR binOpModifier expr                                         #exprOr
    | expr LBRACK duration COLON duration? RBRACK offsetModifier? atModifier?  #exprSubquery
    | unaryExpr                                                          #exprUnary
    | vector                                                             #exprVector
    ;

/*------------------------------------------------------------------
 *  VECTOR (primary expressions)
 *------------------------------------------------------------------*/
vector
    : functionCall                                   #vectorFunction
    | aggregation                                    #vectorAggregation
    | matrixSelector                                 #vectorMatrix
    | instantSelector                                #vectorInstant
    | numberLiteral                                  #vectorNumber
    | stringLiteral                                  #vectorString
    | parenExpr                                      #vectorParen
    ;

parenExpr
    : LPAREN expr RPAREN
    ;

/*------------------------------------------------------------------
 *  UNARY EXPRESSION
 *------------------------------------------------------------------*/
unaryExpr
    : unaryOp expr
    ;

unaryOp
    : ADD
    | SUB
    ;

/*------------------------------------------------------------------
 *  OPERATOR GROUPS (for precedence alternatives)
 *------------------------------------------------------------------*/
mulDivModOp
    : MUL
    | DIV
    | MOD
    ;

addSubOp
    : ADD
    | SUB
    ;

compareOp
    : EQL
    | NE
    | GT
    | LT
    | GE
    | LE
    ;

andUnlessOp
    : AND
    | UNLESS
    ;

/*------------------------------------------------------------------
 *  BINARY OPERATOR MODIFIER
 *  bool? (on|ignoring (group_left|group_right)?)?
 *------------------------------------------------------------------*/
binOpModifier
    : BOOL? (onOrIgnoring (groupModifier)?)?
    ;

onOrIgnoring
    : ON LPAREN labelNameList? RPAREN
    | IGNORING LPAREN labelNameList? RPAREN
    ;

groupModifier
    : GROUP_LEFT (LPAREN labelNameList? RPAREN)?
    | GROUP_RIGHT (LPAREN labelNameList? RPAREN)?
    ;

/*------------------------------------------------------------------
 *  SELECTORS
 *------------------------------------------------------------------*/
instantSelector
    : IDENTIFIER LBRACE labelMatchers RBRACE offsetModifier? atModifier?
    | IDENTIFIER LBRACE RBRACE offsetModifier? atModifier?
    | IDENTIFIER offsetModifier? atModifier?
    | LBRACE labelMatchers RBRACE offsetModifier? atModifier?
    ;

matrixSelector
    : IDENTIFIER LBRACE labelMatchers RBRACE timeRange offsetModifier? atModifier?
    | IDENTIFIER LBRACE RBRACE timeRange offsetModifier? atModifier?
    | IDENTIFIER timeRange offsetModifier? atModifier?
    | LBRACE labelMatchers RBRACE timeRange offsetModifier? atModifier?
    ;

/*------------------------------------------------------------------
 *  LABEL MATCHERS
 *------------------------------------------------------------------*/
labelMatchers
    : labelMatcher (COMMA labelMatcher)* COMMA?
    ;

labelMatcher
    : labelName labelMatcherOp STRING
    ;

labelMatcherOp
    : EQ
    | NE
    | RE
    | NRE
    ;

/*------------------------------------------------------------------
 *  FUNCTION CALLS
 *------------------------------------------------------------------*/
functionCall
    : functionName LPAREN (expr (COMMA expr)*)? RPAREN
    ;

functionName
    : rangeFunctionName
    | mathFunctionName
    | trigFunctionName
    | labelFunctionName
    | timeFunctionName
    | histogramFunctionName
    | ABSENT
    | SCALAR
    | VECTOR
    | SORT
    | SORT_DESC
    | PI
    ;

rangeFunctionName
    : RATE
    | IRATE
    | INCREASE
    | DELTA
    | IDELTA
    | DERIV
    | CHANGES
    | RESETS
    | PREDICT_LINEAR
    | HOLT_WINTERS
    | AVG_OVER_TIME
    | MIN_OVER_TIME
    | MAX_OVER_TIME
    | SUM_OVER_TIME
    | COUNT_OVER_TIME
    | QUANTILE_OVER_TIME
    | STDDEV_OVER_TIME
    | STDVAR_OVER_TIME
    | LAST_OVER_TIME
    | PRESENT_OVER_TIME
    | ABSENT_OVER_TIME
    ;

mathFunctionName
    : ABS
    | CEIL
    | CLAMP
    | CLAMP_MAX
    | CLAMP_MIN
    | EXP
    | FLOOR
    | LN
    | LOG2
    | LOG10
    | ROUND
    | SGN
    | SQRT
    ;

trigFunctionName
    : ACOS
    | ACOSH
    | ASIN
    | ASINH
    | ATAN
    | ATANH
    | COS
    | COSH
    | SIN
    | SINH
    | TAN
    | TANH
    | DEG
    | RAD
    ;

labelFunctionName
    : LABEL_JOIN
    | LABEL_REPLACE
    ;

timeFunctionName
    : TIME
    | TIMESTAMP
    | DAY_OF_MONTH
    | DAY_OF_WEEK
    | DAY_OF_YEAR
    | DAYS_IN_MONTH
    | HOUR
    | MINUTE
    | MONTH
    | YEAR
    ;

histogramFunctionName
    : HISTOGRAM_COUNT
    | HISTOGRAM_SUM
    | HISTOGRAM_FRACTION
    | HISTOGRAM_QUANTILE
    ;

/*------------------------------------------------------------------
 *  AGGREGATIONS
 *------------------------------------------------------------------*/
aggregation
    : aggregationOp LPAREN (expr (COMMA expr)*)? RPAREN grouping?
    | aggregationOp grouping LPAREN (expr (COMMA expr)*)? RPAREN
    ;

aggregationOp
    : AGG_SUM
    | AGG_MIN
    | AGG_MAX
    | AGG_AVG
    | AGG_GROUP
    | AGG_STDDEV
    | AGG_STDVAR
    | AGG_COUNT
    | AGG_COUNT_VALUES
    | AGG_BOTTOMK
    | AGG_TOPK
    | AGG_QUANTILE
    ;

grouping
    : BY LPAREN labelNameList? RPAREN
    | WITHOUT LPAREN labelNameList? RPAREN
    ;

/*------------------------------------------------------------------
 *  MODIFIERS
 *------------------------------------------------------------------*/
timeRange
    : LBRACK duration RBRACK
    ;

offsetModifier
    : OFFSET duration
    ;

atModifier
    : AT NUMBER
    | AT SUB NUMBER
    | AT START LPAREN RPAREN
    | AT END LPAREN RPAREN
    ;

/*------------------------------------------------------------------
 *  LABEL NAMES
 *------------------------------------------------------------------*/
labelName
    : IDENTIFIER
    | keyword
    ;

labelNameList
    : labelName (COMMA labelName)*
    ;

keyword
    : AND
    | OR
    | UNLESS
    | BY
    | WITHOUT
    | ON
    | IGNORING
    | GROUP_LEFT
    | GROUP_RIGHT
    | OFFSET
    | BOOL
    | START
    | END
    | aggregationOp
    ;

/*------------------------------------------------------------------
 *  LITERALS
 *------------------------------------------------------------------*/
numberLiteral
    : NUMBER
    | ADD NUMBER
    | SUB NUMBER
    ;

stringLiteral
    : STRING
    ;

/*------------------------------------------------------------------
 *  DURATION
 *------------------------------------------------------------------*/
duration
    : SUB? DURATION
    ;
