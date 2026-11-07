/*
 * Copyright (c) 2022 by Bart Kiers
 *
 * The MIT license.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * Project      : An ANTLR 4 grammar for Lucene
 * Developed by : Bart Kiers, bart@big-o.nl
 */

/*
 * Adjusted for IceGate to the classic query parser of Apache Lucene 10.5.1
 * (lucene/queryparser/src/java/org/apache/lucene/queryparser/classic/QueryParser.jj),
 * Copyright The Apache Software Foundation, licensed under the Apache License,
 * Version 2.0. The JavaCC rules each ANTLR rule transcribes are quoted in the
 * comment above it. The list of adjustments lives in the crate README.
 */

lexer grammar LuceneLexer;

// <#_WHITESPACE: ( " " | "\t" | "\n" | "\r" | "　") >
// <DEFAULT, Range> SKIP : { < <_WHITESPACE>> }
DEFAULT_SKIP: WHITESPACE -> skip;

// <AND: ("AND" | "&&") >
AND: 'AND' | '&&';

// <OR: ("OR" | "||") >
OR: 'OR' | '||';

// <NOT: ("NOT" | "!") >
NOT: 'NOT' | '!';

// <BAREOPER: ("+"|"-"|"!") <_WHITESPACE> >
BAREOPER: [+\-!] WHITESPACE;

// <PLUS: "+" >
PLUS: '+';

// <MINUS: "-" >
MINUS: '-';

// <LPAREN: "(" >
LPAREN: '(';

// <RPAREN: ")" >
RPAREN: ')';

// <COLON: ":" >
COLON: ':';

// <STAR: "*" >
STAR: '*';

// <CARAT: "^" > : Boost
CARAT: '^' -> mode(BOOST);

// <QUOTED: "\"" (<_QUOTED_CHAR>)* "\"">
QUOTED: '"' QUOTED_CHAR* '"';

// <TERM: <_TERM_START_CHAR> (<_TERM_CHAR>)* >
TERM: TERM_START_CHAR TERM_CHAR*;

// <FUZZY_SLOP: "~" ((<_NUM_CHAR>)+ (( "." (<_NUM_CHAR>)+ )? (<_TERM_CHAR>)*) | (<_TERM_CHAR>)*) >
FUZZY_SLOP: '~' (NUM_CHAR+ ('.' NUM_CHAR+)? TERM_CHAR* | TERM_CHAR*);

// <PREFIXTERM: ("*") | ( <_TERM_START_CHAR> (<_TERM_CHAR>)* "*" ) >
// The lone "*" alternative is removed: STAR, defined first, always wins it.
PREFIXTERM: TERM_START_CHAR TERM_CHAR* '*';

// <WILDTERM: (<_TERM_START_CHAR> | [ "*", "?" ]) (<_TERM_CHAR> | ( [ "*", "?" ] ))* >
WILDTERM: (TERM_START_CHAR | [*?]) (TERM_CHAR | [*?])*;

// <REGEXPTERM: "/" (~[ "/" ] | "\\/" )* "/" >
REGEXPTERM: '/' (~'/' | '\\/')* '/';

// <RANGEIN_START: "[" > : Range
RANGEIN_START: '[' -> mode(RANGE);

// <RANGEEX_START: "{" > : Range
RANGEEX_START: '{' -> mode(RANGE);

// <#_NUM_CHAR: ["0"-"9"] >
fragment NUM_CHAR: [0-9];

// <#_ESCAPED_CHAR: "\\" ~[] >
fragment ESCAPED_CHAR: '\\' .;

// <#_TERM_START_CHAR: ( ~[ " ", "\t", "\n", "\r", "　", "+", "-", "!", "(", ")", ":", "^",
//                          "[", "]", "\"", "{", "}", "~", "*", "?", "\\", "/" ]
//                       | <_ESCAPED_CHAR> ) >
fragment TERM_START_CHAR: ~[ \t\n\r　+\-!():^[\]"{}~*?\\/] | ESCAPED_CHAR;

// <#_TERM_CHAR: ( <_TERM_START_CHAR> | "-" | "+" ) >
fragment TERM_CHAR: TERM_START_CHAR | [\-+];

// <#_WHITESPACE: ( " " | "\t" | "\n" | "\r" | "　") >
fragment WHITESPACE: [ \t\n\r　];

// <#_QUOTED_CHAR: ( ~[ "\"", "\\" ] | <_ESCAPED_CHAR> ) >
fragment QUOTED_CHAR: ~["\\] | ESCAPED_CHAR;

// <Boost> TOKEN : { <NUMBER: (<_NUM_CHAR>)+ ( "." (<_NUM_CHAR>)+ )? > : DEFAULT }
// The Boost state skips no whitespace: anything but a number after "^" is a lexer error.
mode BOOST;

NUMBER: NUM_CHAR+ ('.' NUM_CHAR+)? -> mode(DEFAULT_MODE);

// <Range> TOKEN : { ... }
mode RANGE;

// <DEFAULT, Range> SKIP : { < <_WHITESPACE>> }
RANGE_SKIP: WHITESPACE -> skip;

// <RANGE_TO: "TO">
RANGE_TO: 'TO';

// <RANGEIN_END: "]"> : DEFAULT
RANGEIN_END: ']' -> mode(DEFAULT_MODE);

// <RANGEEX_END: "}"> : DEFAULT
RANGEEX_END: '}' -> mode(DEFAULT_MODE);

// <RANGE_QUOTED: "\"" (~["\""] | "\\\"")+ "\"">
RANGE_QUOTED: '"' (~'"' | '\\"')+ '"';

// <RANGE_GOOP: (~[ " ", "]", "}" ])+ >
RANGE_GOOP: ~[ \]}]+;
