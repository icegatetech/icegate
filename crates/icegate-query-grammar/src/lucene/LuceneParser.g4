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

parser grammar LuceneParser;

options {
    tokenVocab = LuceneLexer;
}

// Query TopLevelQuery(String field) : { q=Query(field) <EOF> }
topLevelQuery
    : query EOF
    ;

// Query Query(String field) :
//   ( LOOKAHEAD(2) firstQuery=MultiTerm(field, clauses) | mods=Modifiers() q=Clause(field) )
//   ( LOOKAHEAD(2) MultiTerm(field, clauses) | conj=Conjunction() mods=Modifiers() q=Clause(field) )*
// The clause list is flat: each clause after the first may carry a conjunction.
// Grouping adjacent bare terms (MultiTerm) and the addClause occurrences are
// applied by the visitor over this list.
query
    : modClause conjoinedClause*
    ;

conjoinedClause
    : conjunction? modClause
    ;

// int Conjunction() : [ <AND> | <OR> ]
conjunction
    : AND
    | OR
    ;

// int Modifiers() : [ <PLUS> | <MINUS> | <NOT> ]
modifier
    : PLUS
    | MINUS
    | NOT
    ;

modClause
    : modifier? clause
    ;

// Query Clause(String field) :
//   [ LOOKAHEAD(2) ( fieldToken=<TERM> <COLON> | <STAR> <COLON> ) ]
//   ( q=Term(field) | <LPAREN> q=Query(field) <RPAREN> [ <CARAT> boost=<NUMBER> ] )
clause
    : fieldName? (term | group)
    ;

fieldName
    : TERM COLON
    | STAR COLON
    ;

group
    : LPAREN query RPAREN boost?
    ;

boost
    : CARAT NUMBER
    ;

// Query Term(String field) :
//   ( term=<TERM> | term=<STAR> | term=<PREFIXTERM> | term=<WILDTERM> | term=<REGEXPTERM>
//     | term=<NUMBER> | term=<BAREOPER> )
//   [ <CARAT> boost=<NUMBER> [ fuzzySlop=<FUZZY_SLOP> ] | fuzzySlop=<FUZZY_SLOP> [ <CARAT> boost=<NUMBER> ] ]
// | ( <RANGEIN_START> | <RANGEEX_START> ) ( goop1=<RANGE_GOOP> | goop1=<RANGE_QUOTED> | goop1=<RANGE_TO> )
//   ( <RANGE_TO> ) ( goop2=<RANGE_GOOP> | goop2=<RANGE_QUOTED> | goop2=<RANGE_TO> )
//   ( <RANGEIN_END> | <RANGEEX_END> ) [ <CARAT> boost=<NUMBER> ]
// | term=<QUOTED> [ <CARAT> boost=<NUMBER> [ fuzzySlop=<FUZZY_SLOP> ] | fuzzySlop=<FUZZY_SLOP> [ <CARAT> boost=<NUMBER> ] ]
// <NUMBER> is lexed only after "^", so the term alternative <NUMBER> cannot occur and is left out.
term
    : TERM termModifiers       # plainTerm
    | STAR termModifiers       # starTerm
    | PREFIXTERM termModifiers # prefixTerm
    | WILDTERM termModifiers   # wildcardTerm
    | REGEXPTERM termModifiers # regexpTerm
    | BAREOPER termModifiers   # bareOperatorTerm
    | QUOTED termModifiers     # quotedTerm
    | range boost?             # rangeTerm
    ;

termModifiers
    : boost FUZZY_SLOP?
    | FUZZY_SLOP boost?
    |
    ;

range
    : rangeStart rangeBound RANGE_TO rangeBound rangeEnd
    ;

rangeStart
    : RANGEIN_START
    | RANGEEX_START
    ;

rangeEnd
    : RANGEIN_END
    | RANGEEX_END
    ;

rangeBound
    : RANGE_GOOP
    | RANGE_QUOTED
    | RANGE_TO
    ;
