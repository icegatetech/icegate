ANTLR4_JAR = ../../../../target/antlr4.jar

.PHONY: install gen clean

install:
	wget -O $(ANTLR4_JAR) https://github.com/antlr4rust/antlr4/releases/download/v0.5.0/antlr4-4.13.3-SNAPSHOT-complete.jar
	chmod +x $(ANTLR4_JAR)

gen: antlr/TraceQLLexer.g4 antlr/TraceQLParser.g4
	java -jar $(ANTLR4_JAR) -Dlanguage=Rust -visitor antlr/TraceQLLexer.g4 antlr/TraceQLParser.g4
	sed -i.bak 's/TraceQLParserParserContext/TraceQLParserContext/g' antlr/traceqlparser.rs && rm antlr/traceqlparser.rs.bak # https://github.com/rrevenantt/antlr4rust/pull/53
	for f in antlr/traceql*.rs; do printf '#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]\n' | cat - "$$f" > "$$f.tmp" && mv "$$f.tmp" "$$f"; done

clean:
	rm -f antlr/traceql*.rs
	rm -f antlr/*.tokens
	rm -f antlr/*.interp
	rm -rf antlr/gen
