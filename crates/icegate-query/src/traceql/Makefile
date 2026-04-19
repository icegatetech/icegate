ANTLR4_JAR = ../../../../target/antlr4.jar

install:
	wget -O $(ANTLR4_JAR) https://github.com/antlr4rust/antlr4/releases/download/v0.5.0/antlr4-4.13.3-SNAPSHOT-complete.jar
	chmod +x $(ANTLR4_JAR)

gen: antlr/TraceQLLexer.g4 antlr/TraceQLParser.g4
	java -jar $(ANTLR4_JAR) -Dlanguage=Rust -visitor antlr/TraceQLLexer.g4 antlr/TraceQLParser.g4
	sed -i.bak 's/TraceQLParserParserContext/TraceQLParserContext/g' antlr/traceqlparser.rs && rm antlr/traceqlparser.rs.bak # https://github.com/rrevenantt/antlr4rust/pull/53
	for f in antlr/traceql*.rs; do sed -i.bak '1s/^/#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]\n/' $$f && rm $$f.bak; done

clean:
	rm antlr/traceql*.rs || true
	rm antlr/*.tokens || true
	rm antlr/*.interp || true
	rm -rf antlr/gen || true
