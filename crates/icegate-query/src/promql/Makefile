ANTLR4_JAR = ../../../../target/antlr4.jar

install:
	wget -O $(ANTLR4_JAR) https://github.com/antlr4rust/antlr4/releases/download/v0.5.0/antlr4-4.13.3-SNAPSHOT-complete.jar
	chmod +x $(ANTLR4_JAR)

gen: antlr/PromQLLexer.g4 antlr/PromQLParser.g4
	java -jar $(ANTLR4_JAR) -Dlanguage=Rust -visitor antlr/PromQLLexer.g4 antlr/PromQLParser.g4
	sed -i.bak 's/PromQLParserParserContext/PromQLParserContext/g' antlr/promqlparser.rs && rm antlr/promqlparser.rs.bak
	for f in antlr/promql*.rs; do sed -i.bak '1s/^/#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]\n/' $$f && rm $$f.bak; done

clean:
	rm antlr/promql*.rs || true
	rm antlr/*.tokens || true
	rm antlr/*.interp || true
	rm -rf antlr/gen || true
