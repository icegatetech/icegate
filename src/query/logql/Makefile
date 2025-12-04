ANTLR4_JAR = ../../../target/antlr4.jar

install:
	wget -O $(ANTLR4_JAR) https://github.com/antlr4rust/antlr4/releases/download/v0.5.0/antlr4-4.13.3-SNAPSHOT-complete.jar
	chmod +x $(ANTLR4_JAR)

gen: antlr/LogQLLexer.g4 antlr/LogQLParser.g4
	java -jar $(ANTLR4_JAR) -Dlanguage=Rust -visitor antlr/LogQLLexer.g4 antlr/LogQLParser.g4
	sed -i '' 's/LogQLParserParserContext/LogQLParserContext/g' antlr/logqlparser.rs # https://github.com/rrevenantt/antlr4rust/pull/53
	for f in antlr/logql*.rs; do sed -i '' '1s/^/#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]\n/' $$f; done

clean:
	rm antlr/logql*.rs || true
	rm antlr/*.tokens || true
	rm antlr/*.interp || true
	rm -rf antlr/gen || true
