# Regenerates the committed Lucene query-string parser from the two grammars.
# The generator jar is the one `make -C crates/icegate-query/src/logql install`
# downloads; the crate README explains every step below.
ANTLR4_JAR = ../../../../target/antlr4.jar
GENERATED = lucenelexer.rs luceneparser.rs luceneparserlistener.rs luceneparservisitor.rs \
	luceneparserbaselistener.rs luceneparserbasevisitor.rs
GRAMMAR_DATA = LuceneLexer.interp LuceneLexer.tokens LuceneParser.interp LuceneParser.tokens

gen: LuceneLexer.g4 LuceneParser.g4
	java -jar $(ANTLR4_JAR) -Dlanguage=Rust -visitor LuceneLexer.g4 LuceneParser.g4
	sed -i.bak 's/LuceneParserParserContext/LuceneParserContext/g' luceneparser.rs && rm luceneparser.rs.bak
	for f in $(GENERATED); do printf '#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]\n' | cat - "$$f" > "$$f.tmp" && mv "$$f.tmp" "$$f"; done
	perl -0pi -e 's/\n*\z/\n/' $(GENERATED) $(GRAMMAR_DATA)

clean:
	rm -f $(GENERATED) $(GRAMMAR_DATA)
