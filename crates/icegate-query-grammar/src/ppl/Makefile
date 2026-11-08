# Vendors and regenerates the PPL parser. UPSTREAM_COMMIT is the commit of the tag the crate README names; the README states the recipe.
ANTLR4_JAR = ../../../../target/antlr4.jar
UPSTREAM_COMMIT = 6bada7629b20aa21b56b08454a7edbc375cd4029
UPSTREAM_RAW = https://raw.githubusercontent.com/opensearch-project/sql/$(UPSTREAM_COMMIT)
GENERATED_RS = opensearchppllexer.rs opensearchpplparser.rs opensearchpplparserlistener.rs \
	opensearchpplparservisitor.rs opensearchpplparserbaselistener.rs opensearchpplparserbasevisitor.rs
GENERATED_DATA = OpenSearchPPLLexer.interp OpenSearchPPLLexer.tokens OpenSearchPPLParser.interp \
	OpenSearchPPLParser.tokens
GENERATED_ALLOW = '\#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]\n'

.PHONY: vendor gen clean

vendor:
	curl -sSfL -o OpenSearchPPLLexer.g4 $(UPSTREAM_RAW)/ppl/src/main/antlr/OpenSearchPPLLexer.g4
	curl -sSfL -o OpenSearchPPLParser.g4 $(UPSTREAM_RAW)/ppl/src/main/antlr/OpenSearchPPLParser.g4
	curl -sSfL -o LICENSE.txt $(UPSTREAM_RAW)/LICENSE.txt
	curl -sSfL -o NOTICE $(UPSTREAM_RAW)/NOTICE
	perl -0pi -e 's/\n*\z/\n/' OpenSearchPPLLexer.g4 OpenSearchPPLParser.g4 LICENSE.txt NOTICE

gen: OpenSearchPPLLexer.g4 OpenSearchPPLParser.g4
	java -jar $(ANTLR4_JAR) -Dlanguage=Rust -visitor OpenSearchPPLLexer.g4 OpenSearchPPLParser.g4
	sed -i.bak 's/OpenSearchPPLParserParserContext/OpenSearchPPLParserContext/g' opensearchpplparser.rs
	rm opensearchpplparser.rs.bak
	for f in $(GENERATED_RS); do printf $(GENERATED_ALLOW) | cat - "$$f" > "$$f.tmp" && mv "$$f.tmp" "$$f"; done
	perl -0pi -e 's/\n*\z/\n/' $(GENERATED_RS) $(GENERATED_DATA)

clean:
	rm -f $(GENERATED_RS) $(GENERATED_DATA)
