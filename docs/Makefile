# IceGate Documentation Generator
# Uses Homebrew-installed structurizr-cli: brew install structurizr-cli

WORKSPACE := workspace.dsl
OUTPUT_DIR := _site

# Directories (relative to docs/)
ADR_DIR := decisions
DOCS_DIR := components
ADR_TEMPLATE := $(ADR_DIR)/template.md

.PHONY: all clean mermaid html plantuml json help install-cli serve validate lite

help:
	@echo "IceGate Architecture Documentation"
	@echo ""
	@echo "Prerequisites:"
	@echo "  brew install structurizr-cli"
	@echo "  docker (for Structurizr Lite)"
	@echo ""
	@echo "Full UI (with ADRs and documentation):"
	@echo "  make lite      - Run Structurizr Lite at http://localhost:8080"
	@echo ""
	@echo "Static Export (diagrams only):"
	@echo "  make html      - Generate static HTML site"
	@echo "  make serve     - Generate HTML and start local server"
	@echo "  make mermaid   - Export diagrams as Mermaid files"
	@echo "  make plantuml  - Export diagrams as PlantUML files"
	@echo "  make json      - Export workspace as JSON"
	@echo ""
	@echo "Utilities:"
	@echo "  make validate  - Validate workspace DSL"
	@echo "  make all       - Export all formats"
	@echo "  make clean     - Remove generated files"

check:
	@command -v structurizr-cli >/dev/null 2>&1 || { \
		echo "Error: structurizr-cli not found. Install with: brew install structurizr-cli"; \
		exit 1; \
	}
	@echo "structurizr-cli found: $$(which structurizr-cli)"

install-cli:
	brew install structurizr-cli

$(OUTPUT_DIR):
	mkdir -p $(OUTPUT_DIR)

html: check $(OUTPUT_DIR)
	structurizr-cli export -workspace $(WORKSPACE) -format static -output $(OUTPUT_DIR)/html
	@echo "Interactive HTML site generated at $(OUTPUT_DIR)/html/"
	@echo "Open $(OUTPUT_DIR)/html/index.html in a browser"

serve: html
	@echo "Starting local server at http://localhost:8000"
	@cd $(OUTPUT_DIR)/html && python3 -m http.server 8000

mermaid: check $(OUTPUT_DIR)
	structurizr-cli export -workspace $(WORKSPACE) -format mermaid -output $(OUTPUT_DIR)
	@echo "Mermaid diagrams exported to $(OUTPUT_DIR)/"

plantuml: check $(OUTPUT_DIR)
	structurizr-cli export -workspace $(WORKSPACE) -format plantuml -output $(OUTPUT_DIR)
	@echo "PlantUML diagrams exported to $(OUTPUT_DIR)/"
	@echo "Convert to PNG: plantuml $(OUTPUT_DIR)/*.puml"
	@echo "Convert to SVG: plantuml -tsvg $(OUTPUT_DIR)/*.puml"

json: check $(OUTPUT_DIR)
	structurizr-cli export -workspace $(WORKSPACE) -format json -output $(OUTPUT_DIR)
	@echo "JSON exported to $(OUTPUT_DIR)/workspace.json"

validate: check
	@echo "Validating workspace..."
	@structurizr-cli validate -workspace $(WORKSPACE) && echo "✓ Workspace is valid" || true

lite:
	@command -v docker >/dev/null 2>&1 || { \
		echo "Error: docker not found. Install Docker Desktop first."; \
		exit 1; \
	}
	@echo "Starting Structurizr Lite..."
	@echo "Open http://localhost:8080 in your browser"
	@echo "Press Ctrl+C to stop"
	@docker run -it --rm -p 8080:8080 -v "$$(pwd):/usr/local/structurizr" structurizr/lite

all: html mermaid

clean:
	rm -rf $(OUTPUT_DIR)
