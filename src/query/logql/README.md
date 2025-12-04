## Generate ANTLR4 parser
https://github.com/rrevenantt/antlr4rust contains details about generator installation and usage.

# LogQL Implementation Status

## Executive Summary

The LogQL implementation is in a transitional state. The **Parser** is mature and covers the full LogQL grammar. The **DataFusion Planner** is partially implemented, supporting basic log selection and filtering, but lacking support for most metric aggregations, binary operations, and complex pipeline stages.

## Detailed Breakdown

### 1. LogQL Parser (`src/query/logql/antlr`)
**Status: ✅ Mature**
- **Coverage:** Comprehensive. Supports selectors, pipeline stages, range/vector aggregations, binary operators, and arithmetic.
- **Tests:** Extensive unit tests in `tests.rs` verify AST generation for complex queries.
- **Integration:** Correctly produces `LogQLExpr` AST used by the planner.

### 2. DataFusion Planner (`src/query/logql/datafusion/planner.rs`)
**Status: 🚧 Partial / In Progress**

| Feature | Status | Implementation Details |
| :--- | :--- | :--- |
| **Log Selection** | ✅ Implemented | Scans `logs` table, applies time range filters. |
| **Label Matchers** | ✅ Implemented | Supports `=`, `!=`, `=~` (regex), `!~` (not regex). |
| **Line Filters** | ⚠️ Partial | **Implemented:** `|=` (contains), `!=` (not contains), `|~` (regex), `!~` (not regex).<br>**Missing:** `!>` (not pattern) is a no-op. IP filtering is a no-op. |
| **Pipeline Parsers** | ❌ Missing | `json`, `logfmt`, `regexp`, `pattern`, `unpack` are all placeholders (no-ops). |
| **Pipeline Formatting** | ❌ Missing | `label_format`, `line_format`, `decolorize` are placeholders (no-ops). |
| **Label Management** | ❌ Missing | `drop` and `keep` stages are placeholders (no-ops). |
| **Label Filters** | ⚠️ Partial | **Implemented:** Numeric, Duration, and Bytes comparisons (`>`, `>=`, `<`, `<=`, `=`, `!=`).<br>**Missing:** IP filtering returns `NotImplemented` error. |
| **Range Aggregation** | ❌ Missing | `rate`, `count_over_time`, `bytes_over_time`, etc., are not mapped to DataFusion functions. Currently returns the raw log stream plan. |
| **Vector Aggregation** | ⚠️ Partial | **Implemented:** `sum`, `avg`, `min`, `max`, `count`.<br>**Missing:** `stddev`, `stdvar`, `bottomk`, `topk` return `NotImplemented` error. |
| **Binary Operations** | ❌ Missing | Arithmetic (`+`, `-`, `*`, `/`, etc.) and logical/set operations between vectors are not implemented. Returns `NotImplemented` error. |
| **Literals** | ✅ Implemented | Scalar literals work. Vector literals and label replace are missing. |


### 3. User Defined Functions (UDFs) (`src/query/logql/datafusion/udf.rs`)
**Status: ❌ Missing**
- `UdfRegistry` exists but is empty.
- Critical UDFs for pipeline stages (e.g., `json_parser`, `ip_match`) are not registered or implemented.

## Recommendations

1.  **Prioritize Pipeline UDFs:** Implement `json_parser` and `logfmt_parser` UDFs to enable the most common log processing workflows.
2.  **Implement Range Aggregations:** The planner needs to map `rate` and `count_over_time` to DataFusion window functions or custom accumulators.
3.  **Enable API Endpoints:** Once the planner can handle basic queries (even if incomplete), wire up the `query` and `query_range` endpoints to execute the plan.
4.  **Binary Operations:** Implement vector matching logic for binary operations to support complex alerting rules.
