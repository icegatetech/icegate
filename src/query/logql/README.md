## Generate ANTLR4 parser
https://github.com/rrevenantt/antlr4rust contains details about generator installation and usage.

# LogQL Implementation Status

## Executive Summary

The LogQL implementation provides a mature **Parser** covering the full LogQL grammar and a **DataFusion Planner** supporting log selection, filtering, and core metric aggregations. The **Loki HTTP API** is functional with `query_range` returning proper response formats for both log queries (streams) and metric queries (matrix).

## Detailed Breakdown

### 1. LogQL Parser (`src/query/logql/antlr`)
**Status: ✅ Mature**
- **Coverage:** Comprehensive. Supports selectors, pipeline stages, range/vector aggregations, binary operators, and arithmetic.
- **Tests:** Extensive unit tests in `tests.rs` verify AST generation for complex queries.
- **Integration:** Correctly produces `LogQLExpr` AST used by the planner.

### 2. DataFusion Planner (`src/query/logql/datafusion/planner.rs`)
**Status: ⚠️ Partial - Core Features Working**

Core log queries and metric aggregations (`rate`, `count_over_time`, `bytes_over_time`, `bytes_rate`) are fully functional. Pipeline parsers and binary operations are not yet implemented.

| Feature | Status | Implementation Details |
| :--- | :--- | :--- |
| **Log Selection** | ✅ Implemented | Scans `logs` table, applies time range filters. |
| **Label Matchers** | ✅ Implemented | Supports `=`, `!=`, `=~` (regex), `!~` (not regex). |
| **Line Filters** | ⚠️ Partial | **Implemented:** `|=` (contains), `!=` (not contains), `|~` (regex), `!~` (not regex).<br>**Missing:** `!>` (not pattern) is a no-op. IP filtering is a no-op. |
| **Pipeline Parsers** | ❌ Missing | `json`, `logfmt`, `regexp`, `pattern`, `unpack` are all placeholders (no-ops). |
| **Pipeline Formatting** | ❌ Missing | `label_format`, `line_format`, `decolorize` are placeholders (no-ops). |
| **Label Management** | ❌ Missing | `drop` and `keep` stages are placeholders (no-ops). |
| **Label Filters** | ⚠️ Partial | **Implemented:** Numeric, Duration, and Bytes comparisons (`>`, `>=`, `<`, `<=`, `=`, `!=`).<br>**Missing:** IP filtering returns `NotImplemented` error. |
| **Range Aggregation** | ⚠️ Partial | **Implemented:** `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate` with time bucketing via `date_bin()` and `offset` modifier support.<br>**Missing:** `absent_over_time`, unwrap-based aggregations (`sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, `stddev_over_time`, `stdvar_over_time`, `quantile_over_time`, `first_over_time`, `last_over_time`, `rate_counter`). |
| **Vector Aggregation** | ⚠️ Partial | **Implemented:** `sum`, `avg`, `min`, `max`, `count`.<br>**Missing:** `stddev`, `stdvar`, `bottomk`, `topk` return `NotImplemented` error. |
| **Binary Operations** | ❌ Missing | Arithmetic (`+`, `-`, `*`, `/`, etc.) and logical/set operations between vectors are not implemented. Returns `NotImplemented` error. |
| **Literals** | ✅ Implemented | Scalar literals work. Vector literals and label replace are missing. |

### 3. Loki HTTP API (`src/query/loki`)
**Status: ✅ Functional**

| Endpoint | Status | Details |
| :--- | :--- | :--- |
| `GET/POST /loki/api/v1/query_range` | ✅ Implemented | Returns `streams` format for log queries, `matrix` format for metric queries. Supports `step` parameter for time bucketing. |
| `GET/POST /loki/api/v1/query` | ✅ Implemented | Instant query endpoint. |
| `GET /loki/api/v1/explain` | ✅ Implemented | Returns logical, optimized, and physical query plans. |
| `GET /loki/api/v1/labels` | ⚠️ Stub | Returns empty array (not yet implemented). |
| `GET /loki/api/v1/label/:name/values` | ⚠️ Stub | Returns empty array (not yet implemented). |
| `GET /ready` | ✅ Implemented | Health check endpoint. |

**Response Formats:**
- **Log queries** (e.g., `{service="x"}`): Returns `resultType: "streams"` with `stream` labels and nanosecond string timestamps.
- **Metric queries** (e.g., `rate({service="x"}[5m])`): Returns `resultType: "matrix"` with `metric` labels and decimal second numeric timestamps.

### 4. User Defined Functions (UDFs) (`src/query/logql/datafusion/udf.rs`)
**Status: ❌ Missing**
- `UdfRegistry` exists but is empty.
- Critical UDFs for pipeline stages (e.g., `json_parser`, `ip_match`) are not registered or implemented.

## Recommendations

1.  **Prioritize Pipeline UDFs:** Implement `json_parser` and `logfmt_parser` UDFs to enable the most common log processing workflows.
2.  **Implement Unwrap-Based Aggregations:** Extend range aggregations to support `sum_over_time`, `avg_over_time`, etc. (requires UDF for unwrap expression).
3.  **Implement Label Endpoints:** Wire up `/labels` and `/label/:name/values` to query distinct label values from the logs table.
4.  **Binary Operations:** Implement vector matching logic for binary operations to support complex alerting rules.
