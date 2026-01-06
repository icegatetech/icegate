## Generate ANTLR4 parser
https://github.com/rrevenantt/antlr4rust contains details about generator installation and usage.

# LogQL Implementation Status

## Executive Summary

The LogQL implementation provides a mature **Parser** covering the full LogQL grammar and a **DataFusion Planner** supporting log selection, filtering, and core metric aggregations. The **Loki HTTP API** is functional with `query_range`, metadata endpoints (`labels`, `label_values`, `series`), returning proper response formats for both log queries (streams) and metric queries (matrix).

## Detailed Breakdown

### 1. LogQL Parser (`src/query/logql/antlr`)
**Status: ✅ Mature**
- **Coverage:** Comprehensive. Supports selectors, pipeline stages, range/vector aggregations, binary operators, and arithmetic.
- **Tests:** Extensive unit tests in `tests.rs` verify AST generation for complex queries.
- **Integration:** Correctly produces `LogQLExpr` AST used by the planner.

### 2. DataFusion Planner (`src/query/logql/datafusion/planner.rs`)
**Status: ⚠️ Partial - Core Features Working**

Core log queries and metric aggregations (`rate`, `count_over_time`, `bytes_over_time`, `bytes_rate`, `absent_over_time`) are fully functional. Pipeline parsers and binary operations are not yet implemented.

| Feature | Status | Implementation Details |
| :--- | :--- | :--- |
| **Log Selection** | ✅ Implemented | Scans `logs` table, applies time range filters. |
| **Label Matchers** | ✅ Implemented | Supports `=`, `!=`, `=~` (regex), `!~` (not regex). |
| **Line Filters** | ⚠️ Partial | **Implemented:** `|=` (contains), `!=` (not contains), `|~` (regex), `!~` (not regex).<br>**Missing:** `!>` (not pattern) returns `NotImplemented` error. IP filtering returns `NotImplemented` error. |
| **Pipeline Parsers** | ❌ Missing | `json`, `logfmt`, `regexp`, `pattern`, `unpack` are all placeholders (no-ops). |
| **Pipeline Formatting** | ❌ Missing | `label_format`, `line_format`, `decolorize` are placeholders (no-ops). |
| **Label Management** | ⚠️ Partial | **Implemented:** `drop` and `keep` with simple names and `=` matcher via UDFs.<br>**Missing:** `!=`, `=~`, `!~` matchers for drop/keep expressions. |
| **Label Filters** | ⚠️ Partial | **Implemented:** Numeric, Duration, and Bytes comparisons (`>`, `>=`, `<`, `<=`, `=`, `!=`).<br>**Missing:** IP filtering returns `NotImplemented` error. |
| **Log-Range Aggregation** | ✅ Implemented | `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`, `absent_over_time` with grid-based time bucketing and `offset` modifier support. |
| **Unwrap Aggregation** | ✅ Implemented | All 10 unwrap aggregations implemented: `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, `stddev_over_time`, `stdvar_over_time`, `quantile_over_time`, `first_over_time`, `last_over_time`, `rate_counter`. Supports `duration()` and `bytes()` conversion functions. **Note:** `rate_counter` includes Prometheus-compatible counter reset detection. |
| **Vector Aggregation** | ⚠️ Partial | **Implemented:** `sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`.<br>**Missing:** `bottomk`, `topk`, `sort`, `sort_desc` return `NotImplemented` error. |
| **Binary Operations** | ❌ Missing | Arithmetic (`+`, `-`, `*`, `/`, etc.) and logical/set operations between vectors are not implemented. Returns `NotImplemented` error. |
| **Literals** | ✅ Implemented | Scalar literals work. Vector literals and label replace are missing. |

### 3. Loki HTTP API (`src/query/loki`)
**Status: ✅ Functional**

| Endpoint | Status | Details |
| :--- | :--- | :--- |
| `GET/POST /loki/api/v1/query_range` | ✅ Implemented | Returns `streams` format for log queries, `matrix` format for metric queries. Supports `step` parameter for time bucketing. |
| `GET/POST /loki/api/v1/query` | ⚠️ Stub | Returns `501 Not Implemented`. Requires `time` parameter and metric-only queries per Loki spec. |
| `GET /loki/api/v1/explain` | ✅ Implemented | Returns logical, optimized, and physical query plans. |
| `GET /loki/api/v1/labels` | ✅ Implemented | Returns distinct label names from indexed columns and attributes MAP. |
| `GET /loki/api/v1/label/:name/values` | ✅ Implemented | Returns distinct values for a specific label. |
| `GET/POST /loki/api/v1/series` | ✅ Implemented | Returns unique label combinations matching selector(s). |
| `GET /ready` | ✅ Implemented | Health check endpoint. |

**Response Formats:**
- **Log queries** (e.g., `{service="x"}`): Returns `resultType: "streams"` with `stream` labels and nanosecond string timestamps.
- **Metric queries** (e.g., `rate({service="x"}[5m])`): Returns `resultType: "matrix"` with `metric` labels and decimal second numeric timestamps.

### 4. User Defined Aggregate Functions (UDAFs) (`src/query/logql/datafusion/udaf.rs`)
**Status: ✅ Implemented**

All log-range aggregations use a shared `GridAccumulator` that provides:
- Time grid generation from start/end/step parameters
- Vectorized grid-major updates (processes all timestamps per grid point)
- State serialization for distributed execution
- Merge logic for combining partial results

| UDAF | Description |
| :--- | :--- |
| `count_over_time` | Counts log entries per time bucket |
| `rate` | Log entry rate (count / range_seconds) |
| `bytes_over_time` | Sums byte lengths of log bodies per bucket |
| `bytes_rate` | Byte throughput rate (bytes / range_seconds) |
| `absent_over_time` | Returns 1 for time ranges with no samples |

### 5. User Defined Functions (UDFs) (`src/query/logql/datafusion/udf.rs`)
**Status: ⚠️ Partial**
- **Implemented:** `map_keep_keys`, `map_drop_keys` (for `keep`/`drop`/`by`/`without` operations).
- **Missing:** `json_parser`, `logfmt_parser`, `ip_match` for pipeline stages.

## Recommendations

1.  **Prioritize Pipeline UDFs:** Implement `json_parser` and `logfmt_parser` UDFs to enable the most common log processing workflows.
2.  **Binary Operations:** Implement vector matching logic for binary operations to support complex alerting rules.
3.  **Instant Query:** Implement `/loki/api/v1/query` endpoint for single-point-in-time metric queries.
