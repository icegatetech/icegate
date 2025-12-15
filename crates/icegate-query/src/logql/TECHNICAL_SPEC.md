# LogQL to DataFusion Transpilation Technical Specification

This document outlines the technical approach for transpiling LogQL queries into Apache DataFusion logical plans. The goal is to leverage DataFusion's query optimization and execution capabilities to query log data stored in an Iceberg data lake.

## 1. Data Schema

The transpilation process relies on a predefined Iceberg schema for logs, as defined in `src/common/schema.rs`. The key fields from the `logs` schema are:

-   `timestamp`: The timestamp of the log entry.
-   `body`: The log message content (string).
-   `service_name`, `severity_text`, `trace_id`, `span_id`: Indexed, top-level fields for efficient filtering.
-   `attributes`: A `Map<String, String>` containing all other log attributes (e.g., from resource, scope, and log records).

## 2. Transpilation Strategy

The transpiler will parse the LogQL query using ANTLR and then walk the parse tree to construct a DataFusion `LogicalPlan`. The process starts with a base plan that scans the logs table. Each part of the LogQL query modifies this plan.

### 2.1. Stream Selector (`{...}`)

The stream selector is translated into a `Filter` logical plan node. The label matching expressions within the selector become a series of predicates, combined with the `AND` operator.

-   **Operators**:
    -   `=`: Equality. Translates to `column = 'value'`.
    -   `!=`: Inequality. Translates to `column != 'value'`.
    -   `=~`: Regex match. Translates to a `regexp_match(column, 'pattern')` expression.
    -   `!~`: Negative regex match. Translates to `NOT regexp_match(column, 'pattern')`.

-   **Attribute Handling**:
    -   **Direct Columns**: If the attribute is a direct column in the schema (e.g., `service_name`), the predicate is applied directly to that column.
    -   **Map Attributes**: If the attribute is not a direct column, it is assumed to be in the `attributes` map. The predicate is applied by first extracting the value from the map. This is currently not fully implemented and will require using `map_extract` or a similar function.

### 2.1.1. Schema Handling

> **Implementation Note:** The schema normalization described in the original design (merging indexed columns into the attributes MAP) was **not implemented**. Indexed columns remain as separate top-level columns throughout query execution.

**Current Implementation:**
- Indexed columns (`service_name`, `severity_text`, `trace_id`, `span_id`) remain as separate columns
- The `attributes` MAP column contains only non-indexed attributes
- Label operations work with both indexed columns and the attributes MAP separately
- For Grafana compatibility, `level` is added as an alias of `severity_text` in output

**Rationale for Current Approach:**
- Avoids MAP type limitations in GROUP BY operations (see Section 13.1)
- Better query performance by keeping indexed columns as direct columns
- Simpler predicate pushdown to Iceberg for indexed columns

**Example:**
```rust
// LogQL: {service_name="api", severity_text="ERROR"}
// Indexed columns filtered directly, attributes MAP preserved separately

let filtered = df
    .filter(col("service_name").eq(lit("api")))?
    .filter(col("severity_text").eq(lit("ERROR")))?;

// Output schema includes both:
// - timestamp, body, service_name, severity_text, trace_id, span_id (columns)
// - attributes (MAP)
// - level (alias of severity_text for Grafana)
```

### 2.2. Pipeline Stages (`|`)

Pipeline stages are processed sequentially, each stage wrapping the logical plan from the previous stage.

#### 2.2.1. Line Filters

Line filters operate on the `body` column and are translated into a `Filter` node.

-   `|= "text"` (contains): `body LIKE '%text%'`
-   `!= "text"` (does not contain): `body NOT LIKE '%text%'`
-   `|~ "regex"` (regex match): `regexp_match(body, 'regex')`
-   `!~ "regex"` (regex does not match): `NOT regexp_match(body, 'regex')`

Multiple line filter conditions can be chained with `OR`.

#### 2.2.2. Parser Expressions

> **Implementation Status: ❌ Not Implemented**
>
> Parser expressions are currently **placeholders (no-ops)** in the implementation. The AST is parsed correctly, but the actual parsing logic is not executed. Logs pass through unchanged.

Parser expressions (`| json`, `| logfmt`, `| regexp`) are designed to extract data from the `body` column and add the extracted values as new columns.

-   **`| json`**: ❌ Placeholder - returns DataFrame unchanged
-   **`| logfmt`**: ❌ Placeholder - returns DataFrame unchanged
-   **`| regexp "<re>"`**: ❌ Placeholder - returns DataFrame unchanged
-   **`| pattern "<pattern>"`**: ❌ Placeholder - returns DataFrame unchanged
-   **`| unpack`**: ❌ Placeholder - returns DataFrame unchanged

**Future Implementation Notes:**

The design below describes the intended implementation:

-   **`| json`**:
    -   Would use `get_json_object(body, '$.field')` expressions to extract fields from JSON log lines.
    -   Each extracted field would be added as a new column in the plan.

-   **`| logfmt`**:
    -   Requires a custom UDF (`logfmt_parser`) to parse the logfmt format.
    -   The UDF would take the `body` column and output a `Struct` of parsed key-value pairs.

-   **`| regexp "<re>"`**:
    -   Requires a custom UDF (`regexp_extractor`) with named capture groups.
    -   Use `(?P<name>...)` syntax for named groups.

**Current Code (Placeholder):**
```rust
fn apply_parser(&self, df: DataFrame, parser: LogParser) -> Result<DataFrame> {
    match parser {
        LogParser::Json(_fields) => Ok(df),      // No-op
        LogParser::Logfmt { .. } => Ok(df),      // No-op
        LogParser::Regexp(_pattern) => Ok(df),   // No-op
        LogParser::Pattern(_pattern) => Ok(df),  // No-op
        LogParser::Unpack => Ok(df),             // No-op
    }
}
```

#### 2.2.3. Label Filter Expressions

These expressions filter the stream based on values of extracted labels (columns). They are translated into a `Filter` node that is applied *after* a parser stage.

-   `| attr = "value"`: `Filter` node with `attr = 'value'`.
-   `| attr != "value"`: `Filter` node with `attr != 'value'`.
-   `| attr =~ "regex"`: `Filter` node with `regexp_match(attr, 'regex')`.
-   `| attr !~ "regex"`: `Filter` node with `NOT regexp_match(attr, 'regex')`.

### 2.2.4. Label Format Expressions

> **Implementation Status: ❌ Not Implemented**
>
> Label format expressions are currently **placeholders (no-ops)**. The AST is parsed correctly, but template expansion is not executed.

Label format expressions are designed to rename and transform labels using Go template syntax.

-   `| label_format new_label="{{.old_label}}"`: ❌ Placeholder - no effect
-   `| label_format combined="{{.label1}}-{{.label2}}"`: ❌ Placeholder - no effect

**Current Code (Placeholder):**
```rust
fn apply_label_format(&self, df: DataFrame, ops: Vec<LabelFormatOp>) -> Result<DataFrame> {
    for op in ops {
        match op {
            LabelFormatOp::Rename { .. } | LabelFormatOp::Template { .. } => {
                // TODO: Implement label rename/template
            },
        }
    }
    Ok(df) // Returns unchanged
}
```

**Future Implementation Notes:**
- Would require a template UDF using `gtmpl` crate for Go template syntax
- Would manipulate the `attributes` MAP using `map_keys()`, `map_values()`, and `make_map()`

### 2.2.5. Line Format Expressions

> **Implementation Status: ❌ Not Implemented**
>
> Line format expressions are currently **placeholders (no-ops)**. The body column is not modified.

Line format expressions are designed to reformat the log line (`body` column) using Go template syntax.

-   `| line_format "{{.label}}: {{.body}}"`: ❌ Placeholder - no effect

**Current Code (Placeholder):**
```rust
PipelineStage::LineFormat(_template) => {
    // TODO: Implement line_format using template engine
    df  // Returns unchanged
}
```

### 2.2.6. Drop and Keep Labels

> **Implementation Status: ✅ Implemented**
>
> Drop and keep operations work on the `attributes` MAP column using custom UDFs.

These expressions control which labels are included in the `attributes` map.

-   `| drop label1, label2`: ✅ Removes specified keys from attributes map
-   `| keep label1, label2`: ✅ Keeps only specified keys in attributes map

**Implementation:**
Uses two custom UDFs defined in `src/query/logql/datafusion/udf.rs`:
- `map_drop_keys(map, keys_array)`: Removes keys present in array
- `map_keep_keys(map, keys_array)`: Keeps only keys present in array

**Drop Example:**
```rust
// LogQL: | drop region, instance
fn apply_drop(df: DataFrame, labels: &[LabelExtraction]) -> Result<DataFrame> {
    let label_literals: Vec<Expr> = labels.iter().map(|l| lit(l.name.as_str())).collect();
    let udf = ScalarUDF::from(MapDropKeys::new());

    let filtered_attrs = udf.call(vec![
        col("attributes"),
        make_array(label_literals),
    ]);

    // Replace attributes column with filtered version
    df.select(/* all columns with attributes replaced */)
}
```

**Keep Example:**
```rust
// LogQL: | keep level, service
fn apply_keep(df: DataFrame, labels: &[LabelExtraction]) -> Result<DataFrame> {
    let label_literals: Vec<Expr> = labels.iter().map(|l| lit(l.name.as_str())).collect();
    let udf = ScalarUDF::from(MapKeepKeys::new());

    let filtered_attrs = udf.call(vec![
        col("attributes"),
        make_array(label_literals),
    ]);

    df.select(/* all columns with attributes replaced */)
}
```

**Limitations:**
- Only simple label names with `=` matcher are supported
- `!=`, `=~`, `!~` matchers for drop/keep are not implemented
- Only operates on `attributes` MAP, not indexed columns

### 2.2.7. Decolorize

> **Implementation Status: ❌ Not Implemented**
>
> Decolorize is currently a **placeholder (no-op)**.

-   `| decolorize`: ❌ Placeholder - body unchanged

**Current Code (Placeholder):**
```rust
const fn apply_decolorize(&self, df: DataFrame) -> Result<DataFrame> {
    // TODO: Implement decolorize
    Ok(df)
}
```

### 2.2.8. Pattern Expression

> **Implementation Status: ❌ Not Implemented**
>
> Pattern expressions are handled as part of parser expressions (see Section 2.2.2).

-   `| pattern "<pattern>"`: ❌ Placeholder - returns DataFrame unchanged

## 3. Range Queries and Time Windows

Range queries in LogQL select log entries within a specific time window. These are essential for metric queries and rate calculations.

### 3.1. Range Vector Selectors

Range vector selectors specify a time window from which to select log entries.

**Syntax:**
- `{selector}[duration]`: Selects logs matching `selector` within the last `duration`.
- Example: `{job="mysql"}[5m]` - Last 5 minutes of logs from job "mysql".

**Duration Units:**
- `ms` - milliseconds
- `s` - seconds
- `m` - minutes
- `h` - hours
- `d` - days
- `w` - weeks
- `y` - years

**Translation to DataFusion:**
- Convert duration to a timestamp filter.
- Apply filter: `timestamp >= (now - duration) AND timestamp <= now`

**Example:**
```rust
// LogQL: {job="mysql"}[5m]
// DataFusion: Add timestamp range filter
use chrono::{Duration, Utc};

let now = Utc::now();
let start = now - Duration::minutes(5);

LogicalPlanBuilder::from(base_plan)
    .filter(
        col("timestamp").gt_eq(lit(start))
            .and(col("timestamp").lt_eq(lit(now)))
    )?
    .build()
```

### 3.2. Offset Modifier

The offset modifier shifts the time window backward by a specified duration.

**Syntax:**
- `{selector}[duration] offset offset_duration`
- Example: `{job="mysql"}[5m] offset 1h` - 5 minutes of logs from 1 hour ago.

**Translation to DataFusion:**
- Subtract both the range duration and offset from current time.
- Filter: `timestamp >= (now - offset - duration) AND timestamp < (now - offset)`

**Example:**
```rust
// LogQL: {job="mysql"}[5m] offset 1h
// DataFusion: Shifted timestamp range
let now = Utc::now();
let offset = Duration::hours(1);
let range = Duration::minutes(5);
let end = now - offset;
let start = end - range;

LogicalPlanBuilder::from(base_plan)
    .filter(
        col("timestamp").gt_eq(lit(start))
            .and(col("timestamp").lt(lit(end)))
    )?
    .build()
```

### 3.3. Time Window Implementation

> **Implementation Note:** The original design proposed using DataFusion Window Functions. The actual implementation uses **custom UDAFs (User-Defined Aggregate Functions)** with a `GridAccumulator` pattern for better efficiency with LogQL's time-series semantics.

See Section 4 for the actual UDAF-based implementation.

## 4. Metric Queries and Range Aggregations

> **Implementation Status: ⚠️ Partial**
>
> Log-range aggregations (`count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`, `absent_over_time`) are fully implemented using custom UDAFs. Unwrap-based aggregations are not implemented.

Metric queries compute aggregated metrics over time ranges, converting log streams into time series data.

### 4.1. Architecture: UDAF-Based Range Aggregations

The implementation uses custom **User-Defined Aggregate Functions (UDAFs)** instead of Window Functions. All UDAFs share a common `GridAccumulator` base that provides:

- **Time grid generation** from start/end/step parameters
- **Vectorized grid-major updates** (processes all timestamps per grid point)
- **State serialization** for distributed execution
- **Merge logic** for combining partial results

**Source:** `src/query/logql/datafusion/udaf.rs`

#### 4.1.1. Implemented UDAFs

| UDAF | Status | Description |
|:-----|:-------|:------------|
| `count_over_time` | ✅ Implemented | Counts log entries per time bucket |
| `rate` | ✅ Implemented | Log entry rate (count / range_seconds) |
| `bytes_over_time` | ✅ Implemented | Sums byte lengths of log bodies per bucket |
| `bytes_rate` | ✅ Implemented | Byte throughput rate (bytes / range_seconds) |
| `absent_over_time` | ✅ Implemented | Returns 1 for time ranges with no samples |

#### 4.1.2. GridAccumulator Algorithm

The `GridAccumulator` uses **grid-major iteration** for efficiency:

```rust
// Instead of: for each timestamp → check all grid points (O(timestamps × grid))
// We do: for each grid point → vectorized check all timestamps

pub fn update_counts(&mut self, timestamps: &TimestampMicrosecondArray) -> Result<()> {
    // Pre-compute duration scalars for vectorized comparison
    let offset_scalar = Scalar::new(DurationMicrosecondArray::from(vec![self.offset_micros]));
    let upper_scalar = Scalar::new(DurationMicrosecondArray::from(vec![self.upper_bound_micros]));

    // Grid-major iteration: for each grid point, find matching timestamps
    for (grid_idx, &grid_val) in self.grid.iter().enumerate() {
        // Broadcast: compute (grid_val - all_timestamps) in one vectorized pass
        let grid_scalar = Scalar::new(TimestampMicrosecondArray::from(vec![grid_val]));
        let diff = numeric::sub(&grid_scalar, timestamps)?;

        // Vectorized bounds check: offset <= diff <= upper_bound
        let lower_ok = cmp::gt_eq(&diff, &offset_scalar)?;
        let upper_ok = cmp::lt_eq(&diff, &upper_scalar)?;
        let mask = boolean::and(&lower_ok, &upper_ok)?;

        // Count matching timestamps
        let count = mask.true_count() as u64;
        self.values[grid_idx] += count;
    }
    Ok(())
}
```

#### 4.1.3. UDAF Arguments

All log-range UDAFs accept these arguments:

| Argument | Type | Description |
|:---------|:-----|:------------|
| `timestamp` | `Timestamp(Microsecond)` | Input timestamps |
| `start` | `Timestamp(Microsecond)` | Grid start time |
| `end` | `Timestamp(Microsecond)` | Grid end time |
| `step` | `Interval` | Step between grid points |
| `range` | `Interval` | Range window size |
| `offset` | `Interval` | Offset for range window |

`bytes_over_time` and `bytes_rate` additionally take `body` column as second argument.

#### 4.1.4. Translation Example

```rust
// LogQL: count_over_time({job="mysql"}[5m])
async fn plan_log_range_aggregation(&self, agg: RangeAggregation) -> Result<DataFrame> {
    // 1. Plan inner LogExpr with extended time range for lookback
    let adjusted_start = self.query_ctx.start - agg.range_expr.range - offset_duration;
    let adjusted_end = self.query_ctx.end - offset_duration;
    let df = self.plan_log(agg.range_expr.log_expr, adjusted_start, adjusted_end).await?;

    // 2. Build UDAF expression
    let udaf = AggregateUDF::from(CountOverTime::new());
    let udaf_expr = udaf.call(vec![
        col("timestamp"),
        start_arg, end_arg, step_arg, range_arg, offset_arg,
    ]);

    // 3. Aggregate with grouping
    let df = df.aggregate(grouping_exprs, vec![udaf_expr.alias("_result")])?;

    // 4. Unnest the result (UDAF returns List<Struct {timestamp, value}>)
    let df = df.unnest_columns(&["_result"])?;
    let df = df.unnest_columns(&["_result"])?;

    // 5. Project to final schema
    df.select(/* timestamp, value columns */)
}
```

### 4.2. Unwrap Expressions

> **Implementation Status: ❌ Not Implemented**
>
> Unwrap-based aggregations return `NotImplemented` error.

Unwrap expressions are designed to extract numeric values from labels for use in range aggregations.

**Not Implemented Functions:**
- `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`
- `stddev_over_time`, `stdvar_over_time`, `quantile_over_time`
- `first_over_time`, `last_over_time`, `rate_counter`

**Current Code:**
```rust
fn plan_unwrap_range_aggregation(&self, _agg: RangeAggregation) -> Result<DataFrame> {
    Err(IceGateError::NotImplemented(
        "Unwrap aggregation not yet implemented".to_string(),
    ))
}
```

**Syntax (for future implementation):**
- `| unwrap label_name`: Extracts numeric value from the label.
- `duration()`: Converts duration strings to seconds.
- `bytes()`: Converts byte strings to numeric values.

**Future Translation to DataFusion:**
```rust
// LogQL: rate({job="mysql"} | unwrap bytes(size) [5m])
// DataFusion: Convert size label to bytes, then calculate rate

// Step 1: Extract and convert the label
let bytes_value = bytes_converter_udf(
    map_extract(col("attributes"), lit("size"))
);

// Step 2: Add as column via projection
let with_unwrapped = LogicalPlanBuilder::from(base_plan)
    .project(vec![
        col("*"),
        bytes_value.alias("_unwrapped_value")
    ])?
    .build()?;

// Step 3: Apply rate calculation
let rate_window = /* window function on _unwrapped_value */ / lit(300.0);
```

### 4.3. Implementation Example: Complete Metric Query

```rust
// Complete example: rate({service_name="api"} | json | unwrap latency [5m])

// Step 1: Base scan with filter
let base = LogicalPlanBuilder::scan(/* ... */)
    .filter(col("service_name").eq(lit("api")))?;

// Step 2: Apply JSON parser (adds extracted fields as columns)
let with_json = base.project(vec![
    col("*"),
    get_json_object(col("body"), lit("$.latency")).alias("latency"),
    // ... other fields
])?;

// Step 3: Unwrap latency (already extracted, ensure numeric type)
let unwrapped = with_json.project(vec![
    col("*"),
    cast(col("latency"), DataType::Float64).alias("_unwrapped_value")
])?;

// Step 4: Apply time window filter
let in_range = unwrapped.filter(
    col("timestamp").gt_eq(lit(start_time))
        .and(col("timestamp").lt_eq(lit(end_time)))
)?;

// Step 5: Calculate rate using window function
let window_frame = /* 5-minute window */;
let count_in_window = Expr::WindowFunction(WindowFunction {
    fun: WindowFunctionDefinition::AggregateFunction(AggregateFunction::Count),
    args: vec![col("_unwrapped_value")],
    partition_by: vec![],
    order_by: vec![col("timestamp").sort(true, false)],
    window_frame: Some(window_frame),
    null_treatment: None,
});

let rate = in_range.project(vec![
    col("timestamp"),
    (count_in_window / lit(300.0)).alias("rate")
])?;
```

### 4.4. Time Grid Gap Filling for Matrix Responses

> **⚠️ NOT IMPLEMENTED**: Gap filling is not implemented. UDAFs return sparse results with only buckets that contain data. Time buckets with no matching logs are omitted from the response.

**Design Intent:** Loki matrix responses should return samples at regular intervals from `start` to `end` with `step` spacing, filling gaps with `0`. The current implementation returns sparse results.

**Current Behavior:**
- UDAFs generate time grids internally via `GridAccumulator`
- Only buckets with actual data are returned
- Missing time buckets are omitted (not filled with zeros)

**Future Implementation:** Would require post-aggregation JOIN with a complete time grid table and COALESCE for gap filling.

## 5. Vector Aggregations

Vector aggregations group and aggregate time series results, similar to GROUP BY in SQL.

### 5.1. Aggregation Operators

| Operator | Description | Status |
|----------|-------------|--------|
| `sum(vector-expression)` | Sum of all samples | ✅ Implemented |
| `avg(vector-expression)` | Average of all samples | ✅ Implemented |
| `min(vector-expression)` | Minimum sample | ✅ Implemented |
| `max(vector-expression)` | Maximum sample | ✅ Implemented |
| `count(vector-expression)` | Count of samples | ✅ Implemented |
| `stddev(vector-expression)` | Standard deviation | ✅ Implemented |
| `stdvar(vector-expression)` | Standard variance | ✅ Implemented |
| `topk(k, vector-expression)` | Top k samples | ❌ Not implemented |
| `bottomk(k, vector-expression)` | Bottom k samples | ❌ Not implemented |
| `sort(vector-expression)` | Sort samples ascending | ❌ Not implemented |
| `sort_desc(vector-expression)` | Sort samples descending | ❌ Not implemented |

### 5.2. Grouping Modifiers

- **`by (label1, label2, ...)`**: Group by specified labels (keep only these labels).
- **`without (label1, label2, ...)`**: Group by all labels except specified ones.

**Examples:**
- `sum by (service_name) (rate({job="mysql"}[5m]))`: Sum rates grouped by service_name.
- `avg without (instance) (count_over_time({job="mysql"}[5m]))`: Average counts, grouping by all labels except instance.

### 5.3. Translation to DataFusion

Vector aggregations translate to `GROUP BY` with appropriate aggregate functions.

**Example:**
```rust
// LogQL: sum by (service_name, severity_text) (rate({job="mysql"}[5m]))
// DataFusion: GROUP BY with SUM

let rate_plan = /* ... rate calculation plan */;

let aggregated = LogicalPlanBuilder::from(rate_plan)
    .aggregate(
        vec![
            col("service_name"),
            col("severity_text")
        ], // GROUP BY columns
        vec![
            sum(col("rate")).alias("sum_rate")
        ]  // Aggregation expressions
    )?
    .build()
```

**Without modifier:**
```rust
// LogQL: sum without (instance) (count_over_time({job="mysql"}[5m]))
// DataFusion: GROUP BY all columns except 'instance'

// Get all columns from the schema except 'instance' and the metric value
let group_columns: Vec<Expr> = schema.fields()
    .iter()
    .filter(|f| f.name() != "instance" && f.name() != "count")
    .map(|f| col(f.name()))
    .collect();

let aggregated = LogicalPlanBuilder::from(count_plan)
    .aggregate(
        group_columns,
        vec![sum(col("count")).alias("sum_count")]
    )?
    .build()
```

## 6. Binary Operations and Vector Matching

> **⚠️ NOT IMPLEMENTED**: Binary operations between vectors are not implemented. The planner returns `NotImplemented` error for all binary operations. Only scalar literals work in simple arithmetic contexts.

Binary operations combine two vector expressions using arithmetic, comparison, or logical operators.

### 6.1. Arithmetic Operators

- `+` - Addition
- `-` - Subtraction
- `*` - Multiplication
- `/` - Division
- `%` - Modulo
- `^` - Exponentiation

**Usage:**
- `vector + scalar`: Adds scalar to each sample.
- `vector1 + vector2`: Element-wise addition (requires matching labels).

**Translation:**
```rust
// LogQL: rate({app="foo"}[1m]) * 60
// DataFusion: Multiply rate by 60
LogicalPlanBuilder::from(rate_plan)
    .project(vec![
        col("*"),
        (col("rate") * lit(60.0)).alias("rate_per_minute")
    ])?
    .build()
```

### 6.2. Comparison Operators

- `==` - Equal
- `!=` - Not equal
- `>` - Greater than
- `>=` - Greater or equal
- `<` - Less than
- `<=` - Less or equal

**Bool Modifier:**
- By default, comparison operators filter (keep only matching samples).
- `bool` modifier: Returns 0 or 1 instead of filtering.

**Translation:**
```rust
// LogQL: rate({app="foo"}[1m]) > 0.5
// DataFusion: Filter rows where rate > 0.5
LogicalPlanBuilder::from(rate_plan)
    .filter(col("rate").gt(lit(0.5)))?
    .build()

// LogQL: rate({app="foo"}[1m]) > bool 0.5
// DataFusion: Return 1 or 0 based on comparison
LogicalPlanBuilder::from(rate_plan)
    .project(vec![
        col("*"),
        case(col("rate").gt(lit(0.5)))
            .when(lit(true), lit(1))
            .otherwise(lit(0))?
            .alias("comparison_result")
    ])?
    .build()
```

### 6.3. Logical Operators

- `and` - Intersection (keep samples present in both vectors).
- `or` - Union (keep samples from either vector).
- `unless` - Difference (keep samples from left not in right).

**Translation:**
These are set operations on label sets, implemented using joins.

```rust
// LogQL: vector1 and vector2
// DataFusion: INNER JOIN on all labels
let joined = LogicalPlanBuilder::from(vector1_plan)
    .join(
        vector2_plan,
        JoinType::Inner,
        (label_columns.clone(), label_columns.clone()),
        None
    )?
    .project(/* left side columns */)?
    .build()

// LogQL: vector1 unless vector2
// DataFusion: LEFT ANTI JOIN
let difference = LogicalPlanBuilder::from(vector1_plan)
    .join(
        vector2_plan,
        JoinType::LeftAnti,
        (label_columns.clone(), label_columns.clone()),
        None
    )?
    .build()
```

### 6.4. Vector Matching Modifiers

These modifiers control how labels are matched in binary operations.

#### 6.4.1. On / Ignoring

- **`on(label1, label2, ...)`**: Match only on specified labels.
- **`ignoring(label1, label2, ...)`**: Ignore specified labels when matching.

**Example:**
```rust
// LogQL: sum(rate({app="foo"}[1m])) / on(service_name) sum(rate({app="bar"}[1m]))
// DataFusion: JOIN only on service_name

let joined = LogicalPlanBuilder::from(left_agg)
    .join(
        right_agg,
        JoinType::Inner,
        (vec![col("service_name")], vec![col("service_name")]),
        None
    )?
    .project(vec![
        col("left.service_name"),
        (col("left.sum_rate") / col("right.sum_rate")).alias("ratio")
    ])?
    .build()
```

#### 6.4.2. Group Left / Group Right

Handle many-to-one or one-to-many relationships in joins.

- **`group_left(label1, ...)`**: Many-to-one (left side has more samples), include labels from right.
- **`group_right(label1, ...)`**: One-to-many (right side has more samples), include labels from left.

**Translation:**
```rust
// LogQL: vector1 / on(service) group_left(region) vector2
// DataFusion: LEFT JOIN, include 'region' from right side

let joined = LogicalPlanBuilder::from(vector1_plan)
    .join(
        vector2_plan,
        JoinType::Left,
        (vec![col("service")], vec![col("service")]),
        None
    )?
    .project(vec![
        col("left.*"),
        col("right.region"), // Include from right side
        (col("left.value") / col("right.value")).alias("ratio")
    ])?
    .build()
```

## 7. User-Defined Functions (UDFs)

> **Implementation Status:** Only map filtering UDFs are implemented. Parser UDFs (`logfmt_parser`, `json_parser`, `regexp_extractor`, `pattern_parser`) and template UDFs (`label_format_template`, `line_format_template`) are designed but not yet implemented.

### 7.0. Implemented UDFs

The following UDFs are currently implemented in `src/query/logql/datafusion/udf.rs`:

| UDF | Purpose | Status |
|-----|---------|--------|
| `map_keep_keys(map, keys_array)` | Keep only specified keys from a `Map<String, String>` | ✅ Implemented |
| `map_drop_keys(map, keys_array)` | Remove specified keys from a `Map<String, String>` | ✅ Implemented |

**Usage:** These UDFs support the `keep`, `drop`, `by`, and `without` operations on the `attributes` MAP column.

```rust
// Example: keep only "level" and "service" keys
// SELECT map_keep_keys(attributes, ARRAY['level', 'service']) FROM logs
// {level: "info", service: "api", method: "GET"} → {level: "info", service: "api"}

// Example: drop "method" key
// SELECT map_drop_keys(attributes, ARRAY['method']) FROM logs
// {level: "info", service: "api", method: "GET"} → {level: "info", service: "api"}
```

---

The following UDFs are necessary to support full LogQL functionality but are **not yet implemented**.

### 7.1. Scalar UDFs (Not Implemented)

#### 7.1.1. `logfmt_parser`

> **❌ NOT IMPLEMENTED**: Pipeline parser stage `| logfmt` is a no-op placeholder.

Parses a logfmt-formatted string and returns a struct of key-value pairs.

**Signature:** `logfmt_parser(body: String, strict: Boolean) -> Struct`

**Parameters:**
- `body`: The log line to parse.
- `strict`: If true, fail on malformed input; if false, skip invalid pairs.

**Implementation:**
```rust
use datafusion::logical_expr::create_udf;
use arrow::datatypes::{DataType, Field};

fn logfmt_parse_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Parse logfmt: key1=value1 key2="value 2" key3=value3
    // Return struct with parsed fields
    // Handle quoted values, escaped characters
    // ...
}

let logfmt_udf = create_udf(
    "logfmt_parser",
    vec![DataType::Utf8, DataType::Boolean],
    Arc::new(DataType::Struct(Fields::from(vec![
        // Dynamic fields based on parsed content
    ]))),
    Volatility::Immutable,
    Arc::new(logfmt_parse_impl),
);

ctx.register_udf(logfmt_udf);
```

#### 7.1.2. `regexp_extractor`

> **❌ NOT IMPLEMENTED**: Pipeline parser stage `| regexp` is a no-op placeholder.

Extracts named capture groups from a string using a regex pattern.

**Signature:** `regexp_extractor(body: String, pattern: String) -> Struct`

**Implementation:**
```rust
use regex::Regex;

fn regexp_extract_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Compile regex with named groups: (?P<name>...)
    // Extract matches into struct fields
    // Return struct with captured values
    // ...
}

let regexp_udf = create_udf(
    "regexp_extractor",
    vec![DataType::Utf8, DataType::Utf8],
    Arc::new(DataType::Struct(/* ... */)),
    Volatility::Immutable,
    Arc::new(regexp_extract_impl),
);
```

#### 7.1.3. `pattern_parser`

> **❌ NOT IMPLEMENTED**: Pipeline parser stage `| pattern` is a no-op placeholder.

Extracts fields from log lines using LogQL pattern syntax.

**Signature:** `pattern_parser(body: String, pattern: String) -> Struct`

**Pattern Syntax:**
- `<field_name>`: Named field placeholder.
- Literal text matches exactly.

**Example:** `<ip> - - [<timestamp>] "<method> <path> <protocol>"`

**Implementation:**
```rust
fn pattern_parse_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Convert pattern to regex by replacing <field> with named groups
    // Example: "<ip> - -" becomes "(?P<ip>\S+) - -"
    // Apply regex extraction
    // ...
}
```

#### 7.1.4. `decolorize`

> **❌ NOT IMPLEMENTED**: Pipeline stage `| decolorize` is a no-op placeholder.

Removes ANSI color codes from strings.

**Signature:** `decolorize(text: String) -> String`

**Implementation:**
```rust
fn decolorize_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Regex: \x1b\[[0-9;]*m
    // Replace all matches with empty string
    let ansi_regex = Regex::new(r"\x1b\[[0-9;]*m").unwrap();
    // Apply to input string array
    // ...
}
```

#### 7.1.5. `duration_parser`

> **❌ NOT IMPLEMENTED**: Duration parsing for label filters uses inline Rust code, not a UDF.

Converts duration strings to seconds (float).

**Signature:** `duration_parser(value: String) -> Float64`

**Supported Formats:**
- `5m` → 300.0
- `1h30m` → 5400.0
- `2.5s` → 2.5

**Implementation:**
```rust
fn duration_parse_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Parse duration string: digit+unit (ms, s, m, h, d, w, y)
    // Convert to seconds as Float64
    // Handle multiple units: "1h30m" = 5400
    // ...
}
```

#### 7.1.6. `bytes_parser`

> **❌ NOT IMPLEMENTED**: Bytes parsing for label filters uses inline Rust code, not a UDF.

Converts byte strings to numeric values.

**Signature:** `bytes_parser(value: String) -> Int64`

**Supported Formats:**
- `5KB` → 5120
- `2.5MB` → 2621440
- `1GB` → 1073741824

**Implementation:**
```rust
fn bytes_parse_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Parse: number + unit (B, KB, MB, GB, TB, etc.)
    // Support both 1000-based (KB) and 1024-based (KiB) units
    // Convert to bytes as Int64
    // ...
}
```

#### 7.1.7. `ip_filter`

> **❌ NOT IMPLEMENTED**: IP filtering returns `NotImplemented` error.

Checks if an IP address is within a CIDR range.

**Signature:** `ip_filter(ip: String, cidr: String) -> Boolean`

**Example:** `ip_filter("192.168.1.50", "192.168.1.0/24")` → true

**Implementation:**
```rust
use ipnetwork::IpNetwork;

fn ip_filter_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Parse IP address and CIDR range
    // Check if IP is within the network
    // Return boolean
    // ...
}
```

#### 7.1.8. `json_extractor` (Enhanced)

> **❌ NOT IMPLEMENTED**: Pipeline parser stage `| json` is a no-op placeholder.

Enhanced JSON extraction with support for nested paths and arrays.

**Signature:** `json_extractor(body: String, paths: List<String>) -> Struct`

**Features:**
- Supports nested paths: `$.user.name`
- Array indexing: `$.items[0].id`
- Multiple field extraction in single call.

#### 7.1.9. `label_format_template_udf`

> **❌ NOT IMPLEMENTED**: Pipeline stage `| label_format` is a no-op placeholder.

Expands Go template strings for label formatting using the `gtmpl` crate.

**Signature:** `label_format_template_udf(template: String, ...fields: String) -> String`

**Parameters:**
- `template`: Go template string with `{{.field}}` placeholders.
- `fields`: Variable number of column values referenced in the template.

**Implementation:**
```rust
use gtmpl::{Template, Context};

fn label_format_template_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // First arg is template string
    let template_str = extract_string_arg(&args[0])?;

    // Parse template using gtmpl
    let template = Template::new(template_str)?;

    // Build context from remaining args
    let mut context = Context::new();
    for (idx, arg) in args[1..].iter().enumerate() {
        let field_name = extract_field_name(idx)?; // Extract from template
        let field_value = extract_string_arg(arg)?;
        context.insert(field_name, field_value);
    }

    // Render template
    let result = template.render(&context)?;
    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
}

let label_format_udf = create_udf(
    "label_format_template_udf",
    vec![DataType::Utf8], // Variadic args
    Arc::new(DataType::Utf8),
    Volatility::Immutable,
    Arc::new(label_format_template_impl),
);
```

#### 7.1.10. `line_format_template_udf`

> **❌ NOT IMPLEMENTED**: Pipeline stage `| line_format` is a no-op placeholder.

Expands Go template strings for line formatting using the `gtmpl` crate.

**Signature:** `line_format_template_udf(template: String, ...fields: String) -> String`

**Parameters:**
- `template`: Go template string with `{{.field}}` placeholders.
- `fields`: Variable number of column values referenced in the template.

**Implementation:**
```rust
use gtmpl::{Template, Context};

fn line_format_template_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Implementation identical to label_format_template_impl
    // Parses template, builds context, renders result
    // ...
}

let line_format_udf = create_udf(
    "line_format_template_udf",
    vec![DataType::Utf8], // Variadic args
    Arc::new(DataType::Utf8),
    Volatility::Immutable,
    Arc::new(line_format_template_impl),
);
```

**Note:** Both template UDFs use the [`gtmpl`](https://crates.io/crates/gtmpl) crate for proper Go template parsing and rendering. This provides:
- Full Go template syntax support
- Proper variable extraction and substitution
- Built-in functions (if, range, etc.)
- Error handling for malformed templates

### 7.2. Aggregate UDFs (UDAFs)

> **Note:** Log-range aggregation UDAFs (`count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`, `absent_over_time`) are implemented in `src/query/logql/datafusion/udaf.rs` using a shared `GridAccumulator` pattern. See Section 4.1 for details. Unwrap-based aggregations are not implemented.

#### 7.2.1. `quantile_over_time`

> **❌ NOT IMPLEMENTED**: Requires unwrap expression support which is not implemented.

Calculates quantiles over time windows.

**Signature:** `quantile_over_time(φ: Float64, value: Float64) -> Float64`

**Implementation:**
```rust
use datafusion::logical_expr::{create_udaf, Accumulator};

#[derive(Debug)]
struct QuantileAccumulator {
    values: Vec<f64>,
    quantile: f64,
}

impl Accumulator for QuantileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Collect values
        // ...
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Sort values and compute quantile
        self.values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let index = (self.quantile * (self.values.len() as f64 - 1.0)) as usize;
        Ok(ScalarValue::Float64(Some(self.values[index])))
    }

    // ... other methods
}
```

### 7.3. UDF Registration

All UDFs must be registered with the DataFusion context before transpilation.

**Example:**
```rust
pub fn register_logql_udfs(ctx: &mut SessionContext) -> Result<()> {
    // Parser UDFs
    ctx.register_udf(create_logfmt_udf());
    ctx.register_udf(create_regexp_extractor_udf());
    ctx.register_udf(create_pattern_parser_udf());
    ctx.register_udf(create_json_extractor_udf());

    // Template UDFs (using gtmpl crate)
    ctx.register_udf(create_label_format_template_udf());
    ctx.register_udf(create_line_format_template_udf());

    // Utility UDFs
    ctx.register_udf(create_decolorize_udf());
    ctx.register_udf(create_duration_parser_udf());
    ctx.register_udf(create_bytes_parser_udf());
    ctx.register_udf(create_ip_filter_udf());

    // Aggregate UDAFs
    ctx.register_udaf(create_quantile_over_time_udaf());

    Ok(())
}
```

## 8. Expression Building Patterns

Common patterns and helper functions for constructing DataFusion expressions.

### 8.1. Label Matcher to Expression

Convert LogQL label matchers to DataFusion filter expressions.

```rust
fn build_label_matcher(label: &str, operator: &str, value: &str, schema: &Schema) -> Result<Expr> {
    // Check if label is a direct column or in attributes map
    let is_direct_column = schema.field_with_name(label).is_ok();

    let label_expr = if is_direct_column {
        col(label)
    } else {
        // Extract from attributes map
        extract_from_attributes(label)
    };

    match operator {
        "=" => Ok(label_expr.eq(lit(value))),
        "!=" => Ok(label_expr.not_eq(lit(value))),
        "=~" => {
            // Regex match
            Ok(Expr::ScalarFunction(ScalarFunction {
                func: BuiltinScalarFunction::RegexpMatch,
                args: vec![label_expr, lit(value)],
            }))
        },
        "!~" => {
            // Negative regex match
            Ok(not(Expr::ScalarFunction(ScalarFunction {
                func: BuiltinScalarFunction::RegexpMatch,
                args: vec![label_expr, lit(value)],
            })))
        },
        _ => Err(DataFusionError::Plan(format!("Unknown operator: {}", operator))),
    }
}
```

### 8.2. Extracting from Attributes Map

Helper function to extract values from the `attributes` map column.

```rust
fn extract_from_attributes(key: &str) -> Expr {
    // Use map_extract to get value for key
    // map_extract returns an array (possibly empty)
    // Use array_element to get first value, or NULL if empty

    let extracted = Expr::ScalarFunction(ScalarFunction {
        func: BuiltinScalarFunction::MapExtract,
        args: vec![col("attributes"), lit(key)],
    });

    // Check if array is empty, return NULL if so, otherwise first element
    Expr::Case(Case {
        expr: None,
        when_then_expr: vec![(
            Expr::ScalarFunction(ScalarFunction {
                func: BuiltinScalarFunction::ArrayLength,
                args: vec![extracted.clone()],
            }).eq(lit(0)),
            Expr::Literal(ScalarValue::Utf8(None))
        )],
        else_expr: Some(Box::new(Expr::ScalarFunction(ScalarFunction {
            func: BuiltinScalarFunction::ArrayElement,
            args: vec![extracted, lit(1)], // Arrays are 1-indexed
        }))),
    })
}
```

### 8.3. Building Line Filters

Convert LogQL line filters to DataFusion expressions.

```rust
fn build_line_filter(operator: &str, pattern: &str) -> Result<Expr> {
    match operator {
        "|=" => {
            // Contains
            Ok(col("body").like(lit(format!("%{}%", pattern))))
        },
        "!=" => {
            // Does not contain
            Ok(not(col("body").like(lit(format!("%{}%", pattern)))))
        },
        "|~" => {
            // Regex match
            Ok(Expr::ScalarFunction(ScalarFunction {
                func: BuiltinScalarFunction::RegexpMatch,
                args: vec![col("body"), lit(pattern)],
            }))
        },
        "!~" => {
            // Regex does not match
            Ok(not(Expr::ScalarFunction(ScalarFunction {
                func: BuiltinScalarFunction::RegexpMatch,
                args: vec![col("body"), lit(pattern)],
            })))
        },
        _ => Err(DataFusionError::Plan(format!("Unknown line filter operator: {}", operator))),
    }
}
```

### 8.4. Type Coercion

Handle type conversions for unwrap and other operations.

```rust
fn coerce_to_numeric(expr: Expr, target_type: DataType) -> Expr {
    Expr::Cast(Cast {
        expr: Box::new(expr),
        data_type: target_type,
    })
}

// Example usage
let numeric_latency = coerce_to_numeric(
    col("latency_str"),
    DataType::Float64
);
```

### 8.5. Null Handling

Strategies for handling NULL values in expressions.

```rust
fn coalesce_null(expr: Expr, default_value: ScalarValue) -> Expr {
    Expr::ScalarFunction(ScalarFunction {
        func: BuiltinScalarFunction::Coalesce,
        args: vec![expr, Expr::Literal(default_value)],
    })
}

// Example: Return 0 if NULL
let safe_count = coalesce_null(col("count"), ScalarValue::Int64(Some(0)));
```

## 9. Query Optimization Strategies

Optimization techniques to improve query performance.

### 9.1. Predicate Pushdown

Push filters as close to the table scan as possible to reduce data read.

**Strategy:**
- Extract all filter conditions from the LogQL query.
- Identify filters that can be applied at scan level (direct columns with simple predicates).
- Apply these filters in the `TableScan` node.

**Example:**
```rust
// LogQL: {service_name="api", severity_text="ERROR"} | json | level="critical"
// Push service_name and severity_text filters to scan

let scan_filters = vec![
    col("service_name").eq(lit("api")),
    col("severity_text").eq(lit("ERROR")),
];

let scan_plan = LogicalPlanBuilder::scan_with_filters(
    "logs",
    table_source,
    None, // projection (all columns)
    scan_filters,
)?;

// Apply json parser and level filter after scan
let with_json = scan_plan.project(/* json extraction */)?;
let final_plan = with_json.filter(col("level").eq(lit("critical")))?;
```

### 9.2. Projection Pushdown

Only read columns that are actually needed.

**Strategy:**
- Analyze the LogQL query to determine which columns are referenced.
- Create a minimal projection list.
- Apply projection early in the plan.

**Example:**
```rust
// LogQL: {service_name="api"} | keep timestamp, body, service_name

let required_columns = vec!["timestamp", "body", "service_name"];

let scan_plan = LogicalPlanBuilder::scan(
    "logs",
    table_source,
    Some(required_columns.iter().map(|&c| col(c)).collect()),
)?;
```

### 9.3. Filter Ordering

Order filters by selectivity and cost to apply cheapest, most selective filters first.

**Cost Ranking (low to high):**
1. Direct column equality (`service_name = "api"`)
2. Direct column comparison (`timestamp > X`)
3. String LIKE (`body LIKE '%error%'`)
4. Regex match (`body =~ 'pattern'`)
5. UDF calls
6. Complex expressions

**Strategy:**
```rust
fn order_filters_by_cost(filters: Vec<Expr>) -> Vec<Expr> {
    let mut sorted = filters;
    sorted.sort_by_key(|expr| match expr {
        Expr::BinaryExpr(BinaryExpr { op: Operator::Eq, left, .. })
            if matches!(**left, Expr::Column(_)) => 1,
        Expr::BinaryExpr(BinaryExpr { op: Operator::Gt | Operator::Lt, .. }) => 2,
        Expr::ScalarFunction(ScalarFunction { func: BuiltinScalarFunction::Like, .. }) => 3,
        Expr::ScalarFunction(ScalarFunction { func: BuiltinScalarFunction::RegexpMatch, .. }) => 4,
        Expr::ScalarUDF(_) => 5,
        _ => 6,
    });
    sorted
}

// Apply filters in order
let mut plan = base_plan;
for filter in order_filters_by_cost(all_filters) {
    plan = LogicalPlanBuilder::from(plan).filter(filter)?.build()?;
}
```

### 9.4. Partition Pruning

Leverage Iceberg partitioning (e.g., by timestamp) to skip entire partitions.

**Strategy:**
- Ensure timestamp filters are extracted and applied at the scan level.
- Iceberg will automatically prune partitions based on these filters.

**Example:**
```rust
// LogQL: {job="mysql"}[5m]
// Extract timestamp range: [now-5m, now]

let time_filter = col("timestamp")
    .gt_eq(lit(start_time))
    .and(col("timestamp").lt_eq(lit(end_time)));

// This filter will enable partition pruning in Iceberg
let scan_plan = LogicalPlanBuilder::scan_with_filters(
    "logs",
    table_source,
    None,
    vec![time_filter],
)?;
```

### 9.5. Aggregation Optimization

Use partial aggregations and push aggregations down when possible.

**Strategy:**
- For grouped aggregations, consider pre-aggregation at partition level.
- Use DataFusion's built-in aggregation optimization.

## 10. Error Handling and Validation

Strategies for validating queries and handling errors gracefully.

### 10.1. Query Validation

**Syntax Validation:**
- Already handled by ANTLR parser.
- Syntax errors will result in parse failures.

**Semantic Validation:**
```rust
fn validate_logql_query(query: &ParsedQuery, schema: &Schema) -> Result<()> {
    // 1. Check that referenced labels exist (as direct columns or in attributes)
    for label in &query.referenced_labels {
        if !schema.field_with_name(label).is_ok() {
            // Label is not a direct column, will be in attributes
            // This is valid, no error
        }
    }

    // 2. Validate aggregation usage
    if query.has_aggregation && !query.has_range_vector {
        return Err(DataFusionError::Plan(
            "Aggregation requires a range vector".to_string()
        ));
    }

    // 3. Check for invalid combinations
    if query.has_unwrap && !query.has_range_aggregation {
        return Err(DataFusionError::Plan(
            "Unwrap requires a range aggregation function".to_string()
        ));
    }

    Ok(())
}
```

### 10.2. Runtime Error Handling

**Parser Failures:**
When JSON, logfmt, or regex parsing fails, add an `__error__` label instead of failing the query.

```rust
// Example: Handle JSON parse errors
fn build_json_parser_with_error_handling(body_col: Expr) -> Vec<Expr> {
    vec![
        col("*"),
        // Try to parse JSON
        when(
            Expr::ScalarFunction(ScalarFunction {
                func: BuiltinScalarFunction::IsNull,
                args: vec![get_json_object(body_col.clone(), lit("$"))],
            })
        )
        .then(lit("JSONParseError"))
        .otherwise(Expr::Literal(ScalarValue::Utf8(None)))
        .alias("__error__"),

        // Extract fields (will be NULL on error)
        get_json_object(body_col.clone(), lit("$.field1")).alias("field1"),
        get_json_object(body_col, lit("$.field2")).alias("field2"),
    ]
}
```

**Type Conversion Errors:**
```rust
// Safe cast with error handling
fn safe_cast(expr: Expr, target_type: DataType, error_value: ScalarValue) -> Expr {
    Expr::ScalarFunction(ScalarFunction {
        func: BuiltinScalarFunction::Coalesce,
        args: vec![
            Expr::TryCast(TryCast {
                expr: Box::new(expr),
                data_type: target_type,
            }),
            Expr::Literal(error_value),
        ],
    })
}
```

### 10.3. Error Propagation

Convert DataFusion errors to user-friendly LogQL error messages.

```rust
fn convert_datafusion_error(err: DataFusionError) -> LogQLError {
    match err {
        DataFusionError::Plan(msg) => LogQLError::QueryPlanError(msg),
        DataFusionError::Execution(msg) => LogQLError::ExecutionError(msg),
        DataFusionError::ArrowError(arrow_err) => {
            LogQLError::DataError(format!("Arrow error: {}", arrow_err))
        },
        _ => LogQLError::UnknownError(err.to_string()),
    }
}
```

## 11. Testing Strategies

Comprehensive testing approach for the transpilation layer.

### 11.1. Unit Tests

Test individual components in isolation.

**Example Tests:**
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_matcher_equality() {
        let schema = create_test_schema();
        let expr = build_label_matcher("service_name", "=", "api", &schema).unwrap();

        assert!(matches!(expr, Expr::BinaryExpr(BinaryExpr { op: Operator::Eq, .. })));
    }

    #[test]
    fn test_label_matcher_regex() {
        let schema = create_test_schema();
        let expr = build_label_matcher("service_name", "=~", "api.*", &schema).unwrap();

        assert!(matches!(expr, Expr::ScalarFunction(_)));
    }

    #[test]
    fn test_line_filter_contains() {
        let expr = build_line_filter("|=", "error").unwrap();

        // Should be: body LIKE '%error%'
        assert!(matches!(expr, Expr::ScalarFunction(ScalarFunction {
            func: BuiltinScalarFunction::Like, ..
        })));
    }

    #[test]
    fn test_extract_from_attributes() {
        let expr = extract_from_attributes("custom_field");

        // Should use map_extract and array_element
        assert!(matches!(expr, Expr::Case(_)));
    }

    #[test]
    fn test_duration_parser() {
        let result = parse_duration("5m").unwrap();
        assert_eq!(result, 300.0);

        let result = parse_duration("1h30m").unwrap();
        assert_eq!(result, 5400.0);
    }

    #[test]
    fn test_bytes_parser() {
        let result = parse_bytes("5KB").unwrap();
        assert_eq!(result, 5120);

        let result = parse_bytes("2.5MB").unwrap();
        assert_eq!(result, 2621440);
    }
}
```

### 11.2. Integration Tests

Test end-to-end query transpilation and execution.

```rust
#[cfg(test)]
mod integration_tests {
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_simple_stream_selector() {
        let ctx = create_test_context().await;
        let query = r#"{service_name="api"}"#;

        let plan = transpile_logql(query, &ctx).unwrap();
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        let results = df.collect().await.unwrap();

        // Verify results contain only service_name="api" logs
        for batch in results {
            let service_name = batch.column_by_name("service_name").unwrap();
            // All values should be "api"
        }
    }

    #[tokio::test]
    async fn test_json_parser_pipeline() {
        let ctx = create_test_context().await;
        let query = r#"{service_name="api"} | json"#;

        let plan = transpile_logql(query, &ctx).unwrap();
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        let results = df.collect().await.unwrap();

        // Verify JSON fields are extracted
        assert!(results[0].schema().field_with_name("field1").is_ok());
    }

    #[tokio::test]
    async fn test_rate_query() {
        let ctx = create_test_context().await;
        let query = r#"rate({job="mysql"}[5m])"#;

        let plan = transpile_logql(query, &ctx).unwrap();
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        let results = df.collect().await.unwrap();

        // Verify rate values are calculated
        assert!(results[0].schema().field_with_name("rate").is_ok());
    }

    #[tokio::test]
    async fn test_vector_aggregation() {
        let ctx = create_test_context().await;
        let query = r#"sum by (service_name) (count_over_time({job="mysql"}[5m]))"#;

        let plan = transpile_logql(query, &ctx).unwrap();
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        let results = df.collect().await.unwrap();

        // Verify grouping and aggregation
        assert!(results[0].schema().field_with_name("service_name").is_ok());
        assert!(results[0].schema().field_with_name("sum").is_ok());
    }
}
```

### 11.3. Test Data Generation

Create representative datasets for testing.

```rust
async fn create_test_context() -> SessionContext {
    let ctx = SessionContext::new();

    // Register UDFs
    register_logql_udfs(&mut ctx).unwrap();

    // Create in-memory test data
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("body", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("severity_text", DataType::Utf8, true),
        // ... other fields
    ]));

    let test_data = vec![
        // Sample log entries
        // ...
    ];

    let batch = RecordBatch::try_new(schema.clone(), test_data).unwrap();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

    ctx.register_table("logs", Arc::new(mem_table)).unwrap();

    ctx
}
```

### 11.4. Performance Testing

Benchmark query performance and identify bottlenecks.

```rust
#[cfg(test)]
mod bench {
    use criterion::{black_box, criterion_group, criterion_main, Criterion};

    fn bench_simple_query(c: &mut Criterion) {
        c.bench_function("simple_stream_selector", |b| {
            b.iter(|| {
                let query = black_box(r#"{service_name="api"}"#);
                transpile_logql(query, &ctx)
            });
        });
    }

    fn bench_complex_query(c: &mut Criterion) {
        c.bench_function("complex_metric_query", |b| {
            b.iter(|| {
                let query = black_box(
                    r#"sum by (service_name) (rate({job="mysql"} | json | unwrap latency [5m]))"#
                );
                transpile_logql(query, &ctx)
            });
        });
    }

    criterion_group!(benches, bench_simple_query, bench_complex_query);
    criterion_main!(benches);
}
```

## 12. DataFusion API Reference

Key DataFusion types and functions used in transpilation.

### 12.1. Core Types

**`LogicalPlan`**: Represents the query execution plan.
- Built using `LogicalPlanBuilder`.

**`Expr`**: Represents expressions (columns, literals, functions, etc.).
- `Expr::Column`: Column reference.
- `Expr::Literal`: Literal value.
- `Expr::BinaryExpr`: Binary operation (e.g., `a = b`).
- `Expr::ScalarFunction`: Built-in scalar function.
- `Expr::ScalarUDF`: User-defined scalar function.
- `Expr::AggregateFunction`: Aggregation function.
- `Expr::WindowFunction`: Window function.
- `Expr::Case`: CASE WHEN expression.
- `Expr::Cast`: Type cast.

### 12.2. LogicalPlanBuilder Methods

**`scan(table_name, table_source, projection)`**: Create a table scan.
**`scan_with_filters(table_name, table_source, projection, filters)`**: Scan with pushed-down filters.
**`filter(predicate)`**: Add a filter node.
**`project(exprs)`**: Add a projection node.
**`aggregate(group_exprs, aggr_exprs)`**: Add an aggregation node.
**`join(right, join_type, on_keys, filter)`**: Add a join node.
**`window(window_exprs)`**: Add window functions.
**`sort(exprs)`**: Add a sort node.
**`limit(skip, fetch)`**: Add a limit node.
**`build()`**: Build the final `LogicalPlan`.

### 12.3. Expression Builders

**`col(name)`**: Create a column reference.
**`lit(value)`**: Create a literal value.
**`binary_expr(left, op, right)`**: Create a binary expression.
**`case(expr)`**: Start a CASE expression.
**`when(condition)`**: Add a WHEN clause.
**`otherwise(expr)`**: Add an ELSE clause.
**`cast(expr, data_type)`**: Create a CAST expression.
**`not(expr)`**: Logical NOT.
**`and(left, right)`**: Logical AND.
**`or(left, right)`**: Logical OR.

### 12.4. Built-in Scalar Functions

**String Functions:**
- `concat(exprs)`: Concatenate strings.
- `substr(string, start, length)`: Substring.
- `lower(string)`: Convert to lowercase.
- `upper(string)`: Convert to uppercase.
- `regexp_match(string, pattern)`: Regex match.
- `regexp_replace(string, pattern, replacement)`: Regex replace.

**Aggregate Functions:**
- `sum(expr)`: Sum.
- `avg(expr)`: Average.
- `count(expr)`: Count.
- `min(expr)`: Minimum.
- `max(expr)`: Maximum.
- `stddev(expr)`: Standard deviation.

**Window Functions:**
- `first_value(expr)`: First value in window.
- `last_value(expr)`: Last value in window.
- `row_number()`: Row number within window.

### 12.5. Data Types

**`DataType::Utf8`**: String type.
**`DataType::Int64`**: 64-bit integer.
**`DataType::Float64`**: 64-bit float.
**`DataType::Boolean`**: Boolean.
**`DataType::Timestamp(unit, tz)`**: Timestamp with time unit and optional timezone.
**`DataType::Struct(fields)`**: Struct type.
**`DataType::List(field)`**: List/array type.
**`DataType::Map(key_type, value_type)`**: Map type.

## 13. Known Limitations

### 13.1. MAP Type Limitations in Grouping and Aggregation

**Status:** Unimplemented - blocked by upstream dependencies

The `attributes` column uses `Map<String, String>` type to store OpenTelemetry attributes. However, MAP types cannot be used in GROUP BY, DISTINCT, ORDER BY, or other row-comparison operations due to limitations in the Arrow/DataFusion/Iceberg stack.

**Root Cause Chain:**

1. **arrow-rs (`arrow-row` crate)**: The row format used for comparisons doesn't support MAP types.
   - Code: [`arrow-row/src/lib.rs`](https://github.com/apache/arrow-rs/blob/main/arrow-row/src/lib.rs)
   - Returns: `ArrowError::NotYetImplemented("not yet implemented: Map(...)")`

2. **DataFusion**: Any operation requiring row comparison on MAP columns fails.
   - Issue: [apache/datafusion#15428](https://github.com/apache/datafusion/issues/15428)
   - Affects: `SELECT DISTINCT`, `GROUP BY`, `ORDER BY` on MAP columns

3. **iceberg-rust**: Complex types (MAP, nested STRUCT) have limited support when reading into Arrow RecordBatch.
   - Issue: [apache/iceberg-rust#405](https://github.com/apache/iceberg-rust/issues/405)
   - Status: Work in progress, complex type support is lower priority

**Error Message:**
```
not yet implemented: Map(Field { name: "key_value", data_type: Struct([
  Field { name: "key", data_type: Utf8, ... },
  Field { name: "value", data_type: Utf8, ... }
]), ... }, false)
```

**Current Workaround:**

The `attributes` column is excluded from default label columns in metric queries:

```rust
fn build_default_label_columns(with: Vec<&str>, without: Vec<&str>) -> Vec<String> {
    // "attributes" excluded - MAP type doesn't support grouping operations
    vec!["account_id", "service_name", "trace_id", "span_id"]
        .into_iter()
        .chain(with.into_iter())
        .filter(|c| !without.contains(c))
        .map(|c| c.to_string())
        .collect()
}
```

**Impact:**

| Feature | Status | Notes |
|---------|--------|-------|
| Filter by attribute (`{foo="bar"}`) | ✅ Works | Uses `get_field()` for individual key access |
| Select attributes in log queries | ❌ Fails | MAP column in result triggers error on `collect()` |
| Group by attribute in metrics | ❌ Fails | Cannot use MAP in GROUP BY |
| Label extraction from attributes | ⚠️ Partial | Individual keys work, full map doesn't |

**Potential Solutions (Future):**

1. **Flatten to JSON string**: Convert MAP to JSON string before collection
   ```rust
   // Hypothetical - to_json() for maps
   col("attributes").to_json().alias("attributes_json")
   ```

2. **Extract specific keys**: Use `get_field()` to extract needed attributes as separate columns
   ```rust
   get_field(col("attributes"), lit("key1")).alias("attr_key1")
   ```

3. **Wait for upstream fixes**: Monitor arrow-rs and iceberg-rust for MAP support improvements

**Tracking:**
- arrow-rs: No specific issue (generic limitation in `arrow-row`)
- DataFusion: [#15428](https://github.com/apache/datafusion/issues/15428)
- iceberg-rust: [#405](https://github.com/apache/iceberg-rust/issues/405)

## 14. Performance Considerations

### 14.1. Memory Management

**Challenge:** Large result sets can consume significant memory.

**Strategies:**
- Use streaming execution where possible.
- Apply limits early in the query plan.
- Leverage DataFusion's built-in memory management and spill-to-disk capabilities.

**Example:**
```rust
// Add limit early if query has a limit clause
if let Some(limit) = query.limit {
    plan = LogicalPlanBuilder::from(plan)
        .limit(0, Some(limit))?
        .build()?;
}
```

### 14.2. Index Utilization

**Strategy:**
- Ensure filters on indexed columns (service_name, severity_text, trace_id, span_id) are pushed to scan.
- Iceberg will use min/max statistics and bloom filters to skip files.

### 14.3. Partition-Aware Processing

**Strategy:**
- Timestamp-based partitioning allows efficient pruning.
- Always extract and push down timestamp range filters.

### 14.4. Query Complexity Limits

**Strategy:**
- Set limits on query complexity to prevent resource exhaustion:
  - Maximum number of pipeline stages.
  - Maximum regex complexity.
  - Maximum aggregation cardinality.

**Example:**
```rust
const MAX_PIPELINE_STAGES: usize = 20;
const MAX_LABEL_CARDINALITY: usize = 10_000;

fn validate_query_complexity(query: &ParsedQuery) -> Result<()> {
    if query.pipeline_stages.len() > MAX_PIPELINE_STAGES {
        return Err(LogQLError::QueryTooComplex(
            "Too many pipeline stages".to_string()
        ));
    }
    Ok(())
}
```

### 14.5. Caching

**Strategy:**
- Cache compiled regex patterns.
- Cache parsed query plans for repeated queries.
- Leverage DataFusion's result caching when appropriate.

## 15. Future Enhancements

### 15.1. Template Variables

Support for Grafana-style template variables in queries.

**Example:** `{service_name="$service"}` where `$service` is replaced at runtime.

### 15.2. Subqueries

Support for nested LogQL queries.

**Example:** `{job="mysql"} | json | latency > (avg_over_time({job="mysql"} | json | unwrap latency [1h]))`

### 15.3. Additional Metric Types

- **Counters with resets:** Automatically detect and handle counter resets in `rate()`.
- **Histograms:** Support for histogram metrics and quantile calculations.

### 15.4. Query Result Caching

Cache query results for frequently executed queries to improve performance.

### 15.5. Distributed Execution

Leverage DataFusion's distributed execution capabilities for very large datasets.

**Approach:**
- Partition data across multiple nodes.
- Execute partial aggregations on each node.
- Combine results in a final aggregation step.

### 15.6. Query Optimization Rules

Implement custom optimization rules specific to LogQL patterns.

**Examples:**
- Combine adjacent filters into a single filter with AND.
- Eliminate redundant projections.
- Reorder pipeline stages for better performance.

### 15.7. Advanced Parser Support

- **Unpack:** `| unpack` - Treat each JSON field as a label.
- **Line filters with IP ranges:** `| ip("192.168.0.0/16")`.
- **Distinct:** Return unique log lines.

---

## Appendix: Complete Example

**LogQL Query:**
```logql
sum by (service_name, severity_text) (
  rate(
    {service_name=~"api.*", severity_text="ERROR"}
    | json
    | level="critical"
    | unwrap duration(response_time)
    [5m]
  )
)
```

**DataFusion Transpilation (Pseudocode):**
```rust
// Step 1: Base scan with pushed filters
let base_plan = LogicalPlanBuilder::scan_with_filters(
    "logs",
    table_source,
    None,
    vec![
        regexp_match(col("service_name"), lit("api.*")),
        col("severity_text").eq(lit("ERROR")),
    ],
)?;

// Step 2: Apply timestamp range filter [5m]
let with_time_filter = base_plan.filter(
    col("timestamp").gt_eq(lit(now - 5.minutes()))
        .and(col("timestamp").lt_eq(lit(now)))
)?;

// Step 3: Apply JSON parser
let with_json = with_time_filter.project(vec![
    col("*"),
    get_json_object(col("body"), lit("$.level")).alias("level"),
    get_json_object(col("body"), lit("$.response_time")).alias("response_time"),
])?;

// Step 4: Filter by level="critical"
let filtered = with_json.filter(col("level").eq(lit("critical")))?;

// Step 5: Unwrap duration(response_time)
let with_unwrapped = filtered.project(vec![
    col("*"),
    duration_parser_udf(col("response_time")).alias("_unwrapped_value")
])?;

// Step 6: Calculate rate using window function
let window_frame = /* 5-minute window */;
let count_window = Expr::WindowFunction(WindowFunction {
    fun: WindowFunctionDefinition::AggregateFunction(AggregateFunction::Count),
    args: vec![col("_unwrapped_value")],
    partition_by: vec![col("service_name"), col("severity_text")],
    order_by: vec![col("timestamp").sort(true, false)],
    window_frame: Some(window_frame),
    null_treatment: None,
});

let with_rate = with_unwrapped.project(vec![
    col("service_name"),
    col("severity_text"),
    col("timestamp"),
    (count_window / lit(300.0)).alias("rate")
])?;

// Step 7: Sum by (service_name, severity_text)
let final_plan = with_rate.aggregate(
    vec![col("service_name"), col("severity_text")],
    vec![sum(col("rate")).alias("sum_rate")]
)?;

final_plan.build()
```
