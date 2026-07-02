//! Global (non-tenant) LLM pricing reference table.
//!
//! Registers `reference.llm.pricing` as an in-memory DataFusion catalog so
//! Flight SQL queries can JOIN `iceberg.icegate.operations` against per-model
//! pricing. This catalog is NOT tenant-scoped: pricing is global reference
//! data, identical for every tenant, and carries no `tenant_id` column (so it
//! must never be registered under the tenant-wrapped `iceberg` catalog).

use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::col;

/// Catalog name for global reference data (never tenant-wrapped).
pub const REFERENCE_CATALOG_NAME: &str = "reference";
/// Schema (namespace) holding LLM reference tables.
pub const REFERENCE_LLM_SCHEMA: &str = "llm";
/// Pricing table name; FQN is `reference.llm.pricing`.
pub const PRICING_TABLE_NAME: &str = "pricing";

/// Arrow schema for `reference.llm.pricing`. Cache columns are nullable
/// (not every model supports prompt caching); the rest are required.
fn pricing_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("input_usd_per_1m", DataType::Float64, false),
        Field::new("output_usd_per_1m", DataType::Float64, false),
        Field::new("cache_read_usd_per_1m", DataType::Float64, true),
        Field::new("cache_write_usd_per_1m", DataType::Float64, true),
        Field::new("currency", DataType::Utf8, false),
    ]))
}

/// Build the seed `RecordBatch` of pricing rows. Illustrative public list rates
/// (USD per 1M tokens) — replace with a maintained rate card. The
/// `(provider, model)` pairs mirror every combination emitted by icegen's LLM
/// trace generator (`otel-log-generator otel --signal traces`), so the synthetic
/// `operations` rows it produces JOIN cleanly against this table. `provider`
/// values MUST match icegate's normalized `operations.provider_name`
/// (`gen_ai.provider.name` ‖ `gen_ai.system` ‖ `llm.system`), which icegate stores
/// verbatim. `cache_*` rates are left NULL for models without published
/// prompt-cache pricing, and `cache_write` is NULL for providers that charge no
/// separate cache-write fee.
///
/// Rows are kept sorted by `(provider, model)` (enforced by
/// `pricing_batch_is_sorted_by_provider_and_model`) so the reference table reads
/// deterministically.
///
/// # Errors
///
/// Returns a `DataFusionError` if the columns do not line up with the schema.
fn pricing_batch() -> DFResult<RecordBatch> {
    let schema = pricing_schema();
    let provider = StringArray::from(vec![
        "anthropic",
        "anthropic",
        "anthropic",
        "aws.bedrock",
        "aws.bedrock",
        "cohere",
        "cohere",
        "gcp.vertex_ai",
        "gcp.vertex_ai",
        "mistral_ai",
        "mistral_ai",
        "openai",
        "openai",
        "openai",
        "openai",
    ]);
    let model = StringArray::from(vec![
        "claude-3-5-haiku",
        "claude-3-5-sonnet",
        "claude-opus-4",
        "amazon.titan-text",
        "anthropic.claude-3-5-sonnet",
        "command-r",
        "command-r-plus",
        "gemini-1.5-pro",
        "gemini-2.0-flash",
        "mistral-large",
        "mistral-small",
        "gpt-4.1",
        "gpt-4o",
        "gpt-4o-mini",
        "o3",
    ]);
    let input = Float64Array::from(vec![
        0.8, 3.0, 15.0, 0.2, 3.0, 0.15, 2.5, 1.25, 0.1, 2.0, 0.2, 2.0, 2.5, 0.15, 2.0,
    ]);
    let output = Float64Array::from(vec![
        4.0, 15.0, 75.0, 0.6, 15.0, 0.6, 10.0, 5.0, 0.4, 6.0, 0.6, 8.0, 10.0, 0.6, 8.0,
    ]);
    let cache_read = Float64Array::from(vec![
        Some(0.08),
        Some(0.3),
        Some(1.5),
        None,
        Some(0.3),
        None,
        None,
        None,
        None,
        None,
        None,
        Some(0.5),
        Some(1.25),
        Some(0.075),
        Some(0.5),
    ]);
    let cache_write = Float64Array::from(vec![
        Some(1.0),
        Some(3.75),
        Some(18.75),
        None,
        Some(3.75),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ]);
    let currency = StringArray::from(vec!["USD"; 15]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(provider),
            Arc::new(model),
            Arc::new(input),
            Arc::new(output),
            Arc::new(cache_read),
            Arc::new(cache_write),
            Arc::new(currency),
        ],
    )
    .map_err(Into::into)
}

/// Build the `reference` catalog provider containing `llm.pricing`.
///
/// The table is declared sorted by `(provider, model)` ascending, matching the
/// seed batch (see [`pricing_batch`]), so the planner can elide an
/// `ORDER BY provider, model`. The declaration is a promise the planner trusts;
/// `pricing_batch_is_sorted_by_provider_and_model` keeps that promise true.
///
/// # Errors
///
/// Returns a `DataFusionError` if the seed batch, `MemTable`, or registration
/// fails.
pub fn reference_catalog() -> DFResult<Arc<dyn CatalogProvider>> {
    let schema = pricing_schema();
    let table = MemTable::try_new(schema, vec![vec![pricing_batch()?]])?.with_sort_order(vec![vec![
        col("provider").sort(true, false),
        col("model").sort(true, false),
    ]]);
    let llm_schema = MemorySchemaProvider::new();
    llm_schema.register_table(PRICING_TABLE_NAME.to_string(), Arc::new(table))?;
    let catalog = MemoryCatalogProvider::new();
    catalog.register_schema(REFERENCE_LLM_SCHEMA, Arc::new(llm_schema) as Arc<dyn SchemaProvider>)?;
    Ok(Arc::new(catalog))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Array;

    use super::*;

    #[test]
    fn pricing_batch_matches_schema_and_has_rows() {
        let batch = pricing_batch().expect("batch builds");
        assert_eq!(batch.schema(), pricing_schema());
        assert_eq!(batch.num_rows(), 15);
    }

    #[test]
    fn pricing_batch_is_sorted_by_provider_and_model() {
        let batch = pricing_batch().expect("batch builds");
        let providers = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("provider is Utf8");
        let models = batch.column(1).as_any().downcast_ref::<StringArray>().expect("model is Utf8");
        let keys: Vec<(&str, &str)> = (0..batch.num_rows()).map(|i| (providers.value(i), models.value(i))).collect();
        let mut expected = keys.clone();
        expected.sort_unstable();
        assert_eq!(keys, expected, "pricing rows must be sorted by (provider, model)");
    }

    #[test]
    fn reference_catalog_exposes_llm_pricing() {
        let catalog = reference_catalog().expect("catalog builds");
        let schema = catalog.schema(REFERENCE_LLM_SCHEMA).expect("llm schema present");
        assert!(schema.table_exist(PRICING_TABLE_NAME));
    }
}
