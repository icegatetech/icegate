//! The provider of a table read from its committed Iceberg snapshot only, with no WAL topic.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNode;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

/// A table without a WAL topic, served by `inner` (the static Iceberg provider), whose scan is
/// handed only filters on scalar values.
///
/// A null check (`IS NULL`, `IS NOT NULL`) of a struct, list or MAP column is answered
/// `Unsupported` and never reaches `inner`: DataFusion evaluates it above the scan. The
/// regression test `exists_on_a_struct_column_is_a_null_check` (`query_dsl::lower`) fails when
/// such a filter reaches the scan. Every other filter gets `inner`'s answer.
#[derive(Debug)]
pub(super) struct IcebergOnlyTableProvider {
    /// The provider reading the committed snapshot.
    inner: Arc<dyn TableProvider>,
}

impl IcebergOnlyTableProvider {
    /// Serve the table of `inner`.
    pub(super) fn new(inner: Arc<dyn TableProvider>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl TableProvider for IcebergOnlyTableProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let schema = self.inner.schema();
        let is_held_back = filters
            .iter()
            .map(|filter| has_nested_null_check(filter, &schema))
            .collect::<DataFusionResult<Vec<bool>>>()?;
        let pushable_filters: Vec<&Expr> = filters
            .iter()
            .zip(&is_held_back)
            .filter(|(_, is_held_back)| !**is_held_back)
            .map(|(filter, _)| *filter)
            .collect();
        let mut inner_answers = self.inner.supports_filters_pushdown(&pushable_filters)?.into_iter();
        is_held_back
            .into_iter()
            .map(|is_held_back| {
                if is_held_back {
                    Ok(TableProviderFilterPushDown::Unsupported)
                } else {
                    inner_answers.next().ok_or_else(|| {
                        DataFusionError::Internal("the inner provider answered fewer filters than it was asked".into())
                    })
                }
            })
            .collect()
    }
}

/// Whether `filter` holds a null check (`IS NULL`, `IS NOT NULL`) of a struct, list or MAP
/// column of `schema`.
fn has_nested_null_check(filter: &Expr, schema: &Schema) -> DataFusionResult<bool> {
    let is_nested_column = |operand: &Expr| {
        matches!(operand, Expr::Column(column)
            if schema.field_with_name(&column.name).is_ok_and(|field| field.data_type().is_nested()))
    };
    filter.exists(|node| {
        Ok(matches!(node, Expr::IsNull(operand) | Expr::IsNotNull(operand) if is_nested_column(operand)))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::{Session, TableProvider};
    use datafusion::datasource::MemTable;
    use datafusion::error::Result as DataFusionResult;
    use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType, col, lit};
    use datafusion::physical_plan::ExecutionPlan;

    use super::IcebergOnlyTableProvider;

    /// Answers `Inexact` for every filter it is asked about, and records how many that was.
    #[derive(Debug)]
    struct InexactPushdownTable {
        schema: SchemaRef,
        asked_filter_counts: Mutex<Vec<usize>>,
    }

    #[async_trait]
    impl TableProvider for InexactPushdownTable {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            MemTable::try_new(Arc::clone(&self.schema), vec![vec![]])?
                .scan(state, projection, filters, limit)
                .await
        }

        fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
            self.asked_filter_counts.lock().unwrap().push(filters.len());
            Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
        }
    }

    /// A null check on a struct, list or MAP column is answered `Unsupported` without asking the
    /// inner provider; every other filter, a null check on a scalar column included, gets the
    /// inner provider's answer, in filter order.
    #[test]
    fn a_null_check_on_a_nested_column_is_kept_above_the_inner_scan() {
        let entries = Field::new(
            "key_value",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new(
                "status",
                DataType::Struct(vec![Field::new("code", DataType::Int32, true)].into()),
                true,
            ),
            Field::new(
                "events",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("attributes", DataType::Map(Arc::new(entries), false), true),
        ]));
        let inner = Arc::new(InexactPushdownTable {
            schema,
            asked_filter_counts: Mutex::new(Vec::new()),
        });
        let provider = IcebergOnlyTableProvider::new(Arc::clone(&inner) as Arc<dyn TableProvider>);
        let filters = [
            col("status").is_not_null(),
            col("body").eq(lit("x")),
            Expr::Not(Box::new(col("events").is_null())),
            col("attributes").is_null(),
            col("body").is_null(),
        ];

        let answers = provider.supports_filters_pushdown(&filters.iter().collect::<Vec<_>>()).unwrap();

        assert_eq!(
            answers,
            vec![
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Unsupported,
                TableProviderFilterPushDown::Inexact,
            ]
        );
        assert_eq!(*inner.asked_filter_counts.lock().unwrap(), vec![2]);
    }
}
