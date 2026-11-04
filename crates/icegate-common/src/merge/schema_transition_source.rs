use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::{
    error::Result,
    iceberg_write::CommonRecordBatchStream,
    merge::{MergeInput, MergeSource},
    schema_transition::SchemaTransition,
};

/// [`MergeSource`] that delivers every input's batches in the table's schema.
///
/// A WAL segment or a data file keeps the schema it was written with, while the
/// table may have gained trailing optional columns since. [`RowGroupsMerger`]
/// refuses inputs of different schemas, and
/// [`write_record_batches_to_parquet`](crate::iceberg_write::write_record_batches_to_parquet)
/// refuses a batch that does not carry the table's columns in the table's
/// order, so each batch goes through a [`SchemaTransition`] to `target_schema`
/// first.
///
/// [`RowGroupsMerger`]: crate::merge::RowGroupsMerger
pub struct SchemaTransitionMergeSource {
    inner: Arc<dyn MergeSource>,
    target_schema: SchemaRef,
}

impl std::fmt::Debug for SchemaTransitionMergeSource {
    // `dyn MergeSource` has no `Debug`, so the wrapped source is left out.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaTransitionMergeSource")
            .field("target_schema", &self.target_schema)
            .finish_non_exhaustive()
    }
}

impl SchemaTransitionMergeSource {
    /// Wrap `inner` so its batches come out in `target_schema`.
    #[must_use]
    pub fn new(inner: Arc<dyn MergeSource>, target_schema: SchemaRef) -> Self {
        Self { inner, target_schema }
    }
}

#[async_trait]
impl MergeSource for SchemaTransitionMergeSource {
    /// Open `input` through the wrapped source and bring each batch to the
    /// target schema.
    ///
    /// The rule is built for the first batch and rebuilt only when a batch's
    /// schema differs from the previous batch's. Schemas are compared by value:
    /// a parquet reader gives every batch a schema `Arc` of its own, so `Arc`
    /// identity alone would rebuild the rule for every batch. A batch that cannot take
    /// the target schema fails the stream with
    /// [`CommonError::SchemaTransition`](crate::error::CommonError::SchemaTransition).
    async fn open(&self, input: &MergeInput, cancel: &CancellationToken) -> Result<CommonRecordBatchStream> {
        let batches = self.inner.open(input, cancel).await?;
        let target_schema = Arc::clone(&self.target_schema);
        let mut transition: Option<SchemaTransition> = None;
        let transitioned = batches.map(move |batch| {
            let batch = batch?;
            let mut rule = match transition.take() {
                Some(rule)
                    if Arc::ptr_eq(rule.source_schema(), batch.schema_ref())
                        || rule.source_schema() == batch.schema_ref() =>
                {
                    rule
                }
                _ => SchemaTransition::try_new(batch.schema(), Arc::clone(&target_schema))?,
            };
            let rebuilt = rule.apply(batch);
            transition = Some(rule);
            rebuilt
        });
        Ok(transitioned.boxed())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::{
        array::{Array, Int64Array, ListArray},
        datatypes::Int64Type,
        record_batch::RecordBatch,
    };
    use futures::TryStreamExt;

    use super::*;
    use crate::{
        error::CommonError,
        merge::{
            MergePosition,
            sort_key::{RowGroupBoundaryKey, RowGroupBoundaryRange},
        },
        schema_transition::{
            SchemaTransitionError,
            test_support::{id_batch, id_field, name_field, schema, tags_field},
        },
    };

    /// Merge source serving each input as its batches, each of its own schema.
    struct FixedBatchesSource {
        batches: HashMap<MergePosition, Vec<RecordBatch>>,
    }

    #[async_trait]
    impl MergeSource for FixedBatchesSource {
        async fn open(&self, input: &MergeInput, _cancel: &CancellationToken) -> Result<CommonRecordBatchStream> {
            let batches = self
                .batches
                .get(&input.position)
                .cloned()
                .ok_or_else(|| CommonError::Write(format!("no batch at position {}", input.position)))?;
            Ok(futures::stream::iter(batches.into_iter().map(Ok)).boxed())
        }
    }

    /// An input the fixture source resolves by position alone; the boundary range is never read.
    fn merge_input(position: MergePosition) -> MergeInput {
        let boundary_range = RowGroupBoundaryRange {
            names: Arc::from([]),
            min_key: RowGroupBoundaryKey::new(Vec::new()),
            max_key: RowGroupBoundaryKey::new(Vec::new()),
        };
        MergeInput::new(position, 0, boundary_range)
    }

    #[tokio::test]
    async fn inputs_of_different_schemas_come_out_in_the_target_schema() {
        let previous = MergePosition::new(0);
        let current = MergePosition::new(1);
        let target = schema(vec![id_field(), tags_field()]);
        let current_batch = RecordBatch::try_new(
            Arc::clone(&target),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(5),
                ])])),
            ],
        )
        .expect("current batch");
        let source = SchemaTransitionMergeSource::new(
            Arc::new(FixedBatchesSource {
                batches: HashMap::from([(previous, vec![id_batch(&[1])]), (current, vec![current_batch])]),
            }),
            Arc::clone(&target),
        );
        let cancel = CancellationToken::new();

        for position in [previous, current] {
            let batches: Vec<RecordBatch> = source
                .open(&merge_input(position), &cancel)
                .await
                .expect("open")
                .try_collect()
                .await
                .expect("read");
            assert_eq!(batches.len(), 1);
            assert_eq!(
                batches[0].schema(),
                target,
                "input {position} must take the target schema"
            );
        }
    }

    /// A parquet reader gives every batch a schema `Arc` of its own: batches of one schema under
    /// distinct `Arc`s share one rule, which shows as their NULL columns sharing one buffer.
    #[tokio::test]
    async fn batches_of_one_schema_under_distinct_arcs_share_one_rule() {
        let position = MergePosition::new(0);
        let first = id_batch(&[1, 2]);
        let second = id_batch(&[3, 4]);
        assert!(
            !Arc::ptr_eq(first.schema_ref(), second.schema_ref()),
            "the fixture batches carry distinct schema Arcs"
        );
        let source = SchemaTransitionMergeSource::new(
            Arc::new(FixedBatchesSource {
                batches: HashMap::from([(position, vec![first, second])]),
            }),
            schema(vec![id_field(), name_field()]),
        );

        let batches: Vec<RecordBatch> = source
            .open(&merge_input(position), &CancellationToken::new())
            .await
            .expect("open")
            .try_collect()
            .await
            .expect("read");

        assert_eq!(batches.len(), 2);
        assert!(
            batches[0].column(1).to_data().ptr_eq(&batches[1].column(1).to_data()),
            "the second batch reuses the rule, and with it the NULL array, of the first"
        );
    }

    #[tokio::test]
    async fn a_batch_that_cannot_take_the_target_schema_fails_the_stream_with_its_reason() {
        let position = MergePosition::new(0);
        let source = SchemaTransitionMergeSource::new(
            Arc::new(FixedBatchesSource {
                batches: HashMap::from([(position, vec![id_batch(&[1])])]),
            }),
            schema(vec![name_field()]),
        );

        let error = source
            .open(&merge_input(position), &CancellationToken::new())
            .await
            .expect("open")
            .try_collect::<Vec<_>>()
            .await
            .err();

        assert!(
            matches!(
                error,
                Some(CommonError::SchemaTransition(SchemaTransitionError::ColumnNotInTable { ref column }))
                    if column == "id"
            ),
            "got {error:?}"
        );
    }
}
