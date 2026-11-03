use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, new_null_array},
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};

use crate::error::{CommonError, Result};

/// Why a batch cannot take the table's schema.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaTransitionError {
    /// The batch carries a column the table schema does not have — the table
    /// was loaded before the column was added, or the column was never added.
    #[error("column '{column}' is not in the table schema")]
    ColumnNotInTable {
        /// Name of the column.
        column: String,
    },
    /// The table requires a column the batch does not carry; NULL cannot fill it.
    #[error("required column '{column}' is missing from the batch")]
    RequiredColumnMissing {
        /// Name of the column.
        column: String,
    },
    /// The column exists on both sides with a different Arrow type or nullability.
    #[error("column '{column}' differs in type or nullability from the table schema")]
    ColumnTypeChanged {
        /// Name of the column.
        column: String,
    },
}

/// Where one column of the table schema comes from.
#[derive(Debug)]
enum ColumnOrigin {
    /// The column at this index of the source batch, taken as is.
    Source(usize),
    /// Absent from the source and optional in the table: filled with NULL.
    /// Holds the NULL array of the longest batch seen so far; shorter batches
    /// take a zero-copy slice of it.
    Absent { null_array: Option<ArrayRef> },
}

/// Rule turning batches of one source schema into batches of the table schema.
///
/// Columns come out in the table's order; a column the source lacks and the
/// table holds as optional is filled with NULL. A source column is taken only
/// when its Arrow `DataType` and nullability equal the table column's; nothing
/// is cast.
#[derive(Debug)]
pub struct SchemaTransition {
    source_schema: SchemaRef,
    target_schema: SchemaRef,
    /// `None` when the source schema equals the target schema: batches pass through.
    column_origins: Option<Vec<ColumnOrigin>>,
}

impl SchemaTransition {
    /// Build the rule taking `source_schema` batches to `target_schema`.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaTransitionError::ColumnNotInTable`] when the source has a
    /// column the target lacks — reported ahead of any other difference — then
    /// [`SchemaTransitionError::ColumnTypeChanged`] or
    /// [`SchemaTransitionError::RequiredColumnMissing`] for the first target
    /// column that cannot be produced.
    pub fn try_new(
        source_schema: SchemaRef,
        target_schema: SchemaRef,
    ) -> std::result::Result<Self, SchemaTransitionError> {
        if source_schema == target_schema {
            return Ok(Self {
                source_schema,
                target_schema,
                column_origins: None,
            });
        }

        let source_index_by_name: HashMap<&str, usize> = source_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| (field.name().as_str(), index))
            .collect();
        let mut column_origins = Vec::with_capacity(target_schema.fields().len());
        let mut matched_columns = 0usize;
        // The target pass runs to the end so it can count the source columns it
        // takes; an extra source column outranks the first target-side error.
        let mut target_error = None;
        for field in target_schema.fields() {
            let origin = match source_index_by_name.get(field.name().as_str()) {
                Some(&index) => {
                    matched_columns += 1;
                    let source_field = &source_schema.fields()[index];
                    if source_field.data_type() != field.data_type()
                        || source_field.is_nullable() != field.is_nullable()
                    {
                        target_error.get_or_insert_with(|| SchemaTransitionError::ColumnTypeChanged {
                            column: field.name().clone(),
                        });
                    }
                    ColumnOrigin::Source(index)
                }
                None if field.is_nullable() => ColumnOrigin::Absent { null_array: None },
                None => {
                    target_error.get_or_insert_with(|| SchemaTransitionError::RequiredColumnMissing {
                        column: field.name().clone(),
                    });
                    ColumnOrigin::Absent { null_array: None }
                }
            };
            column_origins.push(origin);
        }

        if matched_columns != source_schema.fields().len() {
            // Failing path only: name the source column no target column took —
            // one the target lacks, or a repeat of a name the target took once.
            let extra_column = source_schema
                .fields()
                .iter()
                .enumerate()
                .find(|(index, field)| {
                    source_index_by_name.get(field.name().as_str()) != Some(index)
                        || target_schema.field_with_name(field.name()).is_err()
                })
                .map(|(_, field)| field.name().clone());
            if let Some(column) = extra_column {
                return Err(SchemaTransitionError::ColumnNotInTable { column });
            }
        }
        if let Some(error) = target_error {
            return Err(error);
        }

        Ok(Self {
            source_schema,
            target_schema,
            column_origins: Some(column_origins),
        })
    }

    /// Schema of the batches this rule accepts.
    #[must_use]
    pub const fn source_schema(&self) -> &SchemaRef {
        &self.source_schema
    }

    /// Rebuild `batch` in the target schema.
    ///
    /// `batch` must carry [`Self::source_schema`]; a pass-through rule returns
    /// it unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`CommonError::Write`] when `batch` lacks a column the rule
    /// takes from the source, or when Arrow refuses the rebuilt batch.
    pub fn apply(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let Some(column_origins) = self.column_origins.as_mut() else {
            return Ok(batch);
        };

        let row_count = batch.num_rows();
        let mut columns = Vec::with_capacity(column_origins.len());
        for (field, origin) in self.target_schema.fields().iter().zip(column_origins.iter_mut()) {
            let column = match origin {
                ColumnOrigin::Source(index) => Arc::clone(batch.columns().get(*index).ok_or_else(|| {
                    CommonError::Write(format!(
                        "batch has no column at index {index} for '{}' of the table schema",
                        field.name()
                    ))
                })?),
                ColumnOrigin::Absent { null_array } => {
                    let nulls = match null_array.take() {
                        Some(nulls) if nulls.len() >= row_count => nulls,
                        _ => new_null_array(field.data_type(), row_count),
                    };
                    let column = nulls.slice(0, row_count);
                    *null_array = Some(nulls);
                    column
                }
            };
            columns.push(column);
        }

        RecordBatch::try_new_with_options(
            Arc::clone(&self.target_schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .map_err(|err| CommonError::Write(format!("failed to rebuild batch in the table schema: {err}")))
    }
}

/// Schemas and batches shared by the tests of [`SchemaTransition`] and of its users.
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };

    /// A schema of `fields`, under an `Arc` of its own.
    pub(crate) fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    /// Required `Int64` column `id`.
    pub(crate) fn id_field() -> Field {
        Field::new("id", DataType::Int64, false)
    }

    /// Optional `Utf8` column `name`.
    pub(crate) fn name_field() -> Field {
        Field::new("name", DataType::Utf8, true)
    }

    /// Optional `List<Int64>` column `tags`.
    pub(crate) fn tags_field() -> Field {
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            true,
        )
    }

    /// A batch of the `id` column alone, under a schema `Arc` of its own.
    pub(crate) fn id_batch(ids: &[i64]) -> RecordBatch {
        RecordBatch::try_new(schema(vec![id_field()]), vec![Arc::new(Int64Array::from(ids.to_vec()))])
            .expect("id batch")
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, Int64Array, ListArray, StringArray},
        datatypes::{DataType, Field},
    };

    use super::{
        test_support::{id_batch, id_field, name_field, schema, tags_field},
        *,
    };

    fn try_transition(
        source: Vec<Field>,
        target: Vec<Field>,
    ) -> std::result::Result<SchemaTransition, SchemaTransitionError> {
        SchemaTransition::try_new(schema(source), schema(target))
    }

    fn column_names(batch: &RecordBatch) -> Vec<String> {
        batch.schema().fields().iter().map(|field| field.name().clone()).collect()
    }

    #[test]
    fn equal_schemas_pass_the_batch_through() {
        let batch = id_batch(&[1, 2]);
        let mut transition = SchemaTransition::try_new(batch.schema(), schema(vec![id_field()])).expect("transition");

        let result = transition.apply(batch.clone()).expect("apply");

        assert!(Arc::ptr_eq(result.column(0), batch.column(0)));
        assert_eq!(result.schema(), batch.schema());
    }

    #[test]
    fn an_absent_optional_column_is_filled_with_null() {
        let mut transition =
            try_transition(vec![id_field()], vec![id_field(), name_field(), tags_field()]).expect("transition");
        let batch = id_batch(&[1, 2, 3]);

        let result = transition.apply(batch.clone()).expect("apply");

        assert_eq!(column_names(&result), vec!["id", "name", "tags"]);
        assert!(
            Arc::ptr_eq(result.column(0), batch.column(0)),
            "a source column is taken as is"
        );
        let names = result.column(1).as_any().downcast_ref::<StringArray>().expect("name is Utf8");
        assert_eq!(names.len(), 3);
        assert_eq!(names.null_count(), 3);
        let tags = result.column(2).as_any().downcast_ref::<ListArray>().expect("tags is List");
        assert_eq!(tags.len(), 3);
        assert_eq!(tags.null_count(), 3);
    }

    #[test]
    fn result_columns_follow_the_table_order() {
        let source = schema(vec![name_field(), id_field()]);
        let batch = RecordBatch::try_new(
            Arc::clone(&source),
            vec![
                Arc::new(StringArray::from(vec![Some("a")])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .expect("source batch");
        let mut transition =
            SchemaTransition::try_new(source, schema(vec![id_field(), name_field()])).expect("transition");

        let result = transition.apply(batch).expect("apply");

        assert_eq!(column_names(&result), vec!["id", "name"]);
        let ids = result.column(0).as_any().downcast_ref::<Int64Array>().expect("id is Int64");
        assert_eq!(ids.values().to_vec(), vec![7]);
    }

    #[test]
    fn a_source_column_missing_from_the_table_is_refused() {
        let error = try_transition(vec![id_field(), name_field()], vec![id_field()]).err();
        assert_eq!(
            error,
            Some(SchemaTransitionError::ColumnNotInTable {
                column: "name".to_string()
            })
        );
    }

    #[test]
    fn a_missing_required_column_is_refused() {
        let error = try_transition(vec![name_field()], vec![id_field(), name_field()]).err();
        assert_eq!(
            error,
            Some(SchemaTransitionError::RequiredColumnMissing {
                column: "id".to_string()
            })
        );
    }

    #[test]
    fn a_column_of_another_type_or_nullability_is_refused() {
        let changed_type = try_transition(vec![Field::new("id", DataType::Int32, false)], vec![id_field()]).err();
        let changed_nullability = try_transition(vec![Field::new("id", DataType::Int64, true)], vec![id_field()]).err();
        let expected = Some(SchemaTransitionError::ColumnTypeChanged {
            column: "id".to_string(),
        });
        assert_eq!(changed_type, expected);
        assert_eq!(changed_nullability, expected);
    }

    /// The Shifter drops its cached table only for an extra column, so that verdict must win when
    /// the same schemas also differ in a way that alone would end the task.
    #[test]
    fn an_extra_column_is_reported_ahead_of_any_other_difference() {
        let extra_field = || Field::new("extra", DataType::Utf8, true);
        let expected = Some(SchemaTransitionError::ColumnNotInTable {
            column: "extra".to_string(),
        });

        let with_a_retyped_column = try_transition(
            vec![Field::new("id", DataType::Int32, false), extra_field()],
            vec![id_field()],
        )
        .err();
        let with_a_missing_required_column = try_transition(vec![extra_field()], vec![id_field()]).err();

        assert_eq!(with_a_retyped_column, expected);
        assert_eq!(with_a_missing_required_column, expected);
    }

    #[test]
    fn the_null_array_serves_a_shorter_batch_after_a_longer_one_and_grows_for_a_longer_one() {
        let mut transition = try_transition(vec![id_field()], vec![id_field(), name_field()]).expect("transition");

        for ids in [&[1, 2, 3][..], &[4][..], &[5, 6, 7, 8, 9][..]] {
            let result = transition.apply(id_batch(ids)).expect("apply");
            assert_eq!(result.num_rows(), ids.len());
            assert_eq!(result.column(1).len(), ids.len());
            assert_eq!(result.column(1).null_count(), ids.len());
        }
    }
}
