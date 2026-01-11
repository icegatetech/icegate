//! User-defined aggregate functions for LogQL operations in DataFusion.
//!
//! This module provides UDAFs for LogQL-specific aggregation operations,
//! specifically array intersection for label matcher operations.

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, ListArray, TimestampMicrosecondArray},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field, TimeUnit},
    },
    common::{Result, ScalarValue},
    error::DataFusionError,
    logical_expr::{
        Accumulator, AggregateUDFImpl, Signature, Volatility,
        function::{AccumulatorArgs, StateFieldsArgs},
    },
};

// ============================================================================
// ArrayIntersectAgg UDAF
// ============================================================================

/// UDAF: `array_intersect_agg(array_column)` - aggregates multiple arrays and returns their intersection.
///
/// # Purpose
/// Used by label matchers to find values present in ALL input arrays.
/// Each row contributes a sorted array, and the aggregate returns
/// only values present in every input array.
///
/// # Arguments
/// 1. `array`: `List<Timestamp(Microsecond)>` - input arrays of timestamps
///
/// # Returns
/// `List<Timestamp(Microsecond)>` - intersection of all input arrays
///
/// # Algorithm
/// - State: Current intersection result (starts as first array, then narrows)
/// - Update: intersection = `current_state` ∩ `new_array` (two-pointer merge)
/// - Merge: intersection of partial results from different partitions
/// - Evaluate: Return final intersection array
///
/// # Complexity
/// O(n × m) where n = number of arrays, m = average array length
/// Optimized for sorted arrays using two-pointer technique
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayIntersectAgg {
    signature: Signature,
}

impl Default for ArrayIntersectAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayIntersectAgg {
    /// Creates a new `ArrayIntersectAgg` UDAF.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ArrayIntersectAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_intersect_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Return same type as input (List<Timestamp>)
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArrayIntersectAccumulator::new()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        // State is a single List<Timestamp> field containing the current intersection
        Ok(vec![Arc::new(Field::new(
            "intersection",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ))),
            true,
        ))])
    }
}

/// Accumulator for `array_intersect_agg`.
#[derive(Debug)]
struct ArrayIntersectAccumulator {
    /// Current intersection result as raw i64 timestamps (None = first array not yet seen).
    current_intersection: Option<Vec<i64>>,
}

impl ArrayIntersectAccumulator {
    const fn new() -> Self {
        Self {
            current_intersection: None,
        }
    }

    /// Intersect a sorted timestamp array with a sorted Vec using two-pointer algorithm.
    /// Extracts values from the array on-demand, avoiding intermediate Vec allocation.
    /// Both inputs are assumed to be sorted (guaranteed by `date_grid` UDF output).
    fn intersect_ts_array_with_vec(ts_array: &TimestampMicrosecondArray, vec: &[i64]) -> Vec<i64> {
        let mut result = Vec::with_capacity(ts_array.len().min(vec.len()));
        let mut i = 0;
        let mut j = 0;

        while i < ts_array.len() && j < vec.len() {
            let ts_val = ts_array.value(i);
            let vec_val = vec[j];

            match ts_val.cmp(&vec_val) {
                std::cmp::Ordering::Equal => {
                    result.push(ts_val);
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
            }
        }

        result
    }

    /// Convert a Vec of timestamps to a List scalar value.
    fn vec_to_list_scalar(vec: Option<&Vec<i64>>) -> ScalarValue {
        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ));

        let ts_array = vec.map_or_else(
            || Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new())) as ArrayRef,
            |v| Arc::new(TimestampMicrosecondArray::from(v.clone())) as ArrayRef,
        );

        let offsets = OffsetBuffer::from_lengths([ts_array.len()]);
        let list_array = ListArray::new(field, offsets, ts_array, None);
        ScalarValue::List(Arc::new(list_array))
    }

    /// Helper to process a `ListArray` of timestamp arrays and update the intersection.
    /// Used by both `update_batch` and `merge_batch`.
    fn process_list_array(&mut self, list_array: &ListArray) -> Result<()> {
        for row_idx in 0..list_array.len() {
            // Skip null rows
            if list_array.is_null(row_idx) {
                continue;
            }

            // Extract timestamp array from this row
            let timestamps_array = list_array.value(row_idx);
            let ts_array = timestamps_array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| DataFusionError::Plan("Expected TimestampMicrosecondArray".to_string()))?;

            // Update intersection
            self.current_intersection = Some(self.current_intersection.as_ref().map_or_else(
                || (0..ts_array.len()).map(|i| ts_array.value(i)).collect(),
                |current| Self::intersect_ts_array_with_vec(ts_array, current),
            ));
        }

        Ok(())
    }
}

impl Accumulator for ArrayIntersectAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        // Extract ListArray of timestamps
        let list_array = values[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected ListArray".to_string()))?;

        self.process_list_array(list_array)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Return the final intersection as a List scalar
        Ok(Self::vec_to_list_scalar(self.current_intersection.as_ref()))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .current_intersection
                .as_ref()
                .map_or(0, |v| std::mem::size_of::<i64>() * v.capacity())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Serialize current intersection state
        let state_value = Self::vec_to_list_scalar(self.current_intersection.as_ref());
        Ok(vec![state_value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Merge partial intersection results from different partitions
        // state[0] contains intersection arrays from other accumulators

        let list_array = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("Expected ListArray in state".to_string()))?;

        self.process_list_array(list_array)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::Accumulator;

    use super::*;

    #[test]
    fn test_array_intersect_agg_two_arrays() {
        // Test intersection of two arrays
        let mut acc = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // First array: [100, 200, 300]
        let arr1 = TimestampMicrosecondArray::from(vec![100i64, 200, 300]);
        let list1 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr1.len()]),
            Arc::new(arr1) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list1) as ArrayRef]).unwrap();

        // Second array: [200, 300, 400]
        let arr2 = TimestampMicrosecondArray::from(vec![200i64, 300, 400]);
        let list2 = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr2.len()]),
            Arc::new(arr2) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list2) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Intersection should be [200, 300]
                assert_eq!(ts_arr.len(), 2);
                assert_eq!(ts_arr.value(0), 200);
                assert_eq!(ts_arr.value(1), 300);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_three_arrays() {
        // Test intersection of three arrays
        let mut acc = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // Array 1: [100, 200, 300, 400]
        let arr1 = TimestampMicrosecondArray::from(vec![100i64, 200, 300, 400]);
        let list1 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr1.len()]),
            Arc::new(arr1) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list1) as ArrayRef]).unwrap();

        // Array 2: [200, 300, 400, 500]
        let arr2 = TimestampMicrosecondArray::from(vec![200i64, 300, 400, 500]);
        let list2 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr2.len()]),
            Arc::new(arr2) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list2) as ArrayRef]).unwrap();

        // Array 3: [300, 400, 500, 600]
        let arr3 = TimestampMicrosecondArray::from(vec![300i64, 400, 500, 600]);
        let list3 = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr3.len()]),
            Arc::new(arr3) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list3) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Intersection of all three should be [300, 400]
                assert_eq!(ts_arr.len(), 2);
                assert_eq!(ts_arr.value(0), 300);
                assert_eq!(ts_arr.value(1), 400);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_no_intersection() {
        // Test arrays with no common elements
        let mut acc = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // Array 1: [100, 200]
        let arr1 = TimestampMicrosecondArray::from(vec![100i64, 200]);
        let list1 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr1.len()]),
            Arc::new(arr1) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list1) as ArrayRef]).unwrap();

        // Array 2: [300, 400]
        let arr2 = TimestampMicrosecondArray::from(vec![300i64, 400]);
        let list2 = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr2.len()]),
            Arc::new(arr2) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list2) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // No intersection
                assert_eq!(ts_arr.len(), 0);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_single_array() {
        // Test with only one array - should return that array
        let mut acc = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        let arr = TimestampMicrosecondArray::from(vec![100i64, 200, 300]);
        let list = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr.len()]),
            Arc::new(arr) as ArrayRef,
            None,
        );
        acc.update_batch(&[Arc::new(list) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Should return the same array
                assert_eq!(ts_arr.len(), 3);
                assert_eq!(ts_arr.value(0), 100);
                assert_eq!(ts_arr.value(1), 200);
                assert_eq!(ts_arr.value(2), 300);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_empty_array() {
        // Test with empty arrays
        let mut acc = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // Empty array
        let arr = TimestampMicrosecondArray::from(Vec::<i64>::new());
        let list = ListArray::new(field, OffsetBuffer::from_lengths([0]), Arc::new(arr) as ArrayRef, None);
        acc.update_batch(&[Arc::new(list) as ArrayRef]).unwrap();

        let result = acc.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Empty result
                assert_eq!(ts_arr.len(), 0);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_merge_batch() {
        // Test merge_batch for distributed execution
        let mut acc1 = ArrayIntersectAccumulator::new();
        let mut acc2 = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // Acc1: intersection of [100, 200, 300] and [200, 300, 400] = [200, 300]
        let arr1 = TimestampMicrosecondArray::from(vec![100i64, 200, 300]);
        let list1 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr1.len()]),
            Arc::new(arr1) as ArrayRef,
            None,
        );
        acc1.update_batch(&[Arc::new(list1) as ArrayRef]).unwrap();

        let arr2 = TimestampMicrosecondArray::from(vec![200i64, 300, 400]);
        let list2 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr2.len()]),
            Arc::new(arr2) as ArrayRef,
            None,
        );
        acc1.update_batch(&[Arc::new(list2) as ArrayRef]).unwrap();

        // Acc2: intersection of [200, 300, 500] and [200, 300, 600] = [200, 300]
        let arr3 = TimestampMicrosecondArray::from(vec![200i64, 300, 500]);
        let list3 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr3.len()]),
            Arc::new(arr3) as ArrayRef,
            None,
        );
        acc2.update_batch(&[Arc::new(list3) as ArrayRef]).unwrap();

        let arr4 = TimestampMicrosecondArray::from(vec![200i64, 300, 600]);
        let list4 = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr4.len()]),
            Arc::new(arr4) as ArrayRef,
            None,
        );
        acc2.update_batch(&[Arc::new(list4) as ArrayRef]).unwrap();

        // Get state from acc2 and merge into acc1
        let state = acc2.state().unwrap();
        let state_list = match &state[0] {
            ScalarValue::List(arr) => arr.clone(),
            _ => panic!("Expected List state"),
        };

        acc1.merge_batch(&[state_list as ArrayRef]).unwrap();

        let result = acc1.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Final intersection: [200, 300] ∩ [200, 300] = [200, 300]
                assert_eq!(ts_arr.len(), 2);
                assert_eq!(ts_arr.value(0), 200);
                assert_eq!(ts_arr.value(1), 300);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }

    #[test]
    fn test_array_intersect_agg_merge_empty_intersection() {
        // Test that merging an empty intersection yields an empty result
        let mut acc1 = ArrayIntersectAccumulator::new();
        let mut acc2 = ArrayIntersectAccumulator::new();

        let field = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        // Acc1: intersection of [100, 200, 300] and [200, 300, 400] = [200, 300]
        let arr1 = TimestampMicrosecondArray::from(vec![100i64, 200, 300]);
        let list1 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr1.len()]),
            Arc::new(arr1) as ArrayRef,
            None,
        );
        acc1.update_batch(&[Arc::new(list1) as ArrayRef]).unwrap();

        let arr2 = TimestampMicrosecondArray::from(vec![200i64, 300, 400]);
        let list2 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr2.len()]),
            Arc::new(arr2) as ArrayRef,
            None,
        );
        acc1.update_batch(&[Arc::new(list2) as ArrayRef]).unwrap();

        // Acc2: intersection of [500, 600] and [700, 800] = [] (empty)
        let arr3 = TimestampMicrosecondArray::from(vec![500i64, 600]);
        let list3 = ListArray::new(
            field.clone(),
            OffsetBuffer::from_lengths([arr3.len()]),
            Arc::new(arr3) as ArrayRef,
            None,
        );
        acc2.update_batch(&[Arc::new(list3) as ArrayRef]).unwrap();

        let arr4 = TimestampMicrosecondArray::from(vec![700i64, 800]);
        let list4 = ListArray::new(
            field,
            OffsetBuffer::from_lengths([arr4.len()]),
            Arc::new(arr4) as ArrayRef,
            None,
        );
        acc2.update_batch(&[Arc::new(list4) as ArrayRef]).unwrap();

        // Get state from acc2 (empty intersection) and merge into acc1
        let state = acc2.state().unwrap();
        let state_list = match &state[0] {
            ScalarValue::List(arr) => arr.clone(),
            _ => panic!("Expected List state"),
        };

        acc1.merge_batch(&[state_list as ArrayRef]).unwrap();

        let result = acc1.evaluate().unwrap();

        match result {
            ScalarValue::List(list_arr) => {
                let values_array = list_arr.value(0);
                let ts_arr = values_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("Expected TimestampMicrosecondArray");
                // Final intersection: [200, 300] ∩ [] = []
                assert_eq!(ts_arr.len(), 0);
            }
            _ => panic!("Expected List, got {result:?}"),
        }
    }
}
