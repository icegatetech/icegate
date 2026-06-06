//! OTLP metrics -> Arrow transform.
//!
//! Transforms OTLP metric data points into Arrow `RecordBatch`es matching the
//! Iceberg `metrics` schema. One row is emitted per data point.

use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Int64Builder,
        ListBuilder, MapBuilder, RecordBatch, StringBuilder, StructBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::Schema,
};
use iceberg::arrow::schema_to_arrow_schema;
use icegate_common::DEFAULT_TENANT_ID;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::KeyValue,
    metrics::v1::{
        AggregationTemporality, Exemplar, ExponentialHistogram, Gauge, Histogram, Sum, Summary, exemplar,
        exponential_histogram_data_point, metric, number_data_point,
    },
};

use super::attributes::{
    dedupe_dotted_attributes, extract_map_fields_from_nested_struct, extract_map_fields_from_schema_named,
    extract_string_value, flatten_any_value_dotted, is_zero_bytes, list_element_field, list_struct_fields,
    map_field_names, merge_dotted_levels, nanos_to_micros, now_micros, u32_count_to_i32, u64_to_i64,
};

/// Process-wide cache of the derived metrics Arrow schema.
static METRICS_ARROW_SCHEMA: OnceLock<std::result::Result<Arc<Schema>, String>> = OnceLock::new();

/// Returns the Arrow schema for metrics, derived once from the Iceberg metrics
/// schema and cached for the lifetime of the process.
///
/// Uses `icegate_common::schema::metrics_schema()` as the source of truth and
/// converts it to Arrow via `iceberg::arrow::schema_to_arrow_schema()`. The
/// result is memoised in a [`OnceLock`] so the allocation-heavy Iceberg → Arrow
/// conversion runs at most once instead of on every ingest request.
///
/// # Errors
///
/// Returns `IngestError::Validation` if the Iceberg metrics schema cannot be
/// built or converted to Arrow. The schema is statically defined, so this does
/// not happen in practice.
pub fn metrics_arrow_schema() -> crate::error::Result<Arc<Schema>> {
    match METRICS_ARROW_SCHEMA.get_or_init(|| {
        let iceberg_schema = icegate_common::schema::metrics_schema().map_err(|e| e.to_string())?;
        schema_to_arrow_schema(&iceberg_schema).map(Arc::new).map_err(|e| e.to_string())
    }) {
        Ok(schema) => Ok(Arc::clone(schema)),
        Err(message) => Err(crate::error::IngestError::Validation(format!(
            "failed to build metrics Arrow schema: {message}"
        ))),
    }
}

/// `metric_type` string value stored in the table (per `SCHEMA.md`, lowercase).
///
/// One constant per `metric::Data` variant; all five are emitted by the driver.
const METRIC_TYPE_GAUGE: &str = "gauge";
const METRIC_TYPE_SUM: &str = "sum";
const METRIC_TYPE_HISTOGRAM: &str = "histogram";
const METRIC_TYPE_EXPONENTIAL_HISTOGRAM: &str = "exponential_histogram";
const METRIC_TYPE_SUMMARY: &str = "summary";

/// `aggregation_temporality` string values stored in the table (per `SCHEMA.md`).
const AGG_TEMPORALITY_DELTA: &str = "DELTA";
const AGG_TEMPORALITY_CUMULATIVE: &str = "CUMULATIVE";

/// Resource-attribute keys (dotted form) promoted to dedicated top-level columns.
const SERVICE_NAME_KEY: &str = "service.name";
const SERVICE_INSTANCE_ID_KEY: &str = "service.instance.id";

/// Promoted keys suppressed from the merged `attributes` MAP at the resource
/// level so they are not stored twice (once in the column, once in the map) —
/// mirrors the logs `LOG_PROMOTED_RESOURCE_KEYS` rule. A more specific
/// scope/data-point attribute of the same name still overrides.
const METRICS_PROMOTED_RESOURCE_KEYS: &[&str] = &[SERVICE_NAME_KEY, SERVICE_INSTANCE_ID_KEY];

/// Data-point `time_unix_nano` in micros, or the ingest time when unset (0).
const fn timestamp_or_default(time_unix_nano: u64, ingested: i64) -> i64 {
    if time_unix_nano > 0 {
        nanos_to_micros(time_unix_nano)
    } else {
        ingested
    }
}

/// Optional `start_time_unix_nano` in micros; `None` when unset (0).
fn optional_micros(start_time_unix_nano: u64) -> Option<i64> {
    (start_time_unix_nano > 0).then(|| nanos_to_micros(start_time_unix_nano))
}

/// OTLP data-point `flags` (u32) as the schema's `Int` column; `None` when 0.
fn optional_flags(flags: u32) -> crate::error::Result<Option<i32>> {
    (flags != 0).then(|| u32_count_to_i32(flags, "data_point.flags")).transpose()
}

/// Convert OTLP exponential-histogram `Buckets` into `(offset, i64 counts)`.
fn convert_buckets(buckets: &exponential_histogram_data_point::Buckets) -> crate::error::Result<(i32, Vec<i64>)> {
    let counts = buckets
        .bucket_counts
        .iter()
        .map(|c| u64_to_i64(*c, "exponential_histogram.bucket_count"))
        .collect::<crate::error::Result<Vec<_>>>()?;
    Ok((buckets.offset, counts))
}

/// Minimum/maximum exponential-histogram `scale` accepted, per the `OpenTelemetry`
/// data model (Base2 Exponential Histogram: `MaxScale = 20`; the reference minimum
/// is `-10`, beyond which `base = 2^(2^-scale)` overflows `f64`). The wire protocol
/// leaves `scale` unrestricted, so this is a data-model conformance bound enforced
/// as a counted drop, not a hard error.
const EXP_HISTOGRAM_SCALE_MIN: i32 = -10;
const EXP_HISTOGRAM_SCALE_MAX: i32 = 20;

/// True if `value` is present and non-finite (`NaN` or +/-Inf). Such values must
/// never be stored, so the carrying data point is dropped as a strict-conformance
/// violation.
fn is_non_finite(value: Option<f64>) -> bool {
    value.is_some_and(|v| !v.is_finite())
}

/// Whether a standard histogram's `bucket_counts`/`explicit_bounds` lengths are
/// consistent per OTLP: `bucket_counts` carries exactly one more element than
/// `explicit_bounds` (the implicit overflow bucket to +infinity); both empty is the
/// only valid zero-length case.
const fn histogram_buckets_consistent(bucket_counts: usize, explicit_bounds: usize) -> bool {
    if bucket_counts == 0 {
        explicit_bounds == 0
    } else {
        bucket_counts == explicit_bounds + 1
    }
}

/// Map an OTLP `AggregationTemporality` discriminant to its stored string.
///
/// Returns `None` for `Unspecified` (or any unknown value), which the caller
/// treats as a strict-conformance drop for Sum/Histogram/ExponentialHistogram.
const fn temporality_str(value: i32) -> Option<&'static str> {
    if value == AggregationTemporality::Delta as i32 {
        Some(AGG_TEMPORALITY_DELTA)
    } else if value == AggregationTemporality::Cumulative as i32 {
        Some(AGG_TEMPORALITY_CUMULATIVE)
    } else {
        None
    }
}

/// Number of data points carried by a `metric::Data` variant. Summed across a
/// request to bound the emitted row count (one row per data point), used both
/// as the empty-request short-circuit and as the column-builder capacity hint.
fn data_point_count(data: &metric::Data) -> usize {
    match data {
        metric::Data::Gauge(gauge) => gauge.data_points.len(),
        metric::Data::Sum(sum) => sum.data_points.len(),
        metric::Data::Histogram(hist) => hist.data_points.len(),
        metric::Data::ExponentialHistogram(eh) => eh.data_points.len(),
        metric::Data::Summary(summary) => summary.data_points.len(),
    }
}

/// Overlay a data point's own attributes onto the pre-flattened resource+scope
/// `base` map. The base (computed once per scope via [`merge_dotted_levels`])
/// already has the promoted `service.name`/`service.instance.id` keys
/// suppressed; a data-point attribute of the same name still overrides. Cloning
/// the base and overlaying only the data point avoids re-flattening the
/// resource/scope attributes for every single data point.
///
/// Attribute keys are kept in OTel-native **dotted** form (e.g. `http.method`),
/// matching the spans tables. This intentionally differs from the logs table,
/// which stores underscore-normalised keys; any metrics query layer must look
/// attributes up by their dotted key.
fn merge_point_attributes(base: &BTreeMap<String, String>, data_point: &[KeyValue]) -> BTreeMap<String, String> {
    // TODO(low): `data_point` is frequently empty (all dimensions live at resource/scope
    // level), making this clone redundant; returning `Cow::Borrowed(base)` would avoid it.
    let mut merged = base.clone();
    for kv in data_point {
        for (key, value) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
            merged.insert(key, value);
        }
    }
    merged
}

/// Flatten `Metric.metadata` (OTLP `KeyValue`s) into a deduped, dotted-key map.
///
/// Metric-level metadata is shared by every data point of the metric, so it is
/// flattened once per metric (mirroring [`merge_point_attributes`] for the
/// data-point attributes). A `BTreeMap` yields sorted, de-duplicated keys, which
/// a `MAP<String,String>` column requires.
fn flatten_metric_metadata(metadata: &[KeyValue]) -> BTreeMap<String, String> {
    let mut flattened = BTreeMap::new();
    for kv in metadata {
        for (key, value) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
            flattened.insert(key, value);
        }
    }
    flattened
}

/// Per-row context shared by every data point under one resource+scope+metric.
struct RowContext<'a> {
    tenant: &'a str,
    service_name: Option<&'a str>,
    service_instance_id: Option<&'a str>,
    metric_name: &'a str,
    description: &'a str,
    unit: &'a str,
    /// Pre-flattened resource+scope attributes shared by every data point under
    /// this resource/scope (see [`merge_point_attributes`]).
    base_attributes: &'a BTreeMap<String, String>,
    /// Flattened `Metric.metadata`, shared by every data point of this metric.
    metadata: &'a BTreeMap<String, String>,
    ingested: i64,
}

/// Column builders for the metrics table, one entry per data point.
///
/// Every method appends to a disjoint set of columns so that, per row, each of
/// the 31 columns receives exactly one append (value or null).
struct MetricColumns {
    tenant_id: StringBuilder,
    service_name: StringBuilder,
    service_instance_id: StringBuilder,
    timestamp: TimestampMicrosecondBuilder,
    start_timestamp: TimestampMicrosecondBuilder,
    ingested_timestamp: TimestampMicrosecondBuilder,
    metric_name: StringBuilder,
    metric_type: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    aggregation_temporality: StringBuilder,
    is_monotonic: BooleanBuilder,
    attributes: MapBuilder<StringBuilder, StringBuilder>,
    value_double: Float64Builder,
    value_int: Int64Builder,
    count: Int64Builder,
    sum: Float64Builder,
    min: Float64Builder,
    max: Float64Builder,
    bucket_counts: ListBuilder<Int64Builder>,
    explicit_bounds: ListBuilder<Float64Builder>,
    scale: Int32Builder,
    zero_count: Int64Builder,
    zero_threshold: Float64Builder,
    positive_offset: Int32Builder,
    positive_bucket_counts: ListBuilder<Int64Builder>,
    negative_offset: Int32Builder,
    negative_bucket_counts: ListBuilder<Int64Builder>,
    quantile_values: ListBuilder<StructBuilder>,
    flags: Int32Builder,
    exemplars: ListBuilder<StructBuilder>,
    metadata: MapBuilder<StringBuilder, StringBuilder>,
    schema: Arc<Schema>,
}

impl MetricColumns {
    /// Build column builders wired to the metrics Arrow schema's field metadata,
    /// pre-sizing the scalar builders to `capacity` rows (the data-point upper
    /// bound) to avoid repeated buffer reallocations during a large export.
    #[allow(clippy::too_many_lines)]
    fn new(schema: Arc<Schema>, capacity: usize) -> crate::error::Result<Self> {
        let (attr_key, attr_val) = extract_map_fields_from_schema_named(&schema, "attributes")?;
        let bucket_counts_el = list_element_field(&schema, "bucket_counts")?;
        let explicit_bounds_el = list_element_field(&schema, "explicit_bounds")?;
        let positive_bucket_counts_el = list_element_field(&schema, "positive_bucket_counts")?;
        let negative_bucket_counts_el = list_element_field(&schema, "negative_bucket_counts")?;
        let (quantile_el, quantile_fields) = list_struct_fields(&schema, "quantile_values")?;
        let (exemplar_el, exemplar_fields) = list_struct_fields(&schema, "exemplars")?;
        let (exemplar_attr_key, exemplar_attr_val) =
            extract_map_fields_from_nested_struct(&exemplar_fields, "attributes")?;
        let (metadata_key, metadata_val) = extract_map_fields_from_schema_named(&schema, "metadata")?;

        let attributes = MapBuilder::new(Some(map_field_names()), StringBuilder::new(), StringBuilder::new())
            .with_keys_field(attr_key)
            .with_values_field(attr_val);
        let metadata = MapBuilder::new(Some(map_field_names()), StringBuilder::new(), StringBuilder::new())
            .with_keys_field(metadata_key)
            .with_values_field(metadata_val);

        // quantile struct: (quantile: Double, value: Double) in schema order.
        let quantile_values = ListBuilder::new(StructBuilder::new(
            quantile_fields.iter().cloned().collect::<Vec<_>>(),
            vec![
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
            ],
        ))
        .with_field(quantile_el);

        // exemplar struct: (timestamp, value_double, value_int, span_id, trace_id, attributes).
        let exemplars = ListBuilder::new(StructBuilder::new(
            exemplar_fields.iter().cloned().collect::<Vec<_>>(),
            vec![
                Box::new(TimestampMicrosecondBuilder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(FixedSizeBinaryBuilder::new(8)) as Box<dyn ArrayBuilder>,
                Box::new(FixedSizeBinaryBuilder::new(16)) as Box<dyn ArrayBuilder>,
                Box::new(
                    MapBuilder::new(Some(map_field_names()), StringBuilder::new(), StringBuilder::new())
                        .with_keys_field(exemplar_attr_key)
                        .with_values_field(exemplar_attr_val),
                ) as Box<dyn ArrayBuilder>,
            ],
        ))
        .with_field(exemplar_el);

        // Rough average attribute-string byte size, used only to seed the
        // string builders' value buffers; exact sizing is not required.
        let string_bytes = capacity.saturating_mul(16);
        Ok(Self {
            tenant_id: StringBuilder::with_capacity(capacity, string_bytes),
            service_name: StringBuilder::with_capacity(capacity, string_bytes),
            service_instance_id: StringBuilder::with_capacity(capacity, string_bytes),
            timestamp: TimestampMicrosecondBuilder::with_capacity(capacity),
            start_timestamp: TimestampMicrosecondBuilder::with_capacity(capacity),
            ingested_timestamp: TimestampMicrosecondBuilder::with_capacity(capacity),
            metric_name: StringBuilder::with_capacity(capacity, string_bytes),
            metric_type: StringBuilder::with_capacity(capacity, string_bytes),
            description: StringBuilder::with_capacity(capacity, string_bytes),
            unit: StringBuilder::with_capacity(capacity, string_bytes),
            aggregation_temporality: StringBuilder::with_capacity(capacity, string_bytes),
            is_monotonic: BooleanBuilder::with_capacity(capacity),
            attributes,
            value_double: Float64Builder::with_capacity(capacity),
            value_int: Int64Builder::with_capacity(capacity),
            count: Int64Builder::with_capacity(capacity),
            sum: Float64Builder::with_capacity(capacity),
            min: Float64Builder::with_capacity(capacity),
            max: Float64Builder::with_capacity(capacity),
            bucket_counts: ListBuilder::new(Int64Builder::new()).with_field(bucket_counts_el),
            explicit_bounds: ListBuilder::new(Float64Builder::new()).with_field(explicit_bounds_el),
            scale: Int32Builder::with_capacity(capacity),
            zero_count: Int64Builder::with_capacity(capacity),
            zero_threshold: Float64Builder::with_capacity(capacity),
            positive_offset: Int32Builder::with_capacity(capacity),
            positive_bucket_counts: ListBuilder::new(Int64Builder::new()).with_field(positive_bucket_counts_el),
            negative_offset: Int32Builder::with_capacity(capacity),
            negative_bucket_counts: ListBuilder::new(Int64Builder::new()).with_field(negative_bucket_counts_el),
            quantile_values,
            flags: Int32Builder::with_capacity(capacity),
            exemplars,
            metadata,
            schema,
        })
    }

    /// Number of rows (data points) appended so far.
    fn len(&self) -> usize {
        self.tenant_id.len()
    }

    /// Finalize all builders into a `RecordBatch` in metrics schema column order.
    fn finish(mut self) -> crate::error::Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.tenant_id.finish()),
            Arc::new(self.service_name.finish()),
            Arc::new(self.service_instance_id.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.start_timestamp.finish()),
            Arc::new(self.ingested_timestamp.finish()),
            Arc::new(self.metric_name.finish()),
            Arc::new(self.metric_type.finish()),
            Arc::new(self.description.finish()),
            Arc::new(self.unit.finish()),
            Arc::new(self.aggregation_temporality.finish()),
            Arc::new(self.is_monotonic.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.value_double.finish()),
            Arc::new(self.value_int.finish()),
            Arc::new(self.count.finish()),
            Arc::new(self.sum.finish()),
            Arc::new(self.min.finish()),
            Arc::new(self.max.finish()),
            Arc::new(self.bucket_counts.finish()),
            Arc::new(self.explicit_bounds.finish()),
            Arc::new(self.scale.finish()),
            Arc::new(self.zero_count.finish()),
            Arc::new(self.zero_threshold.finish()),
            Arc::new(self.positive_offset.finish()),
            Arc::new(self.positive_bucket_counts.finish()),
            Arc::new(self.negative_offset.finish()),
            Arc::new(self.negative_bucket_counts.finish()),
            Arc::new(self.quantile_values.finish()),
            Arc::new(self.flags.finish()),
            Arc::new(self.exemplars.finish()),
            Arc::new(self.metadata.finish()),
        ];
        RecordBatch::try_new(self.schema, columns).map_err(|e| {
            tracing::error!("Failed to create metrics RecordBatch: {e}");
            crate::error::IngestError::Validation(format!("Failed to create metrics RecordBatch: {e}"))
        })
    }
}

impl MetricColumns {
    /// Columns common to every data point: identity, timestamps, metadata,
    /// attributes, flags. `description`/`unit` empty strings store as null.
    fn append_common(
        &mut self,
        ctx: &RowContext,
        metric_type: &str,
        timestamp: i64,
        start: Option<i64>,
        attributes: &BTreeMap<String, String>,
        flags: Option<i32>,
    ) -> crate::error::Result<()> {
        self.tenant_id.append_value(ctx.tenant);
        match ctx.service_name {
            Some(name) => self.service_name.append_value(name),
            None => self.service_name.append_null(),
        }
        match ctx.service_instance_id {
            Some(v) => self.service_instance_id.append_value(v),
            None => self.service_instance_id.append_null(),
        }
        self.timestamp.append_value(timestamp);
        match start {
            Some(v) => self.start_timestamp.append_value(v),
            None => self.start_timestamp.append_null(),
        }
        self.ingested_timestamp.append_value(ctx.ingested);
        self.metric_name.append_value(ctx.metric_name);
        self.metric_type.append_value(metric_type);
        if ctx.description.is_empty() {
            self.description.append_null();
        } else {
            self.description.append_value(ctx.description);
        }
        if ctx.unit.is_empty() {
            self.unit.append_null();
        } else {
            self.unit.append_value(ctx.unit);
        }
        for (key, value) in attributes {
            self.attributes.keys().append_value(key);
            self.attributes.values().append_value(value);
        }
        self.attributes.append(true)?;
        for (key, value) in ctx.metadata {
            self.metadata.keys().append_value(key);
            self.metadata.values().append_value(value);
        }
        self.metadata.append(true)?;
        match flags {
            Some(f) => self.flags.append_value(f),
            None => self.flags.append_null(),
        }
        Ok(())
    }

    /// `value_double` / `value_int` (mutually exclusive for Gauge/Sum).
    fn append_number_value(&mut self, value_double: Option<f64>, value_int: Option<i64>) {
        match value_double {
            Some(v) => self.value_double.append_value(v),
            None => self.value_double.append_null(),
        }
        match value_int {
            Some(v) => self.value_int.append_value(v),
            None => self.value_int.append_null(),
        }
    }

    /// `aggregation_temporality` / `is_monotonic` (Sum sets both; others null).
    fn append_temporality_monotonic(&mut self, temporality: Option<&str>, is_monotonic: Option<bool>) {
        match temporality {
            Some(t) => self.aggregation_temporality.append_value(t),
            None => self.aggregation_temporality.append_null(),
        }
        match is_monotonic {
            Some(m) => self.is_monotonic.append_value(m),
            None => self.is_monotonic.append_null(),
        }
    }

    /// `count` / `sum` / `min` / `max` (Histogram, `ExponentialHistogram`, Summary).
    fn append_histogram_common(&mut self, count: Option<i64>, sum: Option<f64>, min: Option<f64>, max: Option<f64>) {
        match count {
            Some(v) => self.count.append_value(v),
            None => self.count.append_null(),
        }
        match sum {
            Some(v) => self.sum.append_value(v),
            None => self.sum.append_null(),
        }
        match min {
            Some(v) => self.min.append_value(v),
            None => self.min.append_null(),
        }
        match max {
            Some(v) => self.max.append_value(v),
            None => self.max.append_null(),
        }
    }

    /// `bucket_counts` / `explicit_bounds` (standard Histogram). `None` → null list.
    fn append_standard_histogram(&mut self, bucket_counts: Option<&[i64]>, explicit_bounds: Option<&[f64]>) {
        match bucket_counts {
            Some(values) => {
                for v in values {
                    self.bucket_counts.values().append_value(*v);
                }
                self.bucket_counts.append(true);
            }
            None => self.bucket_counts.append(false),
        }
        match explicit_bounds {
            Some(values) => {
                for v in values {
                    self.explicit_bounds.values().append_value(*v);
                }
                self.explicit_bounds.append(true);
            }
            None => self.explicit_bounds.append(false),
        }
    }

    /// Exponential-histogram columns. `positive`/`negative` carry `(offset, bucket_counts)`.
    fn append_exp_histogram(
        &mut self,
        scale: Option<i32>,
        zero_count: Option<i64>,
        zero_threshold: Option<f64>,
        positive: Option<(i32, &[i64])>,
        negative: Option<(i32, &[i64])>,
    ) {
        match scale {
            Some(v) => self.scale.append_value(v),
            None => self.scale.append_null(),
        }
        match zero_count {
            Some(v) => self.zero_count.append_value(v),
            None => self.zero_count.append_null(),
        }
        match zero_threshold {
            Some(v) => self.zero_threshold.append_value(v),
            None => self.zero_threshold.append_null(),
        }
        match positive {
            Some((offset, counts)) => {
                self.positive_offset.append_value(offset);
                for c in counts {
                    self.positive_bucket_counts.values().append_value(*c);
                }
                self.positive_bucket_counts.append(true);
            }
            None => {
                self.positive_offset.append_null();
                self.positive_bucket_counts.append(false);
            }
        }
        match negative {
            Some((offset, counts)) => {
                self.negative_offset.append_value(offset);
                for c in counts {
                    self.negative_bucket_counts.values().append_value(*c);
                }
                self.negative_bucket_counts.append(true);
            }
            None => {
                self.negative_offset.append_null();
                self.negative_bucket_counts.append(false);
            }
        }
    }

    /// `quantile_values` list<struct{quantile, value}> (Summary). `None` → null list.
    fn append_quantiles(&mut self, quantiles: Option<&[(f64, f64)]>) -> crate::error::Result<()> {
        match quantiles {
            Some(values) => {
                let sb = self.quantile_values.values();
                for (quantile, value) in values {
                    sb.field_builder::<Float64Builder>(0)
                        .ok_or_else(|| crate::error::IngestError::Validation("quantile builder".into()))?
                        .append_value(*quantile);
                    sb.field_builder::<Float64Builder>(1)
                        .ok_or_else(|| crate::error::IngestError::Validation("quantile value builder".into()))?
                        .append_value(*value);
                    sb.append(true);
                }
                self.quantile_values.append(true);
            }
            None => self.quantile_values.append(false),
        }
        Ok(())
    }

    /// `exemplars` list<struct>. Always appends a (possibly empty) list, mirroring
    /// the spans events/links builders. Absent/wrong-length span/trace ids → null.
    fn append_exemplars(&mut self, exemplars: &[Exemplar]) -> crate::error::Result<()> {
        let sb = self.exemplars.values();
        for ex in exemplars {
            let timestamp_builder = sb
                .field_builder::<TimestampMicrosecondBuilder>(0)
                .ok_or_else(|| crate::error::IngestError::Validation("exemplar timestamp builder".into()))?;
            // Unset (`0`) exemplar time stores as null, mirroring `start_timestamp`,
            // rather than the epoch (1970-01-01).
            match optional_micros(ex.time_unix_nano) {
                Some(value) => timestamp_builder.append_value(value),
                None => timestamp_builder.append_null(),
            }
            let (value_double, value_int) = match &ex.value {
                Some(exemplar::Value::AsDouble(d)) => (Some(*d), None),
                Some(exemplar::Value::AsInt(i)) => (None, Some(*i)),
                None => (None, None),
            };
            let vd = sb
                .field_builder::<Float64Builder>(1)
                .ok_or_else(|| crate::error::IngestError::Validation("exemplar value_double builder".into()))?;
            match value_double {
                Some(v) => vd.append_value(v),
                None => vd.append_null(),
            }
            let vi = sb
                .field_builder::<Int64Builder>(2)
                .ok_or_else(|| crate::error::IngestError::Validation("exemplar value_int builder".into()))?;
            match value_int {
                Some(v) => vi.append_value(v),
                None => vi.append_null(),
            }
            let span_b = sb
                .field_builder::<FixedSizeBinaryBuilder>(3)
                .ok_or_else(|| crate::error::IngestError::Validation("exemplar span_id builder".into()))?;
            match <&[u8; 8]>::try_from(ex.span_id.as_slice()) {
                Ok(a) if !is_zero_bytes(a) => span_b.append_value(a)?,
                _ => span_b.append_null(),
            }
            let trace_b = sb
                .field_builder::<FixedSizeBinaryBuilder>(4)
                .ok_or_else(|| crate::error::IngestError::Validation("exemplar trace_id builder".into()))?;
            match <&[u8; 16]>::try_from(ex.trace_id.as_slice()) {
                Ok(a) if !is_zero_bytes(a) => trace_b.append_value(a)?,
                _ => trace_b.append_null(),
            }
            let attr_b = sb
                .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(5)
                .ok_or_else(|| crate::error::IngestError::Validation("exemplar attrs builder".into()))?;
            // Dedupe to one entry per key (mirrors the top-level `attributes`
            // column); a MAP<String,String> with duplicate keys is invalid for
            // downstream readers.
            for (key, value) in dedupe_dotted_attributes(&ex.filtered_attributes) {
                attr_b.keys().append_value(&key);
                attr_b.values().append_value(value);
            }
            attr_b.append(true)?;
            sb.append(true);
        }
        self.exemplars.append(true);
        Ok(())
    }

    /// Append all rows for one Gauge metric. Returns the count of dropped
    /// (value-less) data points.
    fn append_gauge(&mut self, ctx: &RowContext, gauge: &Gauge) -> crate::error::Result<usize> {
        let mut drops = 0;
        for dp in &gauge.data_points {
            let (value_double, value_int) = match &dp.value {
                Some(number_data_point::Value::AsDouble(d)) => (Some(*d), None),
                Some(number_data_point::Value::AsInt(i)) => (None, Some(*i)),
                None => {
                    drops += 1;
                    continue;
                }
            };
            // A non-finite (NaN or +/-Inf) double must not be stored; drop the point.
            if is_non_finite(value_double) {
                drops += 1;
                continue;
            }
            // Out-of-range flags are a strict-conformance drop, counted like a
            // value-less point rather than failing the whole request.
            let Ok(flags) = optional_flags(dp.flags) else {
                drops += 1;
                continue;
            };
            let attributes = merge_point_attributes(ctx.base_attributes, &dp.attributes);
            let timestamp = timestamp_or_default(dp.time_unix_nano, ctx.ingested);
            let start = optional_micros(dp.start_time_unix_nano);
            self.append_common(ctx, METRIC_TYPE_GAUGE, timestamp, start, &attributes, flags)?;
            self.append_number_value(value_double, value_int);
            self.append_temporality_monotonic(None, None);
            self.append_histogram_common(None, None, None, None);
            self.append_standard_histogram(None, None);
            self.append_exp_histogram(None, None, None, None, None);
            self.append_quantiles(None)?;
            self.append_exemplars(&dp.exemplars)?;
        }
        Ok(drops)
    }

    /// Append all rows for one Sum metric. Returns the count of dropped data
    /// points (unspecified temporality or value-less).
    fn append_sum(&mut self, ctx: &RowContext, sum: &Sum) -> crate::error::Result<usize> {
        let mut drops = 0;
        let Some(temporality) = temporality_str(sum.aggregation_temporality) else {
            // Strict OTLP: a Sum with unspecified temporality is invalid; drop all its points.
            return Ok(sum.data_points.len());
        };
        for dp in &sum.data_points {
            let (value_double, value_int) = match &dp.value {
                Some(number_data_point::Value::AsDouble(d)) => (Some(*d), None),
                Some(number_data_point::Value::AsInt(i)) => (None, Some(*i)),
                None => {
                    drops += 1;
                    continue;
                }
            };
            // A non-finite (NaN or +/-Inf) double must not be stored; drop the point.
            if is_non_finite(value_double) {
                drops += 1;
                continue;
            }
            // Out-of-range flags are a strict-conformance drop, counted like a
            // value-less point rather than failing the whole request.
            let Ok(flags) = optional_flags(dp.flags) else {
                drops += 1;
                continue;
            };
            let attributes = merge_point_attributes(ctx.base_attributes, &dp.attributes);
            let timestamp = timestamp_or_default(dp.time_unix_nano, ctx.ingested);
            let start = optional_micros(dp.start_time_unix_nano);
            self.append_common(ctx, METRIC_TYPE_SUM, timestamp, start, &attributes, flags)?;
            self.append_number_value(value_double, value_int);
            self.append_temporality_monotonic(Some(temporality), Some(sum.is_monotonic));
            self.append_histogram_common(None, None, None, None);
            self.append_standard_histogram(None, None);
            self.append_exp_histogram(None, None, None, None, None);
            self.append_quantiles(None)?;
            self.append_exemplars(&dp.exemplars)?;
        }
        Ok(drops)
    }

    /// Append all rows for one standard Histogram metric. Returns the dropped
    /// count (all points when temporality is unspecified, else 0).
    fn append_histogram(&mut self, ctx: &RowContext, hist: &Histogram) -> crate::error::Result<usize> {
        let Some(temporality) = temporality_str(hist.aggregation_temporality) else {
            return Ok(hist.data_points.len());
        };
        let mut drops = 0;
        for dp in &hist.data_points {
            // All fallible conversions run before any column append so a dropped
            // point never leaves a half-written row. Out-of-range count/bucket/
            // flags are counted as strict-conformance drops, not a hard error.
            let (Ok(flags), Ok(count), Ok(buckets)) = (
                optional_flags(dp.flags),
                u64_to_i64(dp.count, "histogram.count"),
                dp.bucket_counts
                    .iter()
                    .map(|c| u64_to_i64(*c, "histogram.bucket_count"))
                    .collect::<crate::error::Result<Vec<_>>>(),
            ) else {
                drops += 1;
                continue;
            };
            // Value invariants: `bucket_counts` must be exactly one longer than
            // `explicit_bounds` (OTLP), and every stored double must be finite.
            if !histogram_buckets_consistent(dp.bucket_counts.len(), dp.explicit_bounds.len())
                || is_non_finite(dp.sum)
                || is_non_finite(dp.min)
                || is_non_finite(dp.max)
                || dp.explicit_bounds.iter().any(|bound| !bound.is_finite())
            {
                drops += 1;
                continue;
            }
            let attributes = merge_point_attributes(ctx.base_attributes, &dp.attributes);
            let timestamp = timestamp_or_default(dp.time_unix_nano, ctx.ingested);
            let start = optional_micros(dp.start_time_unix_nano);
            self.append_common(ctx, METRIC_TYPE_HISTOGRAM, timestamp, start, &attributes, flags)?;
            self.append_number_value(None, None);
            self.append_temporality_monotonic(Some(temporality), None);
            self.append_histogram_common(Some(count), dp.sum, dp.min, dp.max);
            self.append_standard_histogram(Some(&buckets), Some(&dp.explicit_bounds));
            self.append_exp_histogram(None, None, None, None, None);
            self.append_quantiles(None)?;
            self.append_exemplars(&dp.exemplars)?;
        }
        Ok(drops)
    }

    /// Append all rows for one `ExponentialHistogram` metric. Returns the dropped
    /// count (all points when temporality is unspecified, else 0).
    fn append_exponential_histogram(
        &mut self,
        ctx: &RowContext,
        eh: &ExponentialHistogram,
    ) -> crate::error::Result<usize> {
        let Some(temporality) = temporality_str(eh.aggregation_temporality) else {
            return Ok(eh.data_points.len());
        };
        let mut drops = 0;
        for dp in &eh.data_points {
            // All fallible conversions run before any column append; an
            // out-of-range count/zero_count/bucket/flags is a counted drop.
            let (Ok(flags), Ok(count), Ok(zero_count), Ok(positive), Ok(negative)) = (
                optional_flags(dp.flags),
                u64_to_i64(dp.count, "exponential_histogram.count"),
                u64_to_i64(dp.zero_count, "exponential_histogram.zero_count"),
                dp.positive.as_ref().map(convert_buckets).transpose(),
                dp.negative.as_ref().map(convert_buckets).transpose(),
            ) else {
                drops += 1;
                continue;
            };
            // Value invariants: `scale` within the OTel data-model range and every
            // stored double finite.
            if !(EXP_HISTOGRAM_SCALE_MIN..=EXP_HISTOGRAM_SCALE_MAX).contains(&dp.scale)
                || is_non_finite(dp.sum)
                || is_non_finite(dp.min)
                || is_non_finite(dp.max)
                || !dp.zero_threshold.is_finite()
            {
                drops += 1;
                continue;
            }
            let attributes = merge_point_attributes(ctx.base_attributes, &dp.attributes);
            let timestamp = timestamp_or_default(dp.time_unix_nano, ctx.ingested);
            let start = optional_micros(dp.start_time_unix_nano);
            self.append_common(
                ctx,
                METRIC_TYPE_EXPONENTIAL_HISTOGRAM,
                timestamp,
                start,
                &attributes,
                flags,
            )?;
            self.append_number_value(None, None);
            self.append_temporality_monotonic(Some(temporality), None);
            self.append_histogram_common(Some(count), dp.sum, dp.min, dp.max);
            self.append_standard_histogram(None, None);
            self.append_exp_histogram(
                Some(dp.scale),
                Some(zero_count),
                Some(dp.zero_threshold),
                positive.as_ref().map(|(offset, counts)| (*offset, counts.as_slice())),
                negative.as_ref().map(|(offset, counts)| (*offset, counts.as_slice())),
            );
            self.append_quantiles(None)?;
            self.append_exemplars(&dp.exemplars)?;
        }
        Ok(drops)
    }

    /// Append all rows for one Summary metric. Summary has no temporality,
    /// monotonic flag, or exemplars (an empty exemplars list is stored).
    /// Returns the count of dropped data points (out-of-range count or flags).
    fn append_summary(&mut self, ctx: &RowContext, summary: &Summary) -> crate::error::Result<usize> {
        let mut drops = 0;
        for dp in &summary.data_points {
            // Out-of-range count/flags are counted as strict-conformance drops
            // instead of failing the whole request.
            let (Ok(flags), Ok(count)) = (optional_flags(dp.flags), u64_to_i64(dp.count, "summary.count")) else {
                drops += 1;
                continue;
            };
            // Value invariants: `sum` finite and every quantile within [0, 1] with a
            // finite value.
            if !dp.sum.is_finite()
                || dp
                    .quantile_values
                    .iter()
                    .any(|q| !(0.0..=1.0).contains(&q.quantile) || !q.value.is_finite())
            {
                drops += 1;
                continue;
            }
            let attributes = merge_point_attributes(ctx.base_attributes, &dp.attributes);
            let timestamp = timestamp_or_default(dp.time_unix_nano, ctx.ingested);
            let start = optional_micros(dp.start_time_unix_nano);
            let quantiles: Vec<(f64, f64)> = dp.quantile_values.iter().map(|q| (q.quantile, q.value)).collect();
            self.append_common(ctx, METRIC_TYPE_SUMMARY, timestamp, start, &attributes, flags)?;
            self.append_number_value(None, None);
            self.append_temporality_monotonic(None, None);
            self.append_histogram_common(Some(count), Some(dp.sum), None, None);
            self.append_standard_histogram(None, None);
            self.append_exp_histogram(None, None, None, None, None);
            self.append_quantiles(Some(&quantiles))?;
            self.append_exemplars(&[])?;
        }
        Ok(drops)
    }
}

/// Transforms an OTLP metrics export request to an Arrow `RecordBatch`.
///
/// Emits one row per data point across all resource/scope/metric levels.
/// Strict OTLP-conformance violations are dropped and counted in the second
/// return value so the caller can report `rejectedDataPoints`.
///
/// # Returns
///
/// `(Some(batch), drops)` if at least one data point is valid, or
/// `(None, drops)` if no rows remain.
///
/// # Errors
///
/// Returns `IngestError` if schema extraction or `RecordBatch` creation fails.
#[tracing::instrument(skip(request))]
pub fn metrics_to_record_batch(
    request: &ExportMetricsServiceRequest,
    tenant_id: Option<&str>,
) -> crate::error::Result<(Option<RecordBatch>, usize)> {
    let ingested = now_micros()?;
    // Upper bound on emitted rows (one per data point), used both as the
    // empty-request short-circuit and as the builder capacity hint.
    let estimated_rows: usize = request
        .resource_metrics
        .iter()
        .flat_map(|rm| &rm.scope_metrics)
        .flat_map(|sm| &sm.metrics)
        .filter_map(|metric| metric.data.as_ref())
        .map(data_point_count)
        .sum();
    if estimated_rows == 0 {
        return Ok((None, 0));
    }

    let schema = metrics_arrow_schema()?;
    let mut cols = MetricColumns::new(schema, estimated_rows)?;
    let tenant = tenant_id.unwrap_or(DEFAULT_TENANT_ID);
    let empty: Vec<KeyValue> = Vec::new();
    let mut drops: usize = 0;

    for rm in &request.resource_metrics {
        let resource_attrs = rm.resource.as_ref().map_or(&empty, |r| &r.attributes);
        // Absent `service.name` stores null (the column is optional), matching the
        // logs and spans transforms; no synthetic default is substituted.
        let service_name = resource_attrs
            .iter()
            .find(|kv| kv.key == SERVICE_NAME_KEY)
            .and_then(|kv| extract_string_value(kv.value.as_ref()));
        let service_instance_id = resource_attrs
            .iter()
            .find(|kv| kv.key == SERVICE_INSTANCE_ID_KEY)
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for sm in &rm.scope_metrics {
            let scope_attrs = sm.scope.as_ref().map_or(&empty, |s| &s.attributes);
            // Flatten resource + scope attributes once per scope; each data
            // point clones this base and overlays only its own attributes
            // instead of re-flattening resource/scope for every point.
            let base_attributes = merge_dotted_levels(&[resource_attrs, scope_attrs], METRICS_PROMOTED_RESOURCE_KEYS);
            for metric in &sm.metrics {
                let Some(data) = metric.data.as_ref() else {
                    // No data oneof => no data points; nothing to reject.
                    continue;
                };
                let metadata = flatten_metric_metadata(&metric.metadata);
                let ctx = RowContext {
                    tenant,
                    service_name: service_name.as_deref(),
                    service_instance_id: service_instance_id.as_deref(),
                    metric_name: &metric.name,
                    description: &metric.description,
                    unit: &metric.unit,
                    base_attributes: &base_attributes,
                    metadata: &metadata,
                    ingested,
                };
                match data {
                    metric::Data::Gauge(gauge) => drops += cols.append_gauge(&ctx, gauge)?,
                    metric::Data::Sum(sum) => drops += cols.append_sum(&ctx, sum)?,
                    metric::Data::Histogram(hist) => drops += cols.append_histogram(&ctx, hist)?,
                    metric::Data::ExponentialHistogram(eh) => drops += cols.append_exponential_histogram(&ctx, eh)?,
                    metric::Data::Summary(summary) => drops += cols.append_summary(&ctx, summary)?,
                }
            }
        }
    }

    if cols.len() == 0 {
        return Ok((None, drops));
    }
    Ok((Some(cols.finish()?), drops))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_arrow_schema_has_expected_columns() {
        let schema = metrics_arrow_schema().expect("metrics arrow schema");
        // 32 top-level fields per metrics_schema() (field ids 1..=31 plus `metadata` at id 50).
        assert_eq!(schema.fields().len(), 32);
        for name in [
            "tenant_id",
            "service_name",
            "service_instance_id",
            "timestamp",
            "start_timestamp",
            "ingested_timestamp",
            "metric_name",
            "metric_type",
            "description",
            "unit",
            "aggregation_temporality",
            "is_monotonic",
            "attributes",
            "value_double",
            "value_int",
            "count",
            "sum",
            "min",
            "max",
            "bucket_counts",
            "explicit_bounds",
            "scale",
            "zero_count",
            "zero_threshold",
            "positive_offset",
            "positive_bucket_counts",
            "negative_offset",
            "negative_bucket_counts",
            "quantile_values",
            "flags",
            "exemplars",
            "metadata",
        ] {
            assert!(schema.field_with_name(name).is_ok(), "missing column {name}");
        }
    }

    use arrow::array::{Array, Float64Array, Int64Array, MapArray, StringArray, StructArray};
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value},
        metrics::v1::{Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point},
        resource::v1::Resource,
    };

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
        }
    }

    fn gauge_double_dp(v: f64) -> NumberDataPoint {
        NumberDataPoint {
            time_unix_nano: 1_700_000_000_000_000_000,
            value: Some(number_data_point::Value::AsDouble(v)),
            ..Default::default()
        }
    }

    fn request_with(data: metric::Data, name: &str, resource_attrs: Vec<KeyValue>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource_attrs,
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: name.to_string(),
                        data: Some(data),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// Pull row 0 of a `MapArray` column into a `BTreeMap`.
    fn map_pairs(batch: &RecordBatch, column: &str) -> BTreeMap<String, String> {
        let map = batch
            .column_by_name(column)
            .expect("map column")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("MapArray");
        let entries = map.value(0);
        let s = entries.as_any().downcast_ref::<StructArray>().expect("struct");
        let keys = s.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
        let vals = s.column(1).as_any().downcast_ref::<StringArray>().expect("values");
        (0..keys.len())
            .map(|i| (keys.value(i).to_string(), vals.value(i).to_string()))
            .collect()
    }

    #[test]
    fn metrics_to_record_batch_empty_request_returns_none() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        assert!(batch.is_none());
        assert_eq!(drops, 0);
    }

    #[test]
    fn gauge_double_populates_value_double_and_type() {
        let request = request_with(
            metric::Data::Gauge(Gauge {
                data_points: vec![gauge_double_dp(1.5)],
            }),
            "cpu.usage",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, Some("tenant-1")).expect("ok");
        let batch = batch.expect("batch");
        assert_eq!(drops, 0);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 32);

        let metric_type = batch
            .column_by_name("metric_type")
            .expect("metric_type")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(metric_type.value(0), "gauge");

        let value_double = batch
            .column_by_name("value_double")
            .expect("value_double")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64");
        assert!((value_double.value(0) - 1.5).abs() < f64::EPSILON);

        let value_int = batch
            .column_by_name("value_int")
            .expect("value_int")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        assert!(value_int.is_null(0));
    }

    #[test]
    fn metric_metadata_populates_metadata_column() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "svc")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "m".to_string(),
                        metadata: vec![kv("unit_family", "time"), kv("origin", "sdk")],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![gauge_double_dp(1.0)],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let (batch, _) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        let metadata = map_pairs(&batch, "metadata");
        assert_eq!(metadata.get("unit_family"), Some(&"time".to_string()));
        assert_eq!(metadata.get("origin"), Some(&"sdk".to_string()));
    }

    #[test]
    fn gauge_int_populates_value_int() {
        let dp = NumberDataPoint {
            time_unix_nano: 1_700_000_000_000_000_000,
            value: Some(number_data_point::Value::AsInt(42)),
            ..Default::default()
        };
        let request = request_with(
            metric::Data::Gauge(Gauge { data_points: vec![dp] }),
            "queue.depth",
            vec![kv("service.name", "svc")],
        );
        let (batch, _) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        let value_int = batch
            .column_by_name("value_int")
            .expect("value_int")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        assert_eq!(value_int.value(0), 42);
        let value_double = batch
            .column_by_name("value_double")
            .expect("value_double")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64");
        assert!(value_double.is_null(0));
    }

    #[test]
    fn gauge_without_value_is_dropped() {
        let dp = NumberDataPoint {
            time_unix_nano: 1_700_000_000_000_000_000,
            value: None,
            ..Default::default()
        };
        let request = request_with(
            metric::Data::Gauge(Gauge { data_points: vec![dp] }),
            "broken",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        assert!(batch.is_none());
        assert_eq!(drops, 1);
    }

    #[test]
    fn missing_service_name_stores_null() {
        let request = request_with(
            metric::Data::Gauge(Gauge {
                data_points: vec![gauge_double_dp(1.0)],
            }),
            "m",
            vec![],
        );
        let (batch, _) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        let service_name = batch
            .column_by_name("service_name")
            .expect("service_name")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        // `service_name` is optional: an absent `service.name` stores null, matching logs and spans.
        assert!(service_name.is_null(0));
    }

    #[test]
    fn attributes_merge_resource_scope_datapoint_with_datapoint_winning() {
        let mut dp = gauge_double_dp(1.0);
        dp.attributes = vec![kv("host", "h-dp"), kv("dp.only", "v")];
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "svc"), kv("host", "h-res"), kv("res.only", "v")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                        attributes: vec![kv("scope.only", "v")],
                        ..Default::default()
                    }),
                    metrics: vec![Metric {
                        name: "m".to_string(),
                        data: Some(metric::Data::Gauge(Gauge { data_points: vec![dp] })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let (batch, _) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        let attrs = map_pairs(&batch, "attributes");
        assert_eq!(attrs.get("host"), Some(&"h-dp".to_string())); // data point wins
        assert_eq!(attrs.get("res.only"), Some(&"v".to_string()));
        assert_eq!(attrs.get("scope.only"), Some(&"v".to_string()));
        assert_eq!(attrs.get("dp.only"), Some(&"v".to_string()));
        // service.name is promoted to its own column and suppressed from the merged
        // map at the resource level (mirrors logs), so it is not stored twice.
        assert!(!attrs.contains_key("service.name"));
    }

    #[test]
    fn gauge_exemplar_is_materialized() {
        use arrow::array::{FixedSizeBinaryArray, ListArray, TimestampMicrosecondArray};
        use opentelemetry_proto::tonic::metrics::v1::{Exemplar, exemplar};

        let mut dp = gauge_double_dp(2.0);
        dp.exemplars = vec![Exemplar {
            time_unix_nano: 1_700_000_000_000_500_000,
            span_id: vec![2u8; 8],
            trace_id: vec![1u8; 16],
            value: Some(exemplar::Value::AsDouble(2.0)),
            ..Default::default()
        }];
        let request = request_with(
            metric::Data::Gauge(Gauge { data_points: vec![dp] }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, _) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        let exemplars = batch
            .column_by_name("exemplars")
            .expect("exemplars")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        let row0 = exemplars.value(0);
        let s = row0.as_any().downcast_ref::<StructArray>().expect("struct");
        assert_eq!(s.len(), 1);
        let trace = s
            .column_by_name("trace_id")
            .expect("trace_id")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("fixed16");
        assert_eq!(trace.value(0), &vec![1u8; 16][..]);
        let ts = s
            .column_by_name("timestamp")
            .expect("timestamp")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("ts");
        assert_eq!(ts.value(0), 1_700_000_000_000_500); // ns/1000
    }

    #[test]
    fn sum_cumulative_monotonic_int_is_stored() {
        use arrow::array::{BooleanArray, Int64Array, StringArray};
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Sum};

        let dp = NumberDataPoint {
            time_unix_nano: 1_700_000_000_000_000_000,
            value: Some(number_data_point::Value::AsInt(7)),
            ..Default::default()
        };
        let request = request_with(
            metric::Data::Sum(Sum {
                data_points: vec![dp],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            }),
            "requests.total",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        assert_eq!(drops, 0);

        let mtype = batch
            .column_by_name("metric_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(mtype.value(0), "sum");
        let temp = batch
            .column_by_name("aggregation_temporality")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(temp.value(0), "CUMULATIVE");
        let mono = batch
            .column_by_name("is_monotonic")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(mono.value(0));
        let vint = batch
            .column_by_name("value_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(vint.value(0), 7);
    }

    #[test]
    fn sum_delta_double_maps_to_delta_string() {
        use arrow::array::StringArray;
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Sum};

        let request = request_with(
            metric::Data::Sum(Sum {
                data_points: vec![gauge_double_dp(3.0)],
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: false,
            }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, _) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        let temp = batch
            .column_by_name("aggregation_temporality")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(temp.value(0), "DELTA");
    }

    #[test]
    fn sum_with_unspecified_temporality_is_dropped() {
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Sum};

        let request = request_with(
            metric::Data::Sum(Sum {
                data_points: vec![gauge_double_dp(1.0)],
                aggregation_temporality: AggregationTemporality::Unspecified as i32,
                is_monotonic: true,
            }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        assert!(batch.is_none());
        assert_eq!(drops, 1);
    }

    #[test]
    fn histogram_stores_count_sum_buckets_and_bounds() {
        use arrow::array::{Float64Array, Int64Array, ListArray, StringArray};
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Histogram, HistogramDataPoint};

        let dp = HistogramDataPoint {
            time_unix_nano: 1_700_000_000_000_000_000,
            count: 10,
            sum: Some(42.5),
            min: Some(1.0),
            max: Some(9.0),
            bucket_counts: vec![3, 7],
            explicit_bounds: vec![5.0],
            ..Default::default()
        };
        let request = request_with(
            metric::Data::Histogram(Histogram {
                data_points: vec![dp],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            }),
            "latency",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        assert_eq!(drops, 0);

        let mtype = batch
            .column_by_name("metric_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(mtype.value(0), "histogram");
        let count = batch
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 10);
        let sum = batch
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((sum.value(0) - 42.5).abs() < f64::EPSILON);

        let buckets = batch
            .column_by_name("bucket_counts")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let b0 = buckets.value(0);
        let b0 = b0.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(b0.values(), &[3, 7]);

        let bounds = batch
            .column_by_name("explicit_bounds")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let bd0 = bounds.value(0);
        let bd0 = bd0.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(bd0.values(), &[5.0]);
    }

    #[test]
    fn histogram_with_unspecified_temporality_is_dropped() {
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Histogram, HistogramDataPoint};

        let request = request_with(
            metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    count: 1,
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Unspecified as i32,
            }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        assert!(batch.is_none());
        assert_eq!(drops, 1);
    }

    #[test]
    fn exponential_histogram_stores_scale_and_buckets() {
        use arrow::array::{Int32Array, Int64Array, ListArray, StringArray};
        use opentelemetry_proto::tonic::metrics::v1::{
            AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint,
            exponential_histogram_data_point::Buckets,
        };

        let dp = ExponentialHistogramDataPoint {
            time_unix_nano: 1_700_000_000_000_000_000,
            count: 5,
            sum: Some(12.0),
            scale: 2,
            zero_count: 1,
            zero_threshold: 0.5,
            positive: Some(Buckets {
                offset: 3,
                bucket_counts: vec![1, 2, 2],
            }),
            negative: None,
            ..Default::default()
        };
        let request = request_with(
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![dp],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            }),
            "exp.latency",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        assert_eq!(drops, 0);

        let mtype = batch
            .column_by_name("metric_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(mtype.value(0), "exponential_histogram");
        let scale = batch
            .column_by_name("scale")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(scale.value(0), 2);
        let zero_count = batch
            .column_by_name("zero_count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(zero_count.value(0), 1);
        let pos_offset = batch
            .column_by_name("positive_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(pos_offset.value(0), 3);
        let pos = batch
            .column_by_name("positive_bucket_counts")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let p0 = pos.value(0);
        let p0 = p0.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(p0.values(), &[1, 2, 2]);
        // negative is absent -> null list
        let neg = batch
            .column_by_name("negative_bucket_counts")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(neg.is_null(0));
    }

    #[test]
    fn exponential_histogram_with_unspecified_temporality_is_dropped() {
        use opentelemetry_proto::tonic::metrics::v1::{
            AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint,
        };

        let request = request_with(
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    count: 1,
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Unspecified as i32,
            }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        assert!(batch.is_none());
        assert_eq!(drops, 1);
    }

    #[test]
    fn summary_stores_count_sum_and_quantiles() {
        use arrow::array::{Float64Array, Int64Array, ListArray, StringArray, StructArray};
        use opentelemetry_proto::tonic::metrics::v1::{Summary, SummaryDataPoint, summary_data_point::ValueAtQuantile};

        let dp = SummaryDataPoint {
            time_unix_nano: 1_700_000_000_000_000_000,
            count: 100,
            sum: 250.0,
            quantile_values: vec![
                ValueAtQuantile {
                    quantile: 0.5,
                    value: 2.0,
                },
                ValueAtQuantile {
                    quantile: 0.99,
                    value: 9.0,
                },
            ],
            ..Default::default()
        };
        let request = request_with(
            metric::Data::Summary(Summary { data_points: vec![dp] }),
            "rpc.duration",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        assert_eq!(drops, 0);

        let mtype = batch
            .column_by_name("metric_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(mtype.value(0), "summary");
        let count = batch
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 100);
        let sum = batch
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((sum.value(0) - 250.0).abs() < f64::EPSILON);

        let q = batch
            .column_by_name("quantile_values")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let q0 = q.value(0);
        let q0 = q0.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(q0.len(), 2);
        let quantile = q0
            .column_by_name("quantile")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let value = q0
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((quantile.value(1) - 0.99).abs() < f64::EPSILON);
        assert!((value.value(1) - 9.0).abs() < f64::EPSILON);
    }

    #[test]
    fn mixed_batch_keeps_valid_points_and_counts_drops() {
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Gauge, Metric, ScopeMetrics, Sum};

        // One valid gauge metric + one Sum metric with unspecified temporality (dropped).
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "svc")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            name: "ok.gauge".to_string(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![gauge_double_dp(1.0)],
                            })),
                            ..Default::default()
                        },
                        Metric {
                            name: "bad.sum".to_string(),
                            data: Some(metric::Data::Sum(Sum {
                                data_points: vec![gauge_double_dp(2.0)],
                                aggregation_temporality: AggregationTemporality::Unspecified as i32,
                                is_monotonic: true,
                            })),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(drops, 1);
    }

    #[test]
    fn out_of_range_count_drops_point_without_failing_request() {
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Histogram, HistogramDataPoint};

        // A valid gauge plus a histogram point whose count exceeds i64::MAX: the
        // bad point is dropped and counted (partial success), not a hard error
        // that discards the whole request.
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "svc")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            name: "ok.gauge".to_string(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![gauge_double_dp(1.0)],
                            })),
                            ..Default::default()
                        },
                        Metric {
                            name: "bad.hist".to_string(),
                            data: Some(metric::Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint {
                                    time_unix_nano: 1_700_000_000_000_000_000,
                                    count: u64::MAX,
                                    ..Default::default()
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            })),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(drops, 1);
    }

    #[test]
    fn histogram_bucket_bounds_mismatch_is_dropped() {
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Histogram, HistogramDataPoint};

        // bucket_counts must have exactly one more element than explicit_bounds;
        // here 3 != 1 + 1, so the point is an invalid-value drop.
        let request = request_with(
            metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    count: 6,
                    bucket_counts: vec![1, 2, 3],
                    explicit_bounds: vec![5.0],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        assert!(batch.is_none());
        assert_eq!(drops, 1);
    }

    #[test]
    fn non_finite_gauge_value_is_dropped() {
        let request = request_with(
            metric::Data::Gauge(Gauge {
                data_points: vec![gauge_double_dp(f64::NAN)],
            }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        assert!(batch.is_none());
        assert_eq!(drops, 1);
    }

    #[test]
    fn non_finite_histogram_sum_is_dropped() {
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Histogram, HistogramDataPoint};

        // Valid buckets (len 1 == 0 + 1) isolate the drop to the non-finite sum.
        let request = request_with(
            metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    count: 1,
                    sum: Some(f64::INFINITY),
                    bucket_counts: vec![1],
                    explicit_bounds: vec![],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        assert!(batch.is_none());
        assert_eq!(drops, 1);
    }

    #[test]
    fn summary_quantile_out_of_range_is_dropped() {
        use opentelemetry_proto::tonic::metrics::v1::{Summary, SummaryDataPoint, summary_data_point::ValueAtQuantile};

        // A quantile outside [0, 1] is an invalid-value drop.
        let request = request_with(
            metric::Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    count: 10,
                    sum: 5.0,
                    quantile_values: vec![ValueAtQuantile {
                        quantile: 1.5,
                        value: 2.0,
                    }],
                    ..Default::default()
                }],
            }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        assert!(batch.is_none());
        assert_eq!(drops, 1);
    }

    #[test]
    fn exponential_histogram_scale_out_of_range_is_dropped() {
        use opentelemetry_proto::tonic::metrics::v1::{
            AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint,
        };

        // scale far outside the OTel data-model range [-10, 20] is an invalid-value drop.
        let request = request_with(
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    count: 1,
                    scale: 1000,
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, drops) = metrics_to_record_batch(&request, None).expect("ok");
        assert!(batch.is_none());
        assert_eq!(drops, 1);
    }

    #[test]
    fn exemplar_unset_timestamp_is_null_and_attributes_are_deduped() {
        use arrow::array::{ListArray, MapArray, StructArray, TimestampMicrosecondArray};
        use opentelemetry_proto::tonic::metrics::v1::{Exemplar, exemplar};

        let mut dp = gauge_double_dp(1.0);
        dp.exemplars = vec![Exemplar {
            time_unix_nano: 0,                                     // unset -> null timestamp, not epoch 0
            filtered_attributes: vec![kv("k", "a"), kv("k", "b")], // duplicate key -> single entry
            value: Some(exemplar::Value::AsDouble(1.0)),
            ..Default::default()
        }];
        let request = request_with(
            metric::Data::Gauge(Gauge { data_points: vec![dp] }),
            "m",
            vec![kv("service.name", "svc")],
        );
        let (batch, _) = metrics_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        let exemplars = batch
            .column_by_name("exemplars")
            .expect("exemplars")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        let row0 = exemplars.value(0);
        let s = row0.as_any().downcast_ref::<StructArray>().expect("struct");
        let ts = s
            .column_by_name("timestamp")
            .expect("timestamp")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("ts");
        assert!(ts.is_null(0));
        let attrs = s
            .column_by_name("attributes")
            .expect("attributes")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map");
        // Duplicate "k" collapses to a single map entry.
        assert_eq!(attrs.value(0).len(), 1);
    }
}
