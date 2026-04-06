//! OTLP to Arrow transform utilities.
//!
//! Transforms `OpenTelemetry` Protocol log records to Arrow `RecordBatches`
//! matching the Iceberg logs schema.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, ListBuilder, MapBuilder,
        MapFieldNames, RecordBatch, StringBuilder, StructBuilder, TimestampMicrosecondArray,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, Schema},
};
use iceberg::arrow::schema_to_arrow_schema;
use icegate_common::DEFAULT_TENANT_ID;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, any_value::Value},
    metrics::v1::metric,
};

/// Returns the Arrow schema for logs, derived from the Iceberg schema.
///
/// Uses `icegate_common::schema::logs_schema()` as the source of truth
/// and converts it to Arrow format using
/// `iceberg::arrow::schema_to_arrow_schema()`.
///
/// # Panics
///
/// Panics if the Iceberg schema cannot be created or converted to Arrow.
/// This should never happen in practice as the schema is statically defined.
#[allow(clippy::expect_used)]
pub fn logs_arrow_schema() -> Schema {
    let iceberg_schema = icegate_common::schema::logs_schema().expect("logs_schema should always be valid");
    schema_to_arrow_schema(&iceberg_schema).expect("schema conversion should succeed")
}

/// Returns the Arrow schema for metrics, derived from the Iceberg schema.
///
/// Uses `icegate_common::schema::metrics_schema()` as the source of truth
/// and converts it to Arrow format using
/// `iceberg::arrow::schema_to_arrow_schema()`.
///
/// # Panics
///
/// Panics if the Iceberg schema cannot be created or converted to Arrow.
/// This should never happen in practice as the schema is statically defined.
#[allow(clippy::expect_used)]
pub fn metrics_arrow_schema() -> Schema {
    let iceberg_schema = icegate_common::schema::metrics_schema().expect("metrics_schema should always be valid");
    schema_to_arrow_schema(&iceberg_schema).expect("schema conversion should succeed")
}

/// Transforms an OTLP logs export request to an Arrow `RecordBatch`.
///
/// Extracts all log records from the request, merging resource and scope
/// attributes into each log record's attributes.
///
/// # Arguments
///
/// * `request` - The OTLP export logs request
/// * `tenant_id` - Tenant identifier (from request metadata or default)
/// * `account_id` - Optional account identifier
///
/// # Returns
///
/// Arrow `RecordBatch` matching the logs schema wrapped in `Ok(Some(_))`,
/// or `Ok(None)` if no records are present.
///
/// # Errors
///
/// Returns `IngestError` if:
/// - Schema validation fails
/// - `RecordBatch` creation fails
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_possible_truncation)] // Timestamp fits in i64 for practical purposes
#[allow(clippy::expect_used)] // Byte lengths are validated before append
#[tracing::instrument(skip(request))]
pub fn logs_to_record_batch(
    request: &ExportLogsServiceRequest,
    tenant_id: Option<&str>,
) -> crate::error::Result<Option<RecordBatch>> {
    let ingested_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_micros() as i64;

    // Count total log records for capacity hints
    let total_records: usize = request
        .resource_logs
        .iter()
        .flat_map(|rl| &rl.scope_logs)
        .map(|sl| sl.log_records.len())
        .sum();

    if total_records == 0 {
        return Ok(None);
    }

    // Get the Arrow schema to extract correct field types for map builder
    let schema = logs_arrow_schema();
    let (key_field, value_field) = extract_map_fields_from_schema(&schema)?;

    // Initialize builders
    let mut tenant_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut account_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut service_name_builder = StringBuilder::with_capacity(total_records, total_records * 32);
    let mut timestamp_builder = Vec::with_capacity(total_records);
    let mut observed_timestamp_builder = Vec::with_capacity(total_records);
    let mut ingested_timestamp_builder = Vec::with_capacity(total_records);
    let mut trace_id_builder = StringBuilder::with_capacity(total_records, total_records * 32);
    let mut span_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut severity_text_builder = StringBuilder::with_capacity(total_records, total_records * 8);
    let mut body_builder = StringBuilder::with_capacity(total_records, total_records * 256);

    // Create map builder with correct field names matching Iceberg schema
    let field_names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut attributes_builder = MapBuilder::new(Some(field_names), StringBuilder::new(), StringBuilder::new())
        .with_keys_field(key_field)
        .with_values_field(value_field);

    let tenant = tenant_id.unwrap_or(DEFAULT_TENANT_ID);

    // Empty slice for default attributes
    let empty_attrs: Vec<opentelemetry_proto::tonic::common::v1::KeyValue> = Vec::new();

    // Process each resource_logs -> scope_logs -> log_record
    for resource_logs in &request.resource_logs {
        let resource_attrs = resource_logs.resource.as_ref().map_or(&empty_attrs, |r| &r.attributes);

        // Extract service.name from resource attributes
        let service_name = resource_attrs
            .iter()
            .find(|kv| kv.key == "service.name")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));
        let cloud_account_id = resource_attrs
            .iter()
            .find(|kv| kv.key == "cloud.account.id")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for scope_logs in &resource_logs.scope_logs {
            let scope_attrs = scope_logs.scope.as_ref().map_or(&empty_attrs, |s| &s.attributes);

            for log_record in &scope_logs.log_records {
                // tenant_id
                tenant_id_builder.append_value(tenant);

                // account_id (optional - null if not provided)
                if let Some(ref acc) = cloud_account_id {
                    account_id_builder.append_value(acc);
                } else {
                    account_id_builder.append_null();
                }

                // service_name
                if let Some(ref svc) = service_name {
                    service_name_builder.append_value(svc);
                } else {
                    service_name_builder.append_null();
                }

                // timestamp (nanoseconds to microseconds)
                let timestamp_micros = if log_record.time_unix_nano > 0 {
                    (log_record.time_unix_nano / 1000) as i64
                } else {
                    ingested_timestamp
                };
                timestamp_builder.push(timestamp_micros);

                // observed_timestamp
                let observed_micros = if log_record.observed_time_unix_nano > 0 {
                    (log_record.observed_time_unix_nano / 1000) as i64
                } else {
                    ingested_timestamp
                };
                observed_timestamp_builder.push(observed_micros);

                // ingested_timestamp
                ingested_timestamp_builder.push(ingested_timestamp);

                // trace_id and span_id
                let (trace_id_hex, span_id_hex) =
                    process_trace_span_ids(log_record, &mut trace_id_builder, &mut span_id_builder);

                // severity_text
                if log_record.severity_text.is_empty() {
                    severity_text_builder.append_null();
                } else {
                    severity_text_builder.append_value(&log_record.severity_text);
                }

                // body (JSON-serialized for Loki compatibility)
                if let Some(body_str) = serialize_any_value_to_json(log_record.body.as_ref()) {
                    body_builder.append_value(body_str);
                } else {
                    body_builder.append_null();
                }

                // attributes (merged from resource, scope, and log record)
                // Add resource, scope, and log record attributes (flatten nested, normalize keys)
                add_flattened_attributes(resource_attrs, &mut attributes_builder);
                add_flattened_attributes(scope_attrs, &mut attributes_builder);
                add_flattened_attributes(&log_record.attributes, &mut attributes_builder);

                // Duplicate indexed columns into attributes map for query layer
                add_indexed_columns_to_attributes(
                    &mut attributes_builder,
                    trace_id_hex.as_ref(),
                    span_id_hex.as_ref(),
                    &log_record.severity_text,
                    cloud_account_id.as_ref(),
                );

                attributes_builder.append(true).expect("append map entry");
            }
        }
    }

    // Build arrays
    let schema = Arc::new(schema);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_id_builder.finish()),
        Arc::new(account_id_builder.finish()),
        Arc::new(service_name_builder.finish()),
        Arc::new(TimestampMicrosecondArray::from(timestamp_builder)),
        Arc::new(TimestampMicrosecondArray::from(observed_timestamp_builder)),
        Arc::new(TimestampMicrosecondArray::from(ingested_timestamp_builder)),
        Arc::new(trace_id_builder.finish()),
        Arc::new(span_id_builder.finish()),
        Arc::new(severity_text_builder.finish()),
        Arc::new(body_builder.finish()),
        Arc::new(attributes_builder.finish()),
    ];

    RecordBatch::try_new(schema, columns).map(Some).map_err(|e| {
        tracing::error!("Failed to create RecordBatch: {e}");
        crate::error::IngestError::Validation(format!("Failed to create RecordBatch: {e}"))
    })
}

/// Counts total data points across all metric types in an OTLP request.
///
/// Walks the `ResourceMetrics → ScopeMetrics → Metric → Data` hierarchy
/// and sums data points from all metric type variants (gauge, sum, histogram,
/// exponential histogram, summary).
fn count_metric_data_points(request: &ExportMetricsServiceRequest) -> usize {
    request
        .resource_metrics
        .iter()
        .flat_map(|rm| &rm.scope_metrics)
        .flat_map(|sm| &sm.metrics)
        .map(|m| match &m.data {
            Some(metric::Data::Gauge(g)) => g.data_points.len(),
            Some(metric::Data::Sum(s)) => s.data_points.len(),
            Some(metric::Data::Histogram(h)) => h.data_points.len(),
            Some(metric::Data::ExponentialHistogram(eh)) => eh.data_points.len(),
            Some(metric::Data::Summary(s)) => s.data_points.len(),
            None => 0,
        })
        .sum()
}

/// Converts an OTLP `AggregationTemporality` `i32` enum value to a string.
///
/// Returns `None` for unspecified (0) or unknown values.
const fn aggregation_temporality_to_str(value: i32) -> Option<&'static str> {
    match value {
        1 => Some("DELTA"),
        2 => Some("CUMULATIVE"),
        _ => None,
    }
}

/// Helper struct that holds all 32 Arrow column builders for the metrics schema.
///
/// Grouping builders in a struct avoids passing 32+ parameters through functions
/// and keeps the code organized by column category.
struct MetricsBuilders {
    // Identity fields (columns 0-3)
    tenant_id: StringBuilder,
    cloud_account_id: StringBuilder,
    service_name: StringBuilder,
    service_instance_id: StringBuilder,
    // Timestamp fields (columns 4-6)
    timestamp: TimestampMicrosecondBuilder,
    start_timestamp: TimestampMicrosecondBuilder,
    ingested_timestamp: Vec<i64>,
    // Metric identification (columns 7-10)
    metric_name: StringBuilder,
    metric_type: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    // Metric metadata (columns 11-12)
    aggregation_temporality: StringBuilder,
    is_monotonic: BooleanBuilder,
    // Attributes (column 13)
    attributes: MapBuilder<StringBuilder, StringBuilder>,
    // Value fields (columns 14-15)
    value_double: Float64Builder,
    value_int: Int64Builder,
    // Common histogram fields (columns 16-19)
    count: Int64Builder,
    sum: Float64Builder,
    min: Float64Builder,
    max: Float64Builder,
    // Standard histogram fields (columns 20-21)
    bucket_counts: ListBuilder<Int64Builder>,
    explicit_bounds: ListBuilder<Float64Builder>,
    // Exponential histogram fields (columns 22-28)
    scale: Int32Builder,
    zero_count: Int64Builder,
    zero_threshold: Float64Builder,
    positive_offset: Int32Builder,
    positive_bucket_counts: ListBuilder<Int64Builder>,
    negative_offset: Int32Builder,
    negative_bucket_counts: ListBuilder<Int64Builder>,
    // Summary fields (column 29)
    quantile_values: ListBuilder<StructBuilder>,
    // Flags and exemplars (columns 30-31)
    flags: Int32Builder,
    exemplars: ListBuilder<StructBuilder>,
}

impl MetricsBuilders {
    /// Extracts the inner field definition from a List column in the schema.
    ///
    /// Returns the `FieldRef` for the list's element type so that
    /// `ListBuilder` can produce arrays matching the schema exactly.
    fn extract_list_element_field(
        schema: &Schema,
        column_name: &str,
    ) -> crate::error::Result<arrow::datatypes::FieldRef> {
        let field = schema.field_with_name(column_name).map_err(|_| {
            crate::error::IngestError::Validation(format!("Schema must contain a '{column_name}' field"))
        })?;
        match field.data_type() {
            DataType::List(inner) => Ok(inner.clone()),
            _ => Err(crate::error::IngestError::Validation(format!(
                "Expected List type for '{column_name}'"
            ))),
        }
    }

    /// Creates a new `MetricsBuilders` with the given capacity and schema-derived fields.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Expected number of data point rows
    /// * `schema` - The Arrow schema for metrics, used to extract nested field types
    ///
    /// # Errors
    ///
    /// Returns `IngestError::Validation` if the schema does not contain expected fields.
    fn new(capacity: usize, schema: &Schema) -> crate::error::Result<Self> {
        let (key_field, value_field) = extract_map_fields_from_schema(schema)?;

        let field_names = MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let attributes = MapBuilder::new(Some(field_names), StringBuilder::new(), StringBuilder::new())
            .with_keys_field(key_field)
            .with_values_field(value_field);

        // Build quantile_values ListBuilder<StructBuilder> from schema
        let quantile_values = Self::build_quantile_values_builder(schema)?;

        // Build exemplars ListBuilder<StructBuilder> from schema
        let exemplars = Self::build_exemplars_builder(schema)?;

        // Extract inner field definitions for list columns so the builders
        // produce arrays that match the schema's field names and nullability.
        let bucket_counts_field = Self::extract_list_element_field(schema, "bucket_counts")?;
        let explicit_bounds_field = Self::extract_list_element_field(schema, "explicit_bounds")?;
        let positive_bc_field = Self::extract_list_element_field(schema, "positive_bucket_counts")?;
        let negative_bc_field = Self::extract_list_element_field(schema, "negative_bucket_counts")?;

        Ok(Self {
            tenant_id: StringBuilder::with_capacity(capacity, capacity * 16),
            cloud_account_id: StringBuilder::with_capacity(capacity, capacity * 16),
            service_name: StringBuilder::with_capacity(capacity, capacity * 32),
            service_instance_id: StringBuilder::with_capacity(capacity, capacity * 16),
            timestamp: TimestampMicrosecondBuilder::with_capacity(capacity),
            start_timestamp: TimestampMicrosecondBuilder::with_capacity(capacity),
            ingested_timestamp: Vec::with_capacity(capacity),
            metric_name: StringBuilder::with_capacity(capacity, capacity * 32),
            metric_type: StringBuilder::with_capacity(capacity, capacity * 16),
            description: StringBuilder::with_capacity(capacity, capacity * 64),
            unit: StringBuilder::with_capacity(capacity, capacity * 8),
            aggregation_temporality: StringBuilder::with_capacity(capacity, capacity * 12),
            is_monotonic: BooleanBuilder::with_capacity(capacity),
            attributes,
            value_double: Float64Builder::with_capacity(capacity),
            value_int: Int64Builder::with_capacity(capacity),
            count: Int64Builder::with_capacity(capacity),
            sum: Float64Builder::with_capacity(capacity),
            min: Float64Builder::with_capacity(capacity),
            max: Float64Builder::with_capacity(capacity),
            bucket_counts: ListBuilder::new(Int64Builder::new()).with_field(bucket_counts_field),
            explicit_bounds: ListBuilder::new(Float64Builder::new()).with_field(explicit_bounds_field),
            scale: Int32Builder::with_capacity(capacity),
            zero_count: Int64Builder::with_capacity(capacity),
            zero_threshold: Float64Builder::with_capacity(capacity),
            positive_offset: Int32Builder::with_capacity(capacity),
            positive_bucket_counts: ListBuilder::new(Int64Builder::new()).with_field(positive_bc_field),
            negative_offset: Int32Builder::with_capacity(capacity),
            negative_bucket_counts: ListBuilder::new(Int64Builder::new()).with_field(negative_bc_field),
            quantile_values,
            flags: Int32Builder::with_capacity(capacity),
            exemplars,
        })
    }

    /// Builds a `ListBuilder<StructBuilder>` for the `quantile_values` column.
    ///
    /// The struct has fields: `quantile` (Float64) and `value` (Float64),
    /// matching the Iceberg schema definition.
    fn build_quantile_values_builder(schema: &Schema) -> crate::error::Result<ListBuilder<StructBuilder>> {
        let qv_field = schema.field_with_name("quantile_values").map_err(|_| {
            crate::error::IngestError::Validation("Schema must contain a 'quantile_values' field".to_string())
        })?;

        // Extract the inner field and struct fields from List<Struct<quantile, value>>
        let inner_field = match qv_field.data_type() {
            DataType::List(inner) => inner.clone(),
            _ => {
                return Err(crate::error::IngestError::Validation(
                    "Expected List type for quantile_values".to_string(),
                ));
            }
        };

        let struct_fields = match inner_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => {
                return Err(crate::error::IngestError::Validation(
                    "Expected Struct inside quantile_values List".to_string(),
                ));
            }
        };

        let struct_builder = StructBuilder::from_fields(struct_fields, 0);
        Ok(ListBuilder::new(struct_builder).with_field(inner_field))
    }

    /// Builds a `ListBuilder<StructBuilder>` for the `exemplars` column.
    ///
    /// The struct has fields: `timestamp`, `value_double`, `value_int`,
    /// `span_id`, `trace_id`, and `attributes` (Map).
    fn build_exemplars_builder(schema: &Schema) -> crate::error::Result<ListBuilder<StructBuilder>> {
        let ex_field = schema.field_with_name("exemplars").map_err(|_| {
            crate::error::IngestError::Validation("Schema must contain an 'exemplars' field".to_string())
        })?;

        let inner_field = match ex_field.data_type() {
            DataType::List(inner) => inner.clone(),
            _ => {
                return Err(crate::error::IngestError::Validation(
                    "Expected List type for exemplars".to_string(),
                ));
            }
        };

        let struct_fields = match inner_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => {
                return Err(crate::error::IngestError::Validation(
                    "Expected Struct inside exemplars List".to_string(),
                ));
            }
        };

        let struct_builder = StructBuilder::from_fields(struct_fields, 0);
        Ok(ListBuilder::new(struct_builder).with_field(inner_field))
    }

    /// Appends null values for all type-specific columns.
    ///
    /// Called for columns that do not apply to the current metric type.
    /// For example, histogram-specific columns are null for gauge data points.
    fn append_null_gauge_sum_fields(&mut self) {
        self.value_double.append_null();
        self.value_int.append_null();
    }

    /// Appends null values for all histogram-specific columns.
    ///
    /// List columns use `append(false)` (empty null list) instead of
    /// `append_null()` to avoid Iceberg writer issues with null list entries.
    fn append_null_histogram_fields(&mut self) {
        self.count.append_null();
        self.sum.append_null();
        self.min.append_null();
        self.max.append_null();
        self.bucket_counts.append(false);
        self.explicit_bounds.append(false);
    }

    /// Appends null values for all exponential histogram-specific columns.
    fn append_null_exp_histogram_fields(&mut self) {
        self.scale.append_null();
        self.zero_count.append_null();
        self.zero_threshold.append_null();
        self.positive_offset.append_null();
        self.positive_bucket_counts.append(false);
        self.negative_offset.append_null();
        self.negative_bucket_counts.append(false);
    }

    /// Appends null values for the summary-specific `quantile_values` column.
    fn append_null_summary_fields(&mut self) {
        self.quantile_values.append(false);
    }

    /// Finishes all builders and returns the column arrays in schema order.
    ///
    /// # Panics
    ///
    /// Panics if builder finalization fails, which should not happen
    /// if data was appended consistently.
    fn finish(mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.tenant_id.finish()),
            Arc::new(self.cloud_account_id.finish()),
            Arc::new(self.service_name.finish()),
            Arc::new(self.service_instance_id.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.start_timestamp.finish()),
            Arc::new(TimestampMicrosecondArray::from(self.ingested_timestamp)),
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
        ]
    }
}

/// Transforms an OTLP metrics export request to an Arrow `RecordBatch`.
///
/// Extracts all data points from all metric types in the request,
/// merging resource and scope attributes into each data point's attributes.
/// Each data point becomes one row in the output `RecordBatch`.
///
/// # Arguments
///
/// * `request` - The OTLP export metrics request
/// * `tenant_id` - Tenant identifier (from request metadata or default)
///
/// # Returns
///
/// Arrow `RecordBatch` matching the metrics schema wrapped in `Ok(Some(_))`,
/// or `Ok(None)` if no data points are present.
///
/// # Errors
///
/// Returns `IngestError` if:
/// - Schema validation fails
/// - `RecordBatch` creation fails
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::expect_used)]
#[allow(clippy::too_many_lines)]
#[tracing::instrument(skip(request))]
pub fn metrics_to_record_batch(
    request: &ExportMetricsServiceRequest,
    tenant_id: Option<&str>,
) -> crate::error::Result<Option<RecordBatch>> {
    let ingested_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_micros() as i64;

    let total_points = count_metric_data_points(request);
    if total_points == 0 {
        return Ok(None);
    }

    let schema = metrics_arrow_schema();
    let mut b = MetricsBuilders::new(total_points, &schema)?;

    let tenant = tenant_id.unwrap_or(DEFAULT_TENANT_ID);
    let empty_attrs: Vec<opentelemetry_proto::tonic::common::v1::KeyValue> = Vec::new();

    for resource_metrics in &request.resource_metrics {
        let resource_attrs = resource_metrics.resource.as_ref().map_or(&empty_attrs, |r| &r.attributes);

        let service_name = resource_attrs
            .iter()
            .find(|kv| kv.key == "service.name")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));
        let cloud_account_id = resource_attrs
            .iter()
            .find(|kv| kv.key == "cloud.account.id")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));
        let service_instance_id = resource_attrs
            .iter()
            .find(|kv| kv.key == "service.instance.id")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for scope_metrics in &resource_metrics.scope_metrics {
            let scope_attrs = scope_metrics.scope.as_ref().map_or(&empty_attrs, |s| &s.attributes);

            for metric in &scope_metrics.metrics {
                let Some(ref data) = metric.data else {
                    continue;
                };

                match data {
                    metric::Data::Gauge(gauge) => {
                        for dp in &gauge.data_points {
                            append_common_fields(
                                &mut b,
                                tenant,
                                cloud_account_id.as_ref(),
                                service_name.as_ref(),
                                service_instance_id.as_ref(),
                                dp.time_unix_nano,
                                dp.start_time_unix_nano,
                                ingested_timestamp,
                                &metric.name,
                                "gauge",
                                &metric.description,
                                &metric.unit,
                                None, // aggregation_temporality
                                None, // is_monotonic
                                resource_attrs,
                                scope_attrs,
                                &dp.attributes,
                                dp.flags,
                            );
                            // Gauge/Sum value fields
                            match &dp.value {
                                Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                                    v,
                                )) => {
                                    b.value_double.append_value(*v);
                                    b.value_int.append_null();
                                }
                                Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(v)) => {
                                    b.value_double.append_null();
                                    b.value_int.append_value(*v);
                                }
                                None => {
                                    b.append_null_gauge_sum_fields();
                                }
                            }
                            b.append_null_histogram_fields();
                            b.append_null_exp_histogram_fields();
                            b.append_null_summary_fields();
                            b.exemplars.append(false);
                        }
                    }
                    metric::Data::Sum(sum) => {
                        let temporality = aggregation_temporality_to_str(sum.aggregation_temporality);
                        for dp in &sum.data_points {
                            append_common_fields(
                                &mut b,
                                tenant,
                                cloud_account_id.as_ref(),
                                service_name.as_ref(),
                                service_instance_id.as_ref(),
                                dp.time_unix_nano,
                                dp.start_time_unix_nano,
                                ingested_timestamp,
                                &metric.name,
                                "sum",
                                &metric.description,
                                &metric.unit,
                                temporality,
                                Some(sum.is_monotonic),
                                resource_attrs,
                                scope_attrs,
                                &dp.attributes,
                                dp.flags,
                            );
                            match &dp.value {
                                Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                                    v,
                                )) => {
                                    b.value_double.append_value(*v);
                                    b.value_int.append_null();
                                }
                                Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(v)) => {
                                    b.value_double.append_null();
                                    b.value_int.append_value(*v);
                                }
                                None => {
                                    b.append_null_gauge_sum_fields();
                                }
                            }
                            b.append_null_histogram_fields();
                            b.append_null_exp_histogram_fields();
                            b.append_null_summary_fields();
                            b.exemplars.append(false);
                        }
                    }
                    metric::Data::Histogram(histogram) => {
                        let temporality = aggregation_temporality_to_str(histogram.aggregation_temporality);
                        for dp in &histogram.data_points {
                            append_common_fields(
                                &mut b,
                                tenant,
                                cloud_account_id.as_ref(),
                                service_name.as_ref(),
                                service_instance_id.as_ref(),
                                dp.time_unix_nano,
                                dp.start_time_unix_nano,
                                ingested_timestamp,
                                &metric.name,
                                "histogram",
                                &metric.description,
                                &metric.unit,
                                temporality,
                                None,
                                resource_attrs,
                                scope_attrs,
                                &dp.attributes,
                                dp.flags,
                            );
                            // Gauge/Sum value fields are null for histograms
                            b.append_null_gauge_sum_fields();

                            // Histogram-specific fields
                            b.count.append_value(dp.count as i64);
                            match dp.sum {
                                Some(v) => b.sum.append_value(v),
                                None => b.sum.append_null(),
                            }
                            match dp.min {
                                Some(v) => b.min.append_value(v),
                                None => b.min.append_null(),
                            }
                            match dp.max {
                                Some(v) => b.max.append_value(v),
                                None => b.max.append_null(),
                            }
                            // bucket_counts
                            for &bc in &dp.bucket_counts {
                                b.bucket_counts.values().append_value(bc as i64);
                            }
                            b.bucket_counts.append(true);
                            // explicit_bounds
                            for &eb in &dp.explicit_bounds {
                                b.explicit_bounds.values().append_value(eb);
                            }
                            b.explicit_bounds.append(true);

                            b.append_null_exp_histogram_fields();
                            b.append_null_summary_fields();
                            b.exemplars.append(false);
                        }
                    }
                    metric::Data::ExponentialHistogram(exp_histogram) => {
                        let temporality = aggregation_temporality_to_str(exp_histogram.aggregation_temporality);
                        for dp in &exp_histogram.data_points {
                            append_common_fields(
                                &mut b,
                                tenant,
                                cloud_account_id.as_ref(),
                                service_name.as_ref(),
                                service_instance_id.as_ref(),
                                dp.time_unix_nano,
                                dp.start_time_unix_nano,
                                ingested_timestamp,
                                &metric.name,
                                "exponential_histogram",
                                &metric.description,
                                &metric.unit,
                                temporality,
                                None,
                                resource_attrs,
                                scope_attrs,
                                &dp.attributes,
                                dp.flags,
                            );
                            b.append_null_gauge_sum_fields();

                            // Shared histogram fields
                            b.count.append_value(dp.count as i64);
                            match dp.sum {
                                Some(v) => b.sum.append_value(v),
                                None => b.sum.append_null(),
                            }
                            match dp.min {
                                Some(v) => b.min.append_value(v),
                                None => b.min.append_null(),
                            }
                            match dp.max {
                                Some(v) => b.max.append_value(v),
                                None => b.max.append_null(),
                            }
                            // Standard histogram fields are null
                            b.bucket_counts.append_null();
                            b.explicit_bounds.append_null();

                            // Exponential histogram fields
                            b.scale.append_value(dp.scale);
                            b.zero_count.append_value(dp.zero_count as i64);
                            b.zero_threshold.append_value(dp.zero_threshold);

                            if let Some(ref pos) = dp.positive {
                                b.positive_offset.append_value(pos.offset);
                                for &bc in &pos.bucket_counts {
                                    b.positive_bucket_counts.values().append_value(bc as i64);
                                }
                                b.positive_bucket_counts.append(true);
                            } else {
                                b.positive_offset.append_null();
                                b.positive_bucket_counts.append_null();
                            }

                            if let Some(ref neg) = dp.negative {
                                b.negative_offset.append_value(neg.offset);
                                for &bc in &neg.bucket_counts {
                                    b.negative_bucket_counts.values().append_value(bc as i64);
                                }
                                b.negative_bucket_counts.append(true);
                            } else {
                                b.negative_offset.append_null();
                                b.negative_bucket_counts.append_null();
                            }

                            b.append_null_summary_fields();
                            b.exemplars.append(false);
                        }
                    }
                    metric::Data::Summary(summary) => {
                        for dp in &summary.data_points {
                            append_common_fields(
                                &mut b,
                                tenant,
                                cloud_account_id.as_ref(),
                                service_name.as_ref(),
                                service_instance_id.as_ref(),
                                dp.time_unix_nano,
                                dp.start_time_unix_nano,
                                ingested_timestamp,
                                &metric.name,
                                "summary",
                                &metric.description,
                                &metric.unit,
                                None,
                                None,
                                resource_attrs,
                                scope_attrs,
                                &dp.attributes,
                                dp.flags,
                            );
                            b.append_null_gauge_sum_fields();

                            // Summary uses count/sum but not min/max
                            b.count.append_value(dp.count as i64);
                            b.sum.append_value(dp.sum);
                            b.min.append_null();
                            b.max.append_null();
                            b.bucket_counts.append_null();
                            b.explicit_bounds.append_null();

                            b.append_null_exp_histogram_fields();

                            // Quantile values
                            let qv_builder = &mut b.quantile_values;
                            let struct_builder = qv_builder.values();
                            for qv in &dp.quantile_values {
                                struct_builder
                                    .field_builder::<Float64Builder>(0)
                                    .expect("quantile field builder")
                                    .append_value(qv.quantile);
                                struct_builder
                                    .field_builder::<Float64Builder>(1)
                                    .expect("value field builder")
                                    .append_value(qv.value);
                                struct_builder.append(true);
                            }
                            qv_builder.append(true);

                            b.exemplars.append(false);
                        }
                    }
                }
            }
        }
    }

    let schema = Arc::new(schema);
    let columns = b.finish();

    RecordBatch::try_new(schema, columns).map(Some).map_err(|e| {
        tracing::error!("Failed to create metrics RecordBatch: {e}");
        crate::error::IngestError::Validation(format!("Failed to create metrics RecordBatch: {e}"))
    })
}

/// Appends common fields shared by all metric data point types.
///
/// Handles identity fields (tenant, service), timestamps, metric metadata,
/// attributes merging, and flags.
#[allow(clippy::too_many_arguments)]
fn append_common_fields(
    b: &mut MetricsBuilders,
    tenant: &str,
    cloud_account_id: Option<&String>,
    service_name: Option<&String>,
    service_instance_id: Option<&String>,
    time_unix_nano: u64,
    start_time_unix_nano: u64,
    ingested_timestamp: i64,
    metric_name: &str,
    metric_type: &str,
    description: &str,
    unit: &str,
    aggregation_temporality: Option<&str>,
    is_monotonic: Option<bool>,
    resource_attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    scope_attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    dp_attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    flags: u32,
) {
    // Identity
    b.tenant_id.append_value(tenant);
    match cloud_account_id {
        Some(v) => b.cloud_account_id.append_value(v),
        None => b.cloud_account_id.append_null(),
    }
    match service_name {
        Some(v) => b.service_name.append_value(v),
        None => b.service_name.append_null(),
    }
    match service_instance_id {
        Some(v) => b.service_instance_id.append_value(v),
        None => b.service_instance_id.append_null(),
    }

    // Timestamps (nanoseconds to microseconds)
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    let ts_micros = if time_unix_nano > 0 {
        (time_unix_nano / 1000) as i64
    } else {
        ingested_timestamp
    };
    b.timestamp.append_value(ts_micros);

    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    if start_time_unix_nano > 0 {
        b.start_timestamp.append_value((start_time_unix_nano / 1000) as i64);
    } else {
        b.start_timestamp.append_null();
    }

    b.ingested_timestamp.push(ingested_timestamp);

    // Metric identification
    b.metric_name.append_value(normalize_attribute_key(metric_name));
    b.metric_type.append_value(metric_type);
    if description.is_empty() {
        b.description.append_null();
    } else {
        b.description.append_value(description);
    }
    if unit.is_empty() {
        b.unit.append_null();
    } else {
        b.unit.append_value(unit);
    }

    // Metadata
    match aggregation_temporality {
        Some(v) => b.aggregation_temporality.append_value(v),
        None => b.aggregation_temporality.append_null(),
    }
    match is_monotonic {
        Some(v) => b.is_monotonic.append_value(v),
        None => b.is_monotonic.append_null(),
    }

    // Attributes (merged from resource, scope, and data point)
    add_flattened_attributes(resource_attrs, &mut b.attributes);
    add_flattened_attributes(scope_attrs, &mut b.attributes);
    add_flattened_attributes(dp_attrs, &mut b.attributes);
    #[allow(clippy::expect_used)]
    b.attributes.append(true).expect("append map entry");

    // Flags
    #[allow(clippy::cast_possible_wrap)]
    if flags > 0 {
        b.flags.append_value(flags as i32);
    } else {
        b.flags.append_null();
    }
}

/// Extracts a string value from an OTLP `AnyValue` reference.
///
/// Converts various OTLP value types to string representation.
fn extract_any_value_string(value: Option<&AnyValue>) -> Option<String> {
    value.and_then(|v| {
        v.value.as_ref().map(|val| match val {
            Value::StringValue(s) => s.clone(),
            Value::IntValue(i) => i.to_string(),
            Value::DoubleValue(d) => d.to_string(),
            Value::BoolValue(b) => b.to_string(),
            Value::BytesValue(b) => hex::encode(b),
            Value::ArrayValue(arr) => {
                let items: Vec<String> = arr.values.iter().filter_map(|v| extract_any_value_string(Some(v))).collect();
                format!("[{}]", items.join(", "))
            }
            Value::KvlistValue(kvs) => {
                let pairs: Vec<String> = kvs
                    .values
                    .iter()
                    .filter_map(|kv| extract_any_value_string(kv.value.as_ref()).map(|v| format!("{}={}", kv.key, v)))
                    .collect();
                format!("{{{}}}", pairs.join(", "))
            }
        })
    })
}

/// Extracts a string value from an `Option<AnyValue>`.
fn extract_string_value(value: Option<&AnyValue>) -> Option<String> {
    extract_any_value_string(value)
}

/// Serializes an OTLP `AnyValue` to JSON string format.
///
/// This is used specifically for the LogRecord.Body field, which should be
/// JSON-serialized according to Loki requirements.
///
/// # Arguments
///
/// * `value` - OTLP `AnyValue` to serialize
///
/// # Returns
///
/// JSON string representation of the value, or None if value is None
///
/// # Examples
///
/// ```
/// // StringValue("hello") -> "hello" (no quotes)
/// // IntValue(42) -> "42"
/// // ArrayValue([1, 2]) -> "[1,2]"
/// // KvlistValue({a: 1}) -> "{\"a\":1}"
/// ```
fn serialize_any_value_to_json(value: Option<&AnyValue>) -> Option<String> {
    value.and_then(|v| {
        v.value.as_ref().and_then(|val| match val {
            Value::StringValue(s) => Some(s.clone()),
            Value::IntValue(i) => Some(i.to_string()),
            Value::DoubleValue(d) => Some(d.to_string()),
            Value::BoolValue(b) => Some(b.to_string()),
            Value::BytesValue(b) => Some(hex::encode(b)),
            Value::ArrayValue(arr) => {
                let json_array: Vec<serde_json::Value> = arr.values.iter().filter_map(any_value_to_json).collect();
                serde_json::to_string(&json_array).ok()
            }
            Value::KvlistValue(kvs) => {
                let mut json_object = serde_json::Map::new();
                for kv in &kvs.values {
                    if let Some(json_val) = kv.value.as_ref().and_then(any_value_to_json) {
                        json_object.insert(kv.key.clone(), json_val);
                    }
                }
                serde_json::to_string(&json_object).ok()
            }
        })
    })
}

/// Helper to convert `AnyValue` to `serde_json::Value` for JSON serialization.
fn any_value_to_json(value: &AnyValue) -> Option<serde_json::Value> {
    value.value.as_ref().map(|val| match val {
        Value::StringValue(s) => serde_json::Value::String(s.clone()),
        Value::IntValue(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
        Value::DoubleValue(d) => {
            serde_json::Number::from_f64(*d).map_or(serde_json::Value::Null, serde_json::Value::Number)
        }
        Value::BoolValue(b) => serde_json::Value::Bool(*b),
        Value::BytesValue(b) => serde_json::Value::String(hex::encode(b)),
        Value::ArrayValue(arr) => {
            let items: Vec<serde_json::Value> = arr.values.iter().filter_map(any_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        Value::KvlistValue(kvs) => {
            let mut map = serde_json::Map::new();
            for kv in &kvs.values {
                if let Some(v) = kv.value.as_ref().and_then(any_value_to_json) {
                    map.insert(kv.key.clone(), v);
                }
            }
            serde_json::Value::Object(map)
        }
    })
}

/// Checks if a byte slice is all zeros.
fn is_zero_bytes(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}

/// Extracts map field metadata from the Arrow schema for attributes field.
///
/// Returns a tuple of (`key_field`, `value_field`) from the schema's map type definition.
///
/// # Errors
///
/// Returns `IngestError::Validation` if:
/// - Schema does not contain an 'attributes' field
/// - The 'attributes' field is not of Map type
/// - The map entries are not of Struct type
/// - The struct does not contain at least 2 fields (key and value)
fn extract_map_fields_from_schema(
    schema: &Schema,
) -> crate::error::Result<(arrow::datatypes::FieldRef, arrow::datatypes::FieldRef)> {
    let attributes_field = schema
        .field_with_name("attributes")
        .map_err(|_| crate::error::IngestError::Validation("Schema must contain an 'attributes' field".to_string()))?;

    // Extract map field information from schema
    match attributes_field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => {
                if fields.len() < 2 {
                    return Err(crate::error::IngestError::Validation(format!(
                        "Expected at least 2 fields in map entries struct, found {}",
                        fields.len()
                    )));
                }
                Ok((fields[0].clone(), fields[1].clone()))
            }
            _ => Err(crate::error::IngestError::Validation(
                "Expected Struct type for map entries in 'attributes' field".to_string(),
            )),
        },
        _ => Err(crate::error::IngestError::Validation(format!(
            "Expected Map type for 'attributes' field, found {:?}",
            attributes_field.data_type()
        ))),
    }
}

/// Adds flattened attributes to the map builder with key normalization.
///
/// Flattens nested structures and normalizes attribute keys by replacing dots with underscores.
fn add_flattened_attributes(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    attributes_builder: &mut MapBuilder<StringBuilder, StringBuilder>,
) {
    for kv in attributes {
        let flattened = flatten_any_value(&kv.key, kv.value.as_ref());
        for (key, value) in flattened {
            attributes_builder.keys().append_value(normalize_attribute_key(&key));
            attributes_builder.values().append_value(value);
        }
    }
}

/// Processes trace and span IDs, returning hex-encoded values if valid.
///
/// Returns a tuple of (`trace_id_hex`, `span_id_hex`) where each is `Some(String)` if valid,
/// or `None` if invalid or all zeros.
fn process_trace_span_ids(
    log_record: &opentelemetry_proto::tonic::logs::v1::LogRecord,
    trace_id_builder: &mut StringBuilder,
    span_id_builder: &mut StringBuilder,
) -> (Option<String>, Option<String>) {
    // trace_id (16 bytes → 32 hex chars)
    let trace_id_hex = if log_record.trace_id.len() == 16 && !is_zero_bytes(&log_record.trace_id) {
        let hex_string = hex::encode(&log_record.trace_id);
        trace_id_builder.append_value(hex_string.clone());
        Some(hex_string)
    } else {
        trace_id_builder.append_null();
        None
    };

    // span_id (8 bytes → 16 hex chars)
    let span_id_hex = if log_record.span_id.len() == 8 && !is_zero_bytes(&log_record.span_id) {
        let hex_string = hex::encode(&log_record.span_id);
        span_id_builder.append_value(hex_string.clone());
        Some(hex_string)
    } else {
        span_id_builder.append_null();
        None
    };

    (trace_id_hex, span_id_hex)
}

/// Adds indexed columns to the attributes map for query layer compatibility.
///
/// Duplicates `trace_id`, `span_id`, `severity_text`, and `account_id` into the attributes map
/// to allow queries to use indexed columns for filtering then switch to attributes for operations.
fn add_indexed_columns_to_attributes(
    attributes_builder: &mut MapBuilder<StringBuilder, StringBuilder>,
    trace_id_hex: Option<&String>,
    span_id_hex: Option<&String>,
    severity_text: &str,
    cloud_account_id: Option<&String>,
) {
    // trace_id
    if let Some(tid) = trace_id_hex {
        attributes_builder.keys().append_value("trace_id");
        attributes_builder.values().append_value(tid);
    }

    // span_id
    if let Some(sid) = span_id_hex {
        attributes_builder.keys().append_value("span_id");
        attributes_builder.values().append_value(sid);
    }

    // severity_text
    if !severity_text.is_empty() {
        attributes_builder.keys().append_value("severity_text");
        attributes_builder.values().append_value(severity_text);
    }

    // level (alias for severity_text for Grafana compatibility)
    if !severity_text.is_empty() {
        attributes_builder.keys().append_value("level"); // Loki support
        attributes_builder.values().append_value(severity_text);
    }

    // cloud_account_id
    if let Some(acc) = cloud_account_id {
        attributes_builder.keys().append_value("cloud_account_id");
        attributes_builder.values().append_value(acc);
    }
}

/// Recursively flattens an OTLP `AnyValue` into key-value pairs.
///
/// Nested `KvlistValue` structures are flattened using underscore separator,
/// matching Loki's behavior with JSON parser.
///
/// # Arguments
///
/// * `prefix` - Key prefix for nested values (use empty string for root)
/// * `value` - OTLP `AnyValue` to flatten
///
/// # Returns
///
/// Vector of (key, value) string pairs representing flattened structure
///
/// # Examples
///
/// ```
/// // Input: prefix="http", value=KvlistValue({method: "GET", details: {code: 200}})
/// // Output: [("http_method", "GET"), ("http_details_code", "200")]
/// ```
fn flatten_any_value(prefix: &str, value: Option<&AnyValue>) -> Vec<(String, String)> {
    let mut result = Vec::new();

    if let Some(v) = value {
        if let Some(val) = &v.value {
            match val {
                Value::KvlistValue(kvs) => {
                    // Recursively flatten nested key-value lists
                    for kv in &kvs.values {
                        let nested_prefix = if prefix.is_empty() {
                            kv.key.clone()
                        } else {
                            format!("{}_{}", prefix, kv.key)
                        };
                        let nested_pairs = flatten_any_value(&nested_prefix, kv.value.as_ref());
                        result.extend(nested_pairs);
                    }
                }
                Value::ArrayValue(arr) => {
                    // Arrays are stringified, not flattened (no indexable keys)
                    let items: Vec<String> =
                        arr.values.iter().filter_map(|v| extract_any_value_string(Some(v))).collect();
                    let stringified = format!("[{}]", items.join(", "));
                    result.push((prefix.to_string(), stringified));
                }
                _ => {
                    // Primitive types: stringify and return
                    if let Some(s) = extract_any_value_string(Some(v)) {
                        result.push((prefix.to_string(), s));
                    }
                }
            }
        }
    }

    result
}

/// Normalizes an attribute key by replacing dots with underscores.
///
/// `OpenTelemetry` uses dots in attribute names (e.g., "service.name"),
/// but some systems prefer underscores for compatibility.
fn normalize_attribute_key(key: &str) -> String {
    key.replace('.', "_")
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::{
        collector::metrics::v1::ExportMetricsServiceRequest as MetricsRequest,
        common::v1::{InstrumentationScope, KeyValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{
            AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
            HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
            exponential_histogram_data_point::Buckets, number_data_point::Value as NumberValue,
            summary_data_point::ValueAtQuantile,
        },
        resource::v1::Resource,
    };

    use super::*;

    fn make_resource(service_name: &str) -> Resource {
        Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(service_name.to_string())),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        }
    }

    fn make_metrics_request(metrics: Vec<Metric>, service_name: &str) -> MetricsRequest {
        MetricsRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(make_resource(service_name)),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[test]
    fn test_logs_to_record_batch_empty() {
        let request = ExportLogsServiceRequest { resource_logs: vec![] };

        let batch = logs_to_record_batch(&request, None).expect("should not error");
        assert!(batch.is_none());
    }

    #[test]
    fn test_logs_to_record_batch_single_record() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        observed_time_unix_nano: 1_700_000_000_000_000_000,
                        severity_number: 9, // INFO
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("Test log message".to_string())),
                        }),
                        attributes: vec![KeyValue {
                            key: "custom.attr".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("custom-value".to_string())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0; 16],
                        span_id: vec![0; 8],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let batch = logs_to_record_batch(&request, Some("test-tenant")).expect("should not error");
        assert!(batch.is_some());

        let batch = batch.expect("batch should exist");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 11);
    }

    #[test]
    fn test_normalize_attribute_key() {
        assert_eq!(normalize_attribute_key("service.name"), "service_name");
        assert_eq!(normalize_attribute_key("cloud.account.id"), "cloud_account_id");
        assert_eq!(normalize_attribute_key("no_dots"), "no_dots");
        assert_eq!(normalize_attribute_key(""), "");
    }

    #[test]
    fn test_extract_string_value_types() {
        // String
        let v = AnyValue {
            value: Some(Value::StringValue("hello".to_string())),
        };
        assert_eq!(extract_string_value(Some(&v)), Some("hello".to_string()));

        // Int
        let v = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        assert_eq!(extract_string_value(Some(&v)), Some("42".to_string()));

        // Bool
        let v = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert_eq!(extract_string_value(Some(&v)), Some("true".to_string()));

        // None
        assert_eq!(extract_string_value(None), None);
    }

    #[test]
    fn test_body_json_serialization_primitives() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        // String - returned as-is
        let string_val = AnyValue {
            value: Some(Value::StringValue("hello world".to_string())),
        };
        assert_eq!(
            serialize_any_value_to_json(Some(&string_val)),
            Some("hello world".to_string())
        );

        // Int - stringified
        let int_val = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        assert_eq!(serialize_any_value_to_json(Some(&int_val)), Some("42".to_string()));

        // Bool - stringified
        let bool_val = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert_eq!(serialize_any_value_to_json(Some(&bool_val)), Some("true".to_string()));
    }

    #[test]
    fn test_body_json_serialization_array() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, any_value::Value};

        let array_val = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("tag1".to_string())),
                    },
                    AnyValue {
                        value: Some(Value::IntValue(123)),
                    },
                ],
            })),
        };

        let result = serialize_any_value_to_json(Some(&array_val));
        assert!(result.is_some());

        // Should be valid JSON array
        let parsed: serde_json::Value =
            serde_json::from_str(&result.expect("result should exist")).expect("should parse as JSON");
        assert!(parsed.is_array());
        assert_eq!(parsed[0], "tag1");
        assert_eq!(parsed[1], 123);
    }

    #[test]
    fn test_body_json_serialization_object() {
        use opentelemetry_proto::tonic::common::v1::{KeyValueList, any_value::Value};

        let object_val = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "status".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::IntValue(200)),
                        }),
                    },
                    KeyValue {
                        key: "message".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("OK".to_string())),
                        }),
                    },
                ],
            })),
        };

        let result = serialize_any_value_to_json(Some(&object_val));
        assert!(result.is_some());

        // Should be valid JSON object
        let parsed: serde_json::Value =
            serde_json::from_str(&result.expect("result should exist")).expect("should parse as JSON");
        assert!(parsed.is_object());
        assert_eq!(parsed["status"], 200);
        assert_eq!(parsed["message"], "OK");
    }

    #[test]
    fn test_flatten_nested_attributes() {
        use std::collections::HashMap;

        use opentelemetry_proto::tonic::common::v1::{KeyValueList, any_value::Value};

        // Test nested KvlistValue flattening
        let nested_kv = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "method".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("POST".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "details".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![
                                    KeyValue {
                                        key: "status".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::IntValue(200)),
                                        }),
                                    },
                                    KeyValue {
                                        key: "path".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("/api/v1".to_string())),
                                        }),
                                    },
                                ],
                            })),
                        }),
                    },
                ],
            })),
        };

        let flattened = flatten_any_value("http", Some(&nested_kv));

        // Should produce 3 flattened entries
        assert_eq!(flattened.len(), 3);

        // Check flattened keys and values
        let map: HashMap<String, String> = flattened.into_iter().collect();
        assert_eq!(map.get("http_method"), Some(&"POST".to_string()));
        assert_eq!(map.get("http_details_status"), Some(&"200".to_string()));
        assert_eq!(map.get("http_details_path"), Some(&"/api/v1".to_string()));
    }

    #[test]
    fn test_flatten_array_attribute() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, any_value::Value};

        // Arrays should be stringified, not flattened (no meaningful keys)
        let array_value = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("tag1".to_string())),
                    },
                    AnyValue {
                        value: Some(Value::StringValue("tag2".to_string())),
                    },
                ],
            })),
        };

        let flattened = flatten_any_value("tags", Some(&array_value));

        // Should produce single stringified entry
        assert_eq!(flattened.len(), 1);
        assert_eq!(flattened[0].0, "tags");
        assert_eq!(flattened[0].1, "[tag1, tag2]");
    }

    #[test]
    fn test_flatten_primitive_attribute() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        // Primitive values should work as before
        let string_value = AnyValue {
            value: Some(Value::StringValue("simple".to_string())),
        };

        let flattened = flatten_any_value("key", Some(&string_value));

        assert_eq!(flattened.len(), 1);
        assert_eq!(flattened[0].0, "key");
        assert_eq!(flattened[0].1, "simple");
    }

    #[test]
    fn test_metrics_empty_request() {
        let request = MetricsRequest {
            resource_metrics: vec![],
        };
        let result = super::metrics_to_record_batch(&request, None).expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn test_metrics_gauge_double() {
        let metric = Metric {
            name: "cpu_temperature".to_string(),
            description: "CPU temperature in celsius".to_string(),
            unit: "celsius".to_string(),
            metadata: vec![],
            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![KeyValue {
                        key: "host".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("server-1".to_string())),
                        }),
                    }],
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_700_000_000_000_000_000,
                    value: Some(NumberValue::AsDouble(72.5)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let request = make_metrics_request(vec![metric], "temp-service");
        let batch = super::metrics_to_record_batch(&request, Some("test-tenant"))
            .expect("should not error")
            .expect("should have data");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 32);
    }

    #[test]
    fn test_metrics_gauge_int() {
        let metric = Metric {
            name: "active_connections".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_700_000_000_000_000_000,
                    value: Some(NumberValue::AsInt(42)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let request = make_metrics_request(vec![metric], "conn-service");
        let batch = super::metrics_to_record_batch(&request, None)
            .expect("should not error")
            .expect("should have data");

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_metrics_sum_counter() {
        let metric = Metric {
            name: "http_requests_total".to_string(),
            description: "Total HTTP requests".to_string(),
            unit: "1".to_string(),
            metadata: vec![],
            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    attributes: vec![
                        KeyValue {
                            key: "method".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("GET".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "status".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("200".to_string())),
                            }),
                        },
                    ],
                    start_time_unix_nano: 1_699_000_000_000_000_000,
                    time_unix_nano: 1_700_000_000_000_000_000,
                    value: Some(NumberValue::AsDouble(1500.0)),
                    exemplars: vec![],
                    flags: 0,
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
        };

        let request = make_metrics_request(vec![metric], "api-server");
        let batch = super::metrics_to_record_batch(&request, Some("prod"))
            .expect("should not error")
            .expect("should have data");

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_metrics_histogram() {
        let metric = Metric {
            name: "http_request_duration_seconds".to_string(),
            description: "Request duration".to_string(),
            unit: "s".to_string(),
            metadata: vec![],
            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(
                Histogram {
                    data_points: vec![HistogramDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 1_699_000_000_000_000_000,
                        time_unix_nano: 1_700_000_000_000_000_000,
                        count: 100,
                        sum: Some(52.5),
                        bucket_counts: vec![10, 25, 40, 15, 8, 2],
                        explicit_bounds: vec![0.005, 0.01, 0.025, 0.05, 0.1],
                        exemplars: vec![],
                        flags: 0,
                        min: Some(0.001),
                        max: Some(0.250),
                    }],
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                },
            )),
        };

        let request = make_metrics_request(vec![metric], "api-server");
        let batch = super::metrics_to_record_batch(&request, None)
            .expect("should not error")
            .expect("should have data");

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_metrics_exponential_histogram() {
        let metric = Metric {
            name: "request_latency".to_string(),
            description: String::new(),
            unit: "ms".to_string(),
            metadata: vec![],
            data: Some(
                opentelemetry_proto::tonic::metrics::v1::metric::Data::ExponentialHistogram(ExponentialHistogram {
                    data_points: vec![ExponentialHistogramDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 1_699_000_000_000_000_000,
                        time_unix_nano: 1_700_000_000_000_000_000,
                        count: 50,
                        sum: Some(1250.0),
                        scale: 3,
                        zero_count: 2,
                        positive: Some(Buckets {
                            offset: 1,
                            bucket_counts: vec![5, 10, 15, 12, 6],
                        }),
                        negative: Some(Buckets {
                            offset: 0,
                            bucket_counts: vec![],
                        }),
                        flags: 0,
                        exemplars: vec![],
                        min: Some(1.0),
                        max: Some(100.0),
                        zero_threshold: 0.001,
                    }],
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                }),
            ),
        };

        let request = make_metrics_request(vec![metric], "latency-service");
        let batch = super::metrics_to_record_batch(&request, None)
            .expect("should not error")
            .expect("should have data");

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_metrics_summary() {
        let metric = Metric {
            name: "rpc_duration_seconds".to_string(),
            description: String::new(),
            unit: "s".to_string(),
            metadata: vec![],
            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Summary(
                Summary {
                    data_points: vec![SummaryDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 1_699_000_000_000_000_000,
                        time_unix_nano: 1_700_000_000_000_000_000,
                        count: 1000,
                        sum: 5000.0,
                        quantile_values: vec![
                            ValueAtQuantile {
                                quantile: 0.5,
                                value: 4.5,
                            },
                            ValueAtQuantile {
                                quantile: 0.9,
                                value: 8.2,
                            },
                            ValueAtQuantile {
                                quantile: 0.99,
                                value: 15.0,
                            },
                        ],
                        flags: 0,
                    }],
                },
            )),
        };

        let request = make_metrics_request(vec![metric], "rpc-service");
        let batch = super::metrics_to_record_batch(&request, None)
            .expect("should not error")
            .expect("should have data");

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_metrics_attribute_merging() {
        let metric = Metric {
            name: "test_metric".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![KeyValue {
                        key: "dp.attr".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("dp-value".to_string())),
                        }),
                    }],
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_700_000_000_000_000_000,
                    value: Some(NumberValue::AsDouble(1.0)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let request = MetricsRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("attr-test".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "resource.attr".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("res-value".to_string())),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "my-lib".to_string(),
                        version: "1.0".to_string(),
                        attributes: vec![KeyValue {
                            key: "scope.attr".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("scope-value".to_string())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![metric],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let batch = super::metrics_to_record_batch(&request, Some("test-tenant"))
            .expect("should not error")
            .expect("should have data");

        // Should have 1 row with merged attributes from all 3 levels
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_metrics_mixed_types() {
        let gauge = Metric {
            name: "temperature".to_string(),
            description: String::new(),
            unit: "celsius".to_string(),
            metadata: vec![],
            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_700_000_000_000_000_000,
                    value: Some(NumberValue::AsDouble(22.5)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let counter = Metric {
            name: "requests".to_string(),
            description: String::new(),
            unit: "1".to_string(),
            metadata: vec![],
            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(Sum {
                data_points: vec![
                    NumberDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 0,
                        time_unix_nano: 1_700_000_000_000_000_000,
                        value: Some(NumberValue::AsDouble(100.0)),
                        exemplars: vec![],
                        flags: 0,
                    },
                    NumberDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 0,
                        time_unix_nano: 1_700_000_001_000_000_000,
                        value: Some(NumberValue::AsDouble(105.0)),
                        exemplars: vec![],
                        flags: 0,
                    },
                ],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
        };

        let request = make_metrics_request(vec![gauge, counter], "mixed-service");
        let batch = super::metrics_to_record_batch(&request, None)
            .expect("should not error")
            .expect("should have data");

        // 1 gauge data point + 2 sum data points = 3 rows
        assert_eq!(batch.num_rows(), 3);
    }

    /// Test that a metrics `RecordBatch` has correct schema and array types,
    /// and survives a Parquet round-trip with types preserved.
    ///
    /// This reproduces the shift error:
    /// `DataInvalid => The list partner is not a list type`
    #[test]
    fn test_metrics_iceberg_parquet_write() {
        let metric = Metric {
            name: "up".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![KeyValue {
                        key: "job".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("api".to_string())),
                        }),
                    }],
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_700_000_000_000_000_000,
                    value: Some(NumberValue::AsDouble(1.0)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };
        let request = make_metrics_request(vec![metric], "test-svc");
        let batch = super::metrics_to_record_batch(&request, Some("default"))
            .expect("transform should succeed")
            .expect("should have data");

        // Verify the batch schema matches the Iceberg-derived Arrow schema
        let expected_schema = super::metrics_arrow_schema();
        let batch_schema = batch.schema();

        for (i, (expected_field, actual_field)) in expected_schema
            .fields()
            .iter()
            .zip(batch_schema.fields().iter())
            .enumerate()
        {
            assert_eq!(
                expected_field.data_type(),
                actual_field.data_type(),
                "Column {i} '{}' data type mismatch: expected {:?}, got {:?}",
                expected_field.name(),
                expected_field.data_type(),
                actual_field.data_type()
            );
        }

        // Verify all columns that are List type actually produce ListArray
        for (i, field) in batch_schema.fields().iter().enumerate() {
            let col = batch.column(i);
            if matches!(field.data_type(), arrow::datatypes::DataType::List(_)) {
                assert!(
                    col.as_any().downcast_ref::<arrow::array::ListArray>().is_some(),
                    "Column {i} '{}' should be ListArray but is {:?}",
                    field.name(),
                    col.data_type()
                );
            }
        }

        // Verify the batch can be split by partition splitter (same as shift path)
        let iceberg_schema =
            icegate_common::schema::metrics_schema().expect("metrics_schema should be valid");
        let partition_spec = icegate_common::schema::metrics_partition_spec(&iceberg_schema)
            .expect("partition spec should be valid");

        let splitter = iceberg::arrow::record_batch_partition_splitter::RecordBatchPartitionSplitter::try_new_with_computed_values(
            Arc::new(iceberg_schema),
            Arc::new(partition_spec),
        )
        .expect("should create partition splitter");

        let partitioned = splitter.split(&batch);
        assert!(
            partitioned.is_ok(),
            "Partition split failed: {:?}",
            partitioned.err()
        );
        let partitioned = partitioned.unwrap();
        assert!(!partitioned.is_empty(), "should produce at least one partition");

        // Parquet round-trip (simulating WAL write/read) then partition split
        let mut buf = Vec::new();
        {
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).expect("create parquet writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(bytes::Bytes::from(buf), 1024)
                .expect("create parquet reader");
        let wal_batches: Vec<_> = reader.map(|b| b.expect("read batch")).collect();
        assert!(!wal_batches.is_empty(), "should read at least one batch from parquet");

        // Now try to split the round-tripped batch (this is the shift path)
        let wal_batch = &wal_batches[0];
        let partitioned_wal = splitter.split(wal_batch);
        assert!(
            partitioned_wal.is_ok(),
            "Partition split after WAL round-trip failed: {:?}",
            partitioned_wal.err()
        );

        // Verify the partitioned batch still has correct list types
        for (_key, pbatch) in &partitioned {
            for (i, field) in pbatch.schema().fields().iter().enumerate() {
                if matches!(field.data_type(), arrow::datatypes::DataType::List(_)) {
                    let col = pbatch.column(i);
                    assert!(
                        col.as_any().downcast_ref::<arrow::array::ListArray>().is_some(),
                        "After partition split, column {i} '{}' should be ListArray but is {:?}",
                        field.name(),
                        col.data_type()
                    );
                }
            }
        }
    }
}

