//! Benchmarks for `CountOverTime` UDAF and `GridAccumulator`

#![allow(missing_docs, clippy::expect_used, clippy::cast_possible_wrap)]

use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::arrow::array::{ArrayRef, TimestampMicrosecondArray};
use icegate_query::logql::datafusion::udaf::GridAccumulator;

/// Generate test timestamps evenly distributed across a time range
fn generate_timestamps(count: usize, start: i64, end: i64) -> TimestampMicrosecondArray {
    let step = (end - start) / count as i64;
    let timestamps: Vec<i64> = (0..count).map(|i| start + (i as i64 * step)).collect();
    TimestampMicrosecondArray::from(timestamps)
}

/// Benchmark `update_counts` with varying dataset sizes
fn bench_update_counts(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_counts");
    group.measurement_time(Duration::from_secs(30));

    // Test different dataset sizes
    let configs = vec![
        ("small", 100, 10),        // 100 timestamps, 10 grid points
        ("medium", 10_000, 100),   // 10K timestamps, 100 grid points
        ("large", 100_000, 1_000), // 100K timestamps, 1K grid points
    ];

    for (name, num_timestamps, num_grid_points) in configs {
        let start = 0i64;
        let end = i64::from(num_grid_points) * 1_000_000i64; // 1 second per grid point
        let step = 1_000_000i64; // 1 second step
        let range = 500_000i64; // 500ms range
        let offset = 0i64;

        let timestamps = generate_timestamps(num_timestamps, start, end);

        group.bench_with_input(BenchmarkId::new("dataset_size", name), &timestamps, |b, ts| {
            b.iter(|| {
                let mut acc = GridAccumulator::new(
                    black_box(start),
                    black_box(end),
                    black_box(step),
                    black_box(range),
                    black_box(offset),
                )
                .expect("Failed to create accumulator");
                acc.update_counts(black_box(ts)).expect("Update failed");
                acc
            });
        });
    }

    group.finish();
}

/// Benchmark `update_counts` with varying grid sizes (fixed timestamp count)
fn bench_grid_size_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("grid_size_impact");
    group.measurement_time(Duration::from_secs(30));

    let num_timestamps = 10_000; // Fixed timestamp count

    // Test different grid sizes
    let grid_configs = vec![("10_grid", 10), ("100_grid", 100), ("1000_grid", 1_000), ("10000_grid", 10_000)];

    for (name, num_grid_points) in grid_configs {
        let start = 0i64;
        let end = i64::from(num_grid_points) * 1_000_000i64; // 1 second per grid point
        let step = 1_000_000i64; // 1 second step
        let range = 500_000i64; // 500ms range
        let offset = 0i64;

        let timestamps = generate_timestamps(num_timestamps, start, end);

        group.bench_with_input(
            BenchmarkId::new("grid_size", name),
            &(timestamps, start, end, step, range, offset),
            |b, (ts, st, en, stp, rng, off)| {
                b.iter(|| {
                    let mut acc = GridAccumulator::new(
                        black_box(*st),
                        black_box(*en),
                        black_box(*stp),
                        black_box(*rng),
                        black_box(*off),
                    )
                    .expect("Failed to create accumulator");
                    acc.update_counts(black_box(ts)).expect("Update failed");
                    acc
                });
            },
        );
    }

    group.finish();
}

/// Benchmark `update_counts` with different range configurations
fn bench_range_configurations(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_configurations");

    let num_timestamps = 10_000;
    let num_grid_points = 100;
    let start = 0i64;
    let end = i64::from(num_grid_points) * 1_000_000i64;
    let step = 1_000_000i64;

    let timestamps = generate_timestamps(num_timestamps, start, end);

    // Test different range sizes
    let range_configs = vec![
        ("narrow_100ms", 100_000i64), // 100ms range
        ("medium_500ms", 500_000i64), // 500ms range
        ("wide_1s", 1_000_000i64),    // 1s range
        ("wide_5s", 5_000_000i64),    // 5s range
    ];

    for (name, range) in range_configs {
        group.bench_with_input(
            BenchmarkId::new("range", name),
            &(&timestamps, range),
            |b, (ts, rng)| {
                b.iter(|| {
                    let mut acc = GridAccumulator::new(
                        black_box(start),
                        black_box(end),
                        black_box(step),
                        black_box(*rng),
                        black_box(0),
                    )
                    .expect("Failed to create accumulator");
                    acc.update_counts(black_box(ts)).expect("Update failed");
                    acc
                });
            },
        );
    }

    group.finish();
}

/// Benchmark `update_counts` with different offset configurations
fn bench_offset_configurations(c: &mut Criterion) {
    let mut group = c.benchmark_group("offset_configurations");

    let num_timestamps = 10_000;
    let num_grid_points = 100;
    let start = 0i64;
    let end = i64::from(num_grid_points) * 1_000_000i64;
    let step = 1_000_000i64;
    let range = 500_000i64;

    let timestamps = generate_timestamps(num_timestamps, start, end);

    // Test different offsets
    let offset_configs = vec![
        ("no_offset", 0i64),
        ("offset_100ms", 100_000i64),
        ("offset_250ms", 250_000i64),
        ("offset_500ms", 500_000i64),
    ];

    for (name, offset) in offset_configs {
        group.bench_with_input(
            BenchmarkId::new("offset", name),
            &(&timestamps, offset),
            |b, (ts, off)| {
                b.iter(|| {
                    let mut acc = GridAccumulator::new(
                        black_box(start),
                        black_box(end),
                        black_box(step),
                        black_box(range),
                        black_box(*off),
                    )
                    .expect("Failed to create accumulator");
                    acc.update_counts(black_box(ts)).expect("Update failed");
                    acc
                });
            },
        );
    }

    group.finish();
}

/// Benchmark merge operations
fn bench_merge_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_operations");

    let num_grid_points = 100;
    let start = 0i64;
    let end = i64::from(num_grid_points) * 1_000_000i64;
    let step = 1_000_000i64;
    let range = 500_000i64;
    let offset = 0i64;

    // Create two accumulators with different data
    let timestamps1 = generate_timestamps(5_000, start, end / 2);
    let timestamps2 = generate_timestamps(5_000, end / 2, end);

    let mut acc1 = GridAccumulator::new(start, end, step, range, offset).expect("Failed to create accumulator");
    acc1.update_counts(&timestamps1).expect("Update failed");

    let mut acc2 = GridAccumulator::new(start, end, step, range, offset).expect("Failed to create accumulator");
    acc2.update_counts(&timestamps2).expect("Update failed");

    group.bench_function("merge_two_accumulators", |b| {
        b.iter(|| {
            let mut acc_target = GridAccumulator::new(
                black_box(start),
                black_box(end),
                black_box(step),
                black_box(range),
                black_box(offset),
            )
            .expect("Failed to create accumulator");

            let state = acc1.state().expect("Failed to get state");
            let grid_list = match &state[0] {
                datafusion::common::ScalarValue::List(arr) => arr.clone(),
                _ => panic!("Expected List for grid"),
            };
            let values_list = match &state[1] {
                datafusion::common::ScalarValue::List(arr) => arr.clone(),
                _ => panic!("Expected List for values"),
            };

            acc_target
                .merge(black_box(&[grid_list as ArrayRef, values_list as ArrayRef]))
                .expect("Merge failed");
            acc_target
        });
    });

    group.finish();
}

/// Benchmark grid creation
fn bench_grid_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("grid_creation");

    let grid_sizes = vec![
        ("tiny_10", 10),
        ("small_100", 100),
        ("medium_1000", 1_000),
        ("large_10000", 10_000),
    ];

    for (name, num_points) in grid_sizes {
        let start = 0i64;
        let end = i64::from(num_points) * 1_000_000i64;
        let step = 1_000_000i64;
        let range = 500_000i64;
        let offset = 0i64;

        group.bench_with_input(BenchmarkId::new("grid_points", name), &num_points, |b, _| {
            b.iter(|| {
                GridAccumulator::new(
                    black_box(start),
                    black_box(end),
                    black_box(step),
                    black_box(range),
                    black_box(offset),
                )
                .expect("Failed to create accumulator")
            });
        });
    }

    group.finish();
}

/// Benchmark sparse output building
fn bench_sparse_output(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse_output");

    let num_timestamps = 10_000;
    let num_grid_points = 100;
    let start = 0i64;
    let end = i64::from(num_grid_points) * 1_000_000i64;
    let step = 1_000_000i64;
    let range = 500_000i64;
    let offset = 0i64;

    let timestamps = generate_timestamps(num_timestamps, start, end);

    let mut acc = GridAccumulator::new(start, end, step, range, offset).expect("Failed to create accumulator");
    acc.update_counts(&timestamps).expect("Update failed");

    group.bench_function("build_sparse_u64", |b| {
        b.iter(|| black_box(acc.build_sparse_u64(false)).expect("Build sparse failed"));
    });

    group.bench_function("build_sparse_f64", |b| {
        b.iter(|| black_box(acc.build_sparse_f64(1.0)).expect("Build sparse failed"));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_update_counts,
    bench_grid_size_impact,
    bench_range_configurations,
    bench_offset_configurations,
    bench_merge_operations,
    bench_grid_creation,
    bench_sparse_output,
);
criterion_main!(benches);
