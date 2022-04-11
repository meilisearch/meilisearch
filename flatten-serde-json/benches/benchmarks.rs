use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flatten_serde_json::flatten;
use serde_json::json;

pub fn flatten_simple(c: &mut Criterion) {
    let mut input = json!({
      "a": {
        "b": "c",
        "d": "e",
        "f": "g"
      }
    });
    let object = input.as_object_mut().unwrap();

    c.bench_with_input(BenchmarkId::new("flatten", "simple"), &object, |b, input| {
        b.iter(|| flatten(input))
    });
}

pub fn flatten_complex(c: &mut Criterion) {
    let mut input = json!({
      "a": [
        "b",
        ["c", "d"],
        { "e": ["f", "g"] },
        [
            { "h": "i" },
            { "e": ["j", { "z": "y" }] },
        ],
        ["l"],
        "m",
      ]
    });
    let object = input.as_object_mut().unwrap();

    c.bench_with_input(BenchmarkId::new("flatten", "complex"), &object, |b, input| {
        b.iter(|| flatten(input))
    });
}

criterion_group!(benches, flatten_simple, flatten_complex);
criterion_main!(benches);
