use criterion::{criterion_group, criterion_main, Criterion};
use json_depth_checker::should_flatten_from_unchecked_slice;
use serde_json::json;

fn criterion_benchmark(c: &mut Criterion) {
    let null = serde_json::to_vec(&json!(null)).unwrap();
    let bool_true = serde_json::to_vec(&json!(true)).unwrap();
    let bool_false = serde_json::to_vec(&json!(false)).unwrap();
    let integer = serde_json::to_vec(&json!(42)).unwrap();
    let float = serde_json::to_vec(&json!(1456.258)).unwrap();
    let string = serde_json::to_vec(&json!("hello world")).unwrap();
    let object = serde_json::to_vec(&json!({ "hello": "world",})).unwrap();
    let complex_object = serde_json::to_vec(&json!({
        "doggos": [
            { "bernard": true },
            { "michel": 42 },
            false,
        ],
        "bouvier": true,
        "caniche": null,
    }))
    .unwrap();
    let simple_array = serde_json::to_vec(&json!([
        1,
        2,
        3,
        "viva",
        "l\"algeria",
        true,
        "[array]",
        "escaped string \""
    ]))
    .unwrap();
    let array_of_array = serde_json::to_vec(&json!([1, [2, [3]]])).unwrap();
    let array_of_object = serde_json::to_vec(&json!([1, [2, [3]], {}])).unwrap();

    c.bench_function("null", |b| b.iter(|| should_flatten_from_unchecked_slice(&null)));
    c.bench_function("true", |b| b.iter(|| should_flatten_from_unchecked_slice(&bool_true)));
    c.bench_function("false", |b| b.iter(|| should_flatten_from_unchecked_slice(&bool_false)));
    c.bench_function("integer", |b| b.iter(|| should_flatten_from_unchecked_slice(&integer)));
    c.bench_function("float", |b| b.iter(|| should_flatten_from_unchecked_slice(&float)));
    c.bench_function("string", |b| b.iter(|| should_flatten_from_unchecked_slice(&string)));
    c.bench_function("object", |b| b.iter(|| should_flatten_from_unchecked_slice(&object)));
    c.bench_function("complex object", |b| {
        b.iter(|| should_flatten_from_unchecked_slice(&complex_object))
    });
    c.bench_function("simple array", |b| {
        b.iter(|| should_flatten_from_unchecked_slice(&simple_array))
    });
    c.bench_function("array of array", |b| {
        b.iter(|| should_flatten_from_unchecked_slice(&array_of_array))
    });
    c.bench_function("array of object", |b| {
        b.iter(|| should_flatten_from_unchecked_slice(&array_of_object))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
