#![no_main]
use arbitrary_json::ArbitraryValue;
use json_depth_checker::*;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|value: ArbitraryValue| {
    let value = serde_json::Value::from(value);
    let left = should_flatten_from_value(&value);
    let value = serde_json::to_vec(&value).unwrap();
    let right = should_flatten_from_unchecked_slice(&value);

    assert_eq!(left, right);
});
