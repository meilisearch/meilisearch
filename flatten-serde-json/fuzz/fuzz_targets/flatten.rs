#![no_main]
use arbitrary_json::ArbitraryObject;
use flatten_serde_json::flatten;
use json_depth_checker::should_flatten_from_value;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|object: ArbitraryObject| {
    let object = flatten(&object);
    if !object.is_empty() {
        assert!(object.values().any(|value| !should_flatten_from_value(value)));
    }
});
