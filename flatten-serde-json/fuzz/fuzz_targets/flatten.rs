#![no_main]
use arbitrary_json::ArbitraryObject;
use flatten_serde_json::flatten;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|object: ArbitraryObject| {
    let _ = flatten(&object);
});
