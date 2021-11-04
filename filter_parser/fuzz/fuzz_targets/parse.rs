#![no_main]
use filter_parser::{ErrorKind, FilterCondition};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        // When we are fuzzing the parser we can get a stack overflow very easily.
        // But since this doesn't happens with a normal build we are just going to limit the fuzzer to 500 characters.
        if s.len() < 500 {
            match FilterCondition::parse(s) {
                Err(e) if matches!(e.kind(), ErrorKind::InternalError(_)) => {
                    panic!("Found an internal error: `{:?}`", e)
                }
                _ => (),
            }
        }
    }
});
