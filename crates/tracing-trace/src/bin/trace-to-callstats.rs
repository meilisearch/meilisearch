use std::ffi::OsString;
use std::io::Write;

use serde_json::json;

fn main() {
    let input_file = std::env::args_os().nth(1).expect("missing <INPUT> file");
    let input =
        std::io::BufReader::new(std::fs::File::open(&input_file).expect("could not open <INPUT>"));
    let trace = tracing_trace::TraceReader::new(input);
    let profile = tracing_trace::processor::span_stats::to_call_stats(trace).unwrap();
    let mut output_file = OsString::new();
    output_file.push("callstats-");
    output_file.push(input_file);
    let mut output_file = std::io::BufWriter::new(std::fs::File::create(output_file).unwrap());
    for (key, value) in profile {
        serde_json::to_writer(&mut output_file, &json!({key: value})).unwrap();
        writeln!(&mut output_file).unwrap();
    }
    output_file.flush().unwrap();
}
