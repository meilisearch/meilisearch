use tracing::{instrument, Span};
use tracing_error::{ErrorLayer, InstrumentResult, SpanTrace, TracedError};

#[instrument(level = "trace", target = "profile::indexing")]
fn foo() -> Result<(), TracedError<Error>> {
    let _ = bar(40, 2);
    bar(40, 2)
}

#[derive(Debug)]
pub enum Error {
    XTooBig,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("x too big")
    }
}

impl std::error::Error for Error {}

#[instrument(level = "trace", target = "profile::indexing")]
fn bar(x: u32, y: u32) -> Result<(), TracedError<Error>> {
    let handle_ok = spawn_in_current_scope(move || baz(y));
    let handle = spawn_in_current_scope(move || baz(x + y));
    handle_ok.join().unwrap().and(handle.join().unwrap())
}

pub fn spawn_in_current_scope<F, T>(f: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let current = Span::current();
    std::thread::spawn(move || {
        let span = tracing::trace_span!(parent: &current, "thread_spawn", id = ?std::thread::current().id(), name = tracing::field::Empty);
        if let Some(name) = std::thread::current().name() {
            span.record("name", name);
        }
        span.in_scope(f)
    })
}

#[instrument(level = "trace", target = "profile::indexing")]
fn baz(x: u32) -> Result<(), TracedError<Error>> {
    if x > 10 {
        fibo_recursive(10);
        return Err(Error::XTooBig).in_current_span();
    }
    Ok(())
}

#[instrument(level = "trace", target = "profile::indexing")]
fn fibo_recursive(n: u32) -> u32 {
    if n == 0 {
        return 1;
    }
    if n == 1 {
        return 2;
    }
    return fibo_recursive(n - 1) - fibo_recursive(n - 2);
}

use tracing_error::ExtractSpanTrace as _;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_trace::processor;

fn on_panic(info: &std::panic::PanicInfo) {
    let info = info.to_string();
    let trace = SpanTrace::capture();
    tracing::error!(%info, %trace);
}

fn main() {
    let (mut trace, profiling_layer) =
        tracing_trace::TraceWriter::new(std::fs::File::create("trace.json").unwrap(), true);

    let subscriber = tracing_subscriber::registry()
        // any number of other subscriber layers may be added before or
        // after the `ErrorLayer`...
        .with(ErrorLayer::default())
        .with(profiling_layer);

    // set the subscriber as the default for the application
    tracing::subscriber::set_global_default(subscriber).unwrap();

    std::panic::set_hook(Box::new(on_panic));

    let res = foo();

    if let Err(error) = res {
        print_extracted_spantraces(&error)
    }

    while trace.try_receive().unwrap().is_continue() {}

    trace.flush().unwrap();

    let trace = tracing_trace::TraceReader::new(std::fs::File::open("trace.json").unwrap());

    let profile = processor::firefox_profiler::to_firefox_profile(trace, "test").unwrap();
    serde_json::to_writer(std::fs::File::create("processed.json").unwrap(), &profile).unwrap();
}

fn print_extracted_spantraces(error: &(dyn std::error::Error + 'static)) {
    let mut error = Some(error);
    let mut ind = 0;

    eprintln!("Error:");

    while let Some(err) = error {
        if let Some(spantrace) = err.span_trace() {
            eprintln!("found a spantrace:\n{}", color_spantrace::colorize(spantrace));
        } else {
            eprintln!("{:>4}: {}", ind, err);
        }

        error = err.source();
        ind += 1;
    }
}
