use anyhow::Context;
use std::io::LineWriter;
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, Layer};

pub fn setup_logs(log_filter: &str) -> anyhow::Result<()> {
    let filter: tracing_subscriber::filter::Targets =
        log_filter.parse().context("invalid --log-filter")?;

    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_writer(|| LineWriter::new(std::io::stderr()))
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_filter(filter),
    );
    tracing::subscriber::set_global_default(subscriber).context("could not setup logging")?;

    Ok(())
}
