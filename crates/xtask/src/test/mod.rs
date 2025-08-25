use crate::common::{args::CommonArgs, client::Client, logs::setup_logs};
use cargo_metadata::semver::Version;
use clap::Parser;

mod workload;

pub use workload::TestWorkload;

/// Run tests from a workload
#[derive(Parser, Debug)]
pub struct TestDeriveArgs {
    /// Common arguments shared with other commands
    #[command(flatten)]
    common: CommonArgs,

    initial_version: Version,
}

pub fn run(args: TestDeriveArgs) -> anyhow::Result<()> {
    setup_logs(&args.common.log_filter)?;

    // setup clients
    let assets_client = Client::new(
        None,
        args.common.assets_key.as_deref(),
        Some(std::time::Duration::from_secs(3600)), // 1h
    )?;

    Ok(())
}
