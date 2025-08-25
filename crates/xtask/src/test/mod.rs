use clap::Parser;

use crate::common::{args::CommonArgs, logs::setup_logs};

/// Run tests from a workload
#[derive(Parser, Debug)]
pub struct TestDeriveArgs {
    /// Common arguments shared with other commands
    #[command(flatten)]
    common: CommonArgs,
}

pub fn run(args: TestDeriveArgs) -> anyhow::Result<()> {
    setup_logs(&args.common.log_filter)?;

    Ok(())
}
