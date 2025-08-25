use clap::Parser;

use crate::common::args::CommonArgs;

/// Run tests from a workload
#[derive(Parser, Debug)]
pub struct TestDeriveArgs {
    /// Common arguments shared with other commands
    #[command(flatten)]
    common: CommonArgs,
}
