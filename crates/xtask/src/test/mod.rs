use std::time::Duration;

use crate::common::{args::CommonArgs, client::Client, logs::setup_logs, workload::Workload};
use anyhow::{bail, Context};
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
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().enable_time().build()?;
    let _scope = rt.enter();

    rt.block_on(async { run_inner(args).await })?;

    Ok(())
}

async fn run_inner(args: TestDeriveArgs) -> anyhow::Result<()> {
    setup_logs(&args.common.log_filter)?;

    // setup clients
    let assets_client = Client::new(
        None,
        args.common.assets_key.as_deref(),
        Some(Duration::from_secs(3600)), // 1h
    )?;

    let meili_client = Client::new(
        Some("http://127.0.0.1:7700".into()),
        args.common.master_key.as_deref(),
        Some(Duration::from_secs(args.common.tasks_queue_timeout_secs)),
    )?;

    for workload_file in &args.common.workload_file {
        let workload: Workload = serde_json::from_reader(
            std::fs::File::open(workload_file)
                .with_context(|| format!("error opening {}", workload_file.display()))?,
        )
        .with_context(|| format!("error parsing {} as JSON", workload_file.display()))?;

        let Workload::Test(workload) = workload else {
            bail!("workload file {} is not a test workload", workload_file.display());
        };

        match workload.run(&args, &assets_client, &meili_client).await {
            Ok(_) => {
                println!(
                    "✅ Workload {} from file {} completed successfully",
                    workload.name,
                    workload_file.display()
                );
            }
            Err(error) => {
                println!(
                    "❌ Workload {} from file {} failed: {error}",
                    workload.name,
                    workload_file.display()
                );
                return Err(error);
            }
        }
    }

    Ok(())
}
