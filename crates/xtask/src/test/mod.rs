use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::Parser;

use crate::common::args::CommonArgs;
use crate::common::client::Client;
use crate::common::command::SyncMode;
use crate::common::logs::setup_logs;
use crate::common::workload::Workload;
use crate::test::workload::CommandOrBinary;

mod workload;

pub use workload::TestWorkload;

/// Run tests from a workload
#[derive(Parser, Debug)]
pub struct TestDeriveArgs {
    /// Common arguments shared with other commands
    #[command(flatten)]
    common: CommonArgs,

    /// Enables workloads to be rewritten in place to update expected responses.
    #[arg(short, long, default_value_t = false)]
    pub update_responses: bool,

    /// Enables workloads to be rewritten in place to add missing expected responses.
    #[arg(short, long, default_value_t = false)]
    pub add_missing_responses: bool,
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
    let assets_client = Arc::new(Client::new(
        None,
        args.common.assets_key.as_deref(),
        Some(Duration::from_secs(3600)), // 1h
    )?);

    let meili_client = Arc::new(Client::new(
        Some("http://127.0.0.1:7700".into()),
        Some("masterKey"),
        Some(Duration::from_secs(args.common.tasks_queue_timeout_secs)),
    )?);

    let asset_folder = args.common.asset_folder.clone().leak();
    for workload_file in &args.common.workload_file {
        let string = tokio::fs::read_to_string(workload_file)
            .await
            .with_context(|| format!("error reading {}", workload_file.display()))?;
        let workload: Workload = serde_json::from_str(string.trim())
            .with_context(|| format!("error parsing {} as JSON", workload_file.display()))?;

        let Workload::Test(workload) = workload else {
            bail!("workload file {} is not a test workload", workload_file.display());
        };

        let has_faulty_register = workload.commands.iter().any(|c| {
            matches!(c, CommandOrBinary::Command(cmd) if cmd.synchronous == SyncMode::DontWait && !cmd.register.is_empty())
        });
        if has_faulty_register {
            bail!("workload {} contains commands that register values but are marked as --dont-wait. This is not supported because we cannot guarantee the value will be registered before the next command runs.", workload.name);
        }

        let name = workload.name.clone();
        match workload.run(&args, &assets_client, &meili_client, asset_folder).await {
            Ok(_) => match args.update_responses || args.add_missing_responses {
                true => println!(
                    "🛠️ Workload {name} was updated, please check the output and restart the test"
                ),
                false => println!("✅ Workload {name} passed"),
            },
            Err(error) => {
                println!("❌ Workload {name} failed: {error}");
                println!("💡 Is this intentional? If so, rerun with --update-responses to update the workload files.");
                return Err(error);
            }
        }
    }

    Ok(())
}
