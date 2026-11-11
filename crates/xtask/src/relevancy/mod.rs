use std::{path::PathBuf, sync::Arc};

use anyhow::{bail, Context};
use clap::Parser;

use crate::common::{args::CommonArgs, client::Client, logs::setup_logs, workload::Workload};

mod workload;

pub use workload::RelevancyWorkload;

pub fn default_report_folder() -> String {
    "./relevancy/reports/".into()
}

/// Run relevancy benchmarks from a workload
#[derive(Parser, Debug)]
pub struct RelevancyArgs {
    /// Common arguments shared with other commands
    #[command(flatten)]
    common: CommonArgs,

    /// Meilisearch master keys
    #[arg(long)]
    pub master_key: Option<String>,

    /// Directory to output reports.
    #[arg(long, default_value_t = default_report_folder())]
    report_folder: String,

    /// Whether to generate or not a plot of the report alongside it.
    #[arg(long)]
    plot: bool,

    /// Benchmark dashboard API key
    #[arg(long)]
    api_key: Option<String>,

    /// Reason for the benchmark invocation
    #[arg(short, long)]
    reason: Option<String>,

    /// The path to the binary to run.
    ///
    /// If unspecified, runs `cargo run` after building Meilisearch with `cargo build`.
    #[arg(long)]
    binary_path: Option<PathBuf>,
}

pub fn run(args: RelevancyArgs) -> anyhow::Result<()> {
    setup_logs(&args.common.log_filter)?;

    // tokio runtime
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().enable_time().build()?;
    let _scope = rt.enter();

    // setup clients
    let assets_client = Client::new(
        None,
        args.common.assets_key.as_deref(),
        Some(std::time::Duration::from_secs(3600)), // 1h
    )?;

    let meili_client = Arc::new(Client::new(
        Some("http://127.0.0.1:7700".into()),
        args.master_key.as_deref(),
        Some(std::time::Duration::from_secs(args.common.tasks_queue_timeout_secs)),
    )?);

    // enter runtime

    rt.block_on(async {

        tracing::info!(workload_count = args.common.workload_file.len(), "handling workload files");

        // main task
        let workload_runs = tokio::spawn(
            {
                async move {
            for workload_file in args.common.workload_file.iter() {
                let workload: Workload = serde_json::from_reader(
                    std::fs::File::open(workload_file)
                        .with_context(|| format!("error opening {}", workload_file.display()))?,
                )
                .with_context(|| format!("error parsing {} as JSON", workload_file.display()))?;

                let Workload::Relevancy(workload) = workload else {
                    bail!("workload file {} is not a relevancy workload", workload_file.display());
                };


                workload::execute(
                    &assets_client,
                    &meili_client,
                    args.master_key.as_deref(),
                    workload,
                    &args,
                    args.binary_path.as_deref(),
                )
                .await?;

            }
            Ok::<_, anyhow::Error>(())
        }});

        // wait for the end of the main task, handle result
        match workload_runs.await {
            Ok(Ok(())) => {
                tracing::info!("Success");
                println!("☀️ Relevancy benchmark invocation completed, please find the results for your workloads below:");
                Ok::<(), anyhow::Error>(())
            }
            Ok(Err(error)) => {
                tracing::error!(error = %error, "invocation failed, attempting to report the failure to dashboard");
                println!("☔️ Relevancy benchmark invocation failed...");
                println!("{error}");
                tracing::warn!("invocation marked as failed following error");
                Err(error)
            },
            Err(join_error) => {
                match join_error.try_into_panic() {
                    Ok(panic) => {
                        tracing::error!("invocation panicked, attempting to report the failure to dashboard");
                        println!("‼️ Relevancy benchmark invocation panicked 😱");
                        let msg = match panic.downcast_ref::<&'static str>() {
                            Some(s) => *s,
                            None => match panic.downcast_ref::<String>() {
                                Some(s) => &s[..],
                                None => "Box<dyn Any>",
                            },
                        };
                        println!("panicked at {msg}");
                        std::panic::resume_unwind(panic)
                    }
                    Err(_) => {
                        tracing::warn!("task was canceled");
                        println!("🚫 Relevancy benchmark invocation was canceled");
                        Ok(())
                    }
                }
            },
        }

    })?;

    Ok(())
}
