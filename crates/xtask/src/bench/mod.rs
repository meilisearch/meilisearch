mod dashboard;
mod env_info;
mod workload;

use crate::common::args::CommonArgs;
use crate::common::logs::setup_logs;
use crate::common::workload::Workload;
use std::{path::PathBuf, sync::Arc};

use anyhow::{bail, Context};
use clap::Parser;

use crate::common::client::Client;
pub use workload::BenchWorkload;

pub fn default_report_folder() -> String {
    "./bench/reports/".into()
}

pub fn default_dashboard_url() -> String {
    "http://localhost:9001".into()
}

/// Run benchmarks from a workload
#[derive(Parser, Debug)]
pub struct BenchDeriveArgs {
    /// Common arguments shared with other commands
    #[command(flatten)]
    common: CommonArgs,

    /// Meilisearch master keys
    #[arg(long)]
    pub master_key: Option<String>,

    /// URL of the dashboard.
    #[arg(long, default_value_t = default_dashboard_url())]
    dashboard_url: String,

    /// Don't actually send results to the dashboard
    #[arg(long)]
    no_dashboard: bool,

    /// Directory to output reports.
    #[arg(long, default_value_t = default_report_folder())]
    report_folder: String,

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

pub fn run(args: BenchDeriveArgs) -> anyhow::Result<()> {
    setup_logs(&args.common.log_filter)?;

    // fetch environment and build info
    let env = env_info::Environment::generate_from_current_config();
    let build_info = build_info::BuildInfo::from_build();

    // tokio runtime
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().enable_time().build()?;
    let _scope = rt.enter();

    // setup clients
    let assets_client = Client::new(
        None,
        args.common.assets_key.as_deref(),
        Some(std::time::Duration::from_secs(3600)), // 1h
    )?;

    let dashboard_client = if args.no_dashboard {
        dashboard::DashboardClient::new_dry()
    } else {
        dashboard::DashboardClient::new(args.dashboard_url.clone(), args.api_key.as_deref())?
    };

    // reporting uses its own client because keeping the stream open to wait for entries
    // blocks any other requests
    // Also we don't want any pesky timeout because we don't know how much time it will take to recover the full trace
    let logs_client = Client::new(
        Some("http://127.0.0.1:7700/logs/stream".into()),
        args.master_key.as_deref(),
        None,
    )?;

    let meili_client = Arc::new(Client::new(
        Some("http://127.0.0.1:7700".into()),
        args.master_key.as_deref(),
        Some(std::time::Duration::from_secs(args.common.tasks_queue_timeout_secs)),
    )?);

    // enter runtime

    rt.block_on(async {
        dashboard_client.send_machine_info(&env).await?;

        let commit_message = build_info.commit_msg.unwrap_or_default().split('\n').next().unwrap();
        let max_workloads = args.common.workload_file.len();
        let reason: Option<&str> = args.reason.as_deref();
        let invocation_uuid = dashboard_client.create_invocation(build_info.clone(), commit_message, env, max_workloads, reason).await?;

        tracing::info!(workload_count = args.common.workload_file.len(), "handling workload files");

        // main task
        let workload_runs = tokio::spawn(
            {
                let dashboard_client = dashboard_client.clone();
                let mut dashboard_urls = Vec::new();
                async move {
            for workload_file in args.common.workload_file.iter() {
                let workload: Workload = serde_json::from_reader(
                    std::fs::File::open(workload_file)
                        .with_context(|| format!("error opening {}", workload_file.display()))?,
                )
                .with_context(|| format!("error parsing {} as JSON", workload_file.display()))?;

                let Workload::Bench(workload) = workload else {
                    bail!("workload file {} is not a bench workload", workload_file.display());
                };

                let workload_name = workload.name.clone();

                workload::execute(
                    &assets_client,
                    &dashboard_client,
                    &logs_client,
                    &meili_client,
                    invocation_uuid,
                    args.master_key.as_deref(),
                    workload,
                    &args,
                    args.binary_path.as_deref(),
                )
                .await?;

                let result_url = dashboard_client.result_url(&workload_name, &build_info, "main");

                if !result_url.is_empty() {
                dashboard_urls.push(result_url);
                }

                if let Some(branch) = build_info.branch {
                    let result_url = dashboard_client.result_url(&workload_name, &build_info, branch);


                    if !result_url.is_empty() {
                    dashboard_urls.push(result_url);
                    }
                }
            }
            Ok::<_, anyhow::Error>(dashboard_urls)
        }});

        // handle ctrl-c
        let abort_handle = workload_runs.abort_handle();
        tokio::spawn({
            let dashboard_client = dashboard_client.clone();
            dashboard_client.cancel_on_ctrl_c(invocation_uuid, abort_handle)
        });

        // wait for the end of the main task, handle result
        match workload_runs.await {
            Ok(Ok(urls)) => {
                tracing::info!("Success");
                println!("☀️ Benchmark invocation completed, please find the results for your workloads below:");
                for url in urls {
                    println!("- {url}");
                }
                Ok::<(), anyhow::Error>(())
            }
            Ok(Err(error)) => {
                tracing::error!(%invocation_uuid, error = %error, "invocation failed, attempting to report the failure to dashboard");
                dashboard_client.mark_as_failed(invocation_uuid, Some(error.to_string())).await;
                println!("☔️ Benchmark invocation failed...");
                println!("{error}");
                tracing::warn!(%invocation_uuid, "invocation marked as failed following error");
                Err(error)
            },
            Err(join_error) => {
                match join_error.try_into_panic() {
                    Ok(panic) => {
                        tracing::error!("invocation panicked, attempting to report the failure to dashboard");
                        dashboard_client.mark_as_failed( invocation_uuid, Some("Panicked".into())).await;
                        println!("‼️ Benchmark invocation panicked 😱");
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
                        println!("🚫 Benchmark invocation was canceled");
                        Ok(())
                    }
                }
            },
        }

    })?;

    Ok(())
}
