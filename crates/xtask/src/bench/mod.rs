mod assets;
mod client;
mod command;
mod dashboard;
mod env_info;
mod meili_process;
mod workload;

use std::io::LineWriter;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

use self::client::Client;
use self::workload::Workload;

pub fn default_http_addr() -> String {
    "127.0.0.1:7700".to_string()
}
pub fn default_report_folder() -> String {
    "./bench/reports/".into()
}

pub fn default_asset_folder() -> String {
    "./bench/assets/".into()
}

pub fn default_log_filter() -> String {
    "info".into()
}

pub fn default_dashboard_url() -> String {
    "http://localhost:9001".into()
}

/// Run benchmarks from a workload
#[derive(Parser, Debug)]
pub struct BenchDeriveArgs {
    /// Filename of the workload file, pass multiple filenames
    /// to run multiple workloads in the specified order.
    ///
    /// Each workload run will get its own report file.
    #[arg(value_name = "WORKLOAD_FILE", last = false)]
    workload_file: Vec<PathBuf>,

    /// URL of the dashboard.
    #[arg(long, default_value_t = default_dashboard_url())]
    dashboard_url: String,

    /// Don't actually send results to the dashboard
    #[arg(long)]
    no_dashboard: bool,

    /// Directory to output reports.
    #[arg(long, default_value_t = default_report_folder())]
    report_folder: String,

    /// Directory to store the remote assets.
    #[arg(long, default_value_t = default_asset_folder())]
    asset_folder: String,

    /// Log directives
    #[arg(short, long, default_value_t = default_log_filter())]
    log_filter: String,

    /// Benchmark dashboard API key
    #[arg(long)]
    api_key: Option<String>,

    /// Meilisearch master keys
    #[arg(long)]
    master_key: Option<String>,

    /// Authentication bearer for fetching assets
    #[arg(long)]
    assets_key: Option<String>,

    /// Reason for the benchmark invocation
    #[arg(short, long)]
    reason: Option<String>,

    /// The maximum time in seconds we allow for fetching the task queue before timing out.
    #[arg(long, default_value_t = 60)]
    tasks_queue_timeout_secs: u64,

    /// The path to the binary to run.
    ///
    /// If unspecified, runs `cargo run` after building Meilisearch with `cargo build`.
    #[arg(long)]
    binary_path: Option<PathBuf>,
}

pub fn run(args: BenchDeriveArgs) -> anyhow::Result<()> {
    // setup logs
    let filter: tracing_subscriber::filter::Targets =
        args.log_filter.parse().context("invalid --log-filter")?;

    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_writer(|| LineWriter::new(std::io::stderr()))
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_filter(filter),
    );
    tracing::subscriber::set_global_default(subscriber).context("could not setup logging")?;

    // fetch environment and build info
    let env = env_info::Environment::generate_from_current_config();
    let build_info = build_info::BuildInfo::from_build();

    // tokio runtime
    let rt = tokio::runtime::Builder::new_current_thread().enable_io().enable_time().build()?;
    let _scope = rt.enter();

    // setup clients
    let assets_client =
        Client::new(None, args.assets_key.as_deref(), Some(std::time::Duration::from_secs(3600)))?; // 1h

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

    let meili_client = Client::new(
        Some("http://127.0.0.1:7700".into()),
        args.master_key.as_deref(),
        Some(std::time::Duration::from_secs(args.tasks_queue_timeout_secs)),
    )?;

    // enter runtime

    rt.block_on(async {
        dashboard_client.send_machine_info(&env).await?;

        let commit_message = build_info.commit_msg.unwrap_or_default().split('\n').next().unwrap();
        let max_workloads = args.workload_file.len();
        let reason: Option<&str> = args.reason.as_deref();
        let invocation_uuid = dashboard_client.create_invocation(build_info.clone(), commit_message, env, max_workloads, reason).await?;

        tracing::info!(workload_count = args.workload_file.len(), "handling workload files");

        // main task
        let workload_runs = tokio::spawn(
            {
                let dashboard_client = dashboard_client.clone();
                let mut dashboard_urls = Vec::new();
                async move {
            for workload_file in args.workload_file.iter() {
                let workload: Workload = serde_json::from_reader(
                    std::fs::File::open(workload_file)
                        .with_context(|| format!("error opening {}", workload_file.display()))?,
                )
                .with_context(|| format!("error parsing {} as JSON", workload_file.display()))?;

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
