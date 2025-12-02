use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Seek as _, Write as _};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context as _};
use futures_util::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::dashboard::DashboardClient;
use super::BenchDeriveArgs;
use crate::common::assets::{self, Asset};
use crate::common::client::Client;
use crate::common::command::{run_commands, Command};
use crate::common::instance::Binary;
use crate::common::process::{self, delete_db, start_meili};

/// A bench workload.
/// Not to be confused with [a test workload](crate::test::workload::Workload).
#[derive(Serialize, Deserialize, Debug)]
pub struct BenchWorkload {
    pub name: String,
    pub run_count: u16,
    pub extra_cli_args: Vec<String>,
    pub assets: BTreeMap<String, Asset>,
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub precommands: Vec<Command>,
    pub commands: Vec<Command>,
}

async fn run_workload_commands(
    dashboard_client: &DashboardClient,
    logs_client: &Client,
    meili_client: &Arc<Client>,
    workload_uuid: Uuid,
    workload: &BenchWorkload,
    args: &BenchDeriveArgs,
    run_number: u16,
) -> anyhow::Result<JoinHandle<anyhow::Result<File>>> {
    let report_folder = &args.report_folder;
    let workload_name = &workload.name;
    let assets = Arc::new(workload.assets.clone());
    let asset_folder = args.common.asset_folder.clone().leak();

    run_commands(
        meili_client,
        &workload.precommands,
        0,
        &assets,
        asset_folder,
        &mut HashMap::new(),
        false,
    )
    .await?;

    std::fs::create_dir_all(report_folder)
        .with_context(|| format!("could not create report directory at {report_folder}"))?;

    let trace_filename = format!("{report_folder}/{workload_name}-{run_number}-trace.json");
    let report_filename = format!("{report_folder}/{workload_name}-{run_number}-report.json");

    let report_handle = start_report(logs_client, trace_filename, &workload.target).await?;

    run_commands(
        meili_client,
        &workload.commands,
        0,
        &assets,
        asset_folder,
        &mut HashMap::new(),
        false,
    )
    .await?;

    let processor =
        stop_report(dashboard_client, logs_client, workload_uuid, report_filename, report_handle)
            .await?;

    Ok(processor)
}

#[allow(clippy::too_many_arguments)] // not best code quality, but this is a benchmark runner
#[tracing::instrument(skip(assets_client, dashboard_client, logs_client, meili_client, workload, master_key, args), fields(workload = workload.name))]
pub async fn execute(
    assets_client: &Client,
    dashboard_client: &DashboardClient,
    logs_client: &Client,
    meili_client: &Arc<Client>,
    invocation_uuid: Uuid,
    master_key: Option<&str>,
    workload: BenchWorkload,
    args: &BenchDeriveArgs,
    binary_path: Option<&Path>,
) -> anyhow::Result<()> {
    assets::fetch_assets(assets_client, &workload.assets, &args.common.asset_folder).await?;

    let workload_uuid = dashboard_client.create_workload(invocation_uuid, &workload).await?;

    let mut tasks = Vec::new();
    for i in 0..workload.run_count {
        tasks.push(
            execute_run(
                dashboard_client,
                logs_client,
                meili_client,
                workload_uuid,
                master_key,
                &workload,
                args,
                binary_path,
                i,
            )
            .await?,
        );
    }

    let mut reports = Vec::with_capacity(workload.run_count as usize);
    for task in tasks {
        reports.push(
            task.await
                .context("task panicked while processing report")?
                .context("task failed while processing report")?,
        );
    }

    tracing::info!(workload = workload.name, "Successful workload");

    Ok(())
}

#[allow(clippy::too_many_arguments)] // not best code quality, but this is a benchmark runner
#[tracing::instrument(skip(dashboard_client, logs_client, meili_client, workload, master_key, args), fields(workload = %workload.name))]
async fn execute_run(
    dashboard_client: &DashboardClient,
    logs_client: &Client,
    meili_client: &Arc<Client>,
    workload_uuid: Uuid,
    master_key: Option<&str>,
    workload: &BenchWorkload,
    args: &BenchDeriveArgs,
    binary_path: Option<&Path>,
    run_number: u16,
) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<std::fs::File>>> {
    delete_db().await;

    let binary = match binary_path {
        Some(binary_path) => Binary {
            source: crate::common::instance::BinarySource::Path(binary_path.to_owned()),
            extra_cli_args: workload.extra_cli_args.clone(),
        },
        None => Binary {
            source: crate::common::instance::BinarySource::Build {
                edition: crate::common::instance::Edition::Community,
            },
            extra_cli_args: workload.extra_cli_args.clone(),
        },
    };

    let meilisearch =
        start_meili(meili_client, master_key, &binary, &args.common.asset_folder).await?;

    let processor = run_workload_commands(
        dashboard_client,
        logs_client,
        meili_client,
        workload_uuid,
        workload,
        args,
        run_number,
    )
    .await?;

    process::kill_meili(meilisearch).await;

    tracing::info!(run_number, "Successful run");

    Ok(processor)
}

async fn start_report(
    logs_client: &Client,
    filename: String,
    target: &str,
) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<std::fs::File>>> {
    const DEFAULT_TARGET: &str = "indexing::=trace";
    let target = if target.is_empty() { DEFAULT_TARGET } else { target };

    let report_file = std::fs::File::options()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&filename)
        .with_context(|| format!("could not create file at {filename}"))?;
    let mut report_file = std::io::BufWriter::new(report_file);

    let response = logs_client
        .post("")
        .json(&json!({
            "mode": "profile",
            "target": target,
        }))
        .send()
        .await
        .context("failed to start report")?;

    let code = response.status();
    if code.is_client_error() {
        tracing::error!(%code, "request error when trying to start report");
        let response: serde_json::Value = response
            .json()
            .await
            .context("could not deserialize response as JSON")
            .context("response error when trying to start report")?;
        bail!(
            "request error when trying to start report: server responded with error code {code} and '{response}'"
        )
    } else if code.is_server_error() {
        tracing::error!(%code, "server error when trying to start report");
        let response: serde_json::Value = response
            .json()
            .await
            .context("could not deserialize response as JSON")
            .context("response error trying to start report")?;
        bail!("server error when trying to start report: server responded with error code {code} and '{response}'")
    }

    Ok(tokio::task::spawn(async move {
        let mut stream = response.bytes_stream();
        while let Some(bytes) = stream.try_next().await.context("while waiting for report")? {
            report_file
                .write_all(&bytes)
                .with_context(|| format!("while writing report to {filename}"))?;
        }
        report_file.into_inner().with_context(|| format!("while writing report to {filename}"))
    }))
}

async fn stop_report(
    dashboard_client: &DashboardClient,
    logs_client: &Client,
    workload_uuid: Uuid,
    filename: String,
    report_handle: tokio::task::JoinHandle<anyhow::Result<std::fs::File>>,
) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<std::fs::File>>> {
    let response = logs_client.delete("").send().await.context("while stopping report")?;
    if !response.status().is_success() {
        bail!("received HTTP {} while stopping report", response.status())
    }

    let mut file = tokio::time::timeout(std::time::Duration::from_secs(1000), report_handle)
        .await
        .context("while waiting for the end of the report")?
        .context("report writing task panicked")?
        .context("while writing report")?;

    file.rewind().context("while rewinding report file")?;

    let process_handle = tokio::task::spawn({
        let dashboard_client = dashboard_client.clone();
        async move {
            let span = tracing::info_span!("processing trace to report", filename);
            let _guard = span.enter();
            let report = tracing_trace::processor::span_stats::to_call_stats(
                tracing_trace::TraceReader::new(std::io::BufReader::new(file)),
            )
            .context("could not convert trace to report")?;
            let context = || format!("writing report to {filename}");

            dashboard_client.create_run(workload_uuid, &report).await?;

            let mut output_file = std::io::BufWriter::new(
                std::fs::File::options()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .read(true)
                    .open(&filename)
                    .with_context(context)?,
            );

            for (key, value) in report {
                serde_json::to_writer(&mut output_file, &json!({key: value}))
                    .context("serializing span stat")?;
                writeln!(&mut output_file).with_context(context)?;
            }
            output_file.flush().with_context(context)?;
            let mut output_file = output_file.into_inner().with_context(context)?;

            output_file.rewind().context("could not rewind output_file").with_context(context)?;

            Ok(output_file)
        }
    });

    Ok(process_handle)
}
