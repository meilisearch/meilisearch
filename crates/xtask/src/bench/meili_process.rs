use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{bail, Context as _};
use tokio::process::Command;
use tokio::time;

use super::assets::Asset;
use super::client::Client;
use super::workload::Workload;

pub async fn kill(mut meilisearch: tokio::process::Child) {
    let Some(id) = meilisearch.id() else { return };

    match Command::new("kill").args(["--signal=TERM", &id.to_string()]).spawn() {
        Ok(mut cmd) => {
            let Err(error) = cmd.wait().await else { return };
            tracing::warn!(
                error = &error as &dyn std::error::Error,
                "while awaiting the Meilisearch server kill"
            );
        }
        Err(error) => {
            tracing::warn!(
                error = &error as &dyn std::error::Error,
                "while terminating Meilisearch server with a kill -s TERM"
            );
            if let Err(error) = meilisearch.kill().await {
                tracing::warn!(
                    error = &error as &dyn std::error::Error,
                    "while terminating Meilisearch server"
                )
            }
            return;
        }
    };

    match time::timeout(Duration::from_secs(5), meilisearch.wait()).await {
        Ok(_) => (),
        Err(_) => {
            if let Err(error) = meilisearch.kill().await {
                tracing::warn!(
                    error = &error as &dyn std::error::Error,
                    "while terminating Meilisearch server"
                )
            }
        }
    }
}

#[tracing::instrument]
pub async fn build() -> anyhow::Result<()> {
    let mut command = Command::new("cargo");
    command.arg("build").arg("--release").arg("-p").arg("meilisearch");

    command.kill_on_drop(true);

    let mut builder = command.spawn().context("error building Meilisearch")?;

    if !builder.wait().await.context("could not build Meilisearch")?.success() {
        bail!("failed building Meilisearch")
    }

    Ok(())
}

#[tracing::instrument(skip(client, master_key, workload), fields(workload = workload.name))]
pub async fn start(
    client: &Client,
    master_key: Option<&str>,
    workload: &Workload,
    asset_folder: &str,
    mut command: Command,
) -> anyhow::Result<tokio::process::Child> {
    command.arg("--db-path").arg("./_xtask_benchmark.ms");
    if let Some(master_key) = master_key {
        command.arg("--master-key").arg(master_key);
    }
    command.arg("--experimental-enable-logs-route");

    for extra_arg in workload.extra_cli_args.iter() {
        command.arg(extra_arg);
    }

    command.kill_on_drop(true);

    let mut meilisearch = command.spawn().context("Error starting Meilisearch")?;

    wait_for_health(client, &mut meilisearch, &workload.assets, asset_folder).await?;

    Ok(meilisearch)
}

async fn wait_for_health(
    client: &Client,
    meilisearch: &mut tokio::process::Child,
    assets: &BTreeMap<String, Asset>,
    asset_folder: &str,
) -> anyhow::Result<()> {
    for i in 0..100 {
        let res = super::command::run(client.clone(), health_command(), assets, asset_folder).await;
        if res.is_ok() {
            // check that this is actually the current Meilisearch instance that answered us
            if let Some(exit_code) =
                meilisearch.try_wait().context("cannot check Meilisearch server process status")?
            {
                tracing::error!("Got an health response from a different process");
                bail!("Meilisearch server exited early with code {exit_code}");
            }

            return Ok(());
        }
        time::sleep(Duration::from_millis(500)).await;
        // check whether the Meilisearch instance exited early (cut the wait)
        if let Some(exit_code) =
            meilisearch.try_wait().context("cannot check Meilisearch server process status")?
        {
            bail!("Meilisearch server exited early with code {exit_code}");
        }
        tracing::debug!(attempt = i, "Waiting for Meilisearch to go up");
    }
    bail!("meilisearch is not responding")
}

fn health_command() -> super::command::Command {
    super::command::Command {
        route: "/health".into(),
        method: super::client::Method::Get,
        body: Default::default(),
        synchronous: super::command::SyncMode::WaitForResponse,
    }
}

pub fn delete_db() {
    let _ = std::fs::remove_dir_all("./_xtask_benchmark.ms");
}
