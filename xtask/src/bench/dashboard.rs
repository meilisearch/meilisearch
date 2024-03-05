use std::collections::BTreeMap;

use anyhow::{bail, Context};
use serde_json::json;
use tokio::signal::ctrl_c;
use tokio::task::AbortHandle;
use tracing_trace::processor::span_stats::CallStats;
use uuid::Uuid;

use super::client::Client;
use super::env_info;
use super::workload::Workload;

pub async fn cancel_on_ctrl_c(
    invocation_uuid: Uuid,
    dashboard_client: Client,
    abort_handle: AbortHandle,
) {
    tracing::info!("press Ctrl-C to cancel the invocation");
    match ctrl_c().await {
        Ok(()) => {
            tracing::info!(%invocation_uuid, "received Ctrl-C, cancelling invocation");
            mark_as_failed(dashboard_client, invocation_uuid, None).await;
            abort_handle.abort();
        }
        Err(error) => tracing::warn!(
            error = &error as &dyn std::error::Error,
            "failed to listen to Ctrl-C signal, invocation won't be canceled on Ctrl-C"
        ),
    }
}

pub async fn mark_as_failed(
    dashboard_client: Client,
    invocation_uuid: Uuid,
    failure_reason: Option<String>,
) {
    let response = dashboard_client
        .post("cancel-invocation")
        .json(&json!({
            "invocation_uuid": invocation_uuid,
            "failure_reason": failure_reason,
        }))
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(response_error) => {
            tracing::error!(error = &response_error as &dyn std::error::Error, %invocation_uuid, "could not mark invocation as failed");
            return;
        }
    };

    if !response.status().is_success() {
        tracing::error!(
            %invocation_uuid,
            "could not mark invocation as failed: {}",
            response.text().await.unwrap()
        );
        return;
    }
    tracing::warn!(%invocation_uuid, "marked invocation as failed or canceled");
}

pub async fn send_machine_info(
    dashboard_client: &Client,
    env: &env_info::Environment,
) -> anyhow::Result<()> {
    let response = dashboard_client
        .put("machine")
        .json(&json!({"hostname": env.hostname}))
        .send()
        .await
        .context("sending machine information")?;
    if !response.status().is_success() {
        bail!(
            "could not send machine information: {} {}",
            response.status(),
            response.text().await.unwrap_or_else(|_| "unknown".into())
        );
    }
    Ok(())
}

pub async fn create_invocation(
    dashboard_client: &Client,
    build_info: build_info::BuildInfo,
    commit_message: &str,
    env: env_info::Environment,
    max_workloads: usize,
    reason: Option<&str>,
) -> anyhow::Result<Uuid> {
    let response = dashboard_client
        .put("invocation")
        .json(&json!({
            "commit": {
                "sha1": build_info.commit_sha1,
                "message": commit_message,
                "commit_date": build_info.commit_timestamp,
                "branch": build_info.branch,
                "tag": build_info.describe.and_then(|describe| describe.as_tag()),
            },
            "machine_hostname": env.hostname,
            "max_workloads": max_workloads,
            "reason": reason
        }))
        .send()
        .await
        .context("sending invocation")?;
    if !response.status().is_success() {
        bail!(
            "could not send new invocation: {}",
            response.text().await.unwrap_or_else(|_| "unknown".into())
        );
    }
    let invocation_uuid: Uuid =
        response.json().await.context("could not deserialize invocation response as JSON")?;
    Ok(invocation_uuid)
}

pub async fn create_workload(
    dashboard_client: &Client,
    invocation_uuid: Uuid,
    workload: &Workload,
) -> anyhow::Result<Uuid> {
    let response = dashboard_client
        .put("workload")
        .json(&json!({
            "invocation_uuid": invocation_uuid,
            "name": &workload.name,
            "max_runs": workload.run_count,
        }))
        .send()
        .await
        .context("could not create new workload")?;

    if !response.status().is_success() {
        bail!("creating new workload failed: {}", response.text().await.unwrap())
    }

    let workload_uuid: Uuid =
        response.json().await.context("could not deserialize JSON as UUID")?;
    Ok(workload_uuid)
}

pub async fn create_run(
    dashboard_client: Client,
    workload_uuid: Uuid,
    report: &BTreeMap<String, CallStats>,
) -> anyhow::Result<()> {
    let response = dashboard_client
        .put("run")
        .json(&json!({
            "workload_uuid": workload_uuid,
            "data": report
        }))
        .send()
        .await
        .context("sending new run")?;
    if !response.status().is_success() {
        bail!(
            "sending new run failed: {}",
            response.text().await.unwrap_or_else(|_| "unknown".into())
        )
    }
    Ok(())
}
