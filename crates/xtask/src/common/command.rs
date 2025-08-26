use std::collections::BTreeMap;
use std::fmt::Display;
use std::io::Read as _;
use std::sync::Arc;

use anyhow::{bail, Context as _};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use similar_asserts::SimpleDiff;

use crate::common::assets::{fetch_asset, Asset};
use crate::common::client::{Client, Method};

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Command {
    pub route: String,
    pub method: Method,
    #[serde(default)]
    pub body: Body,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_response: Option<serde_json::Value>,
    #[serde(default)]
    synchronous: SyncMode,
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Body {
    Inline {
        inline: serde_json::Value,
    },
    Asset {
        asset: String,
    },
    #[default]
    Empty,
}

impl Body {
    pub fn get(
        self,
        assets: &BTreeMap<String, Asset>,
        asset_folder: &str,
    ) -> anyhow::Result<Option<(Vec<u8>, &'static str)>> {
        Ok(match self {
            Body::Inline { inline: body } => Some((
                serde_json::to_vec(&body)
                    .context("serializing to bytes")
                    .context("while getting inline body")?,
                "application/json",
            )),
            Body::Asset { asset: name } => Some({
                let context = || format!("while getting body from asset '{name}'");
                let (mut file, format) =
                    fetch_asset(&name, assets, asset_folder).with_context(context)?;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).with_context(context)?;
                (buf, format.to_content_type(&name))
            }),
            Body::Empty => None,
        })
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} {} ({:?})", self.method, self.route, self.synchronous)
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
enum SyncMode {
    DontWait,
    #[default]
    WaitForResponse,
    WaitForTask,
}

async fn run_batch(
    client: &Arc<Client>,
    batch: Vec<Command>,
    assets: &Arc<BTreeMap<String, Asset>>,
    asset_folder: &'static str,
    return_response: bool,
) -> anyhow::Result<Vec<(Value, StatusCode)>> {
    let [.., last] = batch.as_slice() else { return Ok(Vec::new()) };
    let sync = last.synchronous;
    let batch_len = batch.len();

    let mut tasks = Vec::with_capacity(batch.len());
    for batch in batch {
        let client2 = Arc::clone(client);
        let assets2 = Arc::clone(assets);
        tasks.push(tokio::spawn(async move {
            run(&client2, &batch, &assets2, asset_folder, return_response).await
        }));
    }

    let mut outputs = Vec::with_capacity(if return_response { batch_len } else { 0 });
    for task in tasks {
        let output = task.await.context("task panicked")??;
        if let Some(output) = output {
            if return_response {
                outputs.push(output);
            }
        }
    }

    match sync {
        SyncMode::DontWait => {}
        SyncMode::WaitForResponse => {}
        SyncMode::WaitForTask => wait_for_tasks(client).await?,
    }

    Ok(outputs)
}

async fn wait_for_tasks(client: &Client) -> anyhow::Result<()> {
    loop {
        let response = client
            .get("tasks?statuses=enqueued,processing")
            .send()
            .await
            .context("could not wait for tasks")?;
        let response: serde_json::Value = response
            .json()
            .await
            .context("could not deserialize response to JSON")
            .context("could not wait for tasks")?;
        match response.get("total") {
            Some(serde_json::Value::Number(number)) => {
                let number = number.as_u64().with_context(|| {
                    format!("waiting for tasks: could not parse 'total' as integer, got {}", number)
                })?;
                if number == 0 {
                    break;
                } else {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            }
            Some(thing_else) => {
                bail!(format!(
                    "waiting for tasks: could not parse 'total' as a number, got '{thing_else}'"
                ))
            }
            None => {
                bail!(format!(
                    "waiting for tasks: expected response to contain 'total', got '{response}'"
                ))
            }
        }
    }
    Ok(())
}

#[tracing::instrument(skip(client, command, assets, asset_folder), fields(command = %command))]
pub async fn run(
    client: &Client,
    command: &Command,
    assets: &BTreeMap<String, Asset>,
    asset_folder: &str,
    return_value: bool,
) -> anyhow::Result<Option<(Value, StatusCode)>> {
    // memtake the body here to leave an empty body in its place, so that command is not partially moved-out
    let body = command
        .body
        .clone()
        .get(assets, asset_folder)
        .with_context(|| format!("while getting body for command {command}"))?;

    let request = client.request(command.method.into(), &command.route);

    let request = if let Some((body, content_type)) = body {
        request.body(body).header(reqwest::header::CONTENT_TYPE, content_type)
    } else {
        request
    };

    let response =
        request.send().await.with_context(|| format!("error sending command: {}", command))?;

    let code = response.status();

    if let Some(expected_status) = command.expected_status {
        if code.as_u16() != expected_status {
            let response = response
                .text()
                .await
                .context("could not read response body as text")
                .context("reading response body when checking expected status")?;
            bail!("unexpected status code: got {}, expected {expected_status}, response body: '{response}'", code.as_u16());
        }
    } else if code.is_client_error() {
        tracing::error!(%command, %code, "error in workload file");
        let response: serde_json::Value = response
            .json()
            .await
            .context("could not deserialize response as JSON")
            .context("parsing error in workload file when sending command")?;
        bail!("error in workload file: server responded with error code {code} and '{response}'")
    } else if code.is_server_error() {
        tracing::error!(%command, %code, "server error");
        let response: serde_json::Value = response
            .json()
            .await
            .context("could not deserialize response as JSON")
            .context("parsing server error when sending command")?;
        bail!("server error: server responded with error code {code} and '{response}'")
    }

    if return_value {
        let response: serde_json::Value = response
            .json()
            .await
            .context("could not deserialize response as JSON")
            .context("parsing response when recording expected response")?;
        return Ok(Some((response, code)));
    } else if let Some(expected_response) = &command.expected_response {
        let response: serde_json::Value = response
            .json()
            .await
            .context("could not deserialize response as JSON")
            .context("parsing response when checking expected response")?;
        if &response != expected_response {
            let expected_pretty = serde_json::to_string_pretty(expected_response)
                .context("serializing expected response as pretty JSON")?;
            let response_pretty = serde_json::to_string_pretty(&response)
                .context("serializing response as pretty JSON")?;
            let diff = SimpleDiff::from_str(&expected_pretty, &response_pretty, "expected", "got");
            bail!("unexpected response:\n{diff}");
        }
    }

    Ok(None)
}

pub async fn run_commands(
    client: &Arc<Client>,
    commands: &[Command],
    assets: &Arc<BTreeMap<String, Asset>>,
    asset_folder: &'static str,
    return_response: bool,
) -> anyhow::Result<Vec<(Value, StatusCode)>> {
    let mut responses = Vec::new();
    for batch in
        commands.split_inclusive(|command| !matches!(command.synchronous, SyncMode::DontWait))
    {
        let mut new_responses =
            run_batch(client, batch.to_vec(), assets, asset_folder, return_response).await?;
        responses.append(&mut new_responses);
    }

    Ok(responses)
}

pub fn health_command() -> Command {
    Command {
        route: "/health".into(),
        method: crate::common::client::Method::Get,
        body: Default::default(),
        synchronous: SyncMode::WaitForResponse,
        expected_status: None,
        expected_response: None,
    }
}
