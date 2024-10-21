use std::collections::BTreeMap;
use std::fmt::Display;
use std::io::Read as _;

use anyhow::{bail, Context as _};
use serde::Deserialize;

use super::assets::{fetch_asset, Asset};
use super::client::{Client, Method};

#[derive(Clone, Deserialize)]
pub struct Command {
    pub route: String,
    pub method: Method,
    #[serde(default)]
    pub body: Body,
    #[serde(default)]
    pub synchronous: SyncMode,
}

#[derive(Default, Clone, Deserialize)]
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

#[derive(Default, Debug, Clone, Copy, Deserialize)]
pub enum SyncMode {
    DontWait,
    #[default]
    WaitForResponse,
    WaitForTask,
}

pub async fn run_batch(
    client: &Client,
    batch: &[Command],
    assets: &BTreeMap<String, Asset>,
    asset_folder: &str,
) -> anyhow::Result<()> {
    let [.., last] = batch else { return Ok(()) };
    let sync = last.synchronous;

    let mut tasks = tokio::task::JoinSet::new();

    for command in batch {
        // FIXME: you probably don't want to copy assets everytime here
        tasks.spawn({
            let client = client.clone();
            let command = command.clone();
            let assets = assets.clone();
            let asset_folder = asset_folder.to_owned();

            async move { run(client, command, &assets, &asset_folder).await }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result
            .context("panicked while executing command")?
            .context("error while executing command")?;
    }

    match sync {
        SyncMode::DontWait => {}
        SyncMode::WaitForResponse => {}
        SyncMode::WaitForTask => wait_for_tasks(client).await?,
    }

    Ok(())
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
    client: Client,
    mut command: Command,
    assets: &BTreeMap<String, Asset>,
    asset_folder: &str,
) -> anyhow::Result<()> {
    // memtake the body here to leave an empty body in its place, so that command is not partially moved-out
    let body = std::mem::take(&mut command.body)
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
    if code.is_client_error() {
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

    Ok(())
}
