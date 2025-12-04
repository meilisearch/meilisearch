use std::collections::{BTreeMap, HashMap};
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

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Command {
    pub route: String,
    pub method: Method,
    #[serde(default)]
    pub body: Body,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_response: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub register: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_variable: Option<String>,
    #[serde(default)]
    pub synchronous: SyncMode,
}

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
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
        registered: &HashMap<String, Value>,
        asset_folder: &str,
    ) -> anyhow::Result<Option<(Vec<u8>, &'static str)>> {
        Ok(match self {
            Body::Inline { inline: mut body } => {
                if !registered.is_empty() {
                    insert_variables(&mut body, registered);
                }

                Some((
                    serde_json::to_vec(&body)
                        .context("serializing to bytes")
                        .context("while getting inline body")?,
                    "application/json",
                ))
            }
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

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum SyncMode {
    DontWait,
    #[default]
    WaitForResponse,
    WaitForTask,
}

async fn run_batch(
    client: &Arc<Client>,
    batch: &[Command],
    first_command_index: usize,
    assets: &Arc<BTreeMap<String, Asset>>,
    asset_folder: &'static str,
    registered: &mut HashMap<String, Value>,
    return_response: bool,
) -> anyhow::Result<Vec<(Value, StatusCode)>> {
    let [.., last] = batch else { return Ok(Vec::new()) };
    let sync = last.synchronous;
    let batch_len = batch.len();

    let mut tasks = Vec::with_capacity(batch.len());
    for (index, command) in batch.iter().cloned().enumerate() {
        let client2 = Arc::clone(client);
        let assets2 = Arc::clone(assets);
        let needs_response = return_response || !command.register.is_empty();
        let registered2 = registered.clone(); // FIXME: cloning the whole map for each command is inefficient
        tasks.push(tokio::spawn(async move {
            run(
                &client2,
                &command,
                first_command_index + index,
                &assets2,
                registered2,
                asset_folder,
                needs_response,
            )
            .await
        }));
    }

    let mut outputs = Vec::with_capacity(if return_response { batch_len } else { 0 });
    for (task, command) in tasks.into_iter().zip(batch.iter()) {
        let output = task.await.context("task panicked")??;
        if let Some(output) = output {
            for (name, path) in &command.register {
                let value = output
                    .0
                    .pointer(path)
                    .with_context(|| format!("could not find path '{path}' in response (required to register '{name}')"))?
                    .clone();
                registered.insert(name.clone(), value);
            }

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

fn json_eq_ignore(reference: &Value, value: &Value) -> bool {
    match reference {
        Value::Null | Value::Bool(_) | Value::Number(_) => reference == value,
        Value::String(s) => (s.starts_with('[') && s.ends_with(']')) || reference == value,
        Value::Array(values) => match value {
            Value::Array(other_values) => {
                if values.len() != other_values.len() {
                    return false;
                }
                for (value, other_value) in values.iter().zip(other_values.iter()) {
                    if !json_eq_ignore(value, other_value) {
                        return false;
                    }
                }
                true
            }
            _ => false,
        },
        Value::Object(map) => match value {
            Value::Object(other_map) => {
                if map.len() != other_map.len() {
                    return false;
                }
                for (key, value) in map.iter() {
                    match other_map.get(key) {
                        Some(other_value) => {
                            if !json_eq_ignore(value, other_value) {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                true
            }
            _ => false,
        },
    }
}

#[tracing::instrument(skip(client, command, assets, registered, asset_folder), fields(command = %command))]
pub async fn run(
    client: &Client,
    command: &Command,
    command_index: usize,
    assets: &BTreeMap<String, Asset>,
    registered: HashMap<String, Value>,
    asset_folder: &str,
    return_value: bool,
) -> anyhow::Result<Option<(Value, StatusCode)>> {
    // Try to replace variables in the route
    let mut route = &command.route;
    let mut owned_route;
    if !registered.is_empty() {
        while let (Some(pos1), Some(pos2)) = (route.find("{{"), route.rfind("}}")) {
            if pos2 > pos1 {
                let name = route[pos1 + 2..pos2].trim();
                if let Some(replacement) = registered.get(name).and_then(|r| r.as_str()) {
                    let mut new_route = String::new();
                    new_route.push_str(&route[..pos1]);
                    new_route.push_str(replacement);
                    new_route.push_str(&route[pos2 + 2..]);
                    owned_route = new_route;
                    route = &owned_route;
                    continue;
                }
            }
            break;
        }
    }

    // memtake the body here to leave an empty body in its place, so that command is not partially moved-out
    let body = command
        .body
        .clone()
        .get(assets, &registered, asset_folder)
        .with_context(|| format!("while getting body for command {command}"))?;

    let mut request = client.request(command.method.into(), route);

    // Replace the api key
    if let Some(var_name) = &command.api_key_variable {
        if let Some(api_key) = registered.get(var_name).and_then(|v| v.as_str()) {
            request = request.header("Authorization", format!("Bearer {api_key}"));
        } else {
            bail!("could not find API key variable '{var_name}' in registered values");
        }
    }

    let request = if let Some((body, content_type)) = body {
        request.body(body).header(reqwest::header::CONTENT_TYPE, content_type)
    } else {
        request
    };

    let response =
        request.send().await.with_context(|| format!("error sending command: {}", command))?;

    let code = response.status();

    if !return_value {
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
            bail!(
                "error in workload file: server responded with error code {code} and '{response}'"
            )
        } else if code.is_server_error() {
            tracing::error!(%command, %code, "server error");
            let response: serde_json::Value = response
                .json()
                .await
                .context("could not deserialize response as JSON")
                .context("parsing server error when sending command")?;
            bail!("server error: server responded with error code {code} and '{response}'")
        }
    }

    if let Some(expected_response) = &command.expected_response {
        let mut evaluated_expected_response;

        let expected_response = if !registered.is_empty() {
            evaluated_expected_response = expected_response.clone();
            insert_variables(&mut evaluated_expected_response, &registered);
            &evaluated_expected_response
        } else {
            expected_response
        };

        let response: serde_json::Value = response
            .json()
            .await
            .context("could not deserialize response as JSON")
            .context("parsing response when checking expected response")?;
        if return_value {
            return Ok(Some((response, code)));
        }
        if !json_eq_ignore(expected_response, &response) {
            let expected_pretty = serde_json::to_string_pretty(expected_response)
                .context("serializing expected response as pretty JSON")?;
            let response_pretty = serde_json::to_string_pretty(&response)
                .context("serializing response as pretty JSON")?;
            let diff = SimpleDiff::from_str(&expected_pretty, &response_pretty, "expected", "got");
            bail!("command #{command_index} unexpected response:\n{diff}");
        }
    } else if return_value {
        let response: serde_json::Value = response
            .json()
            .await
            .context("could not deserialize response as JSON")
            .context("parsing response when recording expected response")?;
        return Ok(Some((response, code)));
    }

    Ok(None)
}

pub async fn run_commands(
    client: &Arc<Client>,
    commands: &[Command],
    mut first_command_index: usize,
    assets: &Arc<BTreeMap<String, Asset>>,
    asset_folder: &'static str,
    registered: &mut HashMap<String, Value>,
    return_response: bool,
) -> anyhow::Result<Vec<(Value, StatusCode)>> {
    let mut responses = Vec::new();
    for batch in
        commands.split_inclusive(|command| !matches!(command.synchronous, SyncMode::DontWait))
    {
        let mut new_responses = run_batch(
            client,
            batch,
            first_command_index,
            assets,
            asset_folder,
            registered,
            return_response,
        )
        .await?;
        responses.append(&mut new_responses);

        first_command_index += batch.len();
    }

    Ok(responses)
}

pub fn health_command() -> Command {
    Command {
        route: "/health".into(),
        method: crate::common::client::Method::Get,
        body: Default::default(),
        register: HashMap::new(),
        synchronous: SyncMode::WaitForResponse,
        expected_status: None,
        expected_response: None,
        api_key_variable: None,
    }
}

pub fn insert_variables(value: &mut Value, registered: &HashMap<String, Value>) {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => (),
        Value::String(s) => {
            if s.starts_with("{{") && s.ends_with("}}") {
                let name = s[2..s.len() - 2].trim();
                if let Some(replacement) = registered.get(name) {
                    *value = replacement.clone();
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                insert_variables(value, registered);
            }
        }
        Value::Object(map) => {
            for (_key, value) in map.iter_mut() {
                insert_variables(value, registered);
            }
        }
    }
}
