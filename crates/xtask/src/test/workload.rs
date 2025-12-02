use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::sync::Arc;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::common::assets::{fetch_assets, Asset};
use crate::common::client::Client;
use crate::common::command::{run_commands, Command};
use crate::common::instance::Binary;
use crate::common::process::{self, delete_db, kill_meili};
use crate::common::workload::Workload;
use crate::test::TestDeriveArgs;

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum CommandOrBinary {
    Command(Command),
    Binary { binary: Binary },
}

enum CommandOrBinaryVec<'a> {
    Commands(Vec<&'a mut Command>),
    Binary(Binary),
}

fn produce_reference_value(value: &mut Value) {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => (),
        Value::String(string) => {
            if time::OffsetDateTime::parse(
                string.as_str(),
                &time::format_description::well_known::Rfc3339,
            )
            .is_ok()
            {
                *string = String::from("[timestamp]");
            } else if uuid::Uuid::parse_str(string).is_ok() {
                *string = String::from("[uuid]");
            }
        }
        Value::Array(values) => {
            for value in values {
                produce_reference_value(value);
            }
        }
        Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                match key.as_str() {
                    "duration" => {
                        *value = Value::String(String::from("[duration]"));
                    }
                    "processingTimeMs" => {
                        *value = Value::String(String::from("[duration]"));
                    }
                    _ => produce_reference_value(value),
                }
            }
        }
    }
}

/// A test workload.
/// Not to be confused with [a bench workload](crate::bench::workload::Workload).
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TestWorkload {
    pub name: String,
    pub binary: Binary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub master_key: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub assets: BTreeMap<String, Asset>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<CommandOrBinary>,
}

impl TestWorkload {
    pub async fn run(
        mut self,
        args: &TestDeriveArgs,
        assets_client: &Client,
        meili_client: &Arc<Client>,
        asset_folder: &'static str,
    ) -> anyhow::Result<()> {
        // Group commands between upgrades
        let mut commands_or_instance = Vec::new();
        let mut current_commands = Vec::new();
        let mut all_releases = Vec::new();

        if let Some(release) = self.binary.as_release() {
            all_releases.push(release);
        }
        for command_or_upgrade in &mut self.commands {
            match command_or_upgrade {
                CommandOrBinary::Command(command) => current_commands.push(command),
                CommandOrBinary::Binary { binary: instance } => {
                    if !current_commands.is_empty() {
                        commands_or_instance.push(CommandOrBinaryVec::Commands(current_commands));
                        current_commands = Vec::new();
                    }
                    commands_or_instance.push(CommandOrBinaryVec::Binary(instance.clone()));
                    if let Some(release) = instance.as_release() {
                        all_releases.push(release);
                    }
                }
            }
        }
        if !current_commands.is_empty() {
            commands_or_instance.push(CommandOrBinaryVec::Commands(current_commands));
        }

        // Fetch assets
        crate::common::instance::add_releases_to_assets(&mut self.assets, all_releases).await?;
        fetch_assets(assets_client, &self.assets, &args.common.asset_folder).await?;

        // Run server
        delete_db().await;
        let mut process = process::start_meili(
            meili_client,
            Some("masterKey"),
            &self.binary,
            &args.common.asset_folder,
        )
        .await?;

        let assets = Arc::new(self.assets.clone());
        let return_responses = args.add_missing_responses || args.update_responses;
        let mut registered = HashMap::new();
        let mut first_command_index = 0;
        for command_or_upgrade in commands_or_instance {
            match command_or_upgrade {
                CommandOrBinaryVec::Commands(commands) => {
                    let cloned: Vec<_> = commands.iter().map(|c| (*c).clone()).collect();
                    let responses = run_commands(
                        meili_client,
                        &cloned,
                        first_command_index,
                        &assets,
                        asset_folder,
                        &mut registered,
                        return_responses,
                    )
                    .await?;
                    first_command_index += cloned.len();
                    if return_responses {
                        assert_eq!(responses.len(), cloned.len());
                        for (command, (mut response, status)) in commands.into_iter().zip(responses)
                        {
                            if args.update_responses
                                || (args.add_missing_responses
                                    && command.expected_response.is_none())
                            {
                                produce_reference_value(&mut response);
                                command.expected_response = Some(response);
                                command.expected_status = Some(status.as_u16());
                            }
                        }
                    }
                }
                CommandOrBinaryVec::Binary(binary) => {
                    kill_meili(process).await;
                    process = process::start_meili(
                        meili_client,
                        Some("masterKey"),
                        &binary,
                        &args.common.asset_folder,
                    )
                    .await?;
                    tracing::info!("Restarted instance with {binary}");
                }
            }
        }

        // Write back the workload if needed
        if return_responses {
            // Filter out the assets we added for the versions
            self.assets.retain(|_, asset| {
                asset.local_location.as_ref().is_none_or(|a| !a.starts_with("meilisearch-"))
            });

            let workload = Workload::Test(self);
            let mut file =
                std::fs::File::create(&args.common.workload_file[0]).with_context(|| {
                    format!("could not open {}", args.common.workload_file[0].display())
                })?;
            serde_json::to_writer_pretty(&file, &workload).with_context(|| {
                format!("could not write to {}", args.common.workload_file[0].display())
            })?;
            file.write_all(b"\n").with_context(|| {
                format!("could not write to {}", args.common.workload_file[0].display())
            })?;
            tracing::info!("Updated workload file {}", args.common.workload_file[0].display());
        }

        Ok(())
    }
}
