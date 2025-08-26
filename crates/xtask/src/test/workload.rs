use anyhow::Context;
use cargo_metadata::semver::Version;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

use crate::{
    common::{
        assets::{fetch_assets, Asset},
        client::Client,
        command::{run_commands, Command},
        process::{self, delete_db, kill_meili},
        workload::Workload,
    },
    test::{
        versions::{expand_assets_with_versions, VersionOrLatest},
        TestDeriveArgs,
    },
};

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum CommandOrUpgrade {
    Command(Command),
    Upgrade { upgrade: VersionOrLatest },
}

enum CommandOrUpgradeVec<'a> {
    Commands(Vec<&'a mut Command>),
    Upgrade(VersionOrLatest),
}

/// A test workload.
/// Not to be confused with [a bench workload](crate::bench::workload::Workload).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestWorkload {
    pub name: String,
    pub initial_version: Version,
    pub assets: BTreeMap<String, Asset>,
    pub commands: Vec<CommandOrUpgrade>,
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
        let mut commands_or_upgrade = Vec::new();
        let mut current_commands = Vec::new();
        let mut all_versions = vec![self.initial_version.clone()];
        for command_or_upgrade in &mut self.commands {
            match command_or_upgrade {
                CommandOrUpgrade::Command(command) => current_commands.push(command),
                CommandOrUpgrade::Upgrade { upgrade } => {
                    if !current_commands.is_empty() {
                        commands_or_upgrade.push(CommandOrUpgradeVec::Commands(current_commands));
                        current_commands = Vec::new();
                    }
                    commands_or_upgrade.push(CommandOrUpgradeVec::Upgrade(upgrade.clone()));
                    if let VersionOrLatest::Version(upgrade) = upgrade {
                        all_versions.push(upgrade.clone());
                    }
                }
            }
        }
        if !current_commands.is_empty() {
            commands_or_upgrade.push(CommandOrUpgradeVec::Commands(current_commands));
        }

        // Fetch assets
        expand_assets_with_versions(&mut self.assets, &all_versions).await?;
        fetch_assets(assets_client, &self.assets, &args.common.asset_folder).await?;

        // Run server
        delete_db().await;
        let binary_path = VersionOrLatest::Version(self.initial_version.clone())
            .binary_path(&args.common.asset_folder)?;
        let mut process = process::start_meili(
            meili_client,
            args.common.master_key.as_deref(),
            &[],
            &self.name,
            binary_path.as_deref(),
        )
        .await?;

        let assets = Arc::new(self.assets.clone());
        let return_responses = dbg!(args.add_missing_responses || args.update_responses);
        for command_or_upgrade in commands_or_upgrade {
            match command_or_upgrade {
                CommandOrUpgradeVec::Commands(commands) => {
                    let cloned: Vec<_> = commands.iter().map(|c| (*c).clone()).collect();
                    let responses = run_commands(
                        meili_client,
                        &cloned,
                        &assets,
                        asset_folder,
                        return_responses,
                    )
                    .await?;
                    if return_responses {
                        assert_eq!(responses.len(), cloned.len());
                        for (command, (response, status)) in commands.into_iter().zip(responses) {
                            if args.update_responses
                                || (dbg!(args.add_missing_responses)
                                    && dbg!(command.expected_response.is_none()))
                            {
                                command.expected_response = Some(response);
                                command.expected_status = Some(status.as_u16());
                            }
                        }
                    }
                }
                CommandOrUpgradeVec::Upgrade(version) => {
                    kill_meili(process).await;
                    let binary_path = version.binary_path(&args.common.asset_folder)?;
                    process = process::start_meili(
                        meili_client,
                        args.common.master_key.as_deref(),
                        &[String::from("--experimental-dumpless-upgrade")],
                        &self.name,
                        binary_path.as_deref(),
                    ).await?;
                    tracing::info!("Upgraded to {version}");
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
            let file = std::fs::File::create(&args.common.workload_file[0]).with_context(|| {
                format!("could not open {}", args.common.workload_file[0].display())
            })?;
            serde_json::to_writer_pretty(file, &workload).with_context(|| {
                format!("could not write to {}", args.common.workload_file[0].display())
            })?;
            tracing::info!("Updated workload file {}", args.common.workload_file[0].display());
        }

        Ok(())
    }
}
