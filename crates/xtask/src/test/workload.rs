use cargo_metadata::semver::Version;
use serde::Deserialize;
use std::collections::BTreeMap;

use crate::{
    common::{
        assets::{fetch_assets, Asset},
        client::Client,
        command::{run_commands, Command},
    },
    test::{versions::expand_assets_with_versions, TestDeriveArgs},
};

#[derive(Clone)]
pub enum VersionOrLatest {
    Version(Version),
    Latest,
}

impl<'a> Deserialize<'a> for VersionOrLatest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;

        if s.eq_ignore_ascii_case("latest") {
            Ok(VersionOrLatest::Latest)
        } else {
            let version = Version::parse(s).map_err(serde::de::Error::custom)?;
            Ok(VersionOrLatest::Version(version))
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum CommandOrUpgrade {
    Command(Command),
    Upgrade { upgrade: VersionOrLatest },
}

enum CommandOrUpgradeVec {
    Commands(Vec<Command>),
    Upgrade(VersionOrLatest),
}

/// A test workload.
/// Not to be confused with [a bench workload](crate::bench::workload::Workload).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestWorkload {
    pub name: String,
    pub initial_version: Version,
    pub assets: BTreeMap<String, Asset>,
    pub commands: Vec<CommandOrUpgrade>,
}

impl TestWorkload {
    pub async fn run(
        &mut self,
        args: &TestDeriveArgs,
        assets_client: &Client,
        meili_client: &Client,
    ) -> anyhow::Result<()> {
        // Group commands between upgrades
        let mut commands_or_upgrade = Vec::new();
        let mut current_commands = Vec::new();
        let mut all_versions = vec![self.initial_version.clone()];
        for command_or_upgrade in &self.commands {
            match command_or_upgrade {
                CommandOrUpgrade::Command(command) => current_commands.push(command.clone()),
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

        // Fetch assets
        expand_assets_with_versions(&mut self.assets, &all_versions).await?;
        fetch_assets(assets_client, &self.assets, &args.common.asset_folder).await?;

        for command_or_upgrade in commands_or_upgrade {
            match command_or_upgrade {
                CommandOrUpgradeVec::Commands(commands) => {
                    run_commands(meili_client, &commands, &self.assets, &args.common.asset_folder)
                        .await?;
                }
                CommandOrUpgradeVec::Upgrade(version) => {
                    todo!()
                }
            }
        }

        Ok(())
    }
}
