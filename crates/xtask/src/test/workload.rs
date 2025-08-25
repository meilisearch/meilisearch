use cargo_metadata::semver::Version;
use serde::Deserialize;
use std::collections::BTreeMap;

use crate::{
    common::{
        assets::{fetch_assets, Asset},
        client::Client,
        command::{run_commands, Command},
    },
    test::TestDeriveArgs,
};

#[derive(Deserialize)]
#[serde(untagged)]
pub enum CommandOrUpgrade {
    Command(Command),
    Upgrade { upgrade: Version },
}

enum CommandOrUpgradeVec {
    Commands(Vec<Command>),
    Upgrade(Version),
}

/// A test workload.
/// Not to be confused with [a bench workload](crate::bench::workload::Workload).
#[derive(Deserialize)]
pub struct TestWorkload {
    pub name: String,
    pub assets: BTreeMap<String, Asset>,
    pub commands: Vec<CommandOrUpgrade>,
}

impl TestWorkload {
    pub async fn run(
        &self,
        args: &TestDeriveArgs,
        assets_client: &Client,
        meili_client: &Client,
    ) -> anyhow::Result<()> {
        // Fetch assets
        fetch_assets(assets_client, &self.assets, &args.common.asset_folder).await?;

        // Group commands between upgrades
        let mut commands_or_upgrade = Vec::new();
        let mut current_commands = Vec::new();
        for command_or_upgrade in &self.commands {
            match command_or_upgrade {
                CommandOrUpgrade::Command(command) => current_commands.push(command.clone()),
                CommandOrUpgrade::Upgrade { upgrade } => {
                    if !current_commands.is_empty() {
                        commands_or_upgrade.push(CommandOrUpgradeVec::Commands(current_commands));
                        current_commands = Vec::new();
                    }
                    commands_or_upgrade.push(CommandOrUpgradeVec::Upgrade(upgrade.clone()));
                }
            }
        }

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
