use cargo_metadata::semver::Version;
use serde::Deserialize;
use std::collections::BTreeMap;

use crate::common::{assets::Asset, command::Command};

#[derive(Deserialize)]
#[serde(untagged)]
pub enum CommandOrUpgrade {
    Command(Command),
    Upgrade { upgrade: Version },
}

/// A test workload.
/// Not to be confused with [a bench workload](crate::bench::workload::Workload).
#[derive(Deserialize)]
pub struct TestWorkload {
    pub name: String,
    pub assets: BTreeMap<String, Asset>,
    pub commands: Vec<CommandOrUpgrade>,
}
