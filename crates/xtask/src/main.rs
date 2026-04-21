use std::collections::HashSet;
use std::process::Stdio;

use anyhow::Context;
use clap::Parser;
use semver::{Prerelease, Version};
use xtask::bench::BenchArgs;
use xtask::test::TestArgs;

/// This is the version of the crate but also the current Meilisearch version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// List features available in the workspace
#[derive(Parser, Debug)]
struct ListFeaturesArgs {
    /// Feature to exclude from the list. Use a comma to separate multiple features.
    #[arg(short, long, value_delimiter = ',')]
    exclude_feature: Vec<String>,
}

/// Create a git tag for the current version
///
/// The tag will of the form prototype-v<version>-<name>.<increment>
#[derive(Parser, Debug)]
struct PrototypeArgs {
    /// Name of the prototype to generate
    name: String,
    /// If true refuses to increment the tag if it already exists
    /// else refuses to generate new tag and expect the tag to exist.
    #[arg(long)]
    generate_new: bool,
}

/// Utilitary commands
#[derive(Parser, Debug)]
#[command(author, version, about, long_about)]
#[command(name = "cargo xtask")]
#[command(bin_name = "cargo xtask")]
#[allow(clippy::large_enum_variant)] // please, that's enough...
enum Command {
    ListFeatures(ListFeaturesArgs),
    Bench(BenchArgs),
    GeneratePrototype(PrototypeArgs),
    Test(TestArgs),
}

fn main() -> anyhow::Result<()> {
    let args = Command::parse();
    match args {
        Command::ListFeatures(args) => list_features(args),
        Command::Bench(args) => xtask::bench::run(args)?,
        Command::GeneratePrototype(args) => generate_prototype(args)?,
        Command::Test(args) => xtask::test::run(args)?,
    }
    Ok(())
}

fn list_features(args: ListFeaturesArgs) {
    let exclude_features: HashSet<_> = args.exclude_feature.into_iter().collect();
    let metadata = cargo_metadata::MetadataCommand::new().no_deps().exec().unwrap();
    let features: Vec<String> = metadata
        .packages
        .iter()
        .flat_map(|package| package.features.keys())
        .filter(|feature| !exclude_features.contains(feature.as_str()))
        .map(|s| s.to_owned())
        .collect();
    let features = features.join(" ");
    println!("{features}")
}

fn generate_prototype(args: PrototypeArgs) -> anyhow::Result<()> {
    let PrototypeArgs { name, generate_new: create_new } = args;

    if name.rsplit_once(['.', '-']).filter(|(_, t)| t.chars().all(char::is_numeric)).is_some() {
        anyhow::bail!(
            "The increment must not be part of the name and will be rather incremented by this command."
        );
    }

    // 1. Fetch the crate version
    let version = Version::parse(VERSION).context("while semver-parsing the crate version")?;

    // 2. Pull tags from remote and retrieve last prototype tag
    std::process::Command::new("git")
        .arg("fetch")
        .arg("--tags")
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .status()?;

    let output = std::process::Command::new("git")
        .arg("tag")
        .args(["--list", "prototype-v*"])
        .stderr(Stdio::inherit())
        .output()?;
    let output =
        String::try_from(output.stdout).context("while converting the tag list into a string")?;

    let mut highest_increment = None;
    for tag in output.lines() {
        let Some(version) = tag.strip_prefix("prototype-v") else {
            continue;
        };
        let Ok(version) = Version::parse(version) else {
            continue;
        };
        let Ok(proto) = PrototypePrerelease::from_str(version.pre.as_str()) else {
            continue;
        };
        if proto.name() == name {
            highest_increment = match highest_increment {
                Some(last) if last < proto.increment() => Some(proto.increment()),
                Some(last) => Some(last),
                None => Some(proto.increment()),
            };
        }
    }

    // 3. Generate the new tag name (without git, just a string)
    let increment = match (create_new, highest_increment) {
        (true, None) => 0,
        (true, Some(increment)) => anyhow::bail!(
            "A prototype with the name `{name}` already exists with increment `{increment}`"
        ),
        (false, None) => anyhow::bail!(
            "Prototype `{name}` is missing and must exist to be incremented.\n\
            Use the --generate-new flag to create a new prototype with an increment at 0."
        ),
        (false, Some(increment)) => {
            increment.checked_add(1).context("While incrementing by one the increment")?
        }
    };

    // Note that we cannot have leading zeros in the increment
    let pre = format!("{name}.{increment}").parse().context("while parsing pre-release name")?;
    let tag_name = Version { pre, ..version };
    println!("prototype-v{tag_name}");

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PrototypePrerelease {
    pre: Prerelease,
}

impl PrototypePrerelease {
    fn from_str(s: &str) -> anyhow::Result<Self> {
        Prerelease::new(s)
            .map_err(Into::into)
            .and_then(|pre| {
                if pre.rsplit_once('.').is_some() {
                    Ok(pre)
                } else {
                    Err(anyhow::anyhow!("Invalid prototype name, missing name or increment"))
                }
            })
            .map(|pre| PrototypePrerelease { pre })
    }

    fn name(&self) -> &str {
        self.pre.rsplit_once('.').expect("Missing prototype name").0
    }

    fn increment(&self) -> u32 {
        self.pre
            .as_str()
            .rsplit_once('.')
            .map(|(_, tail)| tail.parse().expect("Invalid increment"))
            .expect("Missing increment")
    }
}
