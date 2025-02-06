use std::collections::HashSet;

use clap::Parser;
use xtask::bench::BenchDeriveArgs;

/// List features available in the workspace
#[derive(Parser, Debug)]
struct ListFeaturesDeriveArgs {
    /// Feature to exclude from the list. Use a comma to separate multiple features.
    #[arg(short, long, value_delimiter = ',')]
    exclude_feature: Vec<String>,
}

/// Utilitary commands
#[derive(Parser, Debug)]
#[command(author, version, about, long_about)]
#[command(name = "cargo xtask")]
#[command(bin_name = "cargo xtask")]
#[allow(clippy::large_enum_variant)] // please, that's enough...
enum Command {
    ListFeatures(ListFeaturesDeriveArgs),
    Bench(BenchDeriveArgs),
}

fn main() -> anyhow::Result<()> {
    let args = Command::parse();
    match args {
        Command::ListFeatures(args) => list_features(args),
        Command::Bench(args) => xtask::bench::run(args)?,
    }
    Ok(())
}

fn list_features(args: ListFeaturesDeriveArgs) {
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
