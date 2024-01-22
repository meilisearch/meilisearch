use std::collections::HashSet;

use clap::Parser;

/// List features available in the workspace
#[derive(Parser, Debug)]
struct ListFeaturesDeriveArgs {
    /// Feature to exclude from the list. Repeat the argument to exclude multiple features
    #[arg(short, long)]
    exclude_feature: Vec<String>,
}

/// Utilitary commands
#[derive(Parser, Debug)]
#[command(author, version, about, long_about)]
#[command(name = "cargo xtask")]
#[command(bin_name = "cargo xtask")]
enum Command {
    ListFeatures(ListFeaturesDeriveArgs),
}

fn main() {
    let args = Command::parse();
    match args {
        Command::ListFeatures(args) => list_features(args),
    }
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
