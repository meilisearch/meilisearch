use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use meilisearch::routes::MeilisearchApi;
use utoipa::OpenApi;

#[derive(Parser)]
#[command(name = "openapi-generator")]
#[command(about = "Generate OpenAPI specification for Meilisearch")]
struct Cli {
    /// Output file path (default: meilisearch.json)
    #[arg(short, long, value_name = "FILE")]
    output: Option<PathBuf>,

    /// Pretty print the JSON output
    #[arg(short, long)]
    pretty: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Generate the OpenAPI specification
    let openapi = MeilisearchApi::openapi();

    // Determine output path
    let output_path = cli.output.unwrap_or_else(|| PathBuf::from("meilisearch.json"));

    // Serialize to JSON
    let json = if cli.pretty {
        serde_json::to_string_pretty(&openapi)?
    } else {
        serde_json::to_string(&openapi)?
    };

    // Write to file
    std::fs::write(&output_path, json)?;

    println!("OpenAPI specification written to: {}", output_path.display());

    Ok(())
}
