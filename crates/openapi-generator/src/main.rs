use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use meilisearch::routes::MeilisearchApi;
use serde_json::Value;
use utoipa::OpenApi;

/// HTTP methods supported in OpenAPI specifications.
const HTTP_METHODS: &[&str] = &["get", "post", "put", "patch", "delete"];

#[derive(Parser)]
#[command(name = "openapi-generator")]
#[command(about = "Generate OpenAPI specification for Meilisearch")]
struct Cli {
    /// Output file path (default: meilisearch-openapi.json)
    #[arg(short, long, value_name = "FILE")]
    output: Option<PathBuf>,

    /// Pretty print the JSON output
    #[arg(short, long)]
    pretty: bool,

    /// Check that all routes have a summary (useful for CI)
    #[arg(long)]
    check_summaries: bool,

    /// Check for duplicate routes and path issues (useful for CI)
    #[arg(long)]
    check_paths: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Generate the OpenAPI specification
    let openapi = MeilisearchApi::openapi();

    // Convert to serde_json::Value for modification
    let openapi_value: Value = serde_json::to_value(&openapi)?;

    // Check that all routes have summaries if requested
    if cli.check_summaries {
        check_all_routes_have_summaries(&openapi_value)?;
    }

    // Check for path issues (duplicates, malformed paths) if requested
    if cli.check_paths {
        check_path_issues(&openapi_value)?;
    }

    // Determine output path
    let output_path = cli.output.unwrap_or_else(|| PathBuf::from("meilisearch-openapi.json"));

    // Serialize to JSON
    let json = if cli.pretty {
        serde_json::to_string_pretty(&openapi_value)?
    } else {
        serde_json::to_string(&openapi_value)?
    };

    // Write to file
    std::fs::write(&output_path, json)?;

    println!("OpenAPI specification written to: {}", output_path.display());

    Ok(())
}

/// Checks that all routes have a summary field.
///
/// Returns an error if any route is missing a summary.
fn check_all_routes_have_summaries(openapi: &Value) -> Result<()> {
    let paths = openapi
        .get("paths")
        .and_then(|p| p.as_object())
        .context("OpenAPI spec missing 'paths' object")?;

    let mut missing_summaries: Vec<String> = Vec::new();

    for (path, path_item) in paths.iter() {
        let Some(path_item) = path_item.as_object() else {
            continue;
        };

        for method in HTTP_METHODS {
            let Some(operation) = path_item.get(*method) else {
                continue;
            };

            let has_summary =
                operation.get("summary").and_then(|s| s.as_str()).is_some_and(|s| !s.is_empty());

            if !has_summary {
                missing_summaries.push(format!("{} {}", method.to_uppercase(), path));
            }
        }
    }

    if missing_summaries.is_empty() {
        println!("All routes have summaries.");
        Ok(())
    } else {
        missing_summaries.sort();
        eprintln!("The following routes are missing a summary:");
        for route in &missing_summaries {
            eprintln!("  - {}", route);
        }
        eprintln!("\nTo fix this, add a doc-comment (///) above the route handler function.");
        eprintln!("The first line becomes the summary, subsequent lines become the description.");
        eprintln!("\nExample:");
        eprintln!("  /// List webhooks");
        eprintln!("  ///");
        eprintln!("  /// Get the list of all registered webhooks.");
        eprintln!("  #[utoipa::path(...)]");
        eprintln!("  async fn get_webhooks(...) {{ ... }}");
        anyhow::bail!("{} route(s) missing summary", missing_summaries.len());
    }
}

/// Checks for path issues in the OpenAPI specification.
///
/// This function validates that:
/// 1. All paths start with `/`
/// 2. No paths contain double slashes `//`
/// 3. No duplicate paths exist (after normalizing slashes)
///
/// Returns an error if any issues are found.
fn check_path_issues(openapi: &Value) -> Result<()> {
    let paths = openapi
        .get("paths")
        .and_then(|p| p.as_object())
        .context("OpenAPI spec missing 'paths' object")?;

    let mut issues: Vec<String> = Vec::new();
    let mut normalized_paths: HashMap<String, String> = HashMap::new();

    for path in paths.keys() {
        // Check 1: Path must start with /
        if !path.starts_with('/') {
            issues.push(format!("Path does not start with '/': {}", path));
        }

        // Check 2: Path must not contain //
        if path.contains("//") {
            issues.push(format!("Path contains double slashes '//': {}", path));
        }

        // Check 3: Check for duplicates after normalization
        // Normalize by: removing leading/trailing slashes, collapsing multiple slashes
        let normalized = normalize_path(path);
        if let Some(existing) = normalized_paths.get(&normalized) {
            if existing != path {
                issues.push(format!(
                    "Duplicate routes detected (same path after normalization):\n    - {}\n    - {}",
                    existing, path
                ));
            }
        } else {
            normalized_paths.insert(normalized, path.clone());
        }
    }

    if issues.is_empty() {
        println!("All paths are valid (no duplicates or malformed paths).");
        Ok(())
    } else {
        eprintln!("Path issues found in OpenAPI specification:\n");
        for issue in &issues {
            eprintln!("  - {}", issue);
        }
        eprintln!();
        anyhow::bail!("{} path issue(s) found", issues.len());
    }
}

/// Normalizes a path for duplicate detection.
///
/// - Removes leading and trailing slashes
/// - Collapses multiple consecutive slashes into one
fn normalize_path(path: &str) -> String {
    path.split('/').filter(|s| !s.is_empty()).collect::<Vec<_>>().join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("/indexes"), "indexes");
        assert_eq!(normalize_path("/indexes/"), "indexes");
        assert_eq!(normalize_path("indexes"), "indexes");
        assert_eq!(normalize_path("/indexes/{indexUid}"), "indexes/{indexUid}");
        assert_eq!(normalize_path("indexes//{indexUid}"), "indexes/{indexUid}");
        assert_eq!(normalize_path("/indexes//{indexUid}/compact"), "indexes/{indexUid}/compact");
        assert_eq!(normalize_path("//indexes///compact//"), "indexes/compact");
    }

    #[test]
    fn test_check_path_issues_valid() {
        let openapi = json!({
            "paths": {
                "/indexes": {},
                "/indexes/{indexUid}": {},
                "/indexes/{indexUid}/documents": {},
                "/indexes/{indexUid}/compact": {}
            }
        });

        let result = check_path_issues(&openapi);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_path_issues_missing_leading_slash() {
        let openapi = json!({
            "paths": {
                "/indexes": {},
                "indexes/{indexUid}": {}
            }
        });

        let result = check_path_issues(&openapi);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("path issue"));
    }

    #[test]
    fn test_check_path_issues_double_slash() {
        let openapi = json!({
            "paths": {
                "/indexes": {},
                "/indexes//{indexUid}/compact": {}
            }
        });

        let result = check_path_issues(&openapi);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("path issue"));
    }

    #[test]
    fn test_check_path_issues_duplicate_routes() {
        let openapi = json!({
            "paths": {
                "/indexes/{indexUid}/compact": {},
                "indexes//{indexUid}/compact": {}
            }
        });

        let result = check_path_issues(&openapi);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        // Should report at least the duplicate issue (and possibly the missing slash and double slash)
        assert!(err.contains("path issue"));
    }
}
