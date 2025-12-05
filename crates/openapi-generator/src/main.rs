use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use meilisearch::routes::MeilisearchApi;
use regex::Regex;
use serde_json::{json, Value};
use utoipa::OpenApi;

const CODE_SAMPLES_URL: &str = "https://raw.githubusercontent.com/meilisearch/documentation/refs/heads/main/.code-samples.meilisearch.yaml";

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

    /// Skip fetching code samples (offline mode)
    #[arg(long)]
    no_code_samples: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Generate the OpenAPI specification
    let openapi = MeilisearchApi::openapi();

    // Convert to serde_json::Value for modification
    let mut openapi_value: Value = serde_json::to_value(&openapi)?;

    // Fetch and add code samples if not disabled
    if !cli.no_code_samples {
        let code_samples = fetch_code_samples()?;
        add_code_samples_to_openapi(&mut openapi_value, &code_samples)?;
    }

    // Determine output path
    let output_path = cli.output.unwrap_or_else(|| PathBuf::from("meilisearch.json"));

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

/// Fetch and parse code samples from the documentation repository
fn fetch_code_samples() -> Result<HashMap<String, String>> {
    let response = reqwest::blocking::get(CODE_SAMPLES_URL)
        .context("Failed to fetch code samples")?
        .text()
        .context("Failed to read code samples response")?;

    parse_code_samples_yaml(&response)
}

/// Parse the code samples YAML file
/// The format is:
/// ```yaml
/// # key_name
/// some_id: |-
///   curl \
///     -X GET 'URL'
/// ```
/// We extract the comment as the key and everything after `|-` as the value
fn parse_code_samples_yaml(content: &str) -> Result<HashMap<String, String>> {
    let mut samples: HashMap<String, String> = HashMap::new();
    let mut current_key: Option<String> = None;
    let mut current_value: Vec<String> = Vec::new();
    let mut in_code_block = false;
    let mut base_indent: Option<usize> = None;

    // Regex to match the start of a code sample (key: |-)
    let code_start_re = Regex::new(r"^[a-zA-Z0-9_]+:\s*\|-\s*$")?;
    // Regex to match comment lines that define keys
    let comment_re = Regex::new(r"^#\s*([a-zA-Z0-9_]+)\s*$")?;

    for line in content.lines() {
        // Check if this is a comment line defining a new key
        if let Some(caps) = comment_re.captures(line) {
            // Save previous sample if exists
            if let Some(key) = current_key.take() {
                if !current_value.is_empty() {
                    samples.insert(key, current_value.join("\n").trim_end().to_string());
                }
            }
            current_key = Some(caps[1].to_string());
            current_value.clear();
            in_code_block = false;
            base_indent = None;
            continue;
        }

        // Check if this starts a new code block
        if code_start_re.is_match(line) {
            in_code_block = true;
            base_indent = None;
            continue;
        }

        // If we're in a code block and have a current key, collect the content
        if in_code_block && current_key.is_some() {
            // Empty line or line with only whitespace
            if line.trim().is_empty() {
                // Only add empty lines if we've already started collecting
                if !current_value.is_empty() {
                    current_value.push(String::new());
                }
                continue;
            }

            // Calculate indentation
            let indent = line.len() - line.trim_start().len();

            // Set base indent from first non-empty line
            if base_indent.is_none() {
                base_indent = Some(indent);
            }

            // If line has less indentation than base, we've exited the block
            if indent < base_indent.unwrap_or(0) && !line.trim().is_empty() {
                in_code_block = false;
                continue;
            }

            // Remove base indentation and add to value
            let base = base_indent.unwrap_or(0);
            let dedented = if line.len() > base { &line[base..] } else { line.trim_start() };
            current_value.push(dedented.to_string());
        }
    }

    // Don't forget the last sample
    if let Some(key) = current_key {
        if !current_value.is_empty() {
            samples.insert(key, current_value.join("\n").trim_end().to_string());
        }
    }

    Ok(samples)
}

/// Convert an OpenAPI path to a code sample key
/// Path: /indexes/{index_uid}/documents/{document_id}
/// Method: GET
/// Key: get_indexes_indexUid_documents_documentId
fn path_to_key(path: &str, method: &str) -> String {
    let method_lower = method.to_lowercase();

    // Remove leading slash and convert path
    let path_part = path
        .trim_start_matches('/')
        .split('/')
        .map(|segment| {
            if segment.starts_with('{') && segment.ends_with('}') {
                // Convert {param_name} to camelCase
                let param = &segment[1..segment.len() - 1];
                to_camel_case(param)
            } else {
                // Keep path segments as-is, but replace hyphens with underscores
                segment.replace('-', "_")
            }
        })
        .collect::<Vec<_>>()
        .join("_");

    if path_part.is_empty() {
        method_lower
    } else {
        format!("{}_{}", method_lower, path_part)
    }
}

/// Convert snake_case to camelCase
fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = false;

    for (i, c) in s.chars().enumerate() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else if i == 0 {
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}

/// Add code samples to the OpenAPI specification
fn add_code_samples_to_openapi(
    openapi: &mut Value,
    code_samples: &HashMap<String, String>,
) -> Result<()> {
    let paths = openapi
        .get_mut("paths")
        .and_then(|p| p.as_object_mut())
        .context("OpenAPI spec missing 'paths' object")?;

    for (path, path_item) in paths.iter_mut() {
        let path_item = match path_item.as_object_mut() {
            Some(p) => p,
            None => continue,
        };

        let methods = ["get", "post", "put", "patch", "delete"];

        for method in methods {
            if let Some(operation) = path_item.get_mut(method) {
                let key = path_to_key(path, method);

                if let Some(sample) = code_samples.get(&key) {
                    // Create x-codeSamples array according to Redocly spec
                    let code_sample = json!([{
                        "lang": "cURL",
                        "source": sample
                    }]);

                    operation
                        .as_object_mut()
                        .map(|op| op.insert("x-codeSamples".to_string(), code_sample));

                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_to_key() {
        assert_eq!(path_to_key("/indexes", "GET"), "get_indexes");
        assert_eq!(path_to_key("/indexes/{index_uid}", "GET"), "get_indexes_indexUid");
        assert_eq!(
            path_to_key("/indexes/{index_uid}/documents", "POST"),
            "post_indexes_indexUid_documents"
        );
        assert_eq!(
            path_to_key("/indexes/{index_uid}/documents/{document_id}", "GET"),
            "get_indexes_indexUid_documents_documentId"
        );
        assert_eq!(path_to_key("/indexes/{index_uid}/settings/stop-words", "GET"), "get_indexes_indexUid_settings_stop_words");
    }

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("index_uid"), "indexUid");
        assert_eq!(to_camel_case("document_id"), "documentId");
        assert_eq!(to_camel_case("task_uid"), "taskUid");
    }

    #[test]
    fn test_parse_code_samples() {
        let yaml = r#"
# get_indexes
list_all_indexes_1: |-
  curl \
    -X GET 'MEILISEARCH_URL/indexes'
# post_indexes
create_an_index_1: |-
  curl \
    -X POST 'MEILISEARCH_URL/indexes' \
    -H 'Content-Type: application/json' \
    --data-binary '{
      "uid": "movies"
    }'
"#;
        let samples = parse_code_samples_yaml(yaml).unwrap();

        assert_eq!(samples.len(), 2);
        assert!(samples.contains_key("get_indexes"));
        assert!(samples.contains_key("post_indexes"));
        assert!(samples["get_indexes"].contains("curl"));
        assert!(samples["post_indexes"].contains("POST"));
    }
}
