use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use meilisearch::routes::MeilisearchApi;
use serde_json::{json, Value};
use utoipa::OpenApi;

const HTTP_METHODS: &[&str] = &["get", "post", "put", "patch", "delete"];

/// Language used in the documentation repository (contains the key mapping)
const DOCS_LANG: &str = "cURL";

/// Mapping of repository URLs to language names.
/// The "cURL" entry is special: it contains the key mapping used to resolve sample IDs for all SDKs.
const CODE_SAMPLES: &[(&str, &str)] = &[
    ("https://raw.githubusercontent.com/meilisearch/documentation/refs/heads/main/.code-samples.meilisearch.yaml", "cURL"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-dotnet/refs/heads/main/.code-samples.meilisearch.yaml", "C#"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-dart/refs/heads/main/.code-samples.meilisearch.yaml", "Dart"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-go/refs/heads/main/.code-samples.meilisearch.yaml", "Go"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-java/refs/heads/main/.code-samples.meilisearch.yaml", "Java"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-js/refs/heads/main/.code-samples.meilisearch.yaml", "JS"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-php/refs/heads/main/.code-samples.meilisearch.yaml", "PHP"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-python/refs/heads/main/.code-samples.meilisearch.yaml", "Python"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-ruby/refs/heads/main/.code-samples.meilisearch.yaml", "Ruby"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-rust/refs/heads/main/.code-samples.meilisearch.yaml", "Rust"),
    ("https://raw.githubusercontent.com/meilisearch/meilisearch-swift/refs/heads/main/.code-samples.meilisearch.yaml", "Swift"),
];


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

    /// Include Mintlify code samples from SDK repositories
    #[arg(long)]
    with_mintlify_code_samples: bool,

    /// Debug mode: display the mapping table and code samples
    #[arg(long)]
    debug: bool,

    /// Check that all routes have a summary (useful for CI)
    #[arg(long)]
    check_summaries: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Generate the OpenAPI specification
    let openapi = MeilisearchApi::openapi();

    // Convert to serde_json::Value for modification
    let mut openapi_value: Value = serde_json::to_value(&openapi)?;

    // Fetch and add code samples if enabled
    if cli.with_mintlify_code_samples {
        let code_samples = fetch_all_code_samples(cli.debug)?;
        add_code_samples_to_openapi(&mut openapi_value, &code_samples, cli.debug)?;
    }

    // Clean up null descriptions in tags
    clean_null_descriptions(&mut openapi_value);

    // Check that all routes have summaries if requested
    if cli.check_summaries {
        check_all_routes_have_summaries(&openapi_value)?;
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

/// Code sample for a specific language
#[derive(Debug, Clone)]
struct CodeSample {
    lang: String,
    source: String,
}

/// Fetch and parse code samples from all repositories
/// Returns a map from OpenAPI key (e.g., "get_indexes") to a list of code samples for different languages
fn fetch_all_code_samples(debug: bool) -> Result<HashMap<String, Vec<CodeSample>>> {
    // First, fetch the documentation file to get the OpenAPI key -> code sample ID mapping
    let (docs_url, _) = CODE_SAMPLES
        .iter()
        .find(|(_, lang)| *lang == DOCS_LANG)
        .context("Documentation source not found in CODE_SAMPLES")?;

    let docs_content = reqwest::blocking::get(*docs_url)
        .context("Failed to fetch documentation code samples")?
        .text()
        .context("Failed to read documentation code samples response")?;

    // Build mapping from OpenAPI key to code sample ID (only first match per key)
    let openapi_key_to_sample_id = build_openapi_key_mapping(&docs_content);

    // Build final result
    let mut all_samples: HashMap<String, Vec<CodeSample>> = HashMap::new();

    // Loop through all CODE_SAMPLES files
    for (url, lang) in CODE_SAMPLES {
        // Fetch content (reuse docs_content for documentation)
        let content: Cow<'_, str> = if *lang == DOCS_LANG {
            Cow::Borrowed(&docs_content)
        } else {
            match reqwest::blocking::get(*url).and_then(|r| r.text()) {
                Ok(text) => Cow::Owned(text),
                Err(e) => {
                    eprintln!("Warning: Failed to fetch code samples for {}: {}", lang, e);
                    continue;
                }
            }
        };

        // Parse all code samples from this file
        let sample_id_to_code = parse_code_samples_from_file(&content);

        // Add to result using the mapping
        for (openapi_key, sample_id) in &openapi_key_to_sample_id {
            if let Some(source) = sample_id_to_code.get(sample_id) {
                all_samples.entry(openapi_key.clone()).or_default().push(CodeSample {
                    lang: lang.to_string(),
                    source: source.clone(),
                });
            }
        }
    }

    // Debug mode: display mapping table and code samples
    if debug {
        println!("\n=== OpenAPI Key to Sample ID Mapping ===\n");
        let mut keys: Vec<_> = openapi_key_to_sample_id.keys().collect();
        keys.sort();
        for key in keys {
            println!("  {} -> {}", key, openapi_key_to_sample_id[key]);
        }

        println!("\n=== Code Samples ===\n");
        let mut sample_keys: Vec<_> = all_samples.keys().collect();
        sample_keys.sort();
        for key in sample_keys {
            let samples = &all_samples[key];
            let langs: Vec<_> = samples.iter().map(|s| s.lang.as_str()).collect();
            println!("  {} -> {}", key, langs.join(", "));
        }
        println!();
    }

    Ok(all_samples)
}

/// Build a mapping from OpenAPI key to code sample ID from the documentation file.
///
/// The OpenAPI key is found on a line starting with `# ` (hash + space), containing a single word
/// that starts with an HTTP method followed by an underscore (e.g., `# get_indexes`).
/// The code sample ID is the first word of the next line.
/// Only keeps the first code sample ID per OpenAPI key.
///
/// Example input:
/// ```yaml
/// # get_indexes
/// get_indexes_1: |-
///   curl \
///     -X GET 'MEILISEARCH_URL/indexes'
/// get_indexes_2: |-
///   curl \
///     -X GET 'MEILISEARCH_URL/indexes?limit=5'
/// # post_indexes
/// create_indexes_1: |-
///   curl \
///     -X POST 'MEILISEARCH_URL/indexes'
/// ```
///
/// This produces: {"get_indexes": "get_indexes_1", "post_indexes": "create_indexes_1"}
fn build_openapi_key_mapping(content: &str) -> HashMap<String, String> {
    let mut mapping: HashMap<String, String> = HashMap::new();
    let lines: Vec<&str> = content.lines().collect();

    for i in 0..lines.len() {
        let line = lines[i];

        // Check if line starts with "# " and contains exactly one word
        let Some(rest) = line.strip_prefix("# ") else {
            continue;
        };

        let word = rest.trim();

        // Must be a single word (no spaces)
        if word.contains(' ') {
            continue;
        }

        // Must start with an HTTP method followed by underscore
        let starts_with_http_method =
            HTTP_METHODS.iter().any(|method| word.starts_with(&format!("{}_", method)));

        if !starts_with_http_method {
            continue;
        }

        let openapi_key = word.to_string();

        // Only keep first match per key
        if mapping.contains_key(&openapi_key) {
            continue;
        }

        // Get the code sample ID from the next line (first word before `:`)
        if i + 1 < lines.len() {
            let next_line = lines[i + 1];
            if let Some(sample_id) = next_line.split(':').next() {
                let sample_id = sample_id.trim();
                if !sample_id.is_empty() {
                    mapping.insert(openapi_key, sample_id.to_string());
                }
            }
        }
    }

    mapping
}

/// Parse all code samples from a file.
///
/// A code sample ID is found when a line contains `: |-`.
/// The code sample value is everything between `: |-` and:
/// - The next code sample (next line containing `: |-`)
/// - OR a line starting with `#` at column 0 (indented `#` is part of the code sample)
/// - OR the end of file
///
/// Example input:
/// ```yaml
/// get_indexes_1: |-
///   client.getIndexes()
///   # I write something
/// # COMMENT TO IGNORE
/// get_indexes_2: |-
///   client.getIndexes({ limit: 3 })
/// ```
///
/// This produces:
/// - get_indexes_1 -> "client.getIndexes()\n# I write something"
/// - get_indexes_2 -> "client.getIndexes({ limit: 3 })"
fn parse_code_samples_from_file(content: &str) -> HashMap<String, String> {
    let mut samples: HashMap<String, String> = HashMap::new();
    let mut current_sample_id: Option<String> = None;
    let mut current_lines: Vec<String> = Vec::new();
    let mut base_indent: Option<usize> = None;

    for line in content.lines() {
        // Check if this line starts a new code sample (contains `: |-`)
        if line.contains(": |-") {
            // Save previous sample if exists
            if let Some(sample_id) = current_sample_id.take() {
                let value = current_lines.join("\n").trim_end().to_string();
                samples.insert(sample_id, value);
            }
            current_lines.clear();
            base_indent = None;

            // Extract sample ID (first word before `:`)
            if let Some(id) = line.split(':').next() {
                current_sample_id = Some(id.trim().to_string());
            }
            continue;
        }

        // Check if this line ends the current code sample (line starts with `#` at column 0)
        // Indented `#` (spaces or tabs) is part of the code sample
        if line.starts_with('#') {
            // Save current sample and reset
            if let Some(sample_id) = current_sample_id.take() {
                let value = current_lines.join("\n").trim_end().to_string();
                samples.insert(sample_id, value);
            }
            current_lines.clear();
            base_indent = None;
            continue;
        }

        // If we're in a code sample, add this line to the value
        if current_sample_id.is_some() {
            // Handle empty lines
            if line.trim().is_empty() {
                if !current_lines.is_empty() {
                    current_lines.push(String::new());
                }
                continue;
            }

            // Calculate indentation and strip base indent
            let indent = line.len() - line.trim_start().len();
            let base = *base_indent.get_or_insert(indent);

            // Remove base indentation
            let dedented = line.get(base..).unwrap_or_else(|| line.trim_start());
            current_lines.push(dedented.to_string());
        }
    }

    // Don't forget the last sample
    if let Some(sample_id) = current_sample_id {
        let value = current_lines.join("\n").trim_end().to_string();
        samples.insert(sample_id, value);
    }

    samples
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
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = false;

    for (i, c) in s.chars().enumerate() {
        match c {
            '_' => capitalize_next = true,
            _ if capitalize_next => {
                result.push(c.to_ascii_uppercase());
                capitalize_next = false;
            }
            _ if i == 0 => result.push(c.to_ascii_lowercase()),
            _ => result.push(c),
        }
    }

    result
}

/// Add code samples to the OpenAPI specification
fn add_code_samples_to_openapi(
    openapi: &mut Value,
    code_samples: &HashMap<String, Vec<CodeSample>>,
    debug: bool,
) -> Result<()> {
    let paths = openapi
        .get_mut("paths")
        .and_then(|p| p.as_object_mut())
        .context("OpenAPI spec missing 'paths' object")?;

    let mut routes_with_samples: Vec<String> = Vec::new();
    let mut routes_without_samples: Vec<String> = Vec::new();

    // Collect all routes first for sorted debug output
    let mut all_routes: Vec<(String, String, String)> = Vec::new(); // (path, method, key)

    for (path, path_item) in paths.iter_mut() {
        let Some(path_item) = path_item.as_object_mut() else {
            continue;
        };

        for method in HTTP_METHODS {
            let Some(operation) = path_item.get_mut(*method) else {
                continue;
            };

            let key = path_to_key(path, method);
            all_routes.push((path.clone(), method.to_string(), key.clone()));

            if let Some(samples) = code_samples.get(&key) {
                routes_with_samples.push(key);

                // Create x-codeSamples array according to Redocly spec
                // Sort by language name for consistent output
                let mut sorted_samples = samples.clone();
                sorted_samples.sort_by(|a, b| a.lang.cmp(&b.lang));

                let code_sample_array: Vec<Value> = sorted_samples
                    .iter()
                    .map(|sample| {
                        json!({
                            "lang": sample.lang,
                            "source": sample.source
                        })
                    })
                    .collect();

                if let Some(op) = operation.as_object_mut() {
                    op.insert("x-codeSamples".to_string(), json!(code_sample_array));
                }
            } else {
                routes_without_samples.push(key);
            }
        }
    }

    // Debug output
    if debug {
        routes_without_samples.sort();

        if !routes_without_samples.is_empty() {
            println!("=== Routes without code samples ===\n");
            for key in &routes_without_samples {
                println!("  {}", key);
            }
        }

        let total = all_routes.len();
        let with_samples = routes_with_samples.len();
        let without_samples = routes_without_samples.len();
        let percentage = if total > 0 { (with_samples as f64 / total as f64) * 100.0 } else { 0.0 };

        println!("\n=== Summary ===\n");
        println!("  Total routes: {}", total);
        println!("  With code samples: {} ({:.1}%)", with_samples, percentage);
        println!("  Missing code samples: {} ({:.1}%)\n", without_samples, 100.0 - percentage);
    }

    Ok(())
}

/// Clean up null descriptions in tags to make Mintlify work
/// Removes any "description" fields with null values (both JSON null and "null" string)
/// from the tags array and all nested objects
fn clean_null_descriptions(openapi: &mut Value) {
    if let Some(tags) = openapi.get_mut("tags").and_then(|t| t.as_array_mut()) {
        for tag in tags.iter_mut() {
            remove_null_descriptions_recursive(tag);
        }
    }
}

/// Recursively remove all "description" fields that are null or "null" string
fn remove_null_descriptions_recursive(value: &mut Value) {
    if let Some(obj) = value.as_object_mut() {
        // Check and remove description if it's null or "null" string
        if let Some(desc) = obj.get("description") {
            if desc.is_null() || (desc.is_string() && desc.as_str() == Some("null")) {
                obj.remove("description");
            }
        }

        // Recursively process all nested objects
        for (_, v) in obj.iter_mut() {
            remove_null_descriptions_recursive(v);
        }
    } else if let Some(arr) = value.as_array_mut() {
        // Recursively process arrays
        for item in arr.iter_mut() {
            remove_null_descriptions_recursive(item);
        }
    }
}

/// Check that all routes have a summary field.
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

            let has_summary = operation
                .get("summary")
                .and_then(|s| s.as_str())
                .is_some_and(|s| !s.is_empty());

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
        eprintln!(
            "\nTo fix this, add a doc-comment (///) above the route handler function."
        );
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
        assert_eq!(
            path_to_key("/indexes/{index_uid}/settings/stop-words", "GET"),
            "get_indexes_indexUid_settings_stop_words"
        );
    }

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("index_uid"), "indexUid");
        assert_eq!(to_camel_case("document_id"), "documentId");
        assert_eq!(to_camel_case("task_uid"), "taskUid");
    }

    #[test]
    fn test_build_openapi_key_mapping() {
        let yaml = r#"
# get_indexes
get_indexes_1: |-
  curl \
    -X GET 'MEILISEARCH_URL/indexes'
get_indexes_2: |-
  curl \
    -X GET 'MEILISEARCH_URL/indexes?limit=5'
# post_indexes
create_indexes_1: |-
  curl \
    -X POST 'MEILISEARCH_URL/indexes'
# get_version
get_version_1: |-
  curl \
    -X GET 'MEILISEARCH_URL/version'
# COMMENT WITHOUT KEY - SHOULD BE IGNORED
## COMMENT WITHOUT KEY - SHOULD BE IGNORED
unrelated_sample_without_comment: |-
  curl \
    -X GET 'MEILISEARCH_URL/something'
"#;
        let mapping = build_openapi_key_mapping(yaml);

        // Should have 3 OpenAPI keys
        assert_eq!(mapping.len(), 3);
        assert!(mapping.contains_key("get_indexes"));
        assert!(mapping.contains_key("post_indexes"));
        assert!(mapping.contains_key("get_version"));

        // Only keeps the first code sample ID per OpenAPI key
        assert_eq!(mapping["get_indexes"], "get_indexes_1");
        assert_eq!(mapping["post_indexes"], "create_indexes_1");
        assert_eq!(mapping["get_version"], "get_version_1");

        // Comments with multiple words or ## should be ignored and not create keys
        assert!(!mapping.contains_key("COMMENT"));
        assert!(!mapping.contains_key("##"));
    }

    #[test]
    fn test_parse_code_samples_from_file() {
        let yaml = r#"
get_indexes_1: |-
  client.getIndexes()
  # I write something
# COMMENT TO IGNORE
get_indexes_2: |-
  client.getIndexes({ limit: 3 })
update_document: |-
  // Code with blank line

  updateDoc(doc)
  // End

delete_document_1: |-
  client.deleteDocument(1)
no_newline_at_end: |-
  client.update({ id: 1 })
key_with_empty_sample: |-
# This should produce an empty string for the sample
complex_block: |-
  // Some code
    Indented line
    # Indented comment
  Last line
"#;
        let samples = parse_code_samples_from_file(yaml);

        assert_eq!(samples.len(), 7);
        assert!(samples.contains_key("get_indexes_1"));
        assert!(samples.contains_key("get_indexes_2"));
        assert!(samples.contains_key("update_document"));
        assert!(samples.contains_key("delete_document_1"));
        assert!(samples.contains_key("no_newline_at_end"));
        assert!(samples.contains_key("key_with_empty_sample"));
        assert!(samples.contains_key("complex_block"));

        // get_indexes_1 includes indented comment
        assert_eq!(samples["get_indexes_1"], "client.getIndexes()\n# I write something");

        // get_indexes_2 is a single line
        assert_eq!(samples["get_indexes_2"], "client.getIndexes({ limit: 3 })");

        // update_document contains a blank line and some code
        assert_eq!(
            samples["update_document"],
            "// Code with blank line\n\nupdateDoc(doc)\n// End"
        );

        // delete_document_1
        assert_eq!(samples["delete_document_1"], "client.deleteDocument(1)");

        // no_newline_at_end, explicitly just one line
        assert_eq!(samples["no_newline_at_end"], "client.update({ id: 1 })");

        // key_with_empty_sample should be empty string
        assert_eq!(samples["key_with_empty_sample"], "");

        // complex_block preserves indentation and comments
        assert_eq!(
            samples["complex_block"],
            "// Some code\n  Indented line\n  # Indented comment\nLast line"
        );
    }

    #[test]
    fn test_clean_null_descriptions() {
        let mut openapi = json!({
            "tags": [
                {
                    "name": "Test1",
                    "description": "null"
                },
                {
                    "name": "Test2",
                    "description": null
                },
                {
                    "name": "Test3",
                    "description": "Valid description"
                },
                {
                    "name": "Test4",
                    "description": "null",
                    "externalDocs": {
                        "url": "https://example.com",
                        "description": null
                    }
                },
                {
                    "name": "Test5",
                    "externalDocs": {
                        "url": "https://example.com",
                        "description": "null"
                    }
                }
            ]
        });

        clean_null_descriptions(&mut openapi);

        let tags = openapi["tags"].as_array().unwrap();

        // Test1: description "null" should be removed
        assert!(!tags[0].as_object().unwrap().contains_key("description"));

        // Test2: description null should be removed
        assert!(!tags[1].as_object().unwrap().contains_key("description"));

        // Test3: valid description should remain
        assert_eq!(tags[2]["description"], "Valid description");

        // Test4: both tag description and externalDocs description should be removed
        assert!(!tags[3].as_object().unwrap().contains_key("description"));
        assert!(!tags[3]["externalDocs"]
            .as_object()
            .unwrap()
            .contains_key("description"));
        assert_eq!(tags[3]["externalDocs"]["url"], "https://example.com");

        // Test5: externalDocs description "null" should be removed
        assert!(!tags[4]["externalDocs"]
            .as_object()
            .unwrap()
            .contains_key("description"));
        assert_eq!(tags[4]["externalDocs"]["url"], "https://example.com");
    }
}
