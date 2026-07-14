# Concepts

Sourcey renders the OpenAPI spec into:

- One HTML page per top-level tag (group) in the spec, plus a single index page
- A landing page with the full operation index
- Code samples in cURL, JavaScript, TypeScript, Python, Go, Ruby, Rust, Java, PHP, and C#
- llms.txt and llms-full.txt exports for LLM consumption
- A search index over all rendered pages

For a multi-page site, the `openapi` adapter is paired with at least one `markdown` adapter (or another `openapi` adapter) via the `navigation.tabs` configuration.
