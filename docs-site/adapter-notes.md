# Adapter notes

The Sourcey `openapi` adapter reads the spec and produces:

- An index page at `api.html` listing every operation
- One page per OpenAPI tag (group) where supported
- A `search-index.json` covering all operations and their parameters, responses, and code samples
- `llms.txt` and `llms-full.txt` exports

The `markdown` adapter reads `*.md` files referenced by `groups[].pages[]` and renders them as multi-page documentation with TOC, navigation, and dark-mode support.

Both adapters use the same theme and code-sample language configuration, so the rendered site is internally consistent.
