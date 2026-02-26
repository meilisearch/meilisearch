# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Meilisearch is a search engine written in Rust. It exposes a RESTful HTTP API (Actix-web) for search, document management, and index configuration. The core search/indexing engine lives in the `milli` crate. Tokenization is handled externally by the `charabia` library.

## Build & Run Commands

```bash
# Build (dev)
cargo build --locked

# Build (release, recommended for testing performance)
cargo build --release --locked

# Run
cargo run --release

# Run tests (all crates)
cargo test --locked

# Run tests for a specific crate
cargo test --locked -p milli
cargo test --locked -p meilisearch

# Run a single test by name
cargo test --locked -p meilisearch test_name

# Format check
cargo fmt --all -- --check

# Lint
cargo clippy --all-targets -- --deny warnings -D clippy::todo

# Build without default features (CI check)
cargo build --locked --no-default-features --all

# xtask automation tools
cargo xtask --help

# Test with all features (excluding cuda/ollama)
cargo test --workspace --locked --features "$(cargo xtask list-features --exclude-feature cuda,test-ollama)"

# Declarative workload tests
cargo xtask test workloads/tests/*.json

# Generate OpenAPI spec
cd crates/openapi-generator && cargo run --release -- --pretty
```

## Faster Builds

```bash
export LINDERA_CACHE=$HOME/.cache/meili/lindera           # cache tokenizer build artifacts
export MILLI_BENCH_DATASETS_PATH=$HOME/.cache/meili/benches # cache benchmark datasets
export MEILI_NO_VERGEN=1                                    # skip version rebuild (dev only)
```

If you get "Too many open files": `ulimit -Sn 3000`

## Workspace Architecture

20-crate Cargo workspace. Key crates and their relationships:

```
meilisearch (HTTP server, routes, extractors, middleware)
├── meilisearch-types (shared types, settings, task definitions)
│   └── milli (core search engine: indexing, search, ranking, vectors)
│       ├── filter-parser (filter query language parser)
│       ├── flatten-serde-json (JSON flattening)
│       └── json-depth-checker (JSON validation)
├── index-scheduler (async task queue for all index mutations)
├── meilisearch-auth (API key management, multi-tenancy)
├── dump (database dump/restore)
├── file-store (file storage abstraction)
└── http-client (internal HTTP client)
```

Supporting crates: `meilitool` (CLI utility), `meili-snap` (snapshot test helpers), `build-info`, `openapi-generator`, `tracing-trace`, `xtask`, `benchmarks`, `fuzzers`.

External vendored crates in `external-crates/`: `async-openai`, `async-openai-macros`.

### Data Flow

HTTP requests → Actix-web routes (`crates/meilisearch/src/routes/`) → auth check → route handlers → `IndexScheduler` (task queue for mutations) or direct search via `milli` → Heed/LMDB storage.

### Key Modules in `meilisearch`

- `routes/` — HTTP endpoint handlers (indexes, search, documents, settings, chats, etc.)
- `search/` — search queue and request handling
- `extractors/` — request parameter extraction
- `option.rs` — CLI options and configuration

### Key Modules in `milli`

- `index.rs` — main Index interface
- `search/` — search pipeline (full-text, hybrid, vector, faceted)
- `update/` — index mutation operations
- `documents/` — document indexing/storage
- `vector/` — embedding/vector handling

## Testing Conventions

- Use `insta` for snapshot-based testing (preferred over manual assertions)
- Set `MEILI_TEST_FULL_SNAPS=true` to see full snapshots instead of hashes
- The `meili-snap` crate provides custom snapshot macros on top of insta
- Integration tests are in `crates/meilisearch/tests/` organized by feature area
- Enterprise features are gated behind `--features enterprise`

## Code Style

- Rust toolchain: 1.91.1
- Rustfmt: `imports_granularity = "Module"`, `group_imports = "StdExternalCrate"`, `use_small_heuristics = "max"`
- Clippy: `--deny warnings -D clippy::todo`; `tar::Archive::unpack` is disallowed (use `ArchiveExt::safe_unpack`)
- Logging: use `tracing` with structured fields (no string interpolation). Profiling spans use targets `indexing::` or `search::` at TRACE level.
- Commit messages: capitalized, imperative verb, no trailing punctuation

## Feature Flags

- `mini-dashboard` — embedded web dashboard (default)
- `enterprise` — enterprise features (sharding, S3 snapshots)
- `swagger` — enables Scalar API docs at `/scalar`
- Language tokenization features: `chinese`, `japanese`, `hebrew`, `thai`, etc.

## CI Environment

CI sets `RUSTFLAGS="-D warnings"` and `RUST_BACKTRACE=1`. Tests run on Linux (x86_64 + ARM), Windows, and macOS.
