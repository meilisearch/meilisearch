# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Meilisearch is a lightning-fast search engine written in Rust. It's organized as a Rust workspace with multiple crates that handle different aspects of the search engine functionality.

## Architecture

### Core Crates Structure

- **`crates/meilisearch/`** - Main HTTP server implementing the REST API with Actix Web
- **`crates/milli/`** - Core search engine library (indexing, search algorithms, ranking)
- **`crates/index-scheduler/`** - Task scheduling, batching, and index lifecycle management
- **`crates/meilisearch-auth/`** - Authentication and API key management
- **`crates/meilisearch-types/`** - Shared types and data structures
- **`crates/dump/`** - Database dump and restore functionality
- **`crates/meilitool/`** - CLI tool for maintenance operations

### Key Architectural Patterns

1. **Data Flow**:
   - Write: HTTP Request → Task Creation → Index Scheduler → Milli Engine → LMDB Storage
   - Read: HTTP Request → Search Queue → Milli Engine → Response

2. **Concurrency Model**:
   - Single writer, multiple readers for index operations
   - Task batching for improved throughput
   - Search queue for managing concurrent requests

3. **Storage**: LMDB (Lightning Memory-Mapped Database) with separate environments for tasks, auth, and indexes

## Development Commands

### Building and Running

```bash
# Development
cargo run

# Production build with optimizations
cargo run --release

# Build specific crates
cargo build --release -p meilisearch -p meilitool

# Build without default features
cargo build --locked --release --no-default-features --all
```

### Testing

```bash
# Run all tests
cargo test

# Run tests with release optimizations
cargo test --locked --release --all

# Run a specific test
cargo test test_name

# Run tests in a specific crate
cargo test -p milli
```

### Benchmarking

```bash
# List available features
cargo xtask list-features

# Run workload-based benchmarks
cargo xtask bench -- workloads/hackernews.json

# Run benchmarks without dashboard
cargo xtask bench --no-dashboard -- workloads/hackernews.json

# Run criterion benchmarks
cd crates/benchmarks && cargo bench
```

### Performance Optimizations

```bash
# Speed up builds with lindera cache
export LINDERA_CACHE=$HOME/.cache/lindera

# Prevent rebuilds on directory changes (development only)
export MEILI_NO_VERGEN=1

# Enable full snapshot creation for debugging tests
export MEILI_TEST_FULL_SNAPS=true
```

## Testing Strategy

- **Unit tests**: Colocated with source code using `#[cfg(test)]` modules
- **Integration tests**: Located in `crates/meilisearch/tests/`
- **Snapshot testing**: Using `insta` for deterministic testing
- **Test organization**: By feature (auth, documents, search, settings, index operations)

## Important Files and Directories

- `Cargo.toml` - Workspace configuration
- `rust-toolchain.toml` - Rust version (1.85.1)
- `crates/meilisearch/src/main.rs` - Server entry point
- `crates/milli/src/lib.rs` - Core engine entry point
- `crates/meilisearch-mcp/` - MCP server implementation
- `workloads/` - Benchmark workload definitions
- `assets/` - Static assets and demo files

## Feature Flags

Key features that can be enabled/disabled:
- Language-specific tokenizations (chinese, hebrew, japanese, thai, greek, khmer, vietnamese)
- `mini-dashboard` - Web UI for testing
- `metrics` - Prometheus metrics
- `vector-hnsw` - Vector search with CUDA support
- `mcp` - Model Context Protocol server for AI assistants

## Logging and Profiling

The codebase uses `tracing` for structured logging with these conventions:
- Regular logging spans
- Profiling spans (TRACE level, prefixed with `indexing::` or `search::`)
- Benchmarking spans

For indexing profiling, enable the `exportPuffinReports` experimental feature to generate `.puffin` files.

## Common Development Tasks

### Adding a New Route
1. Add route handler in `crates/meilisearch/src/routes/`
2. Update OpenAPI documentation if API changes
3. Add integration tests in `crates/meilisearch/tests/`
4. If MCP is enabled, routes are automatically exposed via MCP

### Modifying Index Operations
1. Core logic lives in `crates/milli/src/update/`
2. Task scheduling in `crates/index-scheduler/src/`
3. HTTP handlers in `crates/meilisearch/src/routes/indexes/`

### Working with Search
1. Search algorithms in `crates/milli/src/search/`
2. Query parsing in `crates/filter-parser/`
3. Search handlers in `crates/meilisearch/src/routes/indexes/search.rs`

### Working with MCP Server
1. MCP implementation in `crates/meilisearch-mcp/`
2. Tools are auto-generated from OpenAPI specification
3. Enable with `--features mcp` flag
4. Access via `/mcp` endpoint (SSE or POST)

## CI/CD and Git Workflow

- Main branch: `main`
- GitHub Merge Queue enforces rebasing and test passing
- Benchmarks run automatically on push to `main`
- Manual benchmark runs: comment `/bench workloads/*.json` on PRs

## Environment Variables

Key environment variables for development:
- `MEILI_NO_ANALYTICS` - Disable telemetry
- `MEILI_DB_PATH` - Database storage location
- `MEILI_HTTP_ADDR` - Server binding address
- `MEILI_MASTER_KEY` - Master API key for authentication