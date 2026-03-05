#!/usr/bin/env bash
set -euo pipefail

# Cluster scenario test runner.
# Usage:
#   ./tests/cluster_scenarios/run.sh                  # Build binary + run tests
#   MEILI_BINARY=path/to/meilisearch ./run.sh         # Use pre-built binary
#   ./tests/cluster_scenarios/run.sh -k test_create   # Pass args to pytest

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

echo "=== Meilisearch Cluster Scenario Tests ==="
echo "Project root: $PROJECT_ROOT"

# --- Build binary if not provided ---
if [ -z "${MEILI_BINARY:-}" ]; then
    echo ""
    echo "--- Building meilisearch with cluster feature ---"
    cargo build --features cluster -p meilisearch --manifest-path "$PROJECT_ROOT/Cargo.toml"
    export MEILI_BINARY="$PROJECT_ROOT/target/debug/meilisearch"
    echo "Binary: $MEILI_BINARY"
fi

if [ ! -f "$MEILI_BINARY" ]; then
    echo "ERROR: Binary not found at $MEILI_BINARY"
    exit 1
fi

# --- Setup Python venv ---
if [ ! -d "$VENV_DIR" ]; then
    echo ""
    echo "--- Creating Python venv ---"
    python3 -m venv "$VENV_DIR"
fi

echo ""
echo "--- Installing Python dependencies ---"
"$VENV_DIR/bin/pip" install -q -r "$SCRIPT_DIR/requirements.txt"

# --- Run tests ---
echo ""
echo "--- Running cluster scenario tests ---"
"$VENV_DIR/bin/pytest" "$SCRIPT_DIR" -v --tb=short "$@"
