#!/usr/bin/env bash
set -eu -o pipefail

check_tag() {
    local expected=$1
    local actual=$2
    local filename=$3

    if [[ $actual != $expected ]]; then
        echo >&2 "Error: the current tag does not match the version in $filename: found $actual, expected $expected"
        return 1
    fi
}

read_version() {
    grep '^version = ' | cut -d \" -f 2
}

if [[ -z "${GITHUB_REF:-}" ]]; then
    echo >&2 "Error: GITHUB_REF is not set"
    exit 1
fi

if [[ ! "$GITHUB_REF" =~ ^refs/tags/v[0-9]+\.[0-9]+\.[0-9]+(-[a-z0-9]+)?$ ]]; then
    echo >&2 "Error: GITHUB_REF is not a valid tag: $GITHUB_REF"
    exit 1
fi

current_tag=${GITHUB_REF#refs/tags/v}
ret=0

toml_tag="$(cat Cargo.toml | read_version)"
check_tag "$current_tag" "$toml_tag" Cargo.toml || ret=1

lock_tag=$(grep -A 1 '^name = "meilisearch-auth"' Cargo.lock | read_version)
check_tag "$current_tag" "$lock_tag" Cargo.lock || ret=1

if (( ret == 0 )); then
    echo 'OK'
fi
exit $ret
