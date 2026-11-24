#!/usr/bin/env bash
set -eu -o pipefail

# Used in our CIs to know if a stable release should move the `latest`, `vX` and `vX.Y` tags (Docker and git).
# Relies only on the semver order of the git tags, so it doesn't depend on when the CIs run.
#
# Usage: is-latest-release.sh vX.Y.Z
# Outputs, in the `$GITHUB_OUTPUT` format:
# - latest=true|false: the tag is the highest stable version
# - latest_major=true|false: the tag is the highest stable version of its major
# - latest_minor=true|false: the tag is the highest stable version of its minor

current_tag=$1
major=${current_tag%%.*}
minor=${current_tag%.*}

# Listing the remote tags works whatever the checkout configuration (shallow clone, no tags fetched...)
stable_tags=$(git ls-remote --tags --refs origin | cut -f 2 | sed 's|^refs/tags/||' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V)

# Outputs true if the current tag is the highest stable tag starting with the given prefix
is_highest() {
    [[ $current_tag == "$(grep "^$1" <<< "$stable_tags" | tail -n 1)" ]] && echo true || echo false
}

echo "latest=$(is_highest v)"
echo "latest_major=$(is_highest "$major\.")"
echo "latest_minor=$(is_highest "$minor\.")"
