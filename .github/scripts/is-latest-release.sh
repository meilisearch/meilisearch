#!/bin/sh

# Used in our CIs to publish the latest Docker image.

# Checks if the current tag ($GITHUB_REF) corresponds to the latest release tag on GitHub
# Returns "true" or "false" (as a string).

GITHUB_API='https://api.github.com/repos/meilisearch/meilisearch/releases'
PNAME='meilisearch'

# FUNCTIONS

# Returns the version of the latest stable version of Meilisearch by setting the $latest variable.
get_latest() {
    # temp_file is needed because the grep would start before the download is over
    temp_file=$(mktemp -q /tmp/$PNAME.XXXXXXXXX)
    latest_release="$GITHUB_API/latest"

    if [ $? -ne 0 ]; then
        echo "$0: Can't create temp file."
        exit 1
    fi

    if [ -z "$GITHUB_PAT" ]; then
        curl -s "$latest_release" > "$temp_file" || return 1
    else
        curl -H "Authorization: token $GITHUB_PAT" -s "$latest_release" > "$temp_file" || return 1
    fi

    latest="$(cat "$temp_file" | grep '"tag_name":' | cut -d ':' -f2 | tr -d '"' | tr -d ',' | tr -d ' ')"

    rm -f "$temp_file"
    return 0
}

# MAIN
current_tag="$(echo $GITHUB_REF | tr -d 'refs/tags/')"
get_latest

if [ "$current_tag" != "$latest" ]; then
    # The current release tag is not the latest
    echo "false"
else
    # The current release tag is the latest
    echo "true"
fi

exit 0
