#!/bin/sh

# GLOBALS

# Colors
RED='\033[31m'
GREEN='\033[32m'
DEFAULT='\033[0m'

# Project name
PNAME='meilisearch'

# Version regexp i.e. v[number].[number].[number]
GREP_SEMVER_REGEXP='v\([0-9]*\)[.]\([0-9]*\)[.]\([0-9]*\)$'

# GitHub API address
GITHUB_API='https://api.github.com/repos/meilisearch/meilisearch/releases'
# GitHub Release address
GITHUB_REL='https://github.com/meilisearch/meilisearch/releases/download/'

# FUNCTIONS

# semverParseInto and semverLT from: https://github.com/cloudflare/semver_bash/blob/master/semver.sh
# usage: semverParseInto version major minor patch special
# version: the string version
# major, minor, patch, special: will be assigned by the function
semverParseInto() {
    local RE='[^0-9]*\([0-9]*\)[.]\([0-9]*\)[.]\([0-9]*\)\([0-9A-Za-z-]*\)'
    # MAJOR
    eval $2=`echo $1 | sed -e "s#$RE#\1#"`
    # MINOR
    eval $3=`echo $1 | sed -e "s#$RE#\2#"`
    # PATCH
    eval $4=`echo $1 | sed -e "s#$RE#\3#"`
    # SPECIAL
    eval $5=`echo $1 | sed -e "s#$RE#\4#"`
}

# usage: semverLT version1 version2
semverLT() {
    local MAJOR_A=0
    local MINOR_A=0
    local PATCH_A=0
    local SPECIAL_A=0

    local MAJOR_B=0
    local MINOR_B=0
    local PATCH_B=0
    local SPECIAL_B=0

    semverParseInto $1 MAJOR_A MINOR_A PATCH_A SPECIAL_A
    semverParseInto $2 MAJOR_B MINOR_B PATCH_B SPECIAL_B

    if [ $MAJOR_A -lt $MAJOR_B ]; then
        return 0
    fi
    if [ $MAJOR_A -le $MAJOR_B ] && [ $MINOR_A -lt $MINOR_B ]; then
        return 0
    fi
    if [ $MAJOR_A -le $MAJOR_B ] && [ $MINOR_A -le $MINOR_B ] && [ $PATCH_A -lt $PATCH_B ]; then
        return 0
    fi
    if [ "_$SPECIAL_A"  == '_' ] && [ "_$SPECIAL_B"  == '_' ] ; then
        return 1
    fi
    if [ "_$SPECIAL_A"  == '_' ] && [ "_$SPECIAL_B"  != '_' ] ; then
        return 1
    fi
    if [ "_$SPECIAL_A"  != '_' ] && [ "_$SPECIAL_B"  == '_' ] ; then
        return 0
    fi
    if [ "_$SPECIAL_A" < "_$SPECIAL_B" ]; then
        return 0
    fi

    return 1
}

# Get a token from: https://github.com/settings/tokens to increase rate limit (from 60 to 5000),
# make sure the token scope is set to 'public_repo'.
# Create GITHUB_PAT environment variable once you acquired the token to start using it.
# Returns the tag of the latest stable release (in terms of semver and not of release date).
get_latest() {
    # temp_file is needed because the grep would start before the download is over
    temp_file=$(mktemp -q /tmp/$PNAME.XXXXXXXXX)
    if [ $? -ne 0 ]; then
        echo "$0: Can't create temp file, bye bye.."
        exit 1
    fi

    if [ -z "$GITHUB_PAT" ]; then
        curl -s $GITHUB_API > "$temp_file" || return 1
    else
        curl -H "Authorization: token $GITHUB_PAT" -s $GITHUB_API > "$temp_file" || return 1
    fi

    releases=$(cat "$temp_file" | \
        grep -E '"tag_name":|"draft":|"prerelease":' \
        | tr -d ',"' | cut -d ':' -f2 | tr -d ' ')
        # Returns a list of [tag_name draft_boolean prerelease_boolean ...]
        # Ex: v0.10.1 false false v0.9.1-rc.1 false true v0.9.0 false false...

    i=0
    latest=''
    current_tag=''
    for release_info in $releases; do
        # Checking tag_name
        if [ $i -eq 0 ]; then
            # If it's not an alpha or beta release
            if echo "$release_info" | grep -q "$GREP_SEMVER_REGEXP"; then
                current_tag=$release_info
            else
                current_tag=''
            fi
            i=1
        # Checking draft boolean
        elif [ $i -eq 1 ]; then
            if [ "$release_info" = 'true' ]; then
                current_tag=''
            fi
            i=2
        # Checking prerelease boolean
        elif [ $i -eq 2 ]; then
            if [ "$release_info" = 'true' ]; then
                current_tag=''
            fi
            i=0
            # If the current_tag is valid
            if [ "$current_tag" != '' ]; then
                # If there is no latest yes
                if [ "$latest" = '' ]; then
                    latest="$current_tag"
                else
                    # Comparing latest and the current tag
                    semverLT $current_tag $latest
                    if [ $? -eq 1 ]; then
                        latest="$current_tag"
                    fi
                fi
            fi
        fi
    done

    rm -f "$temp_file"
    return 0
}

# Gets the OS by setting the $os variable.
# Returns 0 in case of success, 1 otherwise.
get_os() {
    os_name=$(uname -s)
    case "$os_name" in
    'Darwin')
        os='macos'
        ;;
    'Linux')
        os='linux'
        ;;
     'MINGW'*)
        os='windows'
        ;;
    *)
        return 1
    esac
    return 0
}

# Gets the architecture by setting the $archi variable.
# Returns 0 in case of success, 1 otherwise.
get_archi() {
    architecture=$(uname -m)
    case "$architecture" in
    'x86_64' | 'amd64' )
        archi='amd64'
        ;;
    'arm64')
        # MacOS M1
        if [ $os = 'macos' ]; then
            archi='amd64'
        else
            archi='aarch64'
        fi
        ;;
    'aarch64')
        archi='aarch64'
        ;;
    *)
        return 1
    esac
    return 0
}

success_usage() {
    printf "$GREEN%s\n$DEFAULT" "Meilisearch $latest binary successfully downloaded as '$binary_name' file."
    echo ''
    echo 'Run it:'
    echo "    $ ./$PNAME"
    echo 'Usage:'
    echo "    $ ./$PNAME --help"
}

not_available_failure_usage() {
    printf "$RED%s\n$DEFAULT" 'ERROR: Meilisearch binary is not available for your OS distribution or your architecture yet.'
    echo ''
    echo 'However, you can easily compile the binary from the source files.'
    echo 'Follow the steps at the page ("Source" tab): https://docs.meilisearch.com/learn/getting_started/installation.html'
}

fetch_release_failure_usage() {
    echo ''
    printf "$RED%s\n$DEFAULT" 'ERROR: Impossible to get the latest stable version of Meilisearch.'
    echo 'Please let us know about this issue: https://github.com/meilisearch/meilisearch/issues/new/choose'
}

fill_release_variables() {
    # Fill $latest variable.
    if ! get_latest; then
        # TO CHANGE.
        fetch_release_failure_usage
        exit 1
    fi
    if [ "$latest" = '' ]; then
        fetch_release_failure_usage
        exit 1
     fi
     # Fill $os variable.
     if ! get_os; then
        not_available_failure_usage
        exit 1
     fi
     # Fill $archi variable.
     if ! get_archi; then
        not_available_failure_usage
        exit 1
     fi
}

download_binary() {
    fill_release_variables
    echo "Downloading Meilisearch binary $latest for $os, architecture $archi..."
    case "$os" in
        'windows')
            release_file="$PNAME-$os-$archi.exe"
            binary_name="$PNAME.exe"
            ;;
        *)
            release_file="$PNAME-$os-$archi"
            binary_name="$PNAME"
    esac
    # Fetch the Meilisearch binary.
    curl --fail -OL "$GITHUB_REL/$latest/$release_file"
    if [ $? -ne 0 ]; then
        fetch_release_failure_usage
        exit 1
    fi
    mv "$release_file" "$binary_name"
    chmod 744 "$binary_name"
    success_usage
}

# MAIN

main() {
    download_binary
}
main
