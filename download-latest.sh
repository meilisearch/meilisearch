#!/bin/sh

# COLORS
RED='\033[31m'
GREEN='\033[32m'
DEFAULT='\033[0m'

# GLOBALS
GREP_SEMVER_REGEXP='v\([0-9]*\)[.]\([0-9]*\)[.]\([0-9]*\)$' # i.e. v[number].[number].[number]

# FUNCTIONS

# semverParseInto and semverLT from https://github.com/cloudflare/semver_bash/blob/master/semver.sh

# usage: semverParseInto version major minor patch special
# version: the string version
# major, minor, patch, special: will be assigned by the function
semverParseInto() {
    local RE='[^0-9]*\([0-9]*\)[.]\([0-9]*\)[.]\([0-9]*\)\([0-9A-Za-z-]*\)'
    #MAJOR
    eval $2=`echo $1 | sed -e "s#$RE#\1#"`
    #MINOR
    eval $3=`echo $1 | sed -e "s#$RE#\2#"`
    #PATCH
    eval $4=`echo $1 | sed -e "s#$RE#\3#"`
    #SPECIAL
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

# Get a token from https://github.com/settings/tokens to increasae rate limit (from 60 to 5000), make sure the token scope is set to 'public_repo'
# Create GITHUB_PAT enviroment variable once you aquired the token to start using it
# Returns the tag of the latest stable release (in terms of semver and not of release date)
get_latest() {
    temp_file='temp_file' # temp_file needed because the grep would start before the download is over

        if [ -z "$GITHUB_PAT" ]; then
        curl -s 'https://api.github.com/repos/meilisearch/MeiliSearch/releases' > "$temp_file" || return 1
    else
        curl -H "Authorization: token $GITHUB_PAT" -s 'https://api.github.com/repos/meilisearch/MeiliSearch/releases' > "$temp_file" || return 1
    fi

    releases=$(cat "$temp_file" | \
        grep -E "tag_name|draft|prerelease" \
        | tr -d ',"' | cut -d ':' -f2 | tr -d ' ')
        # Returns a list of [tag_name draft_boolean prerelease_boolean ...]
        # Ex: v0.10.1 false false v0.9.1-rc.1 false true v0.9.0 false false...

    i=0
    latest=''
    current_tag=''
    for release_info in $releases; do
        if [ $i -eq 0 ]; then # Cheking tag_name
            if echo "$release_info" | grep -q "$GREP_SEMVER_REGEXP"; then # If it's not an alpha or beta release
                current_tag=$release_info
            else
                current_tag=''
            fi
            i=1
        elif [ $i -eq 1 ]; then # Checking draft boolean
            if [ "$release_info" = 'true' ]; then
                current_tag=''
            fi
            i=2
        elif [ $i -eq 2 ]; then # Checking prerelease boolean
            if [ "$release_info" = 'true' ]; then
                current_tag=''
            fi
            i=0
            if [ "$current_tag" != '' ]; then # If the current_tag is valid
                if [ "$latest" = '' ]; then # If there is no latest yet
                    latest="$current_tag"
                else
                    semverLT $current_tag $latest # Comparing latest and the current tag
                    if [ $? -eq 1 ]; then
                        latest="$current_tag"
                    fi
                fi
            fi
        fi
    done

    rm -f "$temp_file"
    echo $latest
}

# Gets the OS by setting the $os variable
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

# Gets the architecture by setting the $archi variable
# Returns 0 in case of success, 1 otherwise.
get_archi() {
    architecture=$(uname -m)
    case "$architecture" in
    'x86_64' | 'amd64' | 'arm64')
        archi='amd64'
        ;;
    'aarch64')
        archi='armv8'
        ;;
    *)
        return 1
    esac
    return 0
}

success_usage() {
    printf "$GREEN%s\n$DEFAULT" "MeiliSearch $latest binary successfully downloaded as '$binary_name' file."
    echo ''
    echo 'Run it:'
    echo '    $ ./meilisearch'
    echo 'Usage:'
    echo '    $ ./meilisearch --help'
}

failure_usage() {
    printf "$RED%s\n$DEFAULT" 'ERROR: MeiliSearch binary is not available for your OS distribution or your architecture yet.'
    echo ''
    echo 'However, you can easily compile the binary from the source files.'
    echo 'Follow the steps at the page ("Source" tab): https://docs.meilisearch.com/learn/getting_started/installation.html'
}

# MAIN
latest="$(get_latest)"

if [ "$latest" = '' ]; then
    echo ''
    echo 'Impossible to get the latest stable version of MeiliSearch.'
    echo 'Please let us know about this issue: https://github.com/meilisearch/MeiliSearch/issues/new/choose'
    exit 1
fi

if ! get_os; then
    failure_usage
    exit 1
fi

if ! get_archi; then
    failure_usage
    exit 1
fi

echo "Downloading MeiliSearch binary $latest for $os, architecture $archi..."
case "$os" in
    'windows')
        release_file="meilisearch-$os-$archi.exe"
		binary_name='meilisearch.exe'

        ;;
	*)
		release_file="meilisearch-$os-$archi"
		binary_name='meilisearch'

esac
link="https://github.com/meilisearch/MeiliSearch/releases/download/$latest/$release_file"
curl -OL "$link"
mv "$release_file" "$binary_name"
chmod 744 "$binary_name"
success_usage
