#!/bin/sh

# Checks if the current tag should be the latest (in terms of semver and not of release date).
# Ex: previous tag -> v0.10.1
#     new tag -> v0.8.12
#     The new tag should not be the latest
#     So it returns "false", the CI should not run for the release v0.8.2

# Used in GHA in publish-docker-latest.yml
# Returns "true" or "false" (as a string) to be used in the `if` in GHA

# GLOBAL
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
    #MINOR
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
    if [ "_$SPECIAL_A"  == "_" ] && [ "_$SPECIAL_B"  == "_" ] ; then
        return 1
    fi
    if [ "_$SPECIAL_A"  == "_" ] && [ "_$SPECIAL_B"  != "_" ] ; then
        return 1
    fi
    if [ "_$SPECIAL_A"  != "_" ] && [ "_$SPECIAL_B"  == "_" ] ; then
        return 0
    fi
    if [ "_$SPECIAL_A" < "_$SPECIAL_B" ]; then
        return 0
    fi

    return 1
}

# Returns the tag of the latest stable release (in terms of semver and not of release date)
get_latest() {
    temp_file='temp_file' # temp_file needed because the grep would start before the download is over
    curl -s 'https://api.github.com/repos/meilisearch/MeiliSearch/releases' > "$temp_file"
    releases=$(cat "$temp_file" | \
        grep -E "tag_name|draft|prerelease" \
        | tr -d ',"' | cut -d ':' -f2 | tr -d ' ')
        # Returns a list of [tag_name draft_boolean prerelease_boolean ...]
        # Ex: v0.10.1 false false v0.9.1-rc.1 false true v0.9.0 false false...

    i=0
    latest=""
    current_tag=""
    for release_info in $releases; do
        if [ $i -eq 0 ]; then # Cheking tag_name
            if echo "$release_info" | grep -q "$GREP_SEMVER_REGEXP"; then # If it's not an alpha or beta release
                current_tag=$release_info
            else
                current_tag=""
            fi
            i=1
        elif [ $i -eq 1 ]; then # Checking draft boolean
            if [ "$release_info" = "true" ]; then
                current_tag=""
            fi
            i=2
        elif [ $i -eq 2 ]; then # Checking prerelease boolean
            if [ "$release_info" = "true" ]; then
                current_tag=""
            fi
            i=0
            if [ "$current_tag" != "" ]; then # If the current_tag is valid
                if [ "$latest" = "" ]; then # If there is no latest yet
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

# MAIN
current_tag="$(echo $GITHUB_REF | tr -d 'refs/tags/')"
latest="$(get_latest)"

if [ "$current_tag" != "$latest" ]; then
    # The current release tag is not the latest
    echo "false"
else
    # The current release tag is the latest
    echo "true"
fi
