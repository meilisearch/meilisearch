#!/usr/bin/env bash

# Checks if dependency are installed.
(hash curl > /dev/null 2>&1 || echo "${0}: curl command not found in PATH.") && exit 1
(hash jq > /dev/null 2>&1 || echo "${0}: jq command not found in PATH.") && exit 1

#################################### GLOBALS ###################################

# Project name
projectName='meilisearch'
REPO="${projectName}/${projectName}"

# The GitHub API URL that we use to get information about the latest release.
GITHUB_API_URL="https://api.github.com/repos/${REPO}/releases/latest"

# Colors
RED='\033[31m'
GREEN='\033[32m'
DEFAULT='\033[0m'

# A temporary fle where we store the response of the GITHUB_API_URL.
temp_file=$(mktemp -q /tmp/${projectName}.XXXXXXXXX)
# Used to store the latest version.
latestVersion=""
# Used to store the OS to look for.
os=""
# Used to store the Architecture to look for.
archi=""
# Used to store the name of the release file to download.
releaseFile=""
# Used to store the name of the file to provide to the end-user.
binaryName="${projectName}"

################################## FUNCTIONS ###################################


success_usage() {
    printf "${GREEN}%s\n${DEFAULT}" "Meilisearch ${latestVersion} binary successfully downloaded as '${binaryName}' file."
    echo ''
    echo 'Run it:'
    echo "    $ ./${projectName}"
    echo 'Usage:'
    echo "    $ ./${projectName} --help"
}

not_available_failure_usage() {
    printf "${RED}%s\n${DEFAULT}" 'ERROR: Meilisearch binary is not available for your OS distribution or your architecture yet.'
    echo ''
    echo 'However, you can easily compile the binary from the source files.'
    echo 'Follow the steps at the page ("Source" tab): https://docs.meilisearch.com/learn/getting_started/installation.html'
}

fetch_release_failure_usage() {
    echo ''
    printf "${RED}%s\n${DEFAULT}" 'ERROR: Impossible to get the latest stable version of Meilisearch.'
    echo 'Please let us know about this issue: https://github.com/meilisearch/meilisearch/issues/new/choose'
}

# Gets the OS by setting the $os variable.
# Returns 0 in case of success, 1 otherwise.
get_os() {
    osName="$(uname -s)"
    case "${osName}" in
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
        ;;
    esac

    return 0
}


# Gets the architecture by setting the $archi variable.
# Returns 0 in case of success, 1 otherwise.
get_archi() {
    architecture="$(uname -m)"

    case "${architecture}" in
        'x86_64' | 'amd64' )
            archi='amd64'
        ;;
        'arm64')
            # MacOS M1
            if [ ${os} = 'macos' ]; then
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
        ;;
    esac

    return 0
}

# Get a token from: https://github.com/settings/tokens to increase rate limit
# (from 60 to 5000),
# make sure the token scope is set to 'public_repo'.
# Create GITHUB_PAT environment variable once you acquired the token to start
# using it.
# Returns the tag of the latest stable release (in terms of semver and not of
# release date).
get_latest() {
    if [ ${?} -ne 0 ]
    then
        echo "${0}: Can't create temp file, bye bye.."
        exit 1
    fi

    if [ -z "${GITHUB_PAT}" ]
    then
        curl -s ${GITHUB_API_URL} > "${temp_file}" || return 1
    else
        curl -H "Authorization: token ${GITHUB_PAT}" -s ${GITHUB_API_URL} > "${temp_file}" || return 1
    fi

    latestVersion="$(jq -r '.tag_name' ${temp_file})"

    if [ -z "${latestVersion}" ]
    then
        echo "${0}: Can't read latestVersion tag, bye bye.."
        exit 1
    fi

    return 0
}

checkVars() {
    if [ -z "${latestVersion}" ]
    then
        fetch_release_failure_usage
        exit 1
     fi

    # Fill $os variable.
    if [ -z "${os}" ]
     then
        not_available_failure_usage
        exit 1
    fi

    # Fill $archi variable.
    if [ -z "${archi}"]
    then
        not_available_failure_usage
        exit 1
    fi
}


get_releaseFileAndBinaryName() {
    releaseFile="${projectName}-${os}-${archi}"

    if [[ "${os}" == "windows" ]]
    then
        releaseFile="${releaseFile}.exe"
        binaryName="${binaryName}.exe"
    fi
}

# Download the latest binary.
download_binary() {
    echo "Downloading Meilisearch binary ${latestVersion} for ${os}, architecture ${archi}..."

    jqFilter=".assets[] | select (.name | contains(\"${releaseFile}\")) | .browser_download_url"
    downloadLink=`jq -r "${jqFilter}" ${temp_file}`

    # Fetch the Meilisearch binary.
    curl --fail -OL "${downloadLink}"

    if [[ ${?} != 0 ]]
    then
        fetch_release_failure_usage
        exit 1
    fi

    mv "${releaseFile}" "${binaryName}"
    chmod 744 "${binaryName}"
    success_usage
}


################################### BUSINESS ###################################

get_os
get_archi
get_latest
checkVars

get_releaseFileAndBinaryName
download_binary