#!/usr/bin/env bash

# Requirements:
# - critcmp. See: https://github.com/BurntSushi/critcmp
# - curl

# Usage
# $ bash compare.sh json_file1 json_file1
# ex: bash compare.sh songs_main_09a4321.json songs_geosearch_24ec456.json

# Checking that critcmp is installed
command -v critcmp > /dev/null 2>&1
if [[ "$?" -ne 0 ]]; then
    echo 'You must install critcmp to make this script work.'
    echo 'See: https://github.com/BurntSushi/critcmp'
    echo '  $ cargo install critcmp'
    exit 1
fi

s3_url='https://milli-benchmarks.fra1.digitaloceanspaces.com/critcmp_results'

for file in $@
do
    file_s3_url="$s3_url/$file"
    file_local_path="/tmp/$file"

    if [[ ! -f $file_local_path ]]; then
        curl $file_s3_url --output $file_local_path --silent
        if [[ "$?" -ne 0 ]]; then
            echo 'curl command failed.'
            exit 1
        fi
    fi
done

path_list=$(echo " $@" | sed 's/ / \/tmp\//g')

critcmp $path_list
