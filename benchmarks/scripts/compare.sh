#!/usr/bin/env bash

# Requirements:
# - s3cmd and being logged to the DO Space "milli-benchmarks". See: https://docs.digitalocean.com/products/spaces/resources/s3cmd/
# - critcmp. See: https://github.com/BurntSushi/critcmp

# Usage
# $ bash compare.sh json_file1 json_file1
# ex: bash compare.sh songs_main_09a4321.json songs_geosearch_24ec456.json

# Checking that critcmp is installed
command -v critcmp > /dev/null 2>&1
if [[ "$?" -ne 0 ]]; then
    echo 'You must install critcmp to make this script working.'
    echo '$ cargo install critcmp'
    echo 'See: https://github.com/BurntSushi/critcmp'
    exit 1
fi

# Checking that s3cmd is installed
command -v s3cmd > /dev/null 2>&1
if [[ "$?" -ne 0 ]]; then
    echo 'You must install s3cmd to make this script working.'
    echo 'See: https://github.com/s3tools/s3cmd'
    exit 1
fi

if [[ $# -ne 2 ]]
  then
    echo 'Need 2 arguments.'
    echo 'Usage: '
    echo '  $ bash compare.sh file_to_download1 file_to_download2'
    echo 'Ex:'
    echo '  $ bash compare.sh songs_main_09a4321.json songs_geosearch_24ec456.json'
    exit 1
fi

file1="$1"
file2="$2"
s3_path='s3://milli-benchmarks/critcmp_results'
file1_s3_path="$s3_path/$file1"
file2_s3_path="$s3_path/$file2"
file1_local_path="/tmp/$file1"
file2_local_path="/tmp/$file2"

if [[ ! -f "$file1_local_path" ]]; then
    s3cmd get "$file1_s3_path" "$file1_local_path"
    if [[ "$?" -ne 0 ]]; then
	    echo 's3cmd command failed. Check your configuration'
	    exit 1
    fi
else
    echo "$file1 already present in /tmp, no need to download."
fi

if [[ ! -f "$file2_local_path" ]]; then
    s3cmd get "$file2_s3_path" "$file2_local_path"
else
    echo "$file2 already present in /tmp, no need to download."
fi

critcmp --color always "$file1_local_path" "$file2_local_path"
