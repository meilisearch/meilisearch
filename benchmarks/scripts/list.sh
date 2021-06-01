#!/usr/bin/env bash

# Requirements:
# - curl
# - grep

res=$(curl -s https://milli-benchmarks.fra1.digitaloceanspaces.com | grep -oP "(?<=<Key>)[^<]+" | grep -oP --color=never "(?<=^critcmp_results/).+")

for pattern in "$@"
do
	res=$(echo "$res" | grep $pattern)
done

echo "$res"
