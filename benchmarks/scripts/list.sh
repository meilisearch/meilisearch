#!/usr/bin/env bash

# Requirements:
# - curl
# - grep

res=$(curl -s https://milli-benchmarks.fra1.digitaloceanspaces.com | grep -o '<Key>[^<]\+' | cut -c 5- | grep critcmp_results/ | cut -c 18-)

for pattern in "$@"
do
	res=$(echo "$res" | grep $pattern)
done

echo "$res"
