#!/bin/sh

# Checking if current tag matches the package version
current_tag=$(echo $GITHUB_REF | tr -d 'refs/tags/v')
file1='meilisearch-auth/Cargo.toml'
file2='meilisearch-http/Cargo.toml'
file3='meilisearch-lib/Cargo.toml'
file4='meilisearch-types/Cargo.toml'
file5='Cargo.lock'


file5=$(grep -A 1 'name = "meilisearch-auth"' $file5 | grep version)

for file in $file1 $file2 $file3 $file4 $file5;
do
    file_tag=$(grep '^version = ' $file | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')
    if [ "$current_tag" != "$file_tag" ]; then
      echo "Error: the current tag does not match the version in package file(s)."
      echo "$file: found $file_tag - expected $current_tag"
      exit 1
    fi
done

echo 'OK'
exit 0
