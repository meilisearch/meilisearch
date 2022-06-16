#!/bin/sh

# Checking if current tag matches the package version
current_tag=$(echo $GITHUB_REF | tr -d 'refs/tags/v')
file1='meilisearch-auth/Cargo.toml'
file2='meilisearch-http/Cargo.toml'
file3='meilisearch-lib/Cargo.toml'
file4='meilisearch-types/Cargo.toml'
file5='Cargo.lock'

file_tag1=$(grep '^version = ' $file1 | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')
file_tag2=$(grep '^version = ' $file2 | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')
file_tag3=$(grep '^version = ' $file3 | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')
file_tag4=$(grep '^version = ' $file4 | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')
file_tag5=$(grep -A 1 'name = "meilisearch-auth"' $file5 | grep version | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')

if [ "$current_tag" != "$file_tag1" ] || [ "$current_tag" != "$file_tag2" ] || [ "$current_tag" != "$file_tag3" ] || [ "$current_tag" != "$file_tag4" ] || [ "$current_tag" != "$file_tag5" ]; then
  echo "Error: the current tag does not match the version in package file(s)."
  echo "$file1: found $file_tag1 - expected $current_tag"
  echo "$file2: found $file_tag2 - expected $current_tag"
  echo "$file3: found $file_tag3 - expected $current_tag"
  echo "$file4: found $file_tag4 - expected $current_tag"
  echo "$file5: found $file_tag5 - expected $current_tag"
  exit 1
fi

echo 'OK'
exit 0
