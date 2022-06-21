#!/bin/sh

# check_tag $current_tag $file_tag $file_name
function check_tag {
  if [ "$1" != "$2" ]; then
      echo "Error: the current tag does not match the version in $3:"
      echo "Found $1 - expected $2"
      exit 1
  fi
}

current_tag=$(echo $GITHUB_REF | tr -d 'refs/tags/v')

files='*/Cargo.toml'
for file in $files;
do
    file_tag="$(grep '^version = ' $file | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')"
    check_tag $current_tag $file_tag $file
done

lock_file='Cargo.lock'
lock_tag=$(grep -A 1 'name = "meilisearch-auth"' $lock_file | grep version | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')
check_tag $current_tag $lock_tag $lock_file

echo 'OK'
exit 0
