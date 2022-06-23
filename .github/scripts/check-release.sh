#!/bin/bash

# check_tag $current_tag $file_tag $file_name
function check_tag {
  if [[ "$1" != "$2" ]]; then
      echo "Error: the current tag does not match the version in $3: found $2 - expected $1"
      ret=1
  fi
}

ret=0
current_tag=${GITHUB_REF#'refs/tags/v'}

toml_files='*/Cargo.toml'
for toml_file in $toml_files;
do
    file_tag="$(grep '^version = ' $toml_file | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')"
    check_tag $current_tag $file_tag $toml_file
done

lock_file='Cargo.lock'
lock_tag=$(grep -A 1 'name = "meilisearch-auth"' $lock_file | grep version | cut -d '=' -f 2 | tr -d '"' | tr -d ' ')
check_tag $current_tag $lock_tag $lock_file

if [[ "$ret" -eq 0 ]] ; then
  echo 'OK'
fi
exit $ret
