#!/usr/bin/bash

NEW_VERSION=$1

if [ -z "$NEW_VERSION" ]
then
	echo "error: a version number must be provided"
	exit 1
fi

# find current version
CURRENT_VERSION=$(cat **/*.toml | grep meilisearch | grep version | sed 's/.*\([0-9]\+\.[0-9]\+\.[0-9]\+\).*/\1/' | sed "1q;d")

# bump all version in .toml
echo "bumping from version $CURRENT_VERSION to version $NEW_VERSION"
while true
do
	read -r -p "Continue (y/n)?" choice
	case "$choice" in
		y|Y ) break;;
		n|N ) echo "aborting bump" && exit 0;;
		* ) echo "invalid choice";;
	esac
done
# update all crate version
sed -i "s/version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" **/*.toml

printf "running cargo check: "

CARGO_CHECK=$(cargo check 2>&1)

if [ $? != "0" ]
then
	printf "\033[31;1m FAIL \033[0m\n"
	printf "$CARGO_CHECK"
	exit 1
fi
printf "\033[32;1m OK \033[0m\n"
