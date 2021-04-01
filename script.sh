for file in $@
do
	echo "fixing $file"

	# removing the useless spaces
	sd "^ *" "" $file
	# removing the useless lines
	sd "//.*$" "" $file
	# removing the useless linebreaks
	sd "\n" "" $file

	if cargo check --all; then
		echo "$file is fixed"
	else
		echo "I hate $file"
		git checkout -- $file
	fi
done
