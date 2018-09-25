#! /usr/bin/sh

# Notes
#
# $1 (First argument) must be the stop-worlds file (e.g. en.stopwords.txt)
#
# $2 (Second argument) must be the meta file name (e.g. relaxed-colden)

if [ $# -ne 2 ]; then
    echo 'You must specify the stop-words file and the meta file name'
    exit 1
fi

# Kill all processes that have a connection on a given port
kill -9 $(lsof -ti :3030)

# Copy the binary to another place
cp raptor-http.bin /etc/raptor-http

# Run the server on the a specific port and in background
/etc/raptor-http -l 0.0.0.0:3030 --stop-words $1 $2 > /dev/null 2>&1 &
