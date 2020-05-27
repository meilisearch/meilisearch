#!/usr/bin/env bash

# This script is used to run many times the quickcheck tests
# this tests must be prefixed by qc_

export RUST_BACKTRACE=1

while true
do
    cargo test qc_ --release -- --nocapture
    if [[ x$? != x0 ]] ; then
        exit $?
    fi
done
