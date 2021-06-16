#!/usr/bin/env bash

export RUST_BACKTRACE=1

while true
do
    cargo test qc_ --release -- --nocapture
    if [[ x$? != x0 ]] ; then
        exit $?
    fi
done
