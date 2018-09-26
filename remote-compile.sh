#!/bin/sh

# stop at first error
set -ex

ssh remote-compilation << EOF
    set -ex

    cd Documents/raptor-rs
    cargo build --release --bin raptor-http

    tar czf raptor.tar.gz -C target/release raptor-http
EOF

scp remote-compilation:Documents/raptor-rs/raptor.tar.gz /etc/raptor/

tar xzf /etc/raptor/raptor.tar.gz -C /etc/raptor/
