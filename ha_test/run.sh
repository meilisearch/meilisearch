#!/bin/bash

function is_everything_installed {
  everything_ok=yes

  if hash zkli 2>/dev/null; then
    echo "✅ zkli installed"
  else
    everything_ok=no
    echo "🥺 zkli is missing, please run \`cargo install zkli\`"
  fi

  if hash s3cmd 2>/dev/null; then
    echo "✅ s3cmd installed"
  else
    everything_ok=no
    echo "🥺 s3cmd is missing, see how to install it here https://s3tools.org/s3cmd"
  fi

  if [ $everything_ok = "no" ]; then
    echo "Exiting..."
    exit 1
  fi
}

# param: addr of zookeeper
function connect_to_zookeeper {
  if ! zkli -a "$1" ls > /dev/null; then
    echo "zkli can't connect"
    return 1
  fi
}

# param: addr of the s3 bucket
function connect_to_s3 {
  # S3_SECRET_KEY
  # S3_ACCESS_KEY
  # S3_HOST
  # S3_BUCKET

  s3cmd --host="$S3_HOST" --host-bucket="$S3_BUCKET" --access_key="$ACCESS_KEY" --secret_key="$S3_SECRET_KEY" ls

  if $?; then
    echo "s3cmd can't connect"
    return 1
  fi
}

is_everything_installed

ZOOKEEPER_ADDR="localhost:2181"
if ! connect_to_zookeeper $ZOOKEEPER_ADDR; then
  ZOOKEEPER_ADDR="localhost:21811"
  if ! connect_to_zookeeper $ZOOKEEPER_ADDR; then
    echo "Can't connect to zkli"
    exit 1
  fi
fi


connect_to_s3
