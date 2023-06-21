#!/usr/bin/env bash

set -euo pipefail
cargo build --release
rsync --progress target/release/protohacker linode:~/
