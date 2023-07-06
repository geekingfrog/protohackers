#!/usr/bin/env bash

set -euo pipefail
cargo build --release
strip target/release/protohacker
rsync --progress target/release/protohacker linode:~/
