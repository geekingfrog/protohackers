#!/usr/bin/env bash

set -euo pipefail
cargo build --release
rsync target/release/protohacker linode:~/
ssh linode "./protohacker"
