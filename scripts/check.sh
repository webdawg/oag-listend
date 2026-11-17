#!/bin/sh
# Run every quality gate on oag-listend and fail if any one of them does.
#
#   scripts/check.sh           fmt, clippy, rustdoc, tests (debug)
#   scripts/check.sh --release  the same, plus the suite in release
#
# These are the gates the repository is held to. Run them before pushing.
set -eu

cd "$(dirname "$0")/.."

RELEASE=""
if [ "${1:-}" = "--release" ]; then
    RELEASE=--release
elif [ $# -gt 0 ]; then
    echo "usage: $0 [--release]" >&2
    exit 2
fi

echo "== cargo fmt --check"
cargo fmt --check

echo "== cargo clippy --all-targets"
cargo clippy -p oag-listend --all-targets

echo "== rustdoc (-D warnings)"
RUSTDOCFLAGS="-D warnings" cargo doc -p oag-listend --no-deps

echo "== tests${RELEASE:+ (release)}"
# shellcheck disable=SC2086
cargo test -p oag-listend $RELEASE

echo "all gates passed"
