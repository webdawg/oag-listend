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

# Examples are not built by `cargo test`; the shipped-client tests execute push_pull.
echo "== examples"
# shellcheck disable=SC2086
cargo build -p oag-listend --examples $RELEASE

echo "== tests${RELEASE:+ (release)}"
# shellcheck disable=SC2086
cargo test -p oag-listend $RELEASE

echo "== systemd unit"
if ! command -v systemd-analyze >/dev/null 2>&1; then
    echo "   skipped: systemd-analyze is not installed"
else
    # The unit points at /usr/local/bin, which only exists after install. Verify a copy whose
    # ExecStart names a binary this checkout has actually built.
    BIN=""
    for candidate in target/release/oag-listend target/debug/oag-listend; do
        if [ -x "$candidate" ]; then
            BIN="$(pwd)/$candidate"
            break
        fi
    done
    if [ -z "$BIN" ]; then
        echo "   skipped: build the daemon first (no target/{release,debug}/oag-listend)"
    else
        TMP="$(mktemp -d)"
        sed "s#/usr/local/bin/oag-listend#$BIN#" scripts/oag-listend.service \
            > "$TMP/oag-listend.service"
        systemd-analyze verify "$TMP/oag-listend.service"
        rm -rf "$TMP"
    fi
fi

echo "all gates passed"
