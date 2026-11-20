#!/bin/sh
# Run every quality gate on oag-listend and fail if any one of them does.
#
#   scripts/check.sh                          fmt, clippy, rustdoc, examples, tests, systemd unit
#   scripts/check.sh --release                the same, with the suite in release
#   scripts/check.sh --features reticulum     the same, with the opt-in Reticulum carrier
#
# These are the gates the repository is held to. Run them before pushing.
set -eu

cd "$(dirname "$0")/.."

RELEASE=""
FEATURES=""
while [ $# -gt 0 ]; do
    case "$1" in
        --release)
            RELEASE=--release
            ;;
        --features)
            shift
            FEATURES="--features ${1:-}"
            ;;
        --features=*)
            FEATURES="--features ${1#--features=}"
            ;;
        *)
            echo "usage: $0 [--release] [--features FEATURES]" >&2
            exit 2
            ;;
    esac
    shift
done

echo "== cargo fmt --check"
cargo fmt --check

echo "== cargo clippy --all-targets"
# `-D warnings` is the point: without it a new lint is printed and the gate still passes.
# shellcheck disable=SC2086
cargo clippy -p oag-listend --all-targets $FEATURES -- -D warnings

echo "== rustdoc (-D warnings)"
# shellcheck disable=SC2086
RUSTDOCFLAGS="-D warnings" cargo doc -p oag-listend --no-deps $FEATURES

# Examples are not built by `cargo test`; the shipped-client tests execute push_pull.
echo "== examples"
# shellcheck disable=SC2086
cargo build -p oag-listend --examples $RELEASE $FEATURES

echo "== tests${RELEASE:+ (release)}${FEATURES:+ (features: $FEATURES)}"
# shellcheck disable=SC2086
cargo test -p oag-listend $RELEASE $FEATURES

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
