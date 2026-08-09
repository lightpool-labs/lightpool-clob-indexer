#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_DIR="${CRATE_DIR}/target"

export LIGHTPOOL_SKIP_PACKAGE=1
export CARGO_TARGET_DIR="${TARGET_DIR}"
cargo build --release --manifest-path "${CRATE_DIR}/Cargo.toml"

export CARGO_PKG_VERSION="$(
  cargo pkgid --manifest-path "${CRATE_DIR}/Cargo.toml" | sed -E 's/.*[@#]//'
)"
export CARGO_TARGET_DIR="${TARGET_DIR}"

case "$(uname -s)" in
  Linux) export CARGO_CFG_TARGET_OS=linux ;;
  Darwin) export CARGO_CFG_TARGET_OS=macos ;;
  *)
    echo "build-release: unsupported host OS: $(uname -s)" >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64) export CARGO_CFG_TARGET_ARCH=x86_64 ;;
  aarch64 | arm64) export CARGO_CFG_TARGET_ARCH=aarch64 ;;
  *)
    echo "build-release: unsupported host arch: $(uname -m)" >&2
    exit 1
    ;;
esac

export LIGHTPOOL_GIT_COMMIT="$(
  git -C "${CRATE_DIR}" rev-parse --short HEAD 2>/dev/null \
    || git -C "${CRATE_DIR}/.." rev-parse --short HEAD 2>/dev/null \
    || echo unknown
)"
export LIGHTPOOL_PACKAGE_SYNC=1

exec bash "${SCRIPT_DIR}/package.sh"
