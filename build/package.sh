#!/usr/bin/env bash
set -euo pipefail

VERSION="${CARGO_PKG_VERSION:?}"
GIT_COMMIT="${LIGHTPOOL_GIT_COMMIT:-unknown}"
TARGET_DIR="${CARGO_TARGET_DIR:?}"
OS="${CARGO_CFG_TARGET_OS:?}"
ARCH="${CARGO_CFG_TARGET_ARCH:?}"

BINARY="${TARGET_DIR}/release/lightpool-clob-index"
LOCK_DIR="${TARGET_DIR}/.lightpool-clob-index-package.lock.d"

case "${OS}" in
  linux) PLATFORM_OS="linux" ;;
  macos) PLATFORM_OS="darwin" ;;
  *)
    echo "packaging: unsupported target OS: ${OS}" >&2
    exit 1
    ;;
esac

case "${ARCH}" in
  x86_64) PLATFORM_ARCH="amd64" ;;
  aarch64) PLATFORM_ARCH="arm64" ;;
  *)
    echo "packaging: unsupported target arch: ${ARCH}" >&2
    exit 1
    ;;
esac

PLATFORM="${PLATFORM_OS}-${PLATFORM_ARCH}"
PACKAGE_NAME="lightpool-clob-index-v${VERSION}-${PLATFORM}-${GIT_COMMIT}"
STAGING_ROOT="${TARGET_DIR}/package-staging"
STAGING="${STAGING_ROOT}/${PACKAGE_NAME}"
ARCHIVE="${TARGET_DIR}/${PACKAGE_NAME}.tar.gz"

acquire_lock() {
  local attempts=0
  while ! mkdir "${LOCK_DIR}" 2>/dev/null; do
    attempts=$((attempts + 1))
    if [[ "${attempts}" -ge 1500 ]]; then
      echo "packaging: timed out waiting for package lock" >&2
      exit 1
    fi
    sleep 0.2
  done
}

release_lock() {
  rmdir "${LOCK_DIR}" 2>/dev/null || true
}

wait_for_binary() {
  local attempts=0
  local previous_size=-1

  while [[ "${attempts}" -lt 3600 ]]; do
    if [[ -f "${BINARY}" ]]; then
      local current_size
      current_size="$(stat -f%z "${BINARY}" 2>/dev/null || stat -c%s "${BINARY}")"
      if [[ "${current_size}" == "${previous_size}" && "${current_size}" -gt 0 ]]; then
        return 0
      fi
      previous_size="${current_size}"
    fi
    attempts=$((attempts + 1))
    sleep 0.5
  done

  echo "packaging: binary not found at ${BINARY}" >&2
  exit 1
}

create_archive() {
  rm -rf "${STAGING}"
  mkdir -p "${STAGING}/bin"
  cp "${BINARY}" "${STAGING}/bin/lightpool-clob-index"
  chmod +x "${STAGING}/bin/lightpool-clob-index"

  rm -f "${ARCHIVE}"
  tar -czf "${ARCHIVE}" -C "${STAGING_ROOT}" "${PACKAGE_NAME}"
  echo "packaged: ${ARCHIVE}"
}

trap release_lock EXIT
acquire_lock
if [[ "${LIGHTPOOL_PACKAGE_SYNC:-}" == "1" ]]; then
  if [[ ! -f "${BINARY}" ]]; then
    echo "packaging: binary not found at ${BINARY}" >&2
    exit 1
  fi
else
  wait_for_binary
fi
create_archive
