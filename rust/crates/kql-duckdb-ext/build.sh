#!/usr/bin/env bash
# Builds the `kql` DuckDB loadable extension:
#   1. cargo build --release of this crate (a cdylib)
#   2. appends DuckDB's 512-byte extension metadata footer
#   3. verifies the footer, and prints the path of kql.duckdb_extension
#
# Usage: ./build.sh [--test]
#   --test   afterwards run the load test (crates/kql-duckdb-ext-test) against the built file.
#
# Environment:
#   CARGO_TARGET_DIR        cargo target dir (default: <rust>/target)
#   DUCKDB_VERSION          DuckDB version the extension is pinned to (default: derived from the
#                           duckdb-rs version in Cargo.lock, e.g. 1.10506.0 -> v1.5.6)
#   DUCKDB_PLATFORM         platform string (default: detected, e.g. linux_amd64, osx_arm64)
#   KQL_EXTENSION_VERSION   version written into the footer (default: crate version)
#
# Any failing step aborts with a non-zero exit status.
set -euo pipefail

die() { echo "build.sh: error: $*" >&2; exit 1; }

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
rust_root="$(cd "$here/../.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$rust_root/target}"

run_test=0
for arg in "$@"; do
  case "$arg" in
    --test) run_test=1 ;;
    -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
    *) die "unknown argument '$arg'" ;;
  esac
done

command -v cargo >/dev/null || die "cargo not found"

# --- 1. build ---------------------------------------------------------------------------------
echo "build.sh: cargo build --release (target dir: $CARGO_TARGET_DIR)"
cargo build --release --manifest-path "$here/Cargo.toml" || die "cargo build failed"

case "$(uname -s)" in
  Linux)  lib="$CARGO_TARGET_DIR/release/libkql.so" ;;
  Darwin) lib="$CARGO_TARGET_DIR/release/libkql.dylib" ;;
  MINGW*|MSYS*|CYGWIN*) lib="$CARGO_TARGET_DIR/release/kql.dll" ;;
  *) die "unsupported OS $(uname -s)" ;;
esac
[[ -s "$lib" ]] || die "cargo reported success but '$lib' does not exist or is empty"

# --- 2. footer fields -------------------------------------------------------------------------
if [[ -z "${DUCKDB_VERSION:-}" ]]; then
  lock="$here/Cargo.lock"
  [[ -f "$lock" ]] || die "missing $lock"
  # duckdb-rs versions encode the DuckDB version: 1.10506.0 -> v1.5.6
  rs_version="$(awk '/^name = "libduckdb-sys"$/ { getline; gsub(/version = |"/, ""); print; exit }' "$lock")"
  [[ "$rs_version" =~ ^1\.([0-9]+)\.[0-9]+$ ]] || die "cannot parse libduckdb-sys version '$rs_version' from $lock"
  n="${BASH_REMATCH[1]}"
  DUCKDB_VERSION="v$((10#$n / 10000)).$(( (10#$n / 100) % 100 )).$((10#$n % 100))"
fi
[[ "$DUCKDB_VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "bad DUCKDB_VERSION '$DUCKDB_VERSION'"

if [[ -z "${DUCKDB_PLATFORM:-}" ]]; then
  case "$(uname -s)" in
    Linux) os=linux ;; Darwin) os=osx ;; *) os=windows ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) die "unsupported architecture $(uname -m); set DUCKDB_PLATFORM" ;;
  esac
  DUCKDB_PLATFORM="${os}_${arch}"
fi

if [[ -z "${KQL_EXTENSION_VERSION:-}" ]]; then
  KQL_EXTENSION_VERSION="v$(awk -F'"' '/^version = / { print $2; exit }' "$here/Cargo.toml")"
fi

# The extension uses duckdb-rs' bindings, which include DuckDB's *unstable* C API, so it is
# pinned to one DuckDB version (ABI C_STRUCT_UNSTABLE; the host checks DUCKDB_VERSION exactly).
ABI_TYPE=C_STRUCT_UNSTABLE

# --- 3. append the footer -----------------------------------------------------------------------
# Layout of the last 512 bytes (see duckdb/src/main/extension/extension_load.cpp,
# ParseExtensionMetaData): eight 32-byte NUL-padded fields followed by a 256-byte signature.
# Fields, in file order: 3 unused, ABI type, extension version, DuckDB version, platform, magic "4".
out="$CARGO_TARGET_DIR/release/kql.duckdb_extension"
tmp="$out.tmp"
field() {
  local v="$1"
  (( ${#v} <= 32 )) || die "footer field '$v' longer than 32 bytes"
  printf '%s' "$v"
  head -c $((32 - ${#v})) /dev/zero
}
{
  cat "$lib"
  field ""; field ""; field ""
  field "$ABI_TYPE"
  field "$KQL_EXTENSION_VERSION"
  field "$DUCKDB_VERSION"
  field "$DUCKDB_PLATFORM"
  field "4"
  head -c 256 /dev/zero
} > "$tmp" || die "writing $tmp failed"

lib_size=$(wc -c < "$lib")
out_size=$(wc -c < "$tmp")
(( out_size == lib_size + 512 )) || die "footer size mismatch ($lib_size + 512 != $out_size)"
magic="$(tail -c 288 "$tmp" | head -c 1)"
[[ "$magic" == "4" ]] || die "footer verification failed (magic '$magic')"
mv -f "$tmp" "$out"

echo "build.sh: built $out"
echo "build.sh:   duckdb $DUCKDB_VERSION, platform $DUCKDB_PLATFORM, abi $ABI_TYPE, version $KQL_EXTENSION_VERSION"

# --- 4. optional load test ----------------------------------------------------------------------
if (( run_test )); then
  echo "build.sh: running the load test"
  KQL_EXTENSION_PATH="$out" cargo test --manifest-path "$rust_root/Cargo.toml" -p kql-duckdb-ext-test -- --nocapture \
    || die "load test failed"
fi
