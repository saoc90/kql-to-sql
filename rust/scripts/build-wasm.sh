#!/usr/bin/env bash
# Builds the translator for the browser into src/WebDemo/wwwroot/kql-wasm/.
# Needs: rustup target add wasm32-unknown-unknown; cargo install wasm-bindgen-cli --version <same as Cargo.lock>
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
out="${1:-$here/../src/WebDemo/wwwroot/kql-wasm}"
cd "$here"
cargo build -p kql-wasm --release --target wasm32-unknown-unknown
wasm-bindgen --target web --no-typescript --out-dir "$out" "${CARGO_TARGET_DIR:-target}/wasm32-unknown-unknown/release/kql_wasm.wasm"
test -s "$out/kql_wasm_bg.wasm" || { echo "error: wasm-bindgen produced no output" >&2; exit 1; }
ls -l "$out"
