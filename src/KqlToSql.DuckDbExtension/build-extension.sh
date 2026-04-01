#!/usr/bin/env bash

# Build the KQL DuckDB extension with Native AOT for the current platform.
#
# Usage:
#   ./build-extension.sh                  # Release build
#   ./build-extension.sh Debug            # Debug build
#
# Prerequisites:
#   - .NET 10 SDK
#   - DuckDB.ExtensionKit submodule initialized:
#     git submodule update --init --recursive

set -euo pipefail

CONFIGURATION="${1:-Release}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_PATH="$SCRIPT_DIR/KqlToSql.DuckDbExtension.csproj"

# Detect runtime identifier
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Linux)
        case "$ARCH" in
            x86_64)  RID="linux-x64" ;;
            aarch64) RID="linux-arm64" ;;
            *)       echo "Unsupported Linux architecture: $ARCH" >&2; exit 1 ;;
        esac
        ;;
    Darwin)
        case "$ARCH" in
            x86_64)  RID="osx-x64" ;;
            arm64)   RID="osx-arm64" ;;
            *)       echo "Unsupported macOS architecture: $ARCH" >&2; exit 1 ;;
        esac
        ;;
    *)
        echo "Unsupported OS: $OS. Use build-extension.ps1 for Windows." >&2
        exit 1
        ;;
esac

echo "Building KQL DuckDB extension for $RID ($CONFIGURATION)..."

dotnet publish "$PROJECT_PATH" -c "$CONFIGURATION" -r "$RID"

if [ $? -eq 0 ]; then
    echo ""
    echo "Extension built successfully!"
    echo "Extension location: bin/$CONFIGURATION/net10.0/$RID/publish/kql.duckdb_extension"
else
    echo "Extension build failed!" >&2
    exit 1
fi
