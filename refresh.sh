#!/bin/bash
# Rust version of refresh - Much faster than Python!
# Usage examples:
#   ./refresh.sh                   # Refresh all players with 12 workers
#   ./refresh.sh --workers 24      # Refresh with 24 concurrent workers

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY="$SCRIPT_DIR/target/release/refresh"

if [ ! -f "$BINARY" ]; then
    echo "Error: Rust binary not found at $BINARY"
    echo "Please run: cargo build --release"
    exit 1
fi

"$BINARY" "$@"
