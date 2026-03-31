#!/bin/bash
# Rust version of api_test - Much faster than Python!
# Usage examples:
#   ./api_test.sh                    # Fetch last 3 weeks
#   ./api_test.sh --last 5           # Fetch last 5 weeks  
#   ./api_test.sh --week 202612      # Fetch specific week
#   ./api_test.sh --workers 20       # Use 20 concurrent workers

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY="$SCRIPT_DIR/target/release/api_test"

if [ ! -f "$BINARY" ]; then
    echo "Error: Rust binary not found at $BINARY"
    echo "Please run: cargo build --release"
    exit 1
fi

"$BINARY" "$@"
