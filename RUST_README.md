# Skycards DB - Rust Version

This is a high-performance Rust rewrite of the Python API test and refresh scripts.

## Features

✨ **Much Faster**: Rust's zero-copy abstractions and compiled efficiency make these scripts significantly faster than the Python versions.

- **api_test**: Fetches all airport leaderboards and user stats and stores them in SQLite
- **refresh**: Refreshes player stats from the API and updates the database

## Building

The project is pre-built. If you need to rebuild:

```bash
cargo build --release
```

This creates optimized binaries in `target/release/`:
- `api_test` (~5.3 MB)
- `refresh` (~5.2 MB)

## Usage

### Option 1: Use the shell scripts (recommended)

```bash
# API Test - Fetch last 3 weeks of leaderboards
./api_test.sh

# API Test - Fetch last 5 weeks
./api_test.sh --last 5

# API Test - Fetch specific week
./api_test.sh --week 202612

# API Test - Use 20 concurrent workers
./api_test.sh --workers 20

# Refresh - Refresh all player stats with 12 workers
./refresh.sh

# Refresh - Use 24 concurrent workers
./refresh.sh --workers 24
```

### Option 2: Run binaries directly

```bash
./target/release/api_test --last 3
./target/release/refresh --workers 12
```

## API Key

Both scripts use the embedded token from the Python versions. If you need to update it, edit:
- `src/api_test.rs` - `const TOKEN: &str = ...`
- `src/refresh.rs` - `const TOKEN: &str = ...`

Then rebuild with `cargo build --release`.

## Database

Both scripts use `data/DB/highscore.db`. Features:

- WAL mode enabled for concurrent access
- 30-second busy timeout for write contention
- Automatic schema creation
- Batch inserts for performance

## Performance vs Python

Expected improvements:
- **Startup**: ~50ms (vs 500ms+ for Python)
- **Memory**: ~5-10x less memory usage
- **Throughput**: 2-5x faster API request processing
- **Binary size**: ~5 MB each (includes all dependencies)

## Requirements

- macOS 10.13+ (already has Rust if installed via Homebrew)
- 50 MB of disk space

## Troubleshooting

### "Binary not found" error
Run: `cargo build --release`

### "Permission denied" on database
Check database file permissions: `ls -la data/DB/highscore.db`

### Build fails with "Permission denied" on Cargo cache
Try: `sudo chown -R $(whoami) ~/.cargo/registry`

## Architecture

Both applications use:
- **Tokio**: Async runtime for concurrent HTTP requests
- **Reqwest**: HTTP client with connection pooling and retry logic
- **Rusqlite**: SQLite driver with WAL support
- **Indicatif**: Progress bars
- **Serde**: JSON parsing

Concurrency model:
- `api_test`: Concurrent workers per week, with a dedicated writer thread
- `refresh`: Concurrent worker pool with batch updates

## Original Python Versions

The original scripts are still available:
- `api_test.py`
- `Refresh.py`
