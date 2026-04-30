#!/usr/bin/env bash
# Run a multi-statement .sql file in a single Snowflake session (one connection).
#
# The Snowflake CLI defaults here match how most ad-hoc runs work on your laptop:
#   - DOORDASH-DOORDASH  → Okta in the browser (or cached token if valid)
#   - DOORDASH-DOORDASH-JWT → key-pair, no browser (see docs/snowflake_keypair_auth.md)
#
# Examples (from the repo root):
#   SNOWFLAKE_CONNECTION=DOORDASH-DOORDASH \
#     ./ca-geo-weather-daily/scripts/run_snow_sql_file.sh \
#     ca-geo-weather-daily/sql/non_tier_silver_share_movement_decomposition.sql
#
#   # Headless, after `alter user ... rsa_public_key = '...'`
#   ./ca-geo-weather-daily/scripts/run_snow_sql_file.sh some.sql
#
# Usage: run_snow_sql_file.sh <file.sql> [extra args passed to snow sql ... -f]
set -euo pipefail
SNOW="${SNOW:-/opt/homebrew/bin/snow}"
if [[ $# -lt 1 || ! -f "$1" ]]; then
  echo "Usage: $0 <path-to.sql> [extra snow sql -f options]" >&2
  exit 1
fi
FILE="$1"
shift
CONN="${SNOWFLAKE_CONNECTION:-DOORDASH-DOORDASH}"
exec "$SNOW" sql --connection "$CONN" --client-store-temporary-credential -f "$FILE" "$@"
