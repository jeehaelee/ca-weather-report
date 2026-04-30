#!/usr/bin/env bash
# Run arbitrary SQL via Snowflake CLI.
#
# Non-interactive (no Okta): set up key-pair JWT once, then use connection
# DOORDASH-DOORDASH-JWT (see ca-geo-weather-daily/docs/snowflake_keypair_auth.md).
#
# SSO fallback: SNOWFLAKE_CONNECTION=DOORDASH-DOORDASH and run snow connection test
# in a terminal with a browser when the cached token expires.
#
# Usage:
#   ./ca-geo-weather-daily/scripts/run_snow_query.sh "select current_version();"
#   SNOWFLAKE_CONNECTION=DOORDASH-DOORDASH ./ca-geo-weather-daily/scripts/run_snow_query.sh -q "select 1;"
#   ./ca-geo-weather-daily/scripts/run_snow_query.sh --file path/to/query.sql

set -euo pipefail
SNOW="${SNOW:-/opt/homebrew/bin/snow}"
# Default: browser (Okta) — works with cached token when you have run `snow connection test` recently.
# For headless/CI, set SNOWFLAKE_CONNECTION=DOORDASH-DOORDASH-JWT after key-pair setup (see docs in repo).
CONN="${SNOWFLAKE_CONNECTION:-DOORDASH-DOORDASH}"
exec "$SNOW" sql --connection "$CONN" --client-store-temporary-credential "$@"
