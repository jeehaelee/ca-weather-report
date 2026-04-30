#!/usr/bin/env bash
# Generate an RSA key pair for Snowflake key-pair (JWT) authentication.
# Run once; then register the public key in Snowflake (see docs/snowflake_keypair_auth.md).
set -euo pipefail
KEY_DIR="${SNOWFLAKE_KEY_DIR:-$HOME/.snowflake/keys}"
PRIV="$KEY_DIR/snowflake_rsa_key.p8"
PUB="$KEY_DIR/snowflake_rsa_key.pub"
SINGLE="$KEY_DIR/snowflake_rsa_key_singleline.txt"

if [[ -f "$PRIV" ]]; then
  echo "Private key already exists: $PRIV" >&2
  echo "Remove it first only if you intend to rotate keys." >&2
  exit 1
fi

mkdir -p "$KEY_DIR"
openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -inform PEM -out "$PRIV" -nocrypt
openssl rsa -in "$PRIV" -pubout -out "$PUB"
chmod 600 "$PRIV"
chmod 644 "$PUB"
awk '/BEGIN PUBLIC KEY/,/END PUBLIC KEY/' "$PUB" | grep -v "BEGIN\|END" | tr -d '\n' > "$SINGLE"

echo "Created:"
echo "  $PRIV"
echo "  $PUB"
echo "  $SINGLE  (use with ALTER USER ... RSA_PUBLIC_KEY)"
echo ""
echo "Next: in Snowflake, run alter user with rsa_public_key = (contents of $SINGLE)"
echo "  See ca-geo-weather-daily/docs/snowflake_keypair_auth.md for the exact SQL shape."
