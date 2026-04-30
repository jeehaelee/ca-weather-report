# Snowflake CLI without Okta on every run (key-pair / JWT)

DoorDash’s `externalbrowser` flow opens Okta each time the CLI needs a fresh session. **Key-pair authentication** signs a short-lived JWT locally with your private key, so `snow sql` and scripts run without a browser.

## What is already on this machine

- **Private key:** `~/.snowflake/keys/snowflake_rsa_key.p8` (mode `600`)
- **Public key (PEM):** `~/.snowflake/keys/snowflake_rsa_key.pub`
- **Single-line public key body** (for `ALTER USER`): `~/.snowflake/keys/snowflake_rsa_key_singleline.txt`
- **CLI connection:** `[DOORDASH-DOORDASH-JWT]` in `~/.snowflake/connections.toml` with `authenticator = "SNOWFLAKE_JWT"` and `private_key_file` pointing at the `.p8` file

Until Snowflake has your public key, JWT connections fail with **“JWT token is invalid”**. The default connection is **`DOORDASH-DOORDASH`** (browser) so the CLI keeps working until you finish the one-time Snowflake step.

## One-time: register the public key in Snowflake

1. Copy the key string (no PEM headers, one line):

   ```bash
   cat ~/.snowflake/keys/snowflake_rsa_key_singleline.txt
   ```

2. In Snowflake, run the `ALTER USER` (paste the full one-line public key as the string). If you see **“Insufficient privileges to operate on user '…'.”**, your role cannot modify user objects. Ask a **Snowflake / platform admin** (often `SECURITYADMIN` or whoever manages users) to run the same `ALTER USER ... set rsa_public_key = '...'` for your login, using the value from `snowflake_rsa_key_singleline.txt`. Do **not** send the private key (`.p8`).

3. If your login name differs from `JEEHAE.LEE`, use the same `user` value as in `connections.toml` in the `ALTER USER` statement the admin runs.

4. Test JWT (no browser):

   ```bash
   snow connection test --connection DOORDASH-DOORDASH-JWT
   ```

5. Optional — make JWT the default so you do not need `--connection`:

   Edit `~/.snowflake/config.toml` and set:

   ```toml
   default_connection_name = "DOORDASH-DOORDASH-JWT"
   ```

## Daily use

- **Default JWT:** `snow sql -q "select current_user();"`
- **Explicit connection:** `snow sql --connection DOORDASH-DOORDASH-JWT -q "..."`
- **Helper script:** `ca-geo-weather-daily/scripts/run_snow_query.sh` defaults to **`DOORDASH-DOORDASH`** (Okta / cached session). For JWT: `SNOWFLAKE_CONNECTION=DOORDASH-DOORDASH-JWT`.

## Encrypted private key (optional)

If you regenerate the key with a passphrase, set:

```bash
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='your-passphrase'
```

(or use the mechanism your Snowflake CLI version documents for encrypted PKCS#8).

## Security

- Treat `snowflake_rsa_key.p8` like a password: **do not** commit it or sync it to shared drives.
- If the key is exposed, remove it in Snowflake (`alter user ... unset rsa_public_key;`), generate a new pair, and register the new public key.

## If your org disables key-pair auth

You will need **SSO + cached token** (`client_store_temporary_credential` on the browser connection) or another approved non-interactive method (e.g. workload identity), per IT / Snowflake admin policy.
