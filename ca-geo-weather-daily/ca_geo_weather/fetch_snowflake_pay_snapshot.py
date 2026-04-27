"""
Download weather pay experiment rows from Snowflake into data/weather_pay_experiment_snapshot.csv.

Used in CI when Snowflake credentials are in the environment. If SNOWFLAKE_ACCOUNT is unset,
exits 0 without doing anything (local runs can rely on a committed or manual CSV).

Auth (pick one):
  - Password: SNOWFLAKE_USER + SNOWFLAKE_PASSWORD
  - Key pair: SNOWFLAKE_USER + SNOWFLAKE_PRIVATE_KEY (PEM) or SNOWFLAKE_PRIVATE_KEY_B64
"""

from __future__ import annotations

import argparse
import base64
import csv
import os
import sys
from pathlib import Path

# Package root: ca-geo-weather-daily/
_PKG_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_SQL = _PKG_ROOT / "sql" / "weather_pay_experiment_v1_snapshot.sql"
_DEFAULT_OUT = "weather_pay_experiment_snapshot.csv"


def _connect():
    import snowflake.connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    account = os.environ.get("SNOWFLAKE_ACCOUNT", "").strip()
    user = os.environ.get("SNOWFLAKE_USER", "").strip()
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "").strip()
    role = os.environ.get("SNOWFLAKE_ROLE", "").strip()

    if not account or not user or not warehouse:
        raise SystemExit(
            "SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_WAREHOUSE are required for Snowflake fetch."
        )

    kwargs: dict = {
        "user": user,
        "account": account,
        "warehouse": warehouse,
    }
    if role:
        kwargs["role"] = role

    pem_b64 = os.environ.get("SNOWFLAKE_PRIVATE_KEY_B64", "").strip()
    pem = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "").strip()
    if pem_b64:
        pem = base64.b64decode(pem_b64).decode("utf-8")
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip() or None

    password = os.environ.get("SNOWFLAKE_PASSWORD", "").strip()

    if pem:
        p_key = serialization.load_pem_private_key(
            pem.encode("utf-8"),
            password=passphrase.encode("utf-8") if passphrase else None,
            backend=default_backend(),
        )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        kwargs["private_key"] = pkb
    elif password:
        kwargs["password"] = password
    else:
        raise SystemExit(
            "Set either SNOWFLAKE_PASSWORD (password auth) or SNOWFLAKE_PRIVATE_KEY / "
            "SNOWFLAKE_PRIVATE_KEY_B64 (key-pair auth)."
        )

    return snowflake.connector.connect(**kwargs)


def run_fetch(sql_path: Path, out_path: Path) -> None:
    sql = sql_path.read_text(encoding="utf-8")
    # Strip a trailing semicolon if present (executemany is fine either way)
    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()

    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            col_names = [c[0].upper() for c in cur.description] if cur.description else []
        finally:
            cur.close()
    finally:
        conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(col_names)
        for r in rows:
            w.writerow(r)


def main() -> int:
    if not os.environ.get("SNOWFLAKE_ACCOUNT", "").strip():
        print("fetch_snowflake_pay_snapshot: SNOWFLAKE_ACCOUNT not set; skipping (use committed CSV).", file=sys.stderr)
        return 0

    ap = argparse.ArgumentParser(description="Export weather pay snapshot from Snowflake to CSV")
    ap.add_argument("--sql", type=Path, default=_DEFAULT_SQL, help="Path to snapshot SQL")
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output CSV path (default: <data-dir>/" + _DEFAULT_OUT + ")",
    )
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Data directory (default: CA_GEO_DATA_DIR or package data/)",
    )
    args = ap.parse_args()

    from ca_geo_weather.weather import default_data_dir

    data_dir = args.data_dir
    if data_dir is None:
        env_dd = os.environ.get("CA_GEO_DATA_DIR", "").strip()
        data_dir = Path(env_dd) if env_dd else default_data_dir()
    out = args.out if args.out is not None else data_dir / _DEFAULT_OUT

    if not args.sql.is_file():
        print(f"SQL file not found: {args.sql}", file=sys.stderr)
        return 1

    run_fetch(args.sql, out)
    print(f"Wrote {out} ({os.path.getsize(out)} bytes)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
