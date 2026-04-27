# CA geo daily weather email

Sends a daily email (~9:00 AM **America/Los_Angeles**) with a 7-day, hourly-based forecast for California “geo” markets and attaches **one CSV per event** — **`rain`**, **`snow`**, **`sleet`**, **`thunder`** only — with `submarket_id`, `submarket_name`, and `1`/`0` per `YYYY-MM-DD__<Daypart>` column when that submarket’s mapped geo is expected to see that event in that daypart.

**High wind is not included** (no wind-only alerts in the email or attachments).

- **Weather source:** [Open-Meteo](https://open-meteo.com/) (open data; hourly WMO-style weather codes).
- **Submarket mapping:** `data/submarket_region_map.csv` — **use `fact_region` in Snowflake/EDW as the source of truth** to map each `submarket_id` + name to a `geo_key` from `data/geo_centroids.json`. See **`sql/build_submarket_region_map.sql`** (includes a runnable query that lists real CA ESM submarket IDs + names from `maindb_submarket`, and a commented template that joins `fact_region`).
- **Representative point:** each geo uses one lat/lon in `data/geo_centroids.json` (all SMs sharing a geo get the same forecast until you model per-SM points).

If `submarket_region_map.csv` has **only a header** (no data rows), the job still runs: the **email** uses every geo in `geo_centroids.json`, and **no SM-level CSVs** are attached until you add rows.

### Optional: pay gap (starting points vs weather pay experiment)

**3a** — Unchanged: submarket / event CSVs (rain, snow, sleet, thunder) for forecasted bad weather.  
**3b / 3c** — If you add two daily files under `data/`, the report also lists **starting points (SP) missing pay** for a forecasted inclement daypart, compared to `PRODDB.STATIC.WEATHER_PAY_EXPERIMENT_V1` (latest `run_date`, `add_weather_pay = 'yes'`), and attaches **`sp_missing_weather_pay.csv`**.

1. `starting_point_to_submarket.csv` — from **`sql/export_starting_point_to_submarket.sql`** (join `maindb_starting_point` → ESM SMs).  
2. `weather_pay_experiment_snapshot.csv` — from **`sql/weather_pay_experiment_v1_snapshot.sql`** (aligned with the ops team’s pay upload query).

**Logic:** for each (submarket, `forecast_date`, `daypart`) where the report expects any of rain/sleet/snow/thunder, every SP in that submarket in file (1) should have a row in file (2) with a non-blank, non-zero `weather_opt_final` for the same SP + date + daypart. Missing rows become **3c** lines and the narrative **3b** block.

- **SP-level forecast** (finer than SM) is a future step; today we align pay checks to the same dayparts the weather CSV uses, using SM→SP mapping.  
- Set `CA_GEO_PAY_GAP_VERBOSE=1` in GitHub Actions to log when pay-gap files are missing.

## Quick start (local)

```bash
cd ca-geo-weather-daily
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export CA_GEO_9AM_GATE=0
python -m ca_geo_weather.main --skip-9am-gate --dry-run
```

## Send a real email (local)

Set SMTP and recipients (example: Gmail/Workspace app password, or your corporate SMTP):

```bash
export SMTP_HOST=smtp.example.com
export SMTP_PORT=587
export SMTP_USER=you@example.com
export SMTP_PASSWORD=secret
export MAIL_FROM=you@example.com
export MAIL_TO=jeehae.lee@doordash.com
export CA_GEO_9AM_GATE=0
python -m ca_geo_weather.main --skip-9am-gate
```

`MAIL_TO` defaults to `jeehae.lee@doordash.com` if unset.

## GitHub Actions (daily)

1. Push this repo to GitHub (or add `ca-geo-weather-daily` + `.github/workflows/daily-ca-weather.yml` to your org repo).
2. In the repo, add **Actions secrets**:
   - `SMTP_HOST`, `SMTP_PORT` (optional; default 587 in code if unset/empty), `SMTP_USER`, `SMTP_PASSWORD` (or empty if your relay allows)
   - `MAIL_FROM`, `MAIL_TO` (e.g. `jeehae.lee@doordash.com`)
3. **Optional — fresh `weather_pay_experiment_snapshot.csv` from Snowflake** (recommended so you do not commit a daily CSV): if you add **Snowflake** secrets below, the workflow runs `python -m ca_geo_weather.fetch_snowflake_pay_snapshot` before the report and overwrites `data/weather_pay_experiment_snapshot.csv` on the runner. If `SNOWFLAKE_ACCOUNT` is not set, that step is a no-op and you can keep using a file in `data/` or skip pay gap entirely.  
   **What you can use (ask your Snowflake / security team which is allowed for a service-style user):**
   - **Key-pair (JWT)** — Often preferred for automation: a dedicated user with a **public key** registered in Snowflake and the **private key** stored only in GitHub secrets. Set `SNOWFLAKE_USER`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`, and optionally `SNOWFLAKE_ROLE`, plus **either** paste the PEM private key in `SNOWFLAKE_PRIVATE_KEY` **or** a base64-encoded file in `SNOWFLAKE_PRIVATE_KEY_B64`. If the key is encrypted, set `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`.  
   - **Password** — Simpler if your org allows a **bot user** with a password and **no** MFA (MFA blocks non-interactive logins). Set the same account/user/warehouse/role fields and `SNOWFLAKE_PASSWORD`.  
   The SQL that runs is `sql/weather_pay_experiment_v1_snapshot.sql` (same as a manual export). `SNOWFLAKE_ACCOUNT` is usually your org identifier (e.g. `xy12345` or `orgname-xy12345` with region; copy from the Snowflake login URL or your admin).  
4. Run **Actions → “CA geo weather daily” → Run workflow** once to confirm delivery. Scheduled runs use a **9:00am PT gate** so only the cron run that hits the 9:00 hour in LA sends; `workflow_dispatch` does **not** use that gate.

Workflow file: [`.github/workflows/daily-ca-weather.yml`](../.github/workflows/daily-ca-weather.yml).

## Real submarket IDs in the CSVs (required for SM-level attachments)

1. In Mode/Snowflake, run **PART A** in `sql/build_submarket_region_map.sql` to export **all CA earning-standard** `submarket_id` + `submarket_name` (real IDs from `geo_intelligence.public.maindb_submarket`).
2. Either merge a `geo_key` column in a spreadsheet using your business rules, or complete **PART B** in the same file: wire the `region_from_fr` CTE to your **`fact_region`** table, uncomment the big `SELECT`, tune the `CASE` to your `region_label` values, and export the three columns.
3. Save as CSV with header exactly: `submarket_id,submarket_name,geo_key` and overwrite `data/submarket_region_map.csv`, then commit and push (or set `CA_GEO_DATA_DIR` to a folder containing that file).

`data/submarket_region_map.example.csv` shows the expected shape.

## Environment variables

| Variable | Purpose |
|----------|--------|
| `CA_GEO_DATA_DIR` | Optional path to a folder with `geo_centroids.json` + `submarket_region_map.csv` (default: this package’s `data/`). |
| `CA_GEO_9AM_GATE` | `1` = only send if local **America/Los_Angeles** hour is **9** (scheduled runs). `0` = send whenever the job runs. |
| `CA_GEO_DRY_RUN` | `1` = print report, no email. |
| `CA_GEO_NO_PREHEADER` | `1` = omit the “Generated / Source / Forecast window” preheader from the top of the body. |
| `MAIL_TO` | Default `jeehae.lee@doordash.com`. |
| `SMTP_NO_TLS` | `1` = do not `STARTTLS` (rare). |
| `CA_GEO_PAY_GAP_VERBOSE` | `1` = stderr when pay-gap CSVs are not in `data/`. |
| `SNOWFLAKE_ACCOUNT` | If set, `fetch_snowflake_pay_snapshot` runs before the report. See [GitHub Actions](#github-actions-daily). |
| `SNOWFLAKE_USER` | Snowflake user for the fetch step. |
| `SNOWFLAKE_WAREHOUSE` | Warehouse to use for the snapshot query. |
| `SNOWFLAKE_ROLE` | Optional role. |
| `SNOWFLAKE_PASSWORD` | Password auth (alternative to private key). |
| `SNOWFLAKE_PRIVATE_KEY` or `SNOWFLAKE_PRIVATE_KEY_B64` | PEM or base64 private key for key-pair auth. |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | Optional, if the private key is encrypted. |
