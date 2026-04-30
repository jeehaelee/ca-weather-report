from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from ca_geo_weather.csv_export import build_event_csvs, load_submarket_map
from ca_geo_weather.geo_key_resolve import resolve_submarket_rows
from ca_geo_weather.email_send import build_preheader_from_series, send_report
from ca_geo_weather.report import build_body_text, preheader_line, subject_line
from ca_geo_weather.weather import default_data_dir, fetch_open_meteo, load_geo_centroids
from ca_geo_weather.weather import GeoSeries

LA = ZoneInfo("America/Los_Angeles")
SOURCE_LABEL = "Open-Meteo (ECMWF/GFS-based hourly; WMO weather codes)"


def _los_angeles_is_dst(now_la: datetime) -> bool:
    """True during daylight saving (PDT, UTC-7); False in standard time (PST, UTC-8)."""
    off = now_la.utcoffset()
    if off is None:
        return False
    # PST ≈ -28800s, PDT ≈ -25200s; use midpoint so fractional offsets still classify.
    return off.total_seconds() > -27000.0


def _should_send_scheduled_report() -> bool:
    """
    Scheduled runs use two UTC crons so one aligns with ~9am PT in PST vs PDT.
    GitHub often starts scheduled jobs tens of minutes late; a strict LA hour==9
    gate then skips every time. We use github.event.schedule (via CA_GEO_SCHEDULE)
    plus LA clock so only the correct seasonal slot can send, with slack for delay.
    """
    cron = (os.environ.get("CA_GEO_SCHEDULE") or "").strip()
    parts = cron.split()
    utc_hour = datetime.now(timezone.utc).hour
    now_la = datetime.now(LA)
    la_hour = now_la.hour
    dst = _los_angeles_is_dst(now_la)

    if len(parts) >= 2 and parts[1] == "16":
        if dst:
            # PDT: 16 UTC targets ~9am PT; allow drift into late morning.
            return 16 <= utc_hour <= 22 and 9 <= la_hour <= 12
        # PST: 16 UTC is ~8am PT — not our send window.
        return False
    if len(parts) >= 2 and parts[1] == "17":
        if dst:
            # PDT: 17 UTC is ~10am PT — wrong seasonal slot; avoid duplicate email.
            return False
        # PST: 17 UTC targets ~9am PT; allow drift.
        return 17 <= utc_hour <= 23 and 9 <= la_hour <= 12

    # Local / legacy: original strict 9am hour
    return la_hour == 9


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def run() -> int:
    ap = argparse.ArgumentParser(description="CA geo weather daily email + CSVs")
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Directory with geo_centroids.json and submarket_region_map.csv",
    )
    ap.add_argument(
        "--skip-9am-gate",
        action="store_true",
        help="Send even if local time is not 9:00am PT (for manual testing).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print report to stdout; do not send email.",
    )
    args = ap.parse_args()

    data_dir = (args.data_dir or default_data_dir()).resolve()
    centroids_path = data_dir / "geo_centroids.json"
    sm_path = data_dir / "submarket_region_map.csv"

    if not centroids_path.is_file():
        print(f"Missing {centroids_path}", file=sys.stderr)
        return 2
    if not sm_path.is_file():
        print(f"Missing {sm_path}", file=sys.stderr)
        return 2

    gate = os.environ.get("CA_GEO_9AM_GATE", "1").strip().lower() in ("1", "true", "yes")
    if gate and not args.skip_9am_gate and not _env_bool("CA_GEO_DRY_RUN") and not args.dry_run:
        if not _should_send_scheduled_report():
            sched = (os.environ.get("CA_GEO_SCHEDULE") or "").strip()
            print(
                f"Skip send: outside scheduled PT window (now {datetime.now(LA).isoformat(timespec='seconds')}, "
                f"CA_GEO_SCHEDULE={sched!r})",
                file=sys.stderr,
            )
            return 0

    centroids = load_geo_centroids(centroids_path)
    raw_sm = load_submarket_map(sm_path)
    if not raw_sm:
        print(
            "submarket_region_map.csv has no data rows — SM-level CSVs will be skipped. "
            "Run sql/build_submarket_region_map.sql in Snowflake (see README), export CSV, and add rows. "
            "Forecasts for the email will use all geos in geo_centroids.json.",
            file=sys.stderr,
        )
        submarkets = []
        need_keys: set[str] = set(centroids.keys())
    else:
        submarkets, skipped = resolve_submarket_rows(raw_sm, centroids)
        for line in skipped[:40]:
            print(f"Skip (unmapped geo_key): {line}", file=sys.stderr)
        if len(skipped) > 40:
            print(f"... and {len(skipped) - 40} more unmapped rows", file=sys.stderr)
        if not submarkets:
            print(
                "No rows left after geo resolution — SM CSVs skipped; email uses all geos in geo_centroids.json.",
                file=sys.stderr,
            )
            need_keys = set(centroids.keys())
        else:
            need_keys = {r.geo_key for r in submarkets}
    # Parallel fetches: many geos × CI network flakiness. GitHub egress shares IPs — too much
    # concurrency can slow Open-Meteo enough to hit read timeouts. Default to 1 worker on GHA.
    _def_workers = "1" if os.environ.get("GITHUB_ACTIONS", "").lower() == "true" else "3"
    workers = min(8, max(1, int(os.environ.get("CA_GEO_FETCH_WORKERS", _def_workers))))
    series_by_key: dict[str, GeoSeries] = {}
    keys = sorted(need_keys)

    def _fetch(gk: str) -> tuple[str, GeoSeries]:
        return gk, fetch_open_meteo(centroids[gk])

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_fetch, gk): gk for gk in keys}
        for fut in as_completed(futs):
            gk, series = fut.result()
            series_by_key[gk] = series

    all_series = list(series_by_key.values())
    run_date = datetime.now(LA).date()
    if all_series:
        pht = build_preheader_from_series(SOURCE_LABEL, all_series)
    else:
        pht = preheader_line(SOURCE_LABEL, run_date, run_date)

    # Geos with any inclement condition in the hourly series
    affected_geo_keys: list[str] = []
    for gk, gs in series_by_key.items():
        if any(h.events for h in gs.hours):
            affected_geo_keys.append(gk)
    n_markets = len(affected_geo_keys)
    subj = subject_line(run_date, n_markets)

    body_main, _ordered = build_body_text(centroids, series_by_key)
    if n_markets == 0:
        body_main = (
            "=" * 80
            + "\n"
            + "CALIFORNIA GEO WEATHER — NEXT 7 DAYS\n"
            + "=" * 80
            + "\n\n"
            + "No inclement conditions in the 7-day forecast for mapped geos "
            f"(rain / snow / sleet / thunder). Run date: {run_date} PT.\n"
        )

    no_pre = _env_bool("CA_GEO_NO_PREHEADER")
    if no_pre:
        body = body_main
    else:
        body = pht + "\n\n" + body_main

    csvs = build_event_csvs(
        submarkets,
        {k: v for k, v in series_by_key.items()},
    )
    all_csvs = dict(csvs)

    dry = args.dry_run or _env_bool("CA_GEO_DRY_RUN")
    if dry:
        print("SUBJECT:", subj)
        print("---")
        print(body)
        print("--- CSVS ---")
        for ev, c in sorted(all_csvs.items()):
            print(f"### {ev}.csv\n{c[:2000]}{'...' if len(c) > 2000 else ''}")
        return 0

    smtp_host = (os.environ.get("SMTP_HOST") or "").strip()
    smtp_port = int((os.environ.get("SMTP_PORT") or "").strip() or "587")
    smtp_user = (os.environ.get("SMTP_USER") or "").strip()
    smtp_pass = (os.environ.get("SMTP_PASSWORD") or os.environ.get("SMTP_PASS") or "").strip()
    mail_from = (os.environ.get("MAIL_FROM") or smtp_user or "").strip()
    mail_to = (os.environ.get("MAIL_TO") or "jeehae.lee@doordash.com").strip()

    if not smtp_host or not mail_from or not mail_to:
        print(
            "Missing SMTP: set SMTP_HOST, MAIL_FROM, MAIL_TO (and SMTP_USER/SMTP_PASSWORD as needed).",
            file=sys.stderr,
        )
        return 2

    try:
        send_report(
            run_date=run_date,
            body=body,
            preheader="" if no_pre else pht,
            subj=subj,
            csv_by_event=all_csvs,
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_user=smtp_user,
            smtp_password=smtp_pass,
            mail_from=mail_from,
            mail_to=mail_to,
            use_tls=not _env_bool("SMTP_NO_TLS", False),
        )
    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        print(f"Failed to send email: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
