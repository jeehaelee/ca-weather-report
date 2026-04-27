from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Sequence

from ca_geo_weather.dayparts import DAYPARTS, col_name_for_date_and_daypart
from ca_geo_weather.weather import (
    EVENT_TYPES,
    build_event_daypart_flags_for_geo,
    forecast_date_range,
)


@dataclass(frozen=True)
class SubmarketRow:
    submarket_id: str
    submarket_name: str
    geo_key: str


def load_submarket_map(path: Path) -> list[SubmarketRow]:
    rows: list[SubmarketRow] = []
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # tolerate header casing
            def g(*names: str) -> str:
                for n in names:
                    if n in row and row[n] is not None and str(row[n]).strip() != "":
                        return str(row[n]).strip()
                return ""

            sid = g("submarket_id", "sm_id", "SM_ID")
            sname = g("submarket_name", "sm_name", "SM_NAME", "name")
            gk = g("geo_key", "geo", "GEO_KEY")
            if not sid or not gk:
                continue
            rows.append(SubmarketRow(submarket_id=sid, submarket_name=sname, geo_key=gk))
    return rows


def column_names(dates: Sequence[date]) -> list[str]:
    cols: list[str] = []
    for d in dates:
        for dp in DAYPARTS:
            cols.append(col_name_for_date_and_daypart(d, dp))
    return cols


def build_event_csvs(
    submarkets: list[SubmarketRow],
    geo_to_series: dict[str, object],
) -> dict[str, str]:
    """
    Returns event_type -> CSV string.
    Only includes SMs with at least one 1 in that event's matrix.
    """
    from ca_geo_weather.weather import GeoSeries

    dates = forecast_date_range([g for g in geo_to_series.values() if isinstance(g, GeoSeries)])
    if not dates:
        return {}
    col_names = column_names(dates)

    # Precompute per-geo per-event flags: (d,dp) -> 1
    geo_event_flags: dict[str, dict[str, dict[str, int]]] = {}
    for gk, series in geo_to_series.items():
        if not isinstance(series, GeoSeries):
            continue
        by_ev: dict[str, dict[str, int]] = {e: {c: 0 for c in col_names} for e in EVENT_TYPES}
        fl = build_event_daypart_flags_for_geo(series)
        for (ev, d, dp), v in fl.items():
            cname = col_name_for_date_and_daypart(d, dp)
            if cname in by_ev.get(ev, {}):
                by_ev[ev][cname] = max(by_ev[ev][cname], v)
        geo_event_flags[gk] = by_ev

    out: dict[str, str] = {}
    for ev in EVENT_TYPES:
        buf = io.StringIO()
        w = csv.writer(buf)
        header = ["submarket_id", "submarket_name", "event"] + col_names
        w.writerow(header)
        any_row = False
        for sm in submarkets:
            if sm.geo_key not in geo_event_flags:
                continue
            cell = geo_event_flags[sm.geo_key].get(ev, {c: 0 for c in col_names})
            row_vals = [str(cell.get(c, 0) or 0) for c in col_names]
            if not any(x == "1" for x in row_vals):
                continue
            w.writerow([sm.submarket_id, sm.submarket_name, ev] + row_vals)
            any_row = True
        if any_row:
            out[ev] = buf.getvalue()
    return out
