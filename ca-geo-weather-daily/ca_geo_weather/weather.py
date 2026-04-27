from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

from ca_geo_weather.dayparts import Daypart, LocalDateHour, daypart_for_hour, parse_iso_local

# WMO-ish mapping (Open-Meteo / ECMWF weather codes)
# https://open-meteo.com/en/docs
# Note: high-wind-only alerts are intentionally excluded (no wind CSV / no wind in body).


def classify_hour(weathercode: int) -> set[str]:
    out: set[str] = set()
    if weathercode in (95, 96, 97, 98, 99):
        out.add("thunder")
    if weathercode in (56, 57, 66, 67):
        out.add("sleet")
    if weathercode in (71, 73, 75, 77, 85, 86):
        out.add("snow")
    if weathercode in (51, 53, 55, 61, 63, 65, 80, 81, 82):
        out.add("rain")
    if weathercode in (95, 96, 97, 98, 99):
        out.add("rain")
    return out


# Order used in email + CSV attachments (wind excluded by design)
EVENT_TYPES: tuple[str, ...] = ("rain", "sleet", "snow", "thunder")


@dataclass
class GeoCentroid:
    key: str
    label: str
    lat: float
    lon: float


def load_geo_centroids(path: Path) -> dict[str, GeoCentroid]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, GeoCentroid] = {}
    for key, v in raw.items():
        out[key] = GeoCentroid(key=key, label=str(v["label"]), lat=float(v["lat"]), lon=float(v["lon"]))
    return out


@dataclass
class HourlyState:
    """Per geo, per local hour — events active at that time."""

    geo_key: str
    time_local: str
    ldh: LocalDateHour
    events: set[str] = field(default_factory=set)


@dataclass
class GeoSeries:
    geo: GeoCentroid
    hours: list[HourlyState]


def fetch_open_meteo(geo: GeoCentroid) -> GeoSeries:
    base = "https://api.open-meteo.com/v1/forecast"
    params: dict[str, Any] = {
        "latitude": geo.lat,
        "longitude": geo.lon,
        "hourly": ["weathercode", "precipitation"],
        "forecast_days": 7,
        "timezone": "America/Los_Angeles",
    }
    r = requests.get(base, params=params, timeout=60)
    r.raise_for_status()
    j = r.json()
    h = j.get("hourly") or {}
    times: list[str] = list(h.get("time") or [])
    codes: list[int] = [int(x) for x in h.get("weathercode") or []]
    if not times or len(times) != len(codes):
        raise RuntimeError(f"Open-Meteo: unexpected hourly payload for {geo.key}")

    hours: list[HourlyState] = []
    for t, code in zip(times, codes):
        ldh = parse_iso_local(t)
        ev = classify_hour(code)
        hours.append(
            HourlyState(
                geo_key=geo.key,
                time_local=t,
                ldh=ldh,
                events=set(ev),
            )
        )
    return GeoSeries(geo=geo, hours=hours)


def forecast_date_range(geo_series: list[GeoSeries]) -> list[date]:
    if not geo_series:
        return []
    dmin: date | None = None
    dmax: date | None = None
    for g in geo_series:
        for hs in g.hours:
            if dmin is None or hs.ldh.d < dmin:
                dmin = hs.ldh.d
            if dmax is None or hs.ldh.d > dmax:
                dmax = hs.ldh.d
    if dmin is None or dmax is None:
        return []
    out: list[date] = []
    cur = dmin
    while cur <= dmax:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def build_event_daypart_flags_for_geo(geo: GeoSeries) -> dict[tuple[str, date, Daypart], int]:
    """(event, date, daypart) -> 1 if any hour in that local slot has the event."""
    flags: dict[tuple[str, date, Daypart], int] = {}
    for hs in geo.hours:
        for ev in hs.events:
            dp = daypart_for_hour(hs.ldh.hour)
            k = (ev, hs.ldh.d, dp)
            flags[k] = 1
    return flags


def default_data_dir() -> Path:
    env = os.environ.get("CA_GEO_DATA_DIR")
    if env:
        return Path(env).resolve()
    return (Path(__file__).resolve().parent.parent / "data").resolve()
