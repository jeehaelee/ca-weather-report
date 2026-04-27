from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime

from ca_geo_weather.weather import EVENT_TYPES, GeoCentroid, GeoSeries, HourlyState


def _fmt_ampm(hour: int) -> str:
    return datetime(2000, 1, 1, hour, 0, 0).strftime("%I:%M %p").lstrip("0")


def _merge_hours_for_event(hours: list[HourlyState], event: str) -> dict[date, list[tuple[int, int]]]:
    """Per date, list of (start_hour, end_hour_exclusive) contiguous ranges for `event`."""
    hs = [x for x in hours if event in x.events]
    hs.sort(key=lambda x: (x.ldh.d, x.ldh.hour))
    by_date: dict[date, list[int]] = defaultdict(list)
    for x in hs:
        by_date[x.ldh.d].append(x.ldh.hour)
    ranges: dict[date, list[tuple[int, int]]] = {}
    for d, hrs in by_date.items():
        hrs = sorted(set(hrs))
        if not hrs:
            continue
        seg: list[tuple[int, int]] = []
        start = hrs[0]
        prev = hrs[0]
        for h in hrs[1:]:
            if h == prev + 1:
                prev = h
            else:
                seg.append((start, prev + 1))
                start = h
                prev = h
        seg.append((start, prev + 1))
        ranges[d] = seg
    return ranges


def _range_line(d: date, start_h: int, end_h_exc: int) -> str:
    """end_h_exc: model hour after last included hour (so display end is that clock time)."""
    ds = d.strftime("%a %m/%d")
    st = _fmt_ampm(start_h)
    # display exclusive end at end_h_exc o'clock
    et = _fmt_ampm(end_h_exc % 24)
    return f"    • {ds}: {st} – {et} PT"


def build_body_text(
    geos: dict[str, GeoCentroid],
    series_by_key: dict[str, GeoSeries],
) -> tuple[str, list[str]]:
    """
    Returns (body, ordered_geo_keys_with_bad_weather).
    Only geos with at least one inclement hour are included.
    """
    bad_keys: list[str] = []
    blocks: list[str] = []

    def severity_score(gs: GeoSeries) -> int:
        return sum(len(h.events) for h in gs.hours)

    keys_sorted = sorted(
        series_by_key.keys(),
        key=lambda k: (-severity_score(series_by_key[k]), geos[k].label.lower()),
    )

    for gk in keys_sorted:
        gs = series_by_key[gk]
        if not any(h.events for h in gs.hours):
            continue
        bad_keys.append(gk)
        g = geos[gk]
        lines: list[str] = [
            "-" * 80,
            g.label.upper(),
            "-" * 80,
            "Status: ALERT",
            "",
        ]
        # stable order of events as they appear in forecast
        seen_events = [e for e in EVENT_TYPES if any(e in h.events for h in gs.hours)]
        for ei, ev in enumerate(seen_events, start=1):
            lines.append(f"  Event {ei} — {ev.title()}")
            rmap = _merge_hours_for_event(gs.hours, ev)
            if not rmap:
                lines.append("    • (no hourly detail)")
                lines.append("")
                continue
            for d in sorted(rmap.keys()):
                for sh, eh in rmap[d]:
                    lines.append(_range_line(d, sh, eh))
            lines.append("")
        blocks.append("\n".join(lines).rstrip())

    summary_lines: list[str] = [
        "=" * 80,
        "CALIFORNIA GEO WEATHER — NEXT 7 DAYS",
        "=" * 80,
        "",
        "Summary",
        "-------",
        f"• Geos with inclement conditions in window: {len(bad_keys)}",
    ]
    if bad_keys:
        top = bad_keys[0]
        summary_lines.append(
            f"• Highest concern: {geos[top].label} — see hourly ranges below."
        )
    summary_lines.extend(["", "Legend", "------", "Types: Rain, Snow, Sleet, Thunder (WMO weather codes; wind alerts excluded).", ""])
    body = "\n".join(summary_lines) + "\n" + "\n\n".join(blocks) + "\n"
    return body, bad_keys


def subject_line(run_date: date, n_geos: int) -> str:
    mdy = f"{run_date.month}/{run_date.day}/{str(run_date.year)[-2:]}"
    return f"{mdy} CA geo report - {n_geos} markets with bad weather"


def preheader_line(source: str, window_start: date, window_end: date) -> str:
    now = datetime.now().astimezone()
    ts = now.strftime("%I:%M %p %Z").lstrip("0")
    return (
        f"Generated ~{ts} · Source: {source} · "
        f"Forecast window: {window_start:%Y-%m-%d} through {window_end:%Y-%m-%d}"
    )
