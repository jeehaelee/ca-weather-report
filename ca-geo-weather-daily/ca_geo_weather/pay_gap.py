from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from ca_geo_weather.csv_export import SubmarketRow
from ca_geo_weather.dayparts import DAYPARTS, Daypart
from ca_geo_weather.weather import (
    EVENT_TYPES,
    GeoCentroid,
    GeoSeries,
    build_event_daypart_flags_for_geo,
)

# CSV: starting_point_id, submarket_id, sp_name (from maindb_starting_point export)
# Pay snapshot: from weather_pay_experiment_v1 snapshot SQL (see sql/)


@dataclass(frozen=True)
class StartingPointRec:
    starting_point_id: str
    submarket_id: str
    sp_name: str


@dataclass(frozen=True)
class PayGapRow:
    starting_point_id: str
    sp_name: str
    submarket_id: str
    submarket_name: str
    forecast_date: date
    daypart: str
    reason: str


def _cell(row: dict[str, str], *names: str) -> str:
    for n in names:
        for k, v in row.items():
            if k and k.lower() == n.lower() and v is not None and str(v).strip() != "":
                return str(v).strip()
    for n in names:
        if n in row and row[n] is not None and str(row[n]).strip() != "":
            return str(row[n]).strip()
    return ""


def _parse_date(s: str) -> date | None:
    t = str(s).strip()[:10]
    if not t or t.lower() in ("none", "nat"):
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(t, fmt).date()
        except ValueError:
            continue
    try:
        return date.fromisoformat(t)
    except ValueError:
        return None


_WHITESPACE = re.compile(r"[\s_]+")


def normalize_daypart(s: str) -> Daypart | None:
    """Map experiment / free-text daypart to our internal Daypart."""
    t = _WHITESPACE.sub("_", s.strip().lower())
    t = t.replace("late_night", "latenight").replace("late-night", "latenight")
    aliases: dict[str, Daypart] = {
        "early_morning": "Early_Morning",
        "early": "Early_Morning",
        "breakfast": "Breakfast",
        "lunch": "Lunch",
        "snack": "Snack",
        "dinner": "Dinner",
        "latenight": "Latenight",
        "late_night": "Latenight",
    }
    if t in aliases:
        return aliases[t]
    for dp in DAYPARTS:
        if t == dp.lower():
            return dp
    # "Breakfast (5am-11am)" -> breakfast
    head = t.split("(")[0].strip()
    if head in aliases:
        return aliases[head]
    return None


def load_starting_point_map(path: Path) -> list[StartingPointRec]:
    text = path.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    hline, *rest = lines[0], lines[1:]
    if "," in hline and rest and "\t" in (rest[0] or ""):
        header_keys = [x.strip() for x in hline.split(",")]
        rows: list[dict[str, str]] = []
        for line in rest:
            parts = [p.strip() for p in line.split("\t")]
            if len(parts) < len(header_keys):
                continue
            rows.append(dict(zip(header_keys, parts)))
    else:
        first = lines[0]
        delim = "\t" if first.count("\t") >= 1 else ","
        with path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f, delimiter=delim))

    out: list[StartingPointRec] = []
    for row in rows:
        if not any(row.values()):
            continue
        spid = _cell(row, "starting_point_id", "STARTING_POINT_ID", "sp_id", "id")
        smid = _cell(row, "submarket_id", "SUBMARKET_ID", "submarketid")
        name = _cell(row, "sp_name", "name", "SP_NAME", "starting_point_name")
        if not spid or not smid:
            continue
        out.append(StartingPointRec(starting_point_id=spid, submarket_id=smid, sp_name=name))
    return out


def load_pay_coverage_keys(path: Path) -> set[tuple[str, date, Daypart]]:
    """
    (starting_point_id, forecast_date, daypart) for rows that already have pay set.
    """
    text = path.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return set()
    hline = lines[0]
    if "," in hline and len(lines) > 1 and "\t" in lines[1] and "\t" not in hline:
        header_keys = [x.strip() for x in hline.split(",")]
        dicts: list[dict[str, str]] = []
        for line in lines[1:]:
            parts = [p.strip() for p in line.split("\t")]
            if len(parts) < len(header_keys):
                continue
            dicts.append(dict(zip(header_keys, parts)))
    else:
        first = lines[0]
        delim = "\t" if first.count("\t") >= 1 else ","
        with path.open(newline="", encoding="utf-8") as f:
            dicts = list(csv.DictReader(f, delimiter=delim))

    cov: set[tuple[str, date, Daypart]] = set()
    for row in dicts:
        if not any(row.values()):
            continue
        spid = _cell(row, "STARTING_POINT_ID", "starting_point_id", "sp_id")
        fd = _cell(row, "forecast_date", "FORECAST_DATE")
        dp_raw = _cell(row, "daypart", "DAYPART")
        wof = _cell(
            row,
            "WEATHER_OPT_FINAL",
            "weather_opt_final",
            "weatheroptfinal",
        )
        if not spid or not fd or not dp_raw:
            continue
        d = _parse_date(fd)
        if d is None:
            continue
        dp = normalize_daypart(dp_raw)
        if dp is None:
            continue
        # Treated as covered if a row exists and weather_opt_final is set (not blank / not zero if numeric)
        if wof in ("", "0", "0.0", "null", "None"):
            continue
        if wof.lower() in ("0", "nan"):
            continue
        cov.add((spid, d, dp))
    return cov


def _sm_name_by_id(subs: list[SubmarketRow]) -> dict[str, str]:
    m: dict[str, str] = {}
    for s in subs:
        m[str(s.submarket_id)] = s.submarket_name
    return m


def build_submarket_date_daypart_needs(
    submarkets: list[SubmarketRow],
    series_by_key: dict[str, GeoSeries],
) -> dict[str, set[tuple[date, Daypart]]]:
    """
    Per submarket_id, set of (date, daypart) where *any* tracked event
    (rain/sleet/snow/thunder) is on — used to line up with pay uploads.
    """
    # geo -> (date, daypart) with any event
    by_geo: dict[str, set[tuple[date, Daypart]]] = {}
    for gk, gs in series_by_key.items():
        need: set[tuple[date, Daypart]] = set()
        fl = build_event_daypart_flags_for_geo(gs)
        for (ev, d, dp), v in fl.items():
            if v and ev in EVENT_TYPES:
                need.add((d, dp))
        by_geo[gk] = need

    out: dict[str, set[tuple[date, Daypart]]] = {}
    for sm in submarkets:
        gk = sm.geo_key
        if gk not in by_geo:
            continue
        sid = str(sm.submarket_id)
        if sid not in out:
            out[sid] = set()
        out[sid] |= by_geo[gk]
    return out


def compute_pay_gaps(
    submarkets: list[SubmarketRow],
    series_by_key: dict[str, GeoSeries],
    sp_list: list[StartingPointRec],
    coverage: set[tuple[str, date, Daypart]],
) -> list[PayGapRow]:
    need_by_sm = build_submarket_date_daypart_needs(submarkets, series_by_key)
    sm_name = _sm_name_by_id(submarkets)
    sps_by_sm: dict[str, list[StartingPointRec]] = {}
    for r in sp_list:
        sps_by_sm.setdefault(r.submarket_id, []).append(r)

    gaps: list[PayGapRow] = []
    for sm_id, cells in need_by_sm.items():
        sps = sps_by_sm.get(str(sm_id), [])
        sname = sm_name.get(str(sm_id), "")
        for d, dp in sorted(cells):
            for sp in sps:
                if (sp.starting_point_id, d, dp) in coverage:
                    continue
                gaps.append(
                    PayGapRow(
                        starting_point_id=sp.starting_point_id,
                        sp_name=sp.sp_name,
                        submarket_id=sm_id,
                        submarket_name=sname,
                        forecast_date=d,
                        daypart=dp,
                        reason="forecasted inclement in this daypart; no matching pay row in snapshot",
                    )
                )
    gaps.sort(
        key=lambda g: (g.submarket_id, g.forecast_date, g.daypart, g.starting_point_id)
    )
    return gaps


def format_pay_gap_email_section(gaps: list[PayGapRow]) -> str:
    """3b/3c narrative block."""
    lines: list[str] = [
        "",
        "=" * 80,
        "PAY GAP — starting points (vs WEATHER_PAY_EXPERIMENT_V1 snapshot)",
        "=" * 80,
        "",
    ]
    if not gaps:
        lines.extend(
            [
                "3b) No pay gaps: for every forecasted inclement (date, daypart) on covered "
                "submarkets, every starting point in the map had a snapshot row with "
                "weather_opt_final set, or there were no SPs under those submarkets in the map file.",
                "",
            ]
        )
        return "\n".join(lines) + "\n"
    from collections import defaultdict

    by_sm: dict[str, list[PayGapRow]] = defaultdict(list)
    for g in gaps:
        by_sm[g.submarket_id].append(g)

    lines.append(
        f"3b) Submarkets with at least one starting point still missing pay for a "
        f"forecasted daypart: {len(by_sm)}"
    )
    lines.append(
        f"3c) {len(gaps)} SP + date + daypart combinations in the attached CSV (sp_missing_weather_pay.csv)."
    )
    lines.append("")
    for sm_id in sorted(by_sm.keys(), key=str):
        gg = by_sm[sm_id]
        n_sp = len({g.starting_point_id for g in gg})
        nm = gg[0].submarket_name or sm_id
        lines.append(f"  • {nm} (submarket {sm_id}): {n_sp} SPs with ≥1 missing window — {len(gg)} rows in CSV")
    lines.append("")
    return "\n".join(lines) + "\n"


def build_sp_missing_pay_csv(gaps: list[PayGapRow]) -> str:
    """CSV for 3c."""
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(
        [
            "starting_point_id",
            "sp_name",
            "submarket_id",
            "submarket_name",
            "forecast_date",
            "daypart",
            "reason",
        ]
    )
    for g in gaps:
        w.writerow(
            [
                g.starting_point_id,
                g.sp_name,
                g.submarket_id,
                g.submarket_name,
                g.forecast_date.isoformat(),
                g.daypart,
                g.reason,
            ]
        )
    return out.getvalue()


def sps_by_sm_index(sp_list: list[StartingPointRec]) -> dict[str, list[StartingPointRec]]:
    """Convenience index (submarket_id -> starting points) for call sites."""
    m: dict[str, list[StartingPointRec]] = {}
    for r in sp_list:
        m.setdefault(r.submarket_id, []).append(r)
    return m
