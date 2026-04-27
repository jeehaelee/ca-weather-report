from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ca_geo_weather.weather import GeoCentroid

# Human / export labels -> canonical key in geo_centroids.json (only where label match is not enough)
_GEO_KEY_ALIASES: dict[str, str] = {
    "east la": "los_angeles",
    "lax airport (pickup only)": "la_valley",
}


def _strip_noise(s: str) -> str:
    s = s.strip()
    if not s:
        return s
    # "Princeton, CA – don't use" / dash variants
    s = re.sub(r"\s*[–—-]\s*don'?t use\s*$", "", s, flags=re.IGNORECASE).strip()
    if s.upper() in ("CA", "N/A", "NA", "-", ""):
        return ""
    return s


def _build_label_index(centroids: dict[str, "GeoCentroid"]) -> dict[str, str]:
    """Lowercased key or label -> canonical geo_key."""
    index: dict[str, str] = {}
    for k, c in centroids.items():
        index[k.lower()] = k
        index[c.label.lower()] = k
    return index


def resolve_geo_key(raw: str, centroids: dict[str, "GeoCentroid"]) -> str | None:
    """
    Map a cell from submarket_region_map (display name, slug, or note) to a key in geo_centroids.json.
    """
    t = _strip_noise(raw)
    if not t:
        return None
    tl = t.lower()
    if _GEO_KEY_ALIASES.get(tl):
        return _GEO_KEY_ALIASES[tl]
    index = _build_label_index(centroids)
    if tl in index:
        return index[tl]
    # e.g. "los_angeles" with wrong case already handled; try space vs underscore
    unders = re.sub(r"\s+", "_", tl)
    if unders in centroids:
        return unders
    if unders in index:
        return index[unders]
    return None


def resolve_submarket_rows(
    rows: list,
    centroids: dict[str, "GeoCentroid"],
) -> tuple[list, list[str]]:
    """
    Return (resolved SubmarketRow list, skip log lines for stderr).
    """
    from ca_geo_weather.csv_export import SubmarketRow

    out: list[SubmarketRow] = []
    skips: list[str] = []
    for r in rows:
        gk = resolve_geo_key(r.geo_key, centroids)
        if gk is None:
            skips.append(f"submarket_id={r.submarket_id!r} geo={r.geo_key!r}")
            continue
        out.append(SubmarketRow(r.submarket_id, r.submarket_name, gk))
    return out, skips
