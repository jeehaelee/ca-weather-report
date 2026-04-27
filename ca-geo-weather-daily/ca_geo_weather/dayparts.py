from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

Daypart = Literal[
    "Early_Morning",
    "Breakfast",
    "Lunch",
    "Snack",
    "Dinner",
    "Latenight",
]

# Order for CSV / stable column order
DAYPARTS: tuple[Daypart, ...] = (
    "Early_Morning",
    "Breakfast",
    "Lunch",
    "Snack",
    "Dinner",
    "Latenight",
)

DAYPART_LABELS: dict[Daypart, str] = {
    "Early_Morning": "Early Morning (12am-5am)",
    "Breakfast": "Breakfast (5am-11am)",
    "Lunch": "Lunch (11am-2pm)",
    "Snack": "Snack (2pm-5pm)",
    "Dinner": "Dinner (5pm-9pm)",
    "Latenight": "Latenight (9pm-12am)",
}


@dataclass(frozen=True)
class LocalDateHour:
    """Model hour: local time America/Los_Angeles from Open-Meteo with timezone=America/Los_Angeles."""

    d: date
    hour: int  # 0-23, inclusive, one forecast step


def daypart_for_hour(hour: int) -> Daypart:
    if hour in (0, 1, 2, 3, 4):
        return "Early_Morning"
    if 5 <= hour <= 10:
        return "Breakfast"
    if 11 <= hour <= 13:
        return "Lunch"
    if 14 <= hour <= 16:
        return "Snack"
    if 17 <= hour <= 20:
        return "Dinner"
    if 21 <= hour <= 23:
        return "Latenight"
    return "Latenight"


def col_name_for_date_and_daypart(d: date, dp: Daypart) -> str:
    return f"{d.isoformat()}__{dp}"


def parse_iso_local(dt_str: str) -> LocalDateHour:
    """Open-Meteo with timezone=America/Los_Angeles returns '2026-04-27T00:00' (no offset)."""
    s = dt_str
    if len(s) >= 16 and s[13] == "T":
        d = date.fromisoformat(s[0:10])
        h = int(s[11:13])
        return LocalDateHour(d=d, hour=h)
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    # Should not happen when timezone is set on API; keep fallback
    d2 = date(dt.year, dt.month, dt.day)
    return LocalDateHour(d=d2, hour=dt.hour)


