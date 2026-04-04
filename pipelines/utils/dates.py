from datetime import date, timedelta


SEASON_DATE_RANGES: dict[str, tuple[date, date]] = {
    "2024-25": (date(2024, 10, 22), date(2025, 6, 22)),
}


def season_date_range(season: str) -> tuple[date, date]:
    """
    Return the (start_date, end_date) for an NBA season.

    Example:
        season_date_range("2024-25") -> (date(2024, 10, 22), date(2025, 6, 22))

    The start date is opening night. The end date covers the Finals.
    We use a fixed lookup table rather than computing dynamically —
    NBA season dates are known in advance and don't change.
    """
    if season not in SEASON_DATE_RANGES:
        raise ValueError(
            f"Unknown season: {season!r}. "
            f"Available seasons: {list(SEASON_DATE_RANGES.keys())}"
        )
    return SEASON_DATE_RANGES[season]


def yesterday() -> date:
    """Return yesterday's date. Used as the default watermark in daily DAGs."""
    return date.today() - timedelta(days=1)


def today() -> date:
    """Return today's date."""
    return date.today()


def date_to_int(d: date) -> int:
    """
    Convert a date to an integer in YYYYMMDD format.
    Used as the date_key in date_dim.

    Example: date(2024, 10, 22) -> 20241022
    """
    return int(d.strftime("%Y%m%d"))
