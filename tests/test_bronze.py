from __future__ import annotations

from datetime import date

from delu.pipeline.bronze import (
    ACTUAL,
    DEFAULT_START,
    EXAA,
    FORECAST,
    SDAC,
    _planned_requests,
)


def test_morning_plan_covers_the_forecast_month_through_tomorrow() -> None:
    today = date(2026, 9, 7)

    planned, _ = _planned_requests("morning", today=today, start=DEFAULT_START)

    fetched = {(delivery_date, series) for delivery_date, series, *_ in planned}
    month_start = date(2026, 9, 1)
    assert len(planned) == 8 * (len(SDAC) + len(EXAA) + len(FORECAST) + len(ACTUAL))
    assert {(month_start - date.resolution, series) for series, *_ in SDAC}.issubset(
        fetched
    )
    assert {(month_start, series) for series, *_ in EXAA}.issubset(fetched)
    assert {
        (month_start - date.resolution, series) for series, *_ in FORECAST
    }.issubset(fetched)
    assert {
        (month_start - 2 * date.resolution, series) for series, *_ in ACTUAL
    }.issubset(fetched)
    assert {(today, series) for series, *_ in SDAC}.issubset(fetched)


def test_settlement_plan_keeps_fetching_tomorrows_sdac() -> None:
    today = date(2026, 9, 7)

    planned, _ = _planned_requests("settlement", today=today, start=DEFAULT_START)

    fetched = {(delivery_date, series) for delivery_date, series, *_ in planned}
    assert fetched == {(date(2026, 9, 8), series) for series, *_ in SDAC}
