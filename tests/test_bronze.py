from __future__ import annotations

from datetime import date

from delu.pipeline.bronze import DEFAULT_START, SDAC, _planned_requests


def test_morning_plan_fetches_todays_sdac_for_tomorrows_price_lag() -> None:
    today = date(2026, 9, 7)

    planned, _ = _planned_requests("morning", today=today, start=DEFAULT_START)

    fetched = {(delivery_date, series) for delivery_date, series, *_ in planned}
    assert {(today, series) for series, *_ in SDAC}.issubset(fetched)


def test_settlement_plan_keeps_fetching_tomorrows_sdac() -> None:
    today = date(2026, 9, 7)

    planned, _ = _planned_requests("settlement", today=today, start=DEFAULT_START)

    fetched = {(delivery_date, series) for delivery_date, series, *_ in planned}
    assert fetched == {(date(2026, 9, 8), series) for series, *_ in SDAC}
