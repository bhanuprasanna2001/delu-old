from __future__ import annotations

import json
from datetime import UTC, date, datetime
from unittest.mock import Mock

import requests

from delu.pipeline.bronze import (
    ACTUAL,
    DEFAULT_START,
    EXAA,
    FORECAST,
    SDAC,
    WEATHER,
    WEATHER_FALLBACK_MODEL,
    WEATHER_MODEL,
    _fetch_weather,
    _latest_weather_run,
    _planned_requests,
    _weather_parameters,
)


def test_morning_plan_covers_the_forecast_month_through_tomorrow() -> None:
    today = date(2026, 9, 7)

    planned, _ = _planned_requests("morning", today=today, start=DEFAULT_START)

    fetched = {(delivery_date, series) for delivery_date, series, *_ in planned}
    month_start = date(2026, 9, 1)
    assert len(planned) == (
        8 * (len(SDAC) + len(EXAA) + len(FORECAST) + len(ACTUAL)) + len(WEATHER)
    )
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
    assert {(today, series) for series, *_ in WEATHER}.issubset(fetched)


def test_settlement_plan_keeps_fetching_tomorrows_sdac() -> None:
    today = date(2026, 9, 7)

    planned, _ = _planned_requests("settlement", today=today, start=DEFAULT_START)

    fetched = {(delivery_date, series) for delivery_date, series, *_ in planned}
    assert fetched == {(date(2026, 9, 8), series) for series, *_ in SDAC}


def test_weather_waits_until_nine_in_berlin() -> None:
    before = datetime(2026, 9, 8, 6, 59, tzinfo=UTC)
    cutoff = datetime(2026, 9, 8, 7, 0, tzinfo=UTC)

    assert _latest_weather_run(before) == date(2026, 9, 7)
    assert _latest_weather_run(cutoff) == date(2026, 9, 8)


def _response(payload: object) -> Mock:
    response = Mock(spec=requests.Response)
    response.json.return_value = payload
    return response


def test_weather_ignores_shortwave_nulls_when_deciding_on_fallback(
    monkeypatch,
) -> None:
    get = Mock(
        return_value=_response(
            [
                {
                    "hourly": {
                        "temperature_2m": [15.0],
                        "shortwave_radiation": [None],
                    }
                }
            ]
        )
    )
    monkeypatch.setattr("delu.pipeline.bronze.requests.get", get)

    payload = json.loads(_fetch_weather("land", date(2026, 6, 11)))

    assert get.call_count == 1
    assert payload["temperature_fallback"] is None
    assert get.call_args.kwargs["params"]["models"] == WEATHER_MODEL


def test_weather_fetches_temperature_only_fallback_for_primary_temperature_nulls(
    monkeypatch,
) -> None:
    get = Mock(
        side_effect=[
            _response([{"hourly": {"temperature_2m": [None]}}]),
            _response([{"hourly": {"temperature_2m": [14.0]}}]),
        ]
    )
    monkeypatch.setattr("delu.pipeline.bronze.requests.get", get)

    payload = json.loads(_fetch_weather("land", date(2026, 6, 23)))

    assert get.call_count == 2
    assert payload["temperature_fallback"][0]["hourly"]["temperature_2m"] == [14.0]
    fallback = get.call_args.kwargs["params"]
    assert fallback["models"] == WEATHER_FALLBACK_MODEL
    assert fallback["hourly"] == "temperature_2m"


def test_weather_parameters_request_one_model_at_a_time() -> None:
    primary = _weather_parameters("land", date(2026, 6, 23), WEATHER_MODEL)
    fallback = _weather_parameters("land", date(2026, 6, 23), WEATHER_FALLBACK_MODEL)

    assert primary["models"] == WEATHER_MODEL
    assert "shortwave_radiation" in primary["hourly"]
    assert fallback["models"] == WEATHER_FALLBACK_MODEL
    assert fallback["hourly"] == "temperature_2m"
