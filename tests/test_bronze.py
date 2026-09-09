from __future__ import annotations

import json
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, Mock

import pytest
import requests
from entsoe.exceptions import NoMatchingDataError
from pyspark.sql import SparkSession

from delu.pipeline.bronze import (
    ACTUAL,
    ALL,
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
    _should_retry_immediately,
    _weather_parameters,
    ingest,
    latest_settlement_date,
)


def test_morning_plan_fetches_only_tomorrows_inputs() -> None:
    today = date(2026, 9, 7)

    planned = _planned_requests("morning", today=today, start=DEFAULT_START)

    fetched = {(delivery_date, series) for delivery_date, series, *_ in planned}
    expected = {
        *(
            (source_date, series)
            for source_date in (date(2026, 9, 7), date(2026, 9, 6), date(2026, 9, 1))
            for series, *_ in SDAC
        ),
        *((today + date.resolution, series) for series, *_ in EXAA),
        *((today, series) for series, *_ in FORECAST),
        *((today - date.resolution, series) for series, *_ in ACTUAL),
        *((today, series) for series, *_ in WEATHER),
    }
    assert fetched == expected
    assert len(planned) == len(expected)


def test_settlement_plan_fetches_only_the_new_result() -> None:
    today = date(2026, 9, 7)

    planned = _planned_requests("settlement", today=today, start=DEFAULT_START)

    fetched = {(delivery_date, series) for delivery_date, series, *_ in planned}
    assert fetched == {(date(2026, 9, 8), series) for series, *_ in SDAC}


def test_full_plan_uses_the_requested_history_window() -> None:
    planned = _planned_requests(
        "full",
        today=date(2026, 9, 7),
        start=date(2026, 9, 2),
        end=date(2026, 9, 4),
    )

    fetched = {(delivery_date, series) for delivery_date, series, *_ in planned}
    assert len(fetched) == 3 * (len(ALL) + len(WEATHER))
    assert {delivery_date for delivery_date, _ in fetched} == {
        date(2026, 9, 2),
        date(2026, 9, 3),
        date(2026, 9, 4),
    }


def test_full_plan_stops_before_the_incomplete_current_day() -> None:
    planned = _planned_requests(
        "full",
        today=date(2026, 9, 7),
        start=date(2026, 9, 6),
        end=date(2026, 9, 7),
    )

    assert {delivery_date for delivery_date, *_ in planned} == {date(2026, 9, 6)}


def test_full_plan_rejects_a_reversed_history_window() -> None:
    with pytest.raises(ValueError, match="start cannot be after end"):
        _planned_requests(
            "full",
            today=date(2026, 9, 7),
            start=date(2026, 9, 4),
            end=date(2026, 9, 3),
        )


def test_settlement_date_does_not_roll_forward_before_afternoon() -> None:
    before = datetime(2026, 9, 8, 12, 59, tzinfo=UTC)
    cutoff = datetime(2026, 9, 8, 13, 0, tzinfo=UTC)

    assert latest_settlement_date(before) == date(2026, 9, 8)
    assert latest_settlement_date(cutoff) == date(2026, 9, 9)


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


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        pytest.param(530, True, id="entsoe-530"),
        pytest.param(599, True, id="entsoe-599"),
        pytest.param(400, False, id="bad-request"),
        pytest.param(404, False, id="not-found"),
    ],
)
def test_only_transient_http_errors_are_retried_immediately(
    status: int, expected: bool
) -> None:
    response = Mock(spec=requests.Response)
    response.status_code = status

    assert _should_retry_immediately(requests.HTTPError(response=response)) is expected


def test_ingestion_persists_successes_before_reporting_other_failures(
    monkeypatch,
) -> None:
    payload = """<Publication_MarketDocument>
      <TimeSeries>
        <currency_Unit.name>EUR</currency_Unit.name>
        <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
        <curveType>A03</curveType>
        <Period>
          <timeInterval>
            <start>2026-09-07T22:00Z</start>
            <end>2026-09-08T22:00Z</end>
          </timeInterval>
          <resolution>PT15M</resolution>
          <Point><position>1</position><price.amount>50</price.amount></Point>
        </Period>
      </TimeSeries>
    </Publication_MarketDocument>"""

    def query_prices(area, *_args, **_kwargs):
        if area == "AT":
            raise NoMatchingDataError
        return payload

    client = MagicMock()
    client.query_day_ahead_prices.side_effect = query_prices
    monkeypatch.setattr(
        "delu.pipeline.bronze.EntsoeRawClient", Mock(return_value=client)
    )
    monkeypatch.setenv("ENTSOE_API_KEY", "test-key")

    existing = MagicMock()
    existing.where.return_value = existing
    existing.select.return_value = existing
    existing.distinct.return_value = existing
    existing.collect.return_value = []
    update = MagicMock()
    spark = MagicMock(spec=SparkSession)
    spark.catalog.tableExists.return_value = True
    spark.table.return_value = existing
    spark.createDataFrame.return_value = update

    with pytest.raises(RuntimeError, match="1 source response"):
        ingest(
            "settlement",
            run_date=date(2026, 9, 7),
            now=datetime(2026, 9, 7, 13, 0, tzinfo=UTC),
            workers=2,
            spark=spark,
        )

    rows = spark.createDataFrame.call_args.args[0]
    assert len(rows) == 1
    assert rows[0][1] == "de_lu.price.sdac"
    update.write.format.assert_called_once_with("delta")
