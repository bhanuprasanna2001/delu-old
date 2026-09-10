from __future__ import annotations

import json
from datetime import UTC, date, datetime
from functools import partial
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
import requests
from entsoe.exceptions import NoMatchingDataError
from pyspark.sql import SparkSession

from delu.pipeline.bronze import (
    ACTUAL,
    EXAA,
    FORECAST,
    SDAC,
    WEATHER,
    WEATHER_FALLBACK_MODEL,
    WEATHER_MODEL,
    _fetch_weather,
    _planned_requests,
    _weather_parameters,
    ingest,
)


def test_backfill_includes_five_missing_days_and_their_lagged_inputs() -> None:
    planned = _planned_requests(
        today=date(2026, 9, 9),
        start=date(2026, 9, 4),
        end=date(2026, 9, 8),
    )
    requests_by_day = {(day, series) for day, series, *_ in planned}

    assert (date(2026, 9, 4), "de_lu.price.exaa") in requests_by_day
    assert (date(2026, 9, 8), "de_lu.price.exaa") in requests_by_day
    assert (date(2026, 8, 28), "de_lu.price.sdac") in requests_by_day
    assert (date(2026, 9, 2), "de_lu.load.actual") in requests_by_day
    assert (date(2026, 9, 7), "weather.ecmwf_ifs.land") in requests_by_day
    assert len(planned) == len(requests_by_day)


def test_current_day_plan_requests_prices_and_weather_without_a_time_gate() -> None:
    today = date(2026, 9, 9)
    planned = _planned_requests(today=today, start=date(2026, 9, 10))
    requests_by_day = {(day, series) for day, series, *_ in planned}

    assert {
        (date(2026, 9, 10), series) for series, *_ in SDAC + EXAA
    } <= requests_by_day
    assert {(today, series) for series, *_ in FORECAST + WEATHER} <= requests_by_day
    assert {(date(2026, 9, 8), series) for series, *_ in ACTUAL} <= requests_by_day


def test_backfill_rejects_a_reversed_range() -> None:
    with pytest.raises(ValueError, match="start cannot be after end"):
        _planned_requests(
            today=date(2026, 9, 9),
            start=date(2026, 9, 8),
            end=date(2026, 9, 4),
        )


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


PAYLOAD = """<Publication_MarketDocument><TimeSeries>
  <currency_Unit.name>EUR</currency_Unit.name>
  <price_Measure_Unit.name>MWH</price_Measure_Unit.name><curveType>A03</curveType>
  <Period><timeInterval><start>2026-09-07T22:00Z</start><end>2026-09-08T22:00Z</end></timeInterval>
  <resolution>PT15M</resolution>
  <Point><position>1</position><price.amount>50</price.amount></Point>
  </Period></TimeSeries></Publication_MarketDocument>"""


@pytest.fixture
def ingestion(monkeypatch):
    day = date(2026, 9, 8)
    missing = {(day, "de_lu.price.sdac"), (day, "at.price.sdac")}
    stored = {
        (source_day, series)
        for source_day, series, *_ in _planned_requests(today=day, start=day, end=day)
    } - missing
    existing = MagicMock()
    existing.where.return_value = existing
    existing.select.return_value = existing
    existing.distinct.return_value = existing
    existing.collect.side_effect = lambda: [
        SimpleNamespace(delivery_date=day, series=series) for day, series in stored
    ]
    update = MagicMock()
    spark = MagicMock(spec=SparkSession)
    spark.catalog.tableExists.return_value = True
    spark.table.return_value = existing
    spark.createDataFrame.return_value = update
    update.write.format.return_value.mode.return_value.saveAsTable.side_effect = (
        lambda _table: stored.update(
            (row[0], row[1]) for row in spark.createDataFrame.call_args.args[0]
        )
    )
    client = MagicMock()
    monkeypatch.setattr(
        "delu.pipeline.bronze.EntsoeRawClient", Mock(return_value=client)
    )
    monkeypatch.setenv("ENTSOE_API_KEY", "test-key")
    return spark, client, stored


def test_missing_response_is_retried_later_without_refetching_successes(
    ingestion, caplog
) -> None:
    spark, client, stored = ingestion
    client.query_day_ahead_prices.side_effect = [PAYLOAD, NoMatchingDataError()]
    run = partial(
        ingest, start=date(2026, 9, 8), end=date(2026, 9, 8), spark=spark, workers=1
    )

    assert run(now=datetime(2026, 9, 8, 13, 36, tzinfo=UTC)) == 1
    assert "Waiting for data" in caplog.text
    assert (date(2026, 9, 8), "at.price.sdac") not in stored

    client.query_day_ahead_prices.reset_mock(side_effect=True)
    client.query_day_ahead_prices.return_value = PAYLOAD
    assert run(now=datetime(2026, 9, 13, 18, tzinfo=UTC)) == 1
    assert client.query_day_ahead_prices.call_args.args[0] == "AT"
    assert run(now=datetime(2026, 9, 13, 19, tzinfo=UTC)) == 0
    client.query_day_ahead_prices.assert_called_once()


@pytest.mark.parametrize("status", [429, 503, 530, 599, 404])
def test_upstream_http_outages_leave_missing_data_pending(ingestion, status) -> None:
    spark, client, stored = ingestion
    response = Mock(spec=requests.Response, status_code=status)
    client.query_day_ahead_prices.side_effect = requests.HTTPError(response=response)

    assert ingest(start=date(2026, 9, 8), end=date(2026, 9, 8), spark=spark) == 0
    assert (date(2026, 9, 8), "de_lu.price.sdac") not in stored


def test_authentication_failure_remains_an_error_after_successes_are_saved(
    ingestion,
) -> None:
    spark, client, stored = ingestion
    response = Mock(spec=requests.Response, status_code=401)
    client.query_day_ahead_prices.side_effect = [
        PAYLOAD,
        requests.HTTPError(response=response),
    ]

    with pytest.raises(RuntimeError, match="1 source response"):
        ingest(start=date(2026, 9, 8), end=date(2026, 9, 8), workers=1, spark=spark)
    assert (date(2026, 9, 8), "de_lu.price.sdac") in stored


def test_unpublished_weather_run_stays_pending(ingestion, monkeypatch, caplog) -> None:
    spark, _client, stored = ingestion
    stored.add((date(2026, 9, 8), "de_lu.price.sdac"))
    stored.add((date(2026, 9, 8), "at.price.sdac"))
    stored.remove((date(2026, 9, 7), "weather.ecmwf_ifs.land"))
    response = Mock(spec=requests.Response, status_code=400)
    response.text = '{"reason":"The requested model run is not available."}'
    response.raise_for_status.side_effect = requests.HTTPError(response=response)
    monkeypatch.setattr(
        "delu.pipeline.bronze.requests.get", Mock(return_value=response)
    )

    assert ingest(start=date(2026, 9, 8), end=date(2026, 9, 8), spark=spark) == 0
    assert "Waiting for data" in caplog.text
    assert (date(2026, 9, 7), "weather.ecmwf_ifs.land") not in stored
