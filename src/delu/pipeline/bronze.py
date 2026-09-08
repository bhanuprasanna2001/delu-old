from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from datetime import time as datetime_time
from http.client import RemoteDisconnected
from itertools import batched
from socket import gaierror
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from databricks.connect import DatabricksSession
from entsoe import EntsoeRawClient
from entsoe.exceptions import NoMatchingDataError
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLE = "delu.bronze.raw"
SCHEMA = "delivery_date date, series string, payload string, ingested_at timestamp"
DEFAULT_START = date(2025, 10, 1)
BERLIN = ZoneInfo("Europe/Berlin")
WEATHER_URL = "https://single-runs-api.open-meteo.com/v1/forecast"
WEATHER_MODEL = "ecmwf_ifs"
WEATHER_FALLBACK_MODEL = "ecmwf_ifs025"
WEATHER_FIELDS = (
    ("temperature_2m", "temperature_2m_c", "°C"),
    ("wind_speed_100m", "wind_speed_100m_m_s", "m/s"),
    ("wind_direction_100m", "wind_direction_100m_degrees", "°"),
    ("shortwave_radiation", "shortwave_radiation_w_m2", "W/m²"),
    ("cloud_cover", "cloud_cover_pct", "%"),
)
WEATHER_LOCATIONS = (
    ("emden", 53.37, 7.21, "land"),
    ("bremen", 53.08, 8.80, "land"),
    ("hamburg", 53.55, 9.99, "land"),
    ("kiel", 54.32, 10.14, "land"),
    ("rostock", 54.09, 12.14, "land"),
    ("hanover", 52.38, 9.73, "land"),
    ("berlin", 52.52, 13.41, "land"),
    ("muenster", 51.96, 7.63, "land"),
    ("kassel", 51.31, 9.50, "land"),
    ("leipzig", 51.34, 12.37, "land"),
    ("dresden", 51.05, 13.74, "land"),
    ("cologne", 50.94, 6.96, "land"),
    ("frankfurt", 50.11, 8.68, "land"),
    ("erfurt", 50.98, 11.03, "land"),
    ("nuremberg", 49.45, 11.08, "land"),
    ("luxembourg", 49.61, 6.13, "land"),
    ("stuttgart", 48.78, 9.18, "land"),
    ("freiburg", 47.99, 7.85, "land"),
    ("munich", 48.14, 11.58, "land"),
    ("passau", 48.57, 13.46, "land"),
    ("north_sea_west", 54.75, 6.30, "sea"),
    ("north_sea_centre", 54.60, 7.50, "sea"),
    ("north_sea_east", 54.40, 8.40, "sea"),
    ("baltic_west", 54.50, 11.30, "sea"),
    ("baltic_east", 54.50, 13.50, "sea"),
)
WORKERS = 12
BATCH_DAYS = 7
SETTLEMENT_CUTOFF = datetime_time(15)
LOGGER = logging.getLogger(__name__)

Request = tuple[str, str, str, Mapping[str, object]]
PlannedRequest = tuple[date, str, str, str, Mapping[str, object]]
RequestKey = tuple[date, str]

SDAC: tuple[Request, ...] = tuple(
    (f"{area.lower()}.price.sdac", "query_day_ahead_prices", area, {"sequence": 1})
    for area in ("DE_LU", "AT")
)
EXAA: tuple[Request, ...] = tuple(
    (f"{area.lower()}.price.exaa", "query_day_ahead_prices", area, {"sequence": 2})
    for area in ("DE_LU", "AT")
)
FORECAST: tuple[Request, ...] = (
    ("de_lu.load.forecast", "query_load_forecast", "DE_LU", {"process_type": "A01"}),
    *(
        (
            f"de_lu.generation.{kind}.forecast",
            "query_wind_and_solar_forecast",
            "DE_LU",
            {"psr_type": psr_type, "process_type": "A01"},
        )
        for kind, psr_type in {
            "solar": "B16",
            "wind_onshore": "B19",
            "wind_offshore": "B18",
        }.items()
    ),
)
ACTUAL: tuple[Request, ...] = (
    ("de_lu.load.actual", "query_load", "DE_LU", {}),
    *(
        (
            f"de_lu.generation.{kind}.actual",
            "query_generation",
            "DE_LU",
            {"psr_type": psr_type},
        )
        for kind, psr_type in {
            "solar": "B16",
            "wind_onshore": "B19",
            "wind_offshore": "B18",
        }.items()
    ),
)
ALL: tuple[Request, ...] = SDAC + EXAA + FORECAST + ACTUAL
WEATHER: tuple[Request, ...] = tuple(
    (f"weather.{WEATHER_MODEL}.{cell}", "weather", cell, {}) for cell in ("land", "sea")
)


def _latest_weather_run(now: datetime) -> date:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    local = now.astimezone(BERLIN)
    return local.date() - timedelta(days=local.hour < 9)


def latest_settlement_date(now: datetime) -> date:
    """Return the latest delivery date expected to have published SDAC prices."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    local = now.astimezone(BERLIN)
    return local.date() + timedelta(days=local.time() >= SETTLEMENT_CUTOFF)


def _weather_parameters(cell: str, run_date: date, model: str) -> dict[str, str]:
    locations = [location for location in WEATHER_LOCATIONS if location[3] == cell]
    return {
        "latitude": ",".join(str(location[1]) for location in locations),
        "longitude": ",".join(str(location[2]) for location in locations),
        "hourly": (
            "temperature_2m"
            if model == WEATHER_FALLBACK_MODEL
            else ",".join(field for field, _, _ in WEATHER_FIELDS)
        ),
        "models": model,
        "run": f"{run_date.isoformat()}T00:00",
        "forecast_days": "2",
        "timezone": "GMT",
        "wind_speed_unit": "ms",
        "cell_selection": cell,
    }


def _fetch_weather(cell: str, run_date: date) -> str:
    response = requests.get(
        WEATHER_URL,
        params=_weather_parameters(cell, run_date, WEATHER_MODEL),
        headers={"Accept": "application/json"},
        timeout=60,
    )
    response.raise_for_status()
    primary = response.json()
    if not isinstance(primary, list):
        raise ValueError("Weather response must be a list of locations")
    try:
        needs_fallback = any(
            None in location["hourly"]["temperature_2m"] for location in primary
        )
    except (KeyError, TypeError) as exc:
        raise ValueError("Weather response has no hourly temperature data") from exc

    fallback = None
    if needs_fallback:
        response = requests.get(
            WEATHER_URL,
            params=_weather_parameters(cell, run_date, WEATHER_FALLBACK_MODEL),
            headers={"Accept": "application/json"},
            timeout=60,
        )
        response.raise_for_status()
        fallback = response.json()
    return json.dumps(
        {"primary": primary, "temperature_fallback": fallback},
        separators=(",", ":"),
    )


def _planned_requests(
    mode: str,
    *,
    today: date,
    start: date,
    weather_run_date: date | None = None,
    settlement_date: date | None = None,
) -> tuple[
    list[PlannedRequest],
    set[RequestKey],
]:
    weather_run_date = today if weather_run_date is None else weather_run_date
    if mode == "morning":
        first_delivery_date = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        final_delivery_date = today + timedelta(days=1)
        delivery_dates = (
            first_delivery_date + timedelta(days=offset)
            for offset in range((final_delivery_date - first_delivery_date).days + 1)
        )
        first_weather_run = first_delivery_date - timedelta(days=1)
        weather_runs = (
            first_weather_run + timedelta(days=offset)
            for offset in range((weather_run_date - first_weather_run).days + 1)
        )
        return (
            [
                request
                for delivery_date in delivery_dates
                for request in (
                    *((delivery_date - timedelta(days=1), *item) for item in SDAC),
                    *((delivery_date, *item) for item in EXAA),
                    *((delivery_date - timedelta(days=1), *item) for item in FORECAST),
                    *((delivery_date - timedelta(days=2), *item) for item in ACTUAL),
                )
            ]
            + [
                (run_date, *request) for run_date in weather_runs for request in WEATHER
            ],
            set(),
        )
    if mode == "settlement":
        settlement_date = (
            today + timedelta(days=1) if settlement_date is None else settlement_date
        )
        first_settlement_date = (
            settlement_date.replace(day=1) - timedelta(days=1)
        ).replace(day=1)
        return (
            [
                (first_settlement_date + timedelta(days=offset), *request)
                for offset in range((settlement_date - first_settlement_date).days + 1)
                for request in SDAC
            ],
            set(),
        )
    if mode == "full":
        end = today - timedelta(days=1)
        planned = [
            (start + timedelta(days=offset), *request)
            for offset in range((end - start).days + 1)
            for request in ALL
        ]
        refresh_days = {end - timedelta(days=offset) for offset in range(3)}
        refresh = {
            (delivery_date, series)
            for delivery_date, series, *_ in planned
            if delivery_date in refresh_days
        }
        planned.extend(
            (run_date, *request)
            for offset in range(max(0, (weather_run_date - start).days + 1))
            for run_date in (start + timedelta(days=offset),)
            for request in WEATHER
        )
        return planned, refresh
    raise ValueError("mode must be full, morning, or settlement")


def ingest(
    mode: str,
    *,
    start: date = DEFAULT_START,
    now: datetime | None = None,
    workers: int = WORKERS,
    spark: SparkSession | None = None,
) -> int:
    if workers < 1:
        raise ValueError("workers must be at least 1")

    current = datetime.now(UTC) if now is None else now
    if current.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    today = current.astimezone(BERLIN).date()
    spark = (
        spark
        or SparkSession.getActiveSession()
        or DatabricksSession.builder.serverless().getOrCreate()
    )
    spark.sql("CREATE SCHEMA IF NOT EXISTS delu.bronze")
    if not spark.catalog.tableExists(TABLE):
        spark.createDataFrame([], SCHEMA).write.format("delta").saveAsTable(TABLE)

    # Each item is (delivery date or run date, series, method, area, method kwargs).
    planned, refresh = _planned_requests(
        mode,
        today=today,
        start=start,
        weather_run_date=_latest_weather_run(current),
        settlement_date=latest_settlement_date(current),
    )

    if not planned:
        LOGGER.info("Nothing new to ingest.")
        return 0

    existing = {
        (row.delivery_date, row.series)
        for row in spark.table(TABLE)
        .where(
            F.col("delivery_date").isin(
                {delivery_date for delivery_date, *_ in planned}
            )
        )
        .select("delivery_date", "series")
        .distinct()
        .collect()
    }
    planned = [
        request
        for request in planned
        if (request[0], request[1]) in refresh
        or (request[0], request[1]) not in existing
    ]
    if not planned:
        LOGGER.info("Nothing new to ingest.")
        return 0

    api_key = None

    def fetch(
        request: PlannedRequest,
    ) -> tuple[date, str, str]:
        delivery_date, series, method, area, params = request
        period_start = pd.Timestamp(delivery_date, tz=BERLIN)
        period_end = pd.Timestamp(delivery_date + timedelta(days=1), tz=BERLIN)

        for attempt in range(4):
            try:
                if method == "weather":
                    return delivery_date, series, _fetch_weather(area, delivery_date)
                if api_key is None:
                    raise RuntimeError("ENTSO-E API key is unavailable")
                client = EntsoeRawClient(
                    api_key, retry_count=1, retry_delay=0, timeout=60
                )
                payload = getattr(client, method)(
                    area, period_start, period_end, **params
                )
                return delivery_date, series, payload
            except NoMatchingDataError as exc:
                raise RuntimeError(
                    f"No ENTSO-E data for {series} on {delivery_date}"
                ) from exc
            except (
                requests.ConnectionError,
                requests.Timeout,
                requests.HTTPError,
                gaierror,
                RemoteDisconnected,
            ) as exc:
                status = (
                    getattr(exc.response, "status_code", None)
                    if isinstance(exc, requests.HTTPError)
                    else None
                )
                if attempt == 3 or status not in (None, 429, 500, 502, 503, 504, 599):
                    raise
                delay = min(2**attempt, 30) + random.random()
                LOGGER.warning(
                    "Retrying %s for %s in %.1fs: %s", series, delivery_date, delay, exc
                )
                time.sleep(delay)

        raise AssertionError("unreachable")

    written = 0
    groups = (
        [request for request in planned if request[2] == "weather"],
        [request for request in planned if request[2] != "weather"],
    )
    for requests_for_source in groups:
        if not requests_for_source:
            continue
        if requests_for_source[0][2] != "weather":
            api_key = os.getenv("ENTSOE_API_KEY") or DBUtils(spark).secrets.get(
                scope="delu", key="entsoe-api-key"
            )
        for dates in batched(
            sorted({request[0] for request in requests_for_source}), BATCH_DAYS
        ):
            batch = [request for request in requests_for_source if request[0] in dates]
            LOGGER.info(
                "Fetching %d source responses for %s through %s with %d workers.",
                len(batch),
                dates[0],
                dates[-1],
                min(workers, len(batch)),
            )
            with ThreadPoolExecutor(max_workers=min(workers, len(batch))) as pool:
                rows = list(pool.map(fetch, batch))

            ingested_at = datetime.now(UTC)
            spark.createDataFrame(
                [
                    (delivery_date, series, payload, ingested_at)
                    for delivery_date, series, payload in rows
                ],
                SCHEMA,
            ).write.format("delta").mode("append").saveAsTable(TABLE)
            written += len(rows)
            LOGGER.info("Appended %d raw responses to %s.", len(rows), TABLE)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest raw ENTSO-E payloads into delu.bronze.raw"
    )
    parser.add_argument("mode", choices=("full", "morning", "settlement"))
    parser.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START)
    parser.add_argument("--workers", type=int, default=WORKERS)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    ingest(args.mode, start=args.start, workers=args.workers)


if __name__ == "__main__":
    main()
