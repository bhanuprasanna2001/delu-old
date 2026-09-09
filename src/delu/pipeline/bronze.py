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
PRICE_LAGS = (1, 2, 7)
RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504, 530, 599})
LOGGER = logging.getLogger(__name__)

Request = tuple[str, str, str, Mapping[str, object]]
PlannedRequest = tuple[date, str, str, str, Mapping[str, object]]

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
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    local = now.astimezone(BERLIN)
    return local.date() - timedelta(days=local.hour < 9)


def latest_settlement_date(now: datetime) -> date:
    """Return the latest delivery date expected to have published SDAC prices."""
    if now.tzinfo is None or now.utcoffset() is None:
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
        missing_temperatures = sum(
            value is None
            for location in primary
            for value in location["hourly"]["temperature_2m"]
        )
    except (KeyError, TypeError) as exc:
        raise ValueError("Weather response has no hourly temperature data") from exc

    fallback = None
    if missing_temperatures:
        LOGGER.warning(
            "Using %s for %d missing %s temperature values on %s.",
            WEATHER_FALLBACK_MODEL,
            missing_temperatures,
            cell,
            run_date,
        )
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
    end: date | None = None,
    weather_run_date: date | None = None,
    settlement_date: date | None = None,
) -> list[PlannedRequest]:
    weather_run_date = today if weather_run_date is None else weather_run_date
    if mode == "morning":
        delivery_date = today + timedelta(days=1)
        return [
            *(
                (delivery_date - timedelta(days=lag), *request)
                for lag in PRICE_LAGS
                for request in SDAC
            ),
            *((delivery_date, *request) for request in EXAA),
            *((today, *request) for request in FORECAST),
            *((today - timedelta(days=1), *request) for request in ACTUAL),
            *((weather_run_date, *request) for request in WEATHER),
        ]
    if mode == "settlement":
        settlement_date = (
            today + timedelta(days=1) if settlement_date is None else settlement_date
        )
        return [(settlement_date, *request) for request in SDAC]
    if mode == "full":
        if end is not None and start > end:
            raise ValueError("ingestion start cannot be after end")
        final_date = min(end or today - timedelta(days=1), today - timedelta(days=1))
        if start > final_date:
            return []
        return [
            (start + timedelta(days=offset), *request)
            for offset in range((final_date - start).days + 1)
            for request in (*ALL, *WEATHER)
        ]
    raise ValueError("mode must be full, morning, or settlement")


def _should_retry_immediately(exc: Exception) -> bool:
    if not isinstance(exc, requests.HTTPError):
        return True
    return getattr(exc.response, "status_code", None) in RETRYABLE_HTTP_STATUSES


def ingest(
    mode: str,
    *,
    start: date = DEFAULT_START,
    end: date | None = None,
    run_date: date | None = None,
    now: datetime | None = None,
    workers: int = WORKERS,
    spark: SparkSession | None = None,
) -> int:
    if workers < 1:
        raise ValueError("workers must be at least 1")

    current = datetime.now(UTC) if now is None else now
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    today = run_date or current.astimezone(BERLIN).date()
    spark = (
        spark
        or SparkSession.getActiveSession()
        or DatabricksSession.builder.serverless().getOrCreate()
    )
    spark.sql("CREATE SCHEMA IF NOT EXISTS delu.bronze")
    if not spark.catalog.tableExists(TABLE):
        spark.createDataFrame([], SCHEMA).write.format("delta").saveAsTable(TABLE)

    # Each item is (delivery date or run date, series, method, area, method kwargs).
    planned = _planned_requests(
        mode,
        today=today,
        start=start,
        end=end,
        weather_run_date=run_date or _latest_weather_run(current),
        settlement_date=(
            run_date + timedelta(days=1)
            if run_date is not None
            else latest_settlement_date(current)
        ),
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
        request for request in planned if (request[0], request[1]) not in existing
    ]
    if not planned:
        LOGGER.info("Nothing new to ingest.")
        return 0

    # Import after Bronze is loaded because Silver shares the source definitions above.
    from delu.pipeline.silver import validate_payload

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
                    payload = _fetch_weather(area, delivery_date)
                else:
                    if api_key is None:
                        raise RuntimeError("ENTSO-E API key is unavailable")
                    client = EntsoeRawClient(
                        api_key, retry_count=1, retry_delay=0, timeout=60
                    )
                    payload = getattr(client, method)(
                        area, period_start, period_end, **params
                    )
                validate_payload(payload, series, delivery_date)
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
                if attempt == 3 or not _should_retry_immediately(exc):
                    raise
                delay = min(2**attempt, 30) + random.random()
                LOGGER.warning(
                    "Retrying %s for %s in %.1fs after %s.",
                    series,
                    delivery_date,
                    delay,
                    (
                        f"HTTP {getattr(exc.response, 'status_code', 'error')}"
                        if isinstance(exc, requests.HTTPError)
                        else type(exc).__name__
                    ),
                )
                time.sleep(delay)

        raise AssertionError("unreachable")

    written = 0
    failures: list[tuple[date, str, Exception]] = []
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
                futures = [(request, pool.submit(fetch, request)) for request in batch]
                rows = []
                for request, future in futures:
                    try:
                        rows.append(future.result())
                    except Exception as exc:
                        failures.append((request[0], request[1], exc))
                        LOGGER.error(
                            "Could not fetch %s for %s: %s",
                            request[1],
                            request[0],
                            type(exc).__name__,
                        )

            if not rows:
                continue
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
    if failures:
        details = ", ".join(
            f"{series} on {delivery_date} ({type(exc).__name__})"
            for delivery_date, series, exc in failures[:5]
        )
        raise RuntimeError(
            f"{len(failures)} source response(s) failed validation or download: "
            f"{details}"
        ) from None
    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest raw ENTSO-E payloads into delu.bronze.raw"
    )
    parser.add_argument("mode", choices=("full", "morning", "settlement"))
    parser.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument(
        "--run-date",
        type=date.fromisoformat,
        help="Date the scheduled job started, used to make retries deterministic",
    )
    parser.add_argument("--workers", type=int, default=WORKERS)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    ingest(
        args.mode,
        start=args.start,
        end=args.end,
        run_date=args.run_date,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
