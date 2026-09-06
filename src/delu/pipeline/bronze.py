from __future__ import annotations

import argparse
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
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
WORKERS = 12
BATCH_DAYS = 7
LOGGER = logging.getLogger(__name__)

SDAC = tuple(
    (f"{area.lower()}.price.sdac", "query_day_ahead_prices", area, {"sequence": 1})
    for area in ("DE_LU", "AT")
)
EXAA = tuple(
    (f"{area.lower()}.price.exaa", "query_day_ahead_prices", area, {"sequence": 2})
    for area in ("DE_LU", "AT")
)
FORECAST = (
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
ACTUAL = (
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
ALL = SDAC + EXAA + FORECAST + ACTUAL


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

    # Each item is (delivery_date, series, entsoe-py method, area, method kwargs).
    if mode == "morning":
        planned = [
            *((today + timedelta(days=1), *request) for request in EXAA),  # D
            *((today, *request) for request in FORECAST),  # D-1
            *((today - timedelta(days=1), *request) for request in ACTUAL),  # D-2
        ]
        refresh = set()
    elif mode == "settlement":
        planned = [(today + timedelta(days=1), *request) for request in SDAC]  # D
        refresh = set()
    elif mode == "full":
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
    else:
        raise ValueError("mode must be full, morning, or settlement")

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

    api_key = os.getenv("ENTSOE_API_KEY") or DBUtils(spark).secrets.get(
        scope="delu", key="entsoe-api-key"
    )

    def fetch(
        request: tuple[date, str, str, str, dict[str, object]],
    ) -> tuple[date, str, str]:
        delivery_date, series, method, area, params = request
        client = EntsoeRawClient(api_key, retry_count=1, retry_delay=0, timeout=60)
        period_start = pd.Timestamp(delivery_date, tz=BERLIN)
        period_end = pd.Timestamp(delivery_date + timedelta(days=1), tz=BERLIN)

        for attempt in range(4):
            try:
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
                if attempt == 3 or status not in (None, 429, 500, 502, 503, 504):
                    raise
                delay = min(2**attempt, 30) + random.random()
                LOGGER.warning(
                    "Retrying %s for %s in %.1fs: %s", series, delivery_date, delay, exc
                )
                time.sleep(delay)

        raise AssertionError("unreachable")

    written = 0
    for dates in batched(sorted({request[0] for request in planned}), BATCH_DAYS):
        batch = [request for request in planned if request[0] in dates]
        LOGGER.info(
            "Fetching %d ENTSO-E responses for %s through %s with %d workers.",
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
