from __future__ import annotations

import json
import logging
import math
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import cast

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from delu.pipeline.bronze import (
    ALL,
    BERLIN,
    EXAA,
    SDAC,
    WEATHER_FALLBACK_MODEL,
    WEATHER_FIELDS,
    WEATHER_LOCATIONS,
    WEATHER_MODEL,
)
from delu.pipeline.bronze import TABLE as BRONZE_TABLE

TABLE = "delu.silver.measurements"
RESOLUTION = timedelta(minutes=15)
KNOWN_SERIES = frozenset(request[0] for request in ALL)
PRICE_SERIES = frozenset(request[0] for request in SDAC + EXAA)
LOGGER = logging.getLogger(__name__)
SILVER_SCHEMA = StructType(
    [
        StructField("delivery_date", DateType(), nullable=False),
        StructField("delivery_start_utc", TimestampType(), nullable=False),
        StructField("series", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
        StructField("unit", StringType(), nullable=False),
        StructField("source_ingested_at", TimestampType(), nullable=False),
    ]
)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _required_text(element: ET.Element, tag: str) -> str:
    for node in element.iter():
        if _local_name(node.tag) == tag and node.text and node.text.strip():
            return node.text.strip()
    raise ValueError(f"Missing <{tag}> in <{_local_name(element.tag)}>")


def _utc_datetime(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"Timestamp is not timezone-aware: {text!r}")
    return value.astimezone(UTC)


def _expand_curve(
    points: Sequence[tuple[int, float]],
    interval_count: int,
    curve_type: str,
) -> list[float]:
    values: list[float | None] = [None] * interval_count
    for position, value in points:
        if not 1 <= position <= interval_count:
            raise ValueError(
                f"Curve position must be between 1 and {interval_count}; "
                f"found {position}"
            )
        if values[position - 1] is not None:
            raise ValueError(f"Curve contains duplicate position {position}")
        if not math.isfinite(value):
            raise ValueError(f"Curve contains non-finite value at position {position}")
        values[position - 1] = value

    if curve_type == "A01":
        if any(value is None for value in values):
            raise ValueError("A01 curve must contain every interval position")
    elif curve_type == "A03":
        if not values or values[0] is None:
            raise ValueError("A03 curve must start at position 1")
        previous = values[0]
        for index, value in enumerate(values):
            if value is None:
                values[index] = previous
            else:
                previous = value
    else:
        raise ValueError(f"Unsupported curve type {curve_type!r}")

    if any(value is None for value in values):
        raise ValueError("Curve contains an unfilled interval")
    return cast(list[float], values)


def parse_payload(
    payload: str,
    series: str,
    delivery_date: date,
) -> list[tuple[datetime, float]]:
    """Parse one raw ENTSO-E response into complete 15-minute UTC intervals."""
    if series not in KNOWN_SERIES:
        raise ValueError(f"Unknown series {series!r}")
    if not payload or not payload.strip():
        raise ValueError("Payload is empty")
    if delivery_date is None:
        raise ValueError("Delivery date is missing")

    is_price = series in PRICE_SERIES
    value_tag = "price.amount" if is_price else "quantity"
    unit_tag = "price_Measure_Unit.name" if is_price else "quantity_Measure_Unit.name"
    expected_unit = "MWH" if is_price else "MAW"
    day_start = datetime.combine(delivery_date, time.min, BERLIN).astimezone(UTC)
    day_end = datetime.combine(
        delivery_date + timedelta(days=1), time.min, BERLIN
    ).astimezone(UTC)

    try:
        root = ET.fromstring(payload)
        intervals: dict[datetime, float] = {}
        curve_type: str | None = None

        for time_series in root.iter():
            if _local_name(time_series.tag) != "TimeSeries":
                continue

            unit = _required_text(time_series, unit_tag)
            if unit != expected_unit:
                raise ValueError(f"Expected unit {expected_unit!r}, found {unit!r}")
            if is_price:
                currency = _required_text(time_series, "currency_Unit.name")
                if currency != "EUR":
                    raise ValueError(f"Expected currency 'EUR', found {currency!r}")

            curve_type = _required_text(time_series, "curveType")
            for period in time_series.iter():
                if _local_name(period.tag) != "Period":
                    continue

                start = _utc_datetime(_required_text(period, "start"))
                end = _utc_datetime(_required_text(period, "end"))
                resolution = _required_text(period, "resolution")
                if resolution != "PT15M":
                    raise ValueError(
                        f"Expected 15-minute resolution, found {resolution!r}"
                    )
                if start < day_start or end > day_end:
                    raise ValueError("Period falls outside the requested delivery date")

                span = end - start
                if span <= timedelta(0) or span % RESOLUTION != timedelta(0):
                    raise ValueError(
                        "Period must span a positive whole number of intervals"
                    )
                interval_count = int(span // RESOLUTION)
                points = [
                    (
                        int(_required_text(point, "position")),
                        float(_required_text(point, value_tag)),
                    )
                    for point in period.iter()
                    if _local_name(point.tag) == "Point"
                ]

                for position, value in enumerate(
                    _expand_curve(points, interval_count, curve_type)
                ):
                    timestamp = start + position * RESOLUTION
                    previous = intervals.get(timestamp)
                    if previous is not None and previous != value:
                        raise ValueError(
                            f"Conflicting values for interval {timestamp.isoformat()}"
                        )
                    intervals[timestamp] = value

        expected_count = int((day_end - day_start) // RESOLUTION)
        expected = [day_start + offset * RESOLUTION for offset in range(expected_count)]
        if curve_type == "A03" and intervals:
            first = min(intervals)
            for offset in range(int((first - day_start) // RESOLUTION)):
                intervals[day_start + offset * RESOLUTION] = intervals[first]

            last = max(intervals)
            for offset in range(1, int((day_end - last) // RESOLUTION)):
                intervals[last + offset * RESOLUTION] = intervals[last]
        missing = set(expected).difference(intervals)
        extra = set(intervals).difference(expected)
        if missing or extra:
            raise ValueError(
                f"Expected {expected_count} delivery intervals, found {len(intervals)} "
                f"({len(missing)} missing, {len(extra)} outside the delivery date)"
            )
        return [(timestamp, intervals[timestamp]) for timestamp in expected]
    except (ET.ParseError, TypeError, ValueError) as exc:
        raise ValueError(
            f"Could not parse {series} for {delivery_date}: {exc}"
        ) from exc


def _weather_value(
    field: str,
    primary: object,
    fallback: object,
    valid_time: datetime,
    run_start: datetime,
) -> float:
    value = primary
    if value is None and field == "temperature_2m":
        value = fallback
    if value is None and field == "shortwave_radiation" and valid_time == run_start:
        value = 0.0
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"Weather {field} contains an unexpected null or value")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"Weather {field} contains a non-finite value")
    return result


def _weather_times(raw: object, run_start: datetime) -> list[datetime]:
    if not isinstance(raw, list) or len(raw) != 48:
        raise ValueError("Weather response must contain 48 hourly timestamps")
    try:
        values = [datetime.fromisoformat(value) for value in raw]
    except (TypeError, ValueError) as exc:
        raise ValueError("Weather response contains an invalid timestamp") from exc
    values = [
        value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        for value in values
    ]
    expected = [run_start + timedelta(hours=offset) for offset in range(48)]
    if values != expected:
        raise ValueError("Weather response must contain 48 consecutive run hours")
    return values


def _weather_location(
    raw: object,
    location_id: str,
    expected_latitude: float,
    expected_longitude: float,
    run_start: datetime,
) -> tuple[Mapping[str, object], Mapping[str, object], list[datetime]]:
    if not isinstance(raw, Mapping):
        raise ValueError("Each weather location must be an object")
    if raw.get("timezone") != "GMT" or raw.get("utc_offset_seconds") != 0:
        raise ValueError("Weather timestamps must use GMT")
    units = raw.get("hourly_units")
    hourly = raw.get("hourly")
    if not isinstance(units, Mapping) or not isinstance(hourly, Mapping):
        raise ValueError("Weather hourly values and units must be objects")
    if units.get("time") != "iso8601":
        raise ValueError("Weather timestamps must use ISO 8601")
    try:
        latitude = float(raw["latitude"])
        longitude = float(raw["longitude"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Weather response contains invalid coordinates") from exc
    if (
        abs(latitude - expected_latitude) > 0.25
        or abs(longitude - expected_longitude) > 0.25
    ):
        raise ValueError(f"Weather location order changed at {location_id}")
    return units, hourly, _weather_times(hourly.get("time"), run_start)


def _parse_weather_payload(
    payload: str,
    run_date: date,
    cell: str,
) -> list[tuple[date, datetime, str, float, str]]:
    """Parse one weather cell response into next-day location series."""
    locations = [location for location in WEATHER_LOCATIONS if location[3] == cell]
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid weather JSON for {run_date} {cell}") from exc
    if isinstance(decoded, Mapping):
        data = decoded.get("primary")
        fallback_data = decoded.get("temperature_fallback")
    else:
        data = decoded
        fallback_data = None
    if not isinstance(data, list) or len(data) != len(locations):
        raise ValueError(
            f"Expected {len(locations)} weather locations for {cell}, "
            f"found {len(data) if isinstance(data, list) else 'a non-list response'}"
        )
    if fallback_data is not None and (
        not isinstance(fallback_data, list) or len(fallback_data) != len(locations)
    ):
        raise ValueError("Weather temperature fallback has the wrong location count")

    run_start = datetime.combine(run_date, time.min, UTC)
    delivery_date = run_date + timedelta(days=1)
    rows = []
    fallback_locations = fallback_data or [None] * len(locations)
    for raw, fallback_raw, location in zip(
        data, fallback_locations, locations, strict=True
    ):
        location_id, expected_latitude, expected_longitude, _ = location
        units, hourly, valid_times = _weather_location(
            raw,
            location_id,
            expected_latitude,
            expected_longitude,
            run_start,
        )
        separate_fallback = None
        if fallback_raw is not None:
            fallback_units, fallback_hourly, _ = _weather_location(
                fallback_raw,
                location_id,
                expected_latitude,
                expected_longitude,
                run_start,
            )
            separate_fallback = fallback_hourly.get("temperature_2m")
            if (
                not isinstance(separate_fallback, list)
                or len(separate_fallback) != 48
                or fallback_units.get("temperature_2m") != "°C"
            ):
                raise ValueError(
                    "Weather temperature fallback must contain 48 values in °C"
                )
        for field, _, unit in WEATHER_FIELDS:
            primary_name = f"{field}_{WEATHER_MODEL}"
            if primary_name not in hourly:
                primary_name = field
            primary = hourly.get(primary_name)
            fallback_name = f"{field}_{WEATHER_FALLBACK_MODEL}"
            uses_separate_fallback = (
                field == "temperature_2m" and separate_fallback is not None
            )
            fallback = (
                separate_fallback
                if uses_separate_fallback
                else hourly.get(fallback_name)
            )
            if not isinstance(primary, list) or len(primary) != 48:
                raise ValueError(f"Weather {field} must contain 48 hourly values")
            if units.get(primary_name) != unit:
                raise ValueError(f"Weather {field} has an unexpected unit")
            if fallback is not None and (
                not isinstance(fallback, list) or len(fallback) != 48
            ):
                raise ValueError(f"Weather fallback {field} must contain 48 values")
            if (
                fallback is not None
                and not uses_separate_fallback
                and units.get(fallback_name) != unit
            ):
                raise ValueError(f"Weather fallback {field} has an unexpected unit")
            values = [
                _weather_value(
                    field,
                    value,
                    None if fallback is None else fallback[index],
                    valid_time,
                    run_start,
                )
                for index, (valid_time, value) in enumerate(
                    zip(valid_times, primary, strict=True)
                )
            ]
            rows.extend(
                (
                    delivery_date,
                    valid_time,
                    f"weather.{location_id}.{field}",
                    value,
                    unit,
                )
                for valid_time, value in zip(valid_times, values, strict=True)
                if valid_time.astimezone(BERLIN).date() == delivery_date
            )
    return rows


def validate_payload(payload: str, series: str, source_date: date) -> None:
    """Reject an incomplete or non-finite source response before Bronze stores it."""
    if series.startswith(f"weather.{WEATHER_MODEL}."):
        _parse_weather_payload(payload, source_date, series.rsplit(".", 1)[-1])
        return
    parse_payload(payload, series, source_date)


def _validate_raw(raw: DataFrame) -> None:
    columns = ("delivery_date", "series", "payload", "ingested_at")
    stats = raw.agg(
        F.count(F.lit(1)).alias("rows"),
        *[
            F.sum(F.col(column).isNull().cast("long")).alias(column)
            for column in columns
        ],
    ).first()
    if stats is None:
        raise ValueError(f"Could not validate {BRONZE_TABLE}")
    if stats.rows == 0:
        raise ValueError(f"{BRONZE_TABLE} is empty")

    missing = [column for column in columns if stats[column]]
    if missing:
        raise ValueError(f"{BRONZE_TABLE} contains null values in {missing}")


def _parse_rows(
    rows: Iterable[tuple[date, str, str, datetime]],
) -> list[tuple[date, datetime, str, float, str, datetime]]:
    parsed = []
    for delivery_date, series, payload, ingested_at in rows:
        if series.startswith(f"weather.{WEATHER_MODEL}."):
            parsed.extend(
                (*row, ingested_at)
                for row in _parse_weather_payload(
                    payload,
                    delivery_date,
                    series.rsplit(".", 1)[-1],
                )
            )
            continue
        unit = "EUR/MWh" if series in PRICE_SERIES else "MW"
        parsed.extend(
            (
                delivery_date,
                delivery_start_utc,
                series,
                value,
                unit,
                ingested_at,
            )
            for delivery_start_utc, value in parse_payload(
                payload, series, delivery_date
            )
        )
    return parsed


def silver_frame(raw: DataFrame, spark: SparkSession | None = None) -> DataFrame:
    latest = (
        raw.withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("delivery_date", "series").orderBy(
                    F.col("ingested_at").desc()
                )
            ),
        )
        .where(F.col("_rank") == 1)
        .select("delivery_date", "series", "payload", "ingested_at")
    )

    spark = spark or raw.sparkSession
    return spark.createDataFrame(
        _parse_rows(latest.toLocalIterator()),
        schema=SILVER_SCHEMA,
    )


def build(spark: SparkSession | None = None) -> None:
    spark = (
        spark
        or SparkSession.getActiveSession()
        or DatabricksSession.builder.serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if not spark.catalog.tableExists(BRONZE_TABLE):
        LOGGER.info("Waiting for source data.")
        return
    raw = spark.table(BRONZE_TABLE)
    if raw.limit(1).count() == 0:
        LOGGER.info("Waiting for source data. Stored Silver data is preserved.")
        return
    _validate_raw(raw)

    spark.sql("CREATE SCHEMA IF NOT EXISTS delu.silver")
    (
        silver_frame(raw, spark)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(TABLE)
    )
    LOGGER.info("Rebuilt %s from the latest responses in %s.", TABLE, BRONZE_TABLE)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    build()


if __name__ == "__main__":
    main()
