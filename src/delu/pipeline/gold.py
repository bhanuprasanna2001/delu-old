"""Build complete model-input days as their source data becomes available."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, date, datetime, time, timedelta
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from delu.pipeline.bronze import (
    BERLIN,
    PRICE_LAGS,
    RETRYABLE_HTTP_STATUSES,
    WEATHER_FIELDS,
    WEATHER_LOCATIONS,
)
from delu.pipeline.silver import KNOWN_AUTUMN_96_SERIES
from delu.pipeline.silver import TABLE as SILVER_TABLE

TABLE = "delu.gold.model_input"
HOLIDAY_TABLE = "delu.silver.holidays"
HOLIDAYS_URL = "https://openholidaysapi.org/PublicHolidays"
QUARTERS_PER_DAY = 96
KEY = ["delivery_date", "quarter_of_day"]

TARGET = "de_lu.price.sdac"
EXAA = ("de_lu.price.exaa", "at.price.exaa")
FORECAST = (
    "de_lu.load.forecast",
    "de_lu.generation.solar.forecast",
    "de_lu.generation.wind_onshore.forecast",
    "de_lu.generation.wind_offshore.forecast",
)
ACTUAL = (
    "de_lu.load.actual",
    "de_lu.generation.solar.actual",
    "de_lu.generation.wind_onshore.actual",
    "de_lu.generation.wind_offshore.actual",
)
WEATHER_SERIES = tuple(
    f"weather.{location_id}.{field}"
    for location_id, _, _, _ in WEATHER_LOCATIONS
    for field, _, _ in WEATHER_FIELDS
)
WEATHER_FEATURES = tuple(
    f"weather_{location_id}_{column}"
    for location_id, _, _, _ in WEATHER_LOCATIONS
    for _, column, _ in WEATHER_FIELDS
)
REQUIRED_SERIES = (TARGET, *EXAA, *FORECAST, *ACTUAL, *WEATHER_SERIES)
FEATURE_COLUMNS = (
    "price_de_lu_exaa_eur_per_mwh",
    "price_at_exaa_eur_per_mwh",
    *(f"price_de_lu_sdac_lag_{days}d_eur_per_mwh" for days in PRICE_LAGS),
    *(
        "load_forecast_delivery_d_minus_1_mw",
        "solar_forecast_delivery_d_minus_1_mw",
        "wind_onshore_forecast_delivery_d_minus_1_mw",
        "wind_offshore_forecast_delivery_d_minus_1_mw",
        "load_actual_d_minus_2_mw",
        "solar_actual_d_minus_2_mw",
        "wind_onshore_actual_d_minus_2_mw",
        "wind_offshore_actual_d_minus_2_mw",
    ),
    *WEATHER_FEATURES,
)
LOGGER = logging.getLogger(__name__)

HOLIDAY_SCHEMA = StructType(
    [
        StructField("delivery_date", DateType(), nullable=False),
        StructField("is_holiday_de_nationwide", BooleanType(), nullable=False),
        StructField("is_holiday_lu", BooleanType(), nullable=False),
    ]
)


def _validate_silver(silver: DataFrame) -> None:
    """Reject invalid measurements without requiring every source to be present."""
    value = F.col("value")
    invalid = (
        value.isNull()
        | F.isnan(value)
        | (value == F.lit(float("inf")))
        | (value == F.lit(float("-inf")))
    )
    if silver.where(invalid).limit(1).count():
        raise ValueError("Silver contains non-finite measurements")
    duplicate = (
        silver.groupBy("delivery_date", "delivery_start_utc", "series")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise ValueError("Silver contains duplicate measurement keys")


def _normalise_intervals(silver: DataFrame) -> DataFrame:
    """Map physical UTC intervals to 96 local wall-clock quarters."""
    source = (
        silver.where(F.col("series").isin(*REQUIRED_SERIES))
        .select("delivery_date", "delivery_start_utc", "series", "value")
        .withColumn(
            "delivery_start_local",
            F.from_utc_timestamp("delivery_start_utc", "Europe/Berlin"),
        )
        .withColumn(
            "quarter_of_day",
            F.hour("delivery_start_local") * 4
            + F.floor(F.minute("delivery_start_local") / F.lit(15)),
        )
    )
    start = F.to_utc_timestamp(
        F.col("delivery_date").cast("timestamp"), "Europe/Berlin"
    )
    end = F.to_utc_timestamp(
        F.date_add("delivery_date", 1).cast("timestamp"), "Europe/Berlin"
    )
    step = F.when(
        F.col("series").startswith("weather."), F.expr("INTERVAL 1 HOUR")
    ).otherwise(F.expr("INTERVAL 15 MINUTES"))
    grids = (
        source.groupBy("delivery_date", "series")
        .agg(F.sort_array(F.collect_set("delivery_start_utc")).alias("actual"))
        .withColumn("expected", F.sequence(start, end - step, step))
    )
    known_autumn_gap = (
        F.col("series").isin(*KNOWN_AUTUMN_96_SERIES)
        & (F.size("expected") == 100)
        & (F.col("actual") == F.slice("expected", 5, 96))
    )
    if (
        grids.where((F.col("actual") != F.col("expected")) & ~known_autumn_gap)
        .limit(1)
        .count()
    ):
        raise ValueError("Silver contains an incomplete or misaligned physical day")

    values = source.groupBy(*KEY, "series").agg(
        F.avg("value").alias("scalar_mean"),
        F.avg(F.sin(F.radians("value"))).alias("mean_sin"),
        F.avg(F.cos(F.radians("value"))).alias("mean_cos"),
        F.count("*").alias("physical_count"),
    )
    direction = F.col("series").endswith(".wind_direction_100m")
    resultant = F.hypot("mean_sin", "mean_cos")
    if values.where(direction & (resultant < 1e-12)).limit(1).count():
        raise ValueError("Repeated wind directions have an undefined circular mean")
    circular = F.pmod(F.degrees(F.atan2("mean_sin", "mean_cos")), F.lit(360.0))
    circular = F.when(circular > 360 - 1e-9, 0.0).otherwise(circular)
    values = values.select(
        *KEY,
        "series",
        F.when(direction & (F.col("physical_count") > 1), circular)
        .otherwise(F.col("scalar_mean"))
        .alias("value"),
    )
    grid = (
        source.select("delivery_date", "series")
        .distinct()
        .withColumn(
            "quarter_of_day",
            F.explode(F.sequence(F.lit(0), F.lit(QUARTERS_PER_DAY - 1))),
        )
        .join(values, [*KEY, "series"], "left")
    )

    order = Window.partitionBy("delivery_date", "series").orderBy("quarter_of_day")
    previous_q = F.last(
        F.when(F.col("value").isNotNull(), F.col("quarter_of_day")),
        ignorenulls=True,
    ).over(order.rowsBetween(Window.unboundedPreceding, -1))
    previous_value = F.last("value", ignorenulls=True).over(
        order.rowsBetween(Window.unboundedPreceding, -1)
    )
    next_q = F.first(
        F.when(F.col("value").isNotNull(), F.col("quarter_of_day")),
        ignorenulls=True,
    ).over(order.rowsBetween(1, Window.unboundedFollowing))
    next_value = F.first("value", ignorenulls=True).over(
        order.rowsBetween(1, Window.unboundedFollowing)
    )
    interpolated = previous_value + (
        (next_value - previous_value)
        * (F.col("quarter_of_day") - previous_q)
        / (next_q - previous_q)
    )
    weather_value = (
        F.when(direction, previous_value)
        .when(
            F.floor(F.col("quarter_of_day") / 4) == F.floor(previous_q / 4),
            previous_value,
        )
        .otherwise(interpolated)
    )
    filled = (
        F.when(
            F.col("series").startswith("weather."),
            F.coalesce(F.col("value"), weather_value, next_value),
        )
        .when(
            F.col("series").isin(*KNOWN_AUTUMN_96_SERIES),
            F.coalesce(F.col("value"), interpolated, next_value),
        )
        .otherwise(F.coalesce(F.col("value"), interpolated))
    )
    result = grid.withColumn("value", filled).select(*KEY, "series", "value")
    if result.where(F.col("value").isNull()).limit(1).count():
        raise ValueError("Silver contains a gap that cannot be mapped to 96 quarters")
    return result


def _wide(
    intervals: DataFrame, series: tuple[str, ...], names: tuple[str, ...]
) -> DataFrame:
    expressions = [
        F.first(
            F.when(F.col("series") == source, F.col("value")),
            ignorenulls=True,
        ).alias(name)
        for source, name in zip(series, names, strict=True)
    ]
    return intervals.groupBy(*KEY).agg(*expressions)


def _shift_date(frame: DataFrame, days: int) -> DataFrame:
    return frame.withColumn("delivery_date", F.date_add("delivery_date", days))


def _add_price_lags(frame: DataFrame, target: DataFrame) -> DataFrame:
    history = target.where(F.col("price_de_lu_sdac_eur_per_mwh").isNotNull()).select(
        *KEY, "price_de_lu_sdac_eur_per_mwh"
    )
    result = frame
    for days in PRICE_LAGS:
        name = f"price_de_lu_sdac_lag_{days}d_eur_per_mwh"
        lookup = history.select(
            F.date_add("delivery_date", days).alias("delivery_date"),
            "quarter_of_day",
            F.col("price_de_lu_sdac_eur_per_mwh").alias(name),
        )
        result = result.join(lookup, KEY, "inner")
    return result


def _calendar_frame(spark: SparkSession, start: date, end: date) -> DataFrame:
    rows = []
    current = start
    while current <= end:
        for quarter_of_day in range(QUARTERS_PER_DAY):
            hour, quarter = divmod(quarter_of_day, 4)
            rows.append(
                (
                    current,
                    datetime.combine(current, time(hour, quarter * 15)),
                    quarter_of_day,
                )
            )
        current += timedelta(days=1)
    frame = spark.createDataFrame(
        rows,
        schema=StructType(
            [
                StructField("delivery_date", DateType(), nullable=False),
                StructField("delivery_start_local", TimestampType(), nullable=False),
                StructField("quarter_of_day", IntegerType(), nullable=False),
            ]
        ),
    )
    return (
        frame.withColumn("hour", (F.col("quarter_of_day") / 4).cast("int"))
        .withColumn("quarter", F.col("quarter_of_day") % 4)
        .withColumn("day_of_week", F.pmod(F.dayofweek("delivery_date") + 5, F.lit(7)))
        .withColumn("month", F.month("delivery_date"))
        .withColumn("is_weekend", F.col("day_of_week") >= 5)
        .withColumn(
            "season",
            F.when(F.col("month").isin(12, 1, 2), "winter")
            .when(F.col("month").isin(3, 4, 5), "spring")
            .when(F.col("month").isin(6, 7, 8), "summer")
            .otherwise("autumn"),
        )
    )


def _covered_dates(items: list[dict[str, object]]) -> set[date]:
    covered = set()
    for item in items:
        current = date.fromisoformat(str(item["startDate"]))
        end = date.fromisoformat(str(item["endDate"]))
        if current > end:
            raise ValueError("Holiday startDate cannot be after endDate")
        while current <= end:
            covered.add(current)
            current += timedelta(days=1)
    return covered


def _fetch_holidays(country: str, start: date, end: date) -> set[date]:
    query = urlencode(
        {
            "countryIsoCode": country,
            "validFrom": start.isoformat(),
            "validTo": end.isoformat(),
            "languageIsoCode": "EN",
        }
    )
    request = Request(f"{HOLIDAYS_URL}?{query}", headers={"Accept": "application/json"})
    with urlopen(request, timeout=30) as response:
        items = json.load(response)
    if not isinstance(items, list):
        raise TypeError(f"OpenHolidays returned an unexpected response for {country}")
    if country == "DE":
        items = [item for item in items if item.get("nationwide") is True]
    return _covered_dates(items)


def _holiday_frame(spark: SparkSession, start: date, end: date) -> DataFrame:
    expected_days = (end - start).days + 1
    if spark.catalog.tableExists(HOLIDAY_TABLE):
        cached = spark.table(HOLIDAY_TABLE).where(
            F.col("delivery_date").between(F.lit(start), F.lit(end))
        )
        if cached.count() == expected_days:
            return cached

    cache_start = date(start.year, 1, 1)
    cache_end = date(end.year, 12, 31)
    german = _fetch_holidays("DE", cache_start, cache_end)
    luxembourg = _fetch_holidays("LU", cache_start, cache_end)
    dates = [
        cache_start + timedelta(days=offset)
        for offset in range((cache_end - cache_start).days + 1)
    ]
    calendar = spark.createDataFrame(
        [(day, day in german, day in luxembourg) for day in dates],
        schema=HOLIDAY_SCHEMA,
    )
    spark.sql("CREATE SCHEMA IF NOT EXISTS delu.silver")
    (
        calendar.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(HOLIDAY_TABLE)
    )
    return calendar.where(F.col("delivery_date").between(F.lit(start), F.lit(end)))


def _add_residuals(frame: DataFrame) -> DataFrame:
    return frame.withColumn(
        "residual_load_forecast_delivery_d_minus_1_mw",
        F.col("load_forecast_delivery_d_minus_1_mw")
        - F.col("solar_forecast_delivery_d_minus_1_mw")
        - F.col("wind_onshore_forecast_delivery_d_minus_1_mw")
        - F.col("wind_offshore_forecast_delivery_d_minus_1_mw"),
    ).withColumn(
        "residual_load_actual_d_minus_2_mw",
        F.col("load_actual_d_minus_2_mw")
        - F.col("solar_actual_d_minus_2_mw")
        - F.col("wind_onshore_actual_d_minus_2_mw")
        - F.col("wind_offshore_actual_d_minus_2_mw"),
    )


def build(spark: SparkSession | None = None, through: date | None = None) -> None:
    """Build and overwrite ``delu.gold.model_input``."""
    spark = (
        spark
        or SparkSession.getActiveSession()
        or DatabricksSession.builder.serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if not spark.catalog.tableExists(SILVER_TABLE):
        LOGGER.info("Waiting for source measurements.")
        return
    silver = spark.table(SILVER_TABLE)
    _validate_silver(silver)
    intervals = _normalise_intervals(silver)

    target = _wide(intervals, (TARGET,), ("price_de_lu_sdac_eur_per_mwh",))
    exaa = _wide(
        intervals,
        EXAA,
        ("price_de_lu_exaa_eur_per_mwh", "price_at_exaa_eur_per_mwh"),
    )
    forecast = _shift_date(
        _wide(
            intervals,
            FORECAST,
            (
                "load_forecast_delivery_d_minus_1_mw",
                "solar_forecast_delivery_d_minus_1_mw",
                "wind_onshore_forecast_delivery_d_minus_1_mw",
                "wind_offshore_forecast_delivery_d_minus_1_mw",
            ),
        ),
        1,
    )
    actual = _shift_date(
        _wide(
            intervals,
            ACTUAL,
            (
                "load_actual_d_minus_2_mw",
                "solar_actual_d_minus_2_mw",
                "wind_onshore_actual_d_minus_2_mw",
                "wind_offshore_actual_d_minus_2_mw",
            ),
        ),
        2,
    )
    weather = _wide(intervals, WEATHER_SERIES, WEATHER_FEATURES)

    result = (
        exaa.join(forecast, KEY, "inner")
        .join(actual, KEY, "inner")
        .join(weather, KEY, "inner")
        .join(target, KEY, "left")
    )
    result = _add_price_lags(result, target)
    result = _add_residuals(result)

    if through is None:
        through = datetime.now(UTC).astimezone(BERLIN).date() + timedelta(days=1)
    result = result.where(F.col("delivery_date") <= F.lit(through)).dropna(
        subset=list(FEATURE_COLUMNS)
    )
    result = (
        result.withColumn(
            "_quarters", F.count("*").over(Window.partitionBy("delivery_date"))
        )
        .where(F.col("_quarters") == 96)
        .drop("_quarters")
    )
    bounds = result.agg(
        F.min("delivery_date").alias("start"), F.max("delivery_date").alias("end")
    ).first()
    if bounds is None or bounds.start is None or bounds.end is None:
        LOGGER.info("Waiting for complete model inputs. Stored Gold data is preserved.")
        return

    try:
        holidays = _holiday_frame(spark, bounds.start, bounds.end)
    except HTTPError as exc:
        if exc.code not in RETRYABLE_HTTP_STATUSES | {404}:
            raise
        LOGGER.warning(
            "Waiting for holiday data (HTTP %s). Will retry next run.", exc.code
        )
        return
    except URLError as exc:
        LOGGER.warning(
            "Waiting for holiday data (%s). Will retry next run.", exc.reason
        )
        return

    result = result.join(
        _calendar_frame(spark, bounds.start, bounds.end), KEY, "inner"
    ).join(holidays, "delivery_date", "inner")

    output_columns = [
        "delivery_date",
        "delivery_start_local",
        "hour",
        "quarter",
        "quarter_of_day",
        "day_of_week",
        "month",
        "season",
        "is_weekend",
        "is_holiday_de_nationwide",
        "is_holiday_lu",
        "price_de_lu_sdac_eur_per_mwh",
        "price_de_lu_exaa_eur_per_mwh",
        "price_at_exaa_eur_per_mwh",
        *(f"price_de_lu_sdac_lag_{days}d_eur_per_mwh" for days in PRICE_LAGS),
        "load_forecast_delivery_d_minus_1_mw",
        "solar_forecast_delivery_d_minus_1_mw",
        "wind_onshore_forecast_delivery_d_minus_1_mw",
        "wind_offshore_forecast_delivery_d_minus_1_mw",
        "residual_load_forecast_delivery_d_minus_1_mw",
        "load_actual_d_minus_2_mw",
        "solar_actual_d_minus_2_mw",
        "wind_onshore_actual_d_minus_2_mw",
        "wind_offshore_actual_d_minus_2_mw",
        "residual_load_actual_d_minus_2_mw",
        *WEATHER_FEATURES,
    ]
    spark.sql("CREATE SCHEMA IF NOT EXISTS delu.gold")
    (
        result.select(output_columns)
        .orderBy(*KEY)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE)
    )
    LOGGER.info("Rebuilt %s from %s through %s", TABLE, SILVER_TABLE, through)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Gold model-input table")
    # Backfill bounds select work in Bronze, prediction, and evaluation.
    parser.add_argument("--start", help=argparse.SUPPRESS)
    parser.add_argument("--end", help=argparse.SUPPRESS)
    parser.add_argument(
        "--through",
        type=date.fromisoformat,
        help="Inclusive final target date; defaults to tomorrow in Europe/Berlin",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    build(through=args.through)


if __name__ == "__main__":
    main()
