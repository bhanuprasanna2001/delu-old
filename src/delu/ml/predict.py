"""Publish missing forecasts whenever their inputs become available."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, date, datetime, time, timedelta
from typing import Literal

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from databricks.connect import DatabricksSession
from mlflow import MlflowClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from delu.contracts import FEATURE_DATA_VERSION
from delu.ml.data import prepare_daily_data
from delu.ml.model import ConformalPriceForecaster
from delu.ml.tables import merge_delta
from delu.ml.train import MODEL_NAME, TARGET_COVERAGE
from delu.pipeline.bronze import BERLIN
from delu.pipeline.gold import TABLE as GOLD_TABLE

FORECAST_TABLE = "delu.gold.forecasts"
FORECAST_RUN_TABLE = "delu.gold.forecast_runs"
LOGGER = logging.getLogger(__name__)
EXAA_SIGNAL_TIME = time(10, 15)
SDAC_GATE_CLOSURE = time(12)
PublicationStatus = Literal["on_time", "late", "backfill"]


def publication_status(
    delivery_date: date,
    predicted_at: datetime,
) -> PublicationStatus:
    """Classify whether a stored forecast was usable in the auction window."""
    timestamp = (
        predicted_at.replace(tzinfo=UTC)
        if predicted_at.tzinfo is None or predicted_at.utcoffset() is None
        else predicted_at.astimezone(UTC)
    )
    local = timestamp.astimezone(BERLIN)
    if local.date() != delivery_date - timedelta(days=1):
        return "backfill"
    local_time = local.time().replace(tzinfo=None)
    if EXAA_SIGNAL_TIME <= local_time < SDAC_GATE_CLOSURE:
        return "on_time"
    if local_time >= SDAC_GATE_CLOSURE:
        return "late"
    return "backfill"


def _drift_values(
    model: ConformalPriceForecaster,
    features: np.ndarray,
    point: np.ndarray,
) -> tuple[float, float, float]:
    normalized = (features - model.feature_mean_) / model.feature_scale_
    outlier_rate = float(np.mean(np.abs(normalized) > 4))
    max_feature_mean_z = float(np.max(np.abs(normalized.mean(axis=(0, 1)))))
    prediction_mean_z = float((point.mean() - model.target_mean_) / model.target_scale_)
    return outlier_rate, max_feature_mean_z, prediction_mean_z


def forecast_day(
    delivery_date: date,
    *,
    spark: SparkSession | None = None,
    now: datetime | None = None,
) -> str | None:
    """Publish a missing day, or leave it pending until complete inputs arrive."""
    current = datetime.now(UTC) if now is None else now
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")

    spark = (
        spark
        or SparkSession.getActiveSession()
        or DatabricksSession.builder.serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if spark.catalog.tableExists(FORECAST_RUN_TABLE):
        existing = (
            spark.table(FORECAST_RUN_TABLE)
            .where(F.col("delivery_date") == F.lit(delivery_date))
            .first()
        )
        if existing is not None:
            LOGGER.info("Forecast for %s is already published.", delivery_date)
            return str(existing.model_version)
    if not spark.catalog.tableExists(GOLD_TABLE):
        LOGGER.info("Waiting for model inputs for %s.", delivery_date)
        return None
    gold = (
        spark.table(GOLD_TABLE)
        .where(F.col("delivery_date") == F.lit(delivery_date))
        .orderBy("quarter_of_day")
        .toPandas()
    )
    if len(gold) < 96:
        LOGGER.info("Waiting for complete model inputs for %s.", delivery_date)
        return None
    daily = prepare_daily_data(gold, require_targets=False)
    if len(daily) != 1 or daily.dates[0] != delivery_date:
        raise ValueError(f"Gold does not contain one complete day for {delivery_date}")

    mlflow.set_registry_uri("databricks-uc")
    version = MlflowClient().get_model_version_by_alias(MODEL_NAME, "prod")
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{version.version}")
    if not isinstance(model, ConformalPriceForecaster):
        raise TypeError("The production model has an incompatible Python type")
    if getattr(model, "feature_data_version", 1) != FEATURE_DATA_VERSION:
        LOGGER.warning(
            "Waiting for a production model trained on feature data version %s; "
            "model %s uses version %s",
            FEATURE_DATA_VERSION,
            version.version,
            getattr(model, "feature_data_version", 1),
        )
        return None
    prediction = model.predict(daily.features)[0]
    if not np.isfinite(prediction).all() or np.any(prediction[:, 1] > prediction[:, 2]):
        raise ValueError("The production model returned invalid prediction intervals")

    outlier_rate, max_feature_mean_z, prediction_mean_z = _drift_values(
        model, daily.features, prediction[:, 0]
    )
    published_at = current if now is not None else datetime.now(UTC)
    predicted_at = published_at.astimezone(UTC).replace(tzinfo=None)
    status = publication_status(delivery_date, published_at)
    sorted_gold = gold.sort_values("quarter_of_day")
    forecast = pd.DataFrame(
        {
            "delivery_date": [delivery_date] * 96,
            "delivery_start_local": sorted_gold["delivery_start_local"].to_numpy(),
            "quarter_of_day": np.arange(96, dtype=np.int32),
            "predicted_price_eur_per_mwh": prediction[:, 0],
            "lower_price_eur_per_mwh": prediction[:, 1],
            "upper_price_eur_per_mwh": prediction[:, 2],
            "nominal_coverage": TARGET_COVERAGE,
            "model_version": version.version,
            "predicted_at": predicted_at,
        }
    )
    run = pd.DataFrame(
        [
            {
                "delivery_date": delivery_date,
                "model_version": version.version,
                "predicted_at": predicted_at,
                "publication_status": status,
                "data_outlier_rate": outlier_rate,
                "max_feature_mean_z": max_feature_mean_z,
                "prediction_mean_z": prediction_mean_z,
            }
        ]
    )
    merge_delta(
        spark,
        spark.createDataFrame(forecast),
        table=FORECAST_TABLE,
        keys=("delivery_date", "quarter_of_day"),
    )
    merge_delta(
        spark,
        spark.createDataFrame(run),
        table=FORECAST_RUN_TABLE,
        keys=("delivery_date",),
        evolve_schema=True,
    )
    LOGGER.info(
        "Saved 96 %s forecasts for %s using %s version %s",
        status,
        delivery_date,
        MODEL_NAME,
        version.version,
    )
    return version.version


def forecast_pending(
    spark: SparkSession,
    *,
    start: date | None = None,
    end: date | None = None,
) -> None:
    """Fill missing days from the beginning of published forecast history."""
    end = end or datetime.now(UTC).astimezone(BERLIN).date() + timedelta(days=1)
    if not spark.catalog.tableExists(GOLD_TABLE):
        LOGGER.info("Waiting for model inputs.")
        return
    ready = (
        spark.table(GOLD_TABLE)
        .groupBy("delivery_date")
        .count()
        .where(F.col("count") == 96)
        .select("delivery_date")
    )
    if spark.catalog.tableExists(FORECAST_RUN_TABLE):
        published = spark.table(FORECAST_RUN_TABLE).select("delivery_date")
        first = published.agg(F.min("delivery_date").alias("day")).first()
        start = start or (first.day if first is not None else None)
        ready = ready.join(published, "delivery_date", "left_anti")
    if start is None:
        mlflow.set_registry_uri("databricks-uc")
        version = MlflowClient().get_model_version_by_alias(MODEL_NAME, "prod")
        start = (
            datetime.fromtimestamp(version.creation_timestamp / 1000, UTC)
            .astimezone(BERLIN)
            .date()
        )
    if start > end:
        raise ValueError("forecast start cannot be after end")
    for row in (
        ready.where(F.col("delivery_date").between(start, end))
        .orderBy("delivery_date")
        .collect()
    ):
        forecast_day(row.delivery_date, spark=spark)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery-date", type=date.fromisoformat)
    parser.add_argument("--start", help="First delivery date to backfill")
    parser.add_argument("--end", help="Last delivery date; defaults to tomorrow")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    if args.delivery_date is not None:
        forecast_day(args.delivery_date)
        return
    forecast_pending(
        DatabricksSession.builder.serverless().getOrCreate(),
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
    )


if __name__ == "__main__":
    main()
