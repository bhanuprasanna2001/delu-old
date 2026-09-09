"""Publish one genuine next-day forecast before the market result is available."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, date, datetime, timedelta

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from databricks.connect import DatabricksSession
from mlflow import MlflowClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from delu.ml.data import prepare_daily_data
from delu.ml.model import ConformalPriceForecaster
from delu.ml.tables import merge_delta
from delu.ml.train import MODEL_NAME, TARGET_COVERAGE
from delu.pipeline.bronze import BERLIN, SETTLEMENT_CUTOFF
from delu.pipeline.gold import TABLE as GOLD_TABLE

FORECAST_TABLE = "delu.gold.forecasts"
FORECAST_RUN_TABLE = "delu.gold.forecast_runs"
LOGGER = logging.getLogger(__name__)


def _validate_forecast_time(delivery_date: date, current: datetime) -> None:
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    local = current.astimezone(BERLIN)
    if delivery_date != local.date() + timedelta(days=1):
        raise ValueError("Production forecasts are only allowed for tomorrow")
    if local.time() >= SETTLEMENT_CUTOFF:
        raise ValueError(
            "The 15:00 Europe/Berlin production forecast cutoff has passed"
        )


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
) -> str:
    """Forecast tomorrow, failing if inputs are incomplete or the cutoff passed."""
    current = datetime.now(UTC) if now is None else now
    _validate_forecast_time(delivery_date, current)

    spark = (
        spark
        or SparkSession.getActiveSession()
        or DatabricksSession.builder.serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    gold = (
        spark.table(GOLD_TABLE)
        .where(F.col("delivery_date") == F.lit(delivery_date))
        .orderBy("quarter_of_day")
        .toPandas()
    )
    daily = prepare_daily_data(gold, require_targets=False)
    if len(daily) != 1 or daily.dates[0] != delivery_date:
        raise ValueError(f"Gold does not contain one complete day for {delivery_date}")

    mlflow.set_registry_uri("databricks-uc")
    version = MlflowClient().get_model_version_by_alias(MODEL_NAME, "prod")
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{version.version}")
    if not isinstance(model, ConformalPriceForecaster):
        raise TypeError("The production model has an incompatible Python type")
    prediction = model.predict(daily.features)[0]
    if not np.isfinite(prediction).all() or np.any(prediction[:, 1] > prediction[:, 2]):
        raise ValueError("The production model returned invalid prediction intervals")

    outlier_rate, max_feature_mean_z, prediction_mean_z = _drift_values(
        model, daily.features, prediction[:, 0]
    )
    published_at = current if now is not None else datetime.now(UTC)
    _validate_forecast_time(delivery_date, published_at)
    predicted_at = published_at.astimezone(UTC).replace(tzinfo=None)
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
    )
    LOGGER.info(
        "Saved 96 forecasts for %s using %s version %s",
        delivery_date,
        MODEL_NAME,
        version.version,
    )
    return version.version


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    dates = parser.add_mutually_exclusive_group()
    dates.add_argument("--delivery-date", type=date.fromisoformat)
    dates.add_argument(
        "--run-date",
        type=date.fromisoformat,
        help="Date the scheduled job started, used to make retries deterministic",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    current = datetime.now(UTC)
    run_date = args.run_date or current.astimezone(BERLIN).date()
    forecast_day(args.delivery_date or run_date + timedelta(days=1), now=current)


if __name__ == "__main__":
    main()
