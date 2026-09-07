"""Run the 11:30 batch forecast and repair current-month gaps."""

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
from delu.pipeline.bronze import BERLIN
from delu.pipeline.gold import TABLE as GOLD_TABLE

FORECAST_TABLE = "delu.gold.forecasts"
FORECAST_RUN_TABLE = "delu.gold.forecast_runs"
LOGGER = logging.getLogger(__name__)


def _spark_session(spark: SparkSession | None) -> SparkSession:
    session = (
        spark
        or SparkSession.getActiveSession()
        or DatabricksSession.builder.serverless().getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    return session


def _dates_to_forecast(
    start: date,
    end: date,
    complete: set[date],
) -> tuple[date, ...]:
    if start > end:
        raise ValueError("forecast start cannot be after end")
    dates = (
        start + timedelta(days=offset) for offset in range((end - start).days + 1)
    )
    missing = [
        delivery_date for delivery_date in dates if delivery_date not in complete
    ]
    if end in missing:
        missing.remove(end)
        missing.insert(0, end)
    return tuple(missing)


def _complete_forecast_dates(
    spark: SparkSession,
    *,
    start: date,
    end: date,
) -> set[date]:
    if not spark.catalog.tableExists(FORECAST_TABLE) or not spark.catalog.tableExists(
        FORECAST_RUN_TABLE
    ):
        return set()

    finite_prices = (
        F.col("predicted_price_eur_per_mwh").isNotNull()
        & ~F.isnan("predicted_price_eur_per_mwh")
        & F.col("lower_price_eur_per_mwh").isNotNull()
        & ~F.isnan("lower_price_eur_per_mwh")
        & F.col("upper_price_eur_per_mwh").isNotNull()
        & ~F.isnan("upper_price_eur_per_mwh")
    )
    valid_interval = (
        F.col("quarter_of_day").between(0, 95)
        & finite_prices
        & (F.col("lower_price_eur_per_mwh") <= F.col("predicted_price_eur_per_mwh"))
        & (F.col("predicted_price_eur_per_mwh") <= F.col("upper_price_eur_per_mwh"))
        & F.col("nominal_coverage").between(0, 1)
        & F.col("model_version").isNotNull()
        & F.col("predicted_at").isNotNull()
    )
    complete_forecasts = (
        spark.table(FORECAST_TABLE)
        .where(F.col("delivery_date").between(F.lit(start), F.lit(end)))
        .groupBy("delivery_date")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct(
                F.when(valid_interval, F.col("quarter_of_day"))
            ).alias("valid_quarters"),
            F.countDistinct("model_version").alias("model_versions"),
            F.first("model_version").alias("model_version"),
        )
        .where(
            (F.col("rows") == 96)
            & (F.col("valid_quarters") == 96)
            & (F.col("model_versions") == 1)
        )
        .select("delivery_date", "model_version")
    )
    finite_run_metrics = (
        F.col("data_outlier_rate").isNotNull()
        & ~F.isnan("data_outlier_rate")
        & F.col("max_feature_mean_z").isNotNull()
        & ~F.isnan("max_feature_mean_z")
        & F.col("prediction_mean_z").isNotNull()
        & ~F.isnan("prediction_mean_z")
    )
    complete_runs = (
        spark.table(FORECAST_RUN_TABLE)
        .where(F.col("delivery_date").between(F.lit(start), F.lit(end)))
        .where(
            finite_run_metrics
            & F.col("model_version").isNotNull()
            & F.col("predicted_at").isNotNull()
        )
        .select("delivery_date", "model_version")
        .distinct()
    )
    return {
        row.delivery_date
        for row in complete_forecasts.join(
            complete_runs,
            ["delivery_date", "model_version"],
            "inner",
        )
        .select("delivery_date")
        .collect()
    }


def _production_model() -> tuple[ConformalPriceForecaster, str]:
    mlflow.set_registry_uri("databricks-uc")
    version = MlflowClient().get_model_version_by_alias(MODEL_NAME, "prod")
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{version.version}")
    if not isinstance(model, ConformalPriceForecaster):
        raise TypeError("The production model has an incompatible Python type")
    return model, version.version


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
) -> str:
    """Forecast 96 local quarters and merge the immutable daily result."""
    spark = _spark_session(spark)
    model, version = _production_model()
    _save_forecast_day(delivery_date, spark=spark, model=model, version=version)
    return version


def _save_forecast_day(
    delivery_date: date,
    *,
    spark: SparkSession,
    model: ConformalPriceForecaster,
    version: str,
) -> None:
    gold = (
        spark.table(GOLD_TABLE)
        .where(F.col("delivery_date") == F.lit(delivery_date))
        .orderBy("quarter_of_day")
        .toPandas()
    )
    daily = prepare_daily_data(gold, require_targets=False)
    if len(daily) != 1 or daily.dates[0] != delivery_date:
        raise ValueError(f"Gold does not contain one complete day for {delivery_date}")

    prediction = model.predict(daily.features)[0]
    if not np.isfinite(prediction).all() or np.any(prediction[:, 1] > prediction[:, 2]):
        raise ValueError("The production model returned invalid prediction intervals")

    predicted_at = datetime.now(UTC).replace(tzinfo=None)
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
            "model_version": version,
            "predicted_at": predicted_at,
        }
    )
    merge_delta(
        spark,
        spark.createDataFrame(forecast),
        table=FORECAST_TABLE,
        keys=("delivery_date", "quarter_of_day"),
    )

    outlier_rate, max_feature_mean_z, prediction_mean_z = _drift_values(
        model, daily.features, prediction[:, 0]
    )
    run = pd.DataFrame(
        [
            {
                "delivery_date": delivery_date,
                "model_version": version,
                "predicted_at": predicted_at,
                "data_outlier_rate": outlier_rate,
                "max_feature_mean_z": max_feature_mean_z,
                "prediction_mean_z": prediction_mean_z,
            }
        ]
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
        version,
    )


def backfill_forecasts(
    start: date,
    end: date,
    *,
    spark: SparkSession | None = None,
) -> tuple[date, ...]:
    """Forecast missing or incomplete days in an inclusive delivery-date window."""
    spark = _spark_session(spark)
    missing = _dates_to_forecast(
        start,
        end,
        _complete_forecast_dates(spark, start=start, end=end),
    )
    if not missing:
        LOGGER.info("Forecasts are complete from %s through %s", start, end)
        return ()

    model, version = _production_model()
    for delivery_date in missing:
        _save_forecast_day(
            delivery_date,
            spark=spark,
            model=model,
            version=version,
        )
    return missing


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery-date", type=date.fromisoformat)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    if args.delivery_date is not None:
        forecast_day(args.delivery_date)
        return

    today = datetime.now(BERLIN).date()
    backfill_forecasts(today.replace(day=1), today + timedelta(days=1))


if __name__ == "__main__":
    main()
