"""Evaluate the released SDAC result and run daily drift checks."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, date, datetime, timedelta

import mlflow
import pandas as pd
from databricks.connect import DatabricksSession
from mlflow import MlflowClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from delu.ml.metrics import forecast_metrics
from delu.ml.monitoring import assess_drift
from delu.ml.predict import FORECAST_RUN_TABLE, FORECAST_TABLE
from delu.ml.tables import merge_delta
from delu.ml.train import MODEL_NAME, TARGET_COVERAGE
from delu.pipeline.bronze import BERLIN
from delu.pipeline.gold import TABLE as GOLD_TABLE

METRICS_TABLE = "delu.gold.forecast_metrics"
LOGGER = logging.getLogger(__name__)


def evaluate_day(
    delivery_date: date,
    *,
    spark: SparkSession | None = None,
    fail_on_drift: bool = True,
) -> dict[str, float]:
    """Join forecast to truth, persist metrics, and alert on material drift."""
    spark = (
        spark
        or SparkSession.getActiveSession()
        or DatabricksSession.builder.serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    forecast = spark.table(FORECAST_TABLE).where(
        F.col("delivery_date") == F.lit(delivery_date)
    )
    actual = spark.table(GOLD_TABLE).where(
        F.col("delivery_date") == F.lit(delivery_date)
    )
    joined = (
        forecast.alias("forecast")
        .join(actual.alias("actual"), ["delivery_date", "quarter_of_day"], "inner")
        .select(
            "delivery_date",
            "quarter_of_day",
            "forecast.model_version",
            "forecast.predicted_price_eur_per_mwh",
            "forecast.lower_price_eur_per_mwh",
            "forecast.upper_price_eur_per_mwh",
            "actual.price_de_lu_sdac_eur_per_mwh",
            "actual.price_de_lu_exaa_eur_per_mwh",
            "actual.price_de_lu_sdac_lag_7d_eur_per_mwh",
        )
        .orderBy("quarter_of_day")
        .toPandas()
    )
    if len(joined) != 96 or joined.isna().to_numpy().any():
        raise ValueError(
            f"Expected 96 complete forecast and target rows for {delivery_date}"
        )
    versions = joined["model_version"].astype("string").unique()
    if len(versions) != 1:
        raise ValueError(f"Forecast rows contain multiple model versions: {versions}")
    version = str(versions[0])

    metrics = forecast_metrics(
        joined["price_de_lu_sdac_eur_per_mwh"],
        joined["predicted_price_eur_per_mwh"],
        joined["lower_price_eur_per_mwh"],
        joined["upper_price_eur_per_mwh"],
        coverage=TARGET_COVERAGE,
    )
    target = joined["price_de_lu_sdac_eur_per_mwh"]
    point = joined["predicted_price_eur_per_mwh"]
    metrics["bias"] = float((point - target).mean())
    metrics["baseline_exaa_mae"] = float(
        (joined["price_de_lu_exaa_eur_per_mwh"] - target).abs().mean()
    )
    metrics["baseline_7d_mae"] = float(
        (joined["price_de_lu_sdac_lag_7d_eur_per_mwh"] - target).abs().mean()
    )
    evaluated_at = datetime.now(UTC).replace(tzinfo=None)
    metric_row: dict[str, object] = {
        "delivery_date": delivery_date,
        "model_version": version,
        **metrics,
        "evaluated_at": evaluated_at,
        "monitoring_status": "pending",
        "monitoring_reasons": "",
        "rolling_7d_mae": metrics["mae"],
        "rolling_7d_baseline_exaa_mae": metrics["baseline_exaa_mae"],
        "rolling_28d_picp": metrics["picp"],
    }
    updates = spark.createDataFrame(pd.DataFrame([metric_row]))
    merge_delta(
        spark,
        updates,
        table=METRICS_TABLE,
        keys=("delivery_date",),
    )

    history = (
        spark.table(METRICS_TABLE)
        .where(F.col("delivery_date") <= F.lit(delivery_date))
        .select("delivery_date", "mae", "baseline_exaa_mae", "picp")
        .orderBy("delivery_date")
        .toPandas()
    )
    run = (
        spark.table(FORECAST_RUN_TABLE)
        .where(F.col("delivery_date") == F.lit(delivery_date))
        .select("data_outlier_rate", "prediction_mean_z")
        .first()
    )
    if run is None:
        raise ValueError(f"Forecast run metadata is missing for {delivery_date}")
    mlflow.set_registry_uri("databricks-uc")
    model_version = MlflowClient().get_model_version(MODEL_NAME, version)
    reference = model_version.tags.get("test_mae")
    reference_mae = None if reference is None else float(reference)
    monitoring = assess_drift(
        history,
        data_outlier_rate=float(run.data_outlier_rate),
        prediction_mean_z=float(run.prediction_mean_z),
        reference_mae=reference_mae,
        target_coverage=TARGET_COVERAGE,
    )
    metric_row.update(
        {
            "monitoring_status": monitoring.status,
            "monitoring_reasons": "; ".join(monitoring.reasons),
            "rolling_7d_mae": monitoring.rolling_7d_mae,
            "rolling_7d_baseline_exaa_mae": (monitoring.rolling_7d_baseline_exaa_mae),
            "rolling_28d_picp": monitoring.rolling_28d_picp,
        }
    )
    merge_delta(
        spark,
        spark.createDataFrame(pd.DataFrame([metric_row])),
        table=METRICS_TABLE,
        keys=("delivery_date",),
    )
    LOGGER.info("Evaluated %s: %s", delivery_date, metrics)
    if monitoring.reasons:
        message = "; ".join(monitoring.reasons)
        LOGGER.error("Forecast monitoring alert for %s: %s", delivery_date, message)
        if fail_on_drift:
            raise RuntimeError(message)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery-date", type=date.fromisoformat)
    parser.add_argument("--no-fail-on-drift", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    delivery_date = args.delivery_date or datetime.now(BERLIN).date() + timedelta(
        days=1
    )
    evaluate_day(delivery_date, fail_on_drift=not args.no_fail_on_drift)


if __name__ == "__main__":
    main()
