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
from delu.pipeline.bronze import BERLIN, DEFAULT_START
from delu.pipeline.gold import TABLE as GOLD_TABLE

METRICS_TABLE = "delu.gold.forecast_metrics"
LOGGER = logging.getLogger(__name__)


def _evaluation_dates(
    spark: SparkSession,
    *,
    start: date,
    end: date,
) -> tuple[date, ...]:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if not all(
        spark.catalog.tableExists(table) for table in (FORECAST_TABLE, GOLD_TABLE)
    ):
        LOGGER.info("Waiting for forecasts and actual prices.")
        return ()
    forecast = (
        spark.table(FORECAST_TABLE)
        .where(F.col("delivery_date").between(F.lit(start), F.lit(end)))
        .groupBy("delivery_date")
        .agg(F.countDistinct("quarter_of_day").alias("quarters"))
        .where(F.col("quarters") == 96)
        .select("delivery_date")
    )
    actual = (
        spark.table(GOLD_TABLE)
        .where(F.col("delivery_date").between(F.lit(start), F.lit(end)))
        .where(F.col("price_de_lu_sdac_eur_per_mwh").isNotNull())
        .groupBy("delivery_date")
        .agg(F.countDistinct("quarter_of_day").alias("quarters"))
        .where(F.col("quarters") == 96)
        .select("delivery_date")
    )
    eligible = forecast.join(actual, "delivery_date", "inner")
    pending = eligible
    if spark.catalog.tableExists(METRICS_TABLE):
        pending = eligible.join(
            spark.table(METRICS_TABLE)
            .where(~F.col("monitoring_status").isin("pending", "late"))
            .select("delivery_date"),
            "delivery_date",
            "left_anti",
        )
    first = pending.agg(F.min("delivery_date").alias("day")).first()
    if first is None or first.day is None:
        return ()
    # Recompute subsequent rolling metrics when an older gap has been filled.
    return tuple(
        row.delivery_date
        for row in eligible.where(F.col("delivery_date") >= F.lit(first.day))
        .orderBy("delivery_date")
        .collect()
    )


def evaluate_day(
    delivery_date: date,
    *,
    spark: SparkSession | None = None,
) -> dict[str, float] | None:
    """Evaluate available prices and record monitoring without blocking data work."""
    spark = (
        spark
        or SparkSession.getActiveSession()
        or DatabricksSession.builder.serverless().getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if not all(
        spark.catalog.tableExists(table) for table in (FORECAST_TABLE, GOLD_TABLE)
    ):
        LOGGER.info("Waiting for forecasts and actual prices for %s.", delivery_date)
        return None
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
            "forecast.predicted_at",
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
    if len(joined) < 96 or joined.isna().to_numpy().any():
        LOGGER.info(
            "Waiting for complete forecasts and actual prices for %s.", delivery_date
        )
        return None
    if len(joined) != 96 or joined["quarter_of_day"].tolist() != list(range(96)):
        raise ValueError(
            f"Invalid forecast and target quarter grid for {delivery_date}"
        )
    versions = joined["model_version"].astype("string").unique()
    if len(versions) != 1:
        raise ValueError(f"Forecast rows contain multiple model versions: {versions}")
    version = str(versions[0])
    predicted_at = joined["predicted_at"].unique()
    if len(predicted_at) != 1:
        raise ValueError("Forecast rows contain multiple prediction timestamps")
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
    }
    history_columns = ("delivery_date", "mae", "baseline_exaa_mae", "picp")
    current_history = pd.DataFrame(
        [
            {
                "delivery_date": delivery_date,
                "mae": metrics["mae"],
                "baseline_exaa_mae": metrics["baseline_exaa_mae"],
                "picp": metrics["picp"],
            }
        ]
    )
    if spark.catalog.tableExists(METRICS_TABLE):
        prior = (
            spark.table(METRICS_TABLE)
            .where(F.col("delivery_date") < F.lit(delivery_date))
            .where(F.col("monitoring_status") != "pending")
        )
        prior_history = (
            prior.select(*history_columns).orderBy("delivery_date").toPandas()
        )
        history = pd.concat([prior_history, current_history], ignore_index=True)
    else:
        history = current_history
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
        LOGGER.warning("Forecast monitoring alert for %s: %s", delivery_date, message)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery-date", type=date.fromisoformat)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    if args.delivery_date is not None:
        evaluate_day(args.delivery_date)
        return

    now = datetime.now(UTC)
    end = (
        date.fromisoformat(args.end)
        if args.end
        else now.astimezone(BERLIN).date() + timedelta(days=1)
    )
    start = date.fromisoformat(args.start) if args.start else DEFAULT_START
    if start > end:
        raise ValueError("evaluation start cannot be after end")
    spark = DatabricksSession.builder.serverless().getOrCreate()
    for delivery_date in _evaluation_dates(spark, start=start, end=end):
        evaluate_day(delivery_date, spark=spark)


if __name__ == "__main__":
    main()
