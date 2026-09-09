"""Evaluate the released SDAC result and run daily drift checks."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, date, datetime

import mlflow
import pandas as pd
from databricks.connect import DatabricksSession
from mlflow import MlflowClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from delu.ml.metrics import forecast_metrics
from delu.ml.monitoring import assess_drift
from delu.ml.predict import FORECAST_RUN_TABLE, FORECAST_TABLE
from delu.ml.tables import merge_delta
from delu.ml.train import MODEL_NAME, TARGET_COVERAGE
from delu.pipeline.bronze import SETTLEMENT_CUTOFF, latest_settlement_date
from delu.pipeline.gold import TABLE as GOLD_TABLE

METRICS_TABLE = "delu.gold.forecast_metrics"
LOGGER = logging.getLogger(__name__)


def _production_forecasts(frame: DataFrame) -> DataFrame:
    predicted_local = F.from_utc_timestamp("predicted_at", "Europe/Berlin")
    return frame.where(
        (F.to_date(predicted_local) == F.date_sub("delivery_date", 1))
        & (F.hour(predicted_local) < SETTLEMENT_CUTOFF.hour)
    )


def _evaluation_dates(
    spark: SparkSession,
    *,
    start: date,
    end: date,
) -> tuple[date, ...]:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    forecast = (
        _production_forecasts(spark.table(FORECAST_TABLE))
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
    if spark.catalog.tableExists(METRICS_TABLE):
        eligible = eligible.join(
            spark.table(METRICS_TABLE)
            .where(F.col("monitoring_status") != "pending")
            .select("delivery_date"),
            "delivery_date",
            "left_anti",
        )
    return tuple(
        row.delivery_date for row in eligible.orderBy("delivery_date").collect()
    )


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
    forecast = _production_forecasts(spark.table(FORECAST_TABLE)).where(
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
        prior_history = (
            spark.table(METRICS_TABLE)
            .where(F.col("delivery_date") < F.lit(delivery_date))
            .where(F.col("monitoring_status") != "pending")
            .select(*history_columns)
            .orderBy("delivery_date")
            .toPandas()
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
        LOGGER.error("Forecast monitoring alert for %s: %s", delivery_date, message)
        if fail_on_drift:
            raise RuntimeError(message)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery-date", type=date.fromisoformat)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--no-fail-on-drift", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    if args.delivery_date is not None:
        evaluate_day(args.delivery_date, fail_on_drift=not args.no_fail_on_drift)
        return

    now = datetime.now(UTC)
    end = args.end or latest_settlement_date(now)
    start = args.start or (end.replace(day=1) - date.resolution).replace(day=1)
    if start > end:
        raise ValueError("evaluation start cannot be after end")
    spark = DatabricksSession.builder.serverless().getOrCreate()
    failures: list[str] = []
    for delivery_date in _evaluation_dates(spark, start=start, end=end):
        try:
            evaluate_day(
                delivery_date,
                spark=spark,
                fail_on_drift=not args.no_fail_on_drift,
            )
        except RuntimeError as exc:
            failures.append(f"{delivery_date}: {exc}")
    if failures:
        raise RuntimeError("; ".join(failures))


if __name__ == "__main__":
    main()
