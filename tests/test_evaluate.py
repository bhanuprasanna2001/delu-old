from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, create_autospec

import numpy as np
import pandas as pd
import pytest
from mlflow import MlflowClient
from pyspark.sql import SparkSession

from delu.ml import evaluate
from delu.ml.evaluate import (
    METRICS_TABLE,
    _evaluation_dates,
    _physical_quarter_count,
    evaluate_day,
)
from delu.ml.predict import FORECAST_RUN_TABLE, FORECAST_TABLE
from delu.pipeline.gold import TABLE as GOLD_TABLE
from delu.pipeline.silver import TABLE as SILVER_TABLE


@pytest.fixture
def evaluation(monkeypatch):
    joined = pd.DataFrame(
        {
            "delivery_date": [date(2026, 9, 5)] * 96,
            "quarter_of_day": range(96),
            "model_version": "1",
            "predicted_at": datetime(2026, 9, 4, 9, 30, tzinfo=UTC),
            "forecast_kind": "operational",
            "predicted_price_eur_per_mwh": 50.0,
            "lower_price_eur_per_mwh": 40.0,
            "upper_price_eur_per_mwh": 60.0,
            "price_de_lu_sdac_eur_per_mwh": 51.0,
            "price_de_lu_exaa_eur_per_mwh": 49.0,
            "price_de_lu_sdac_lag_7d_eur_per_mwh": 48.0,
        }
    )
    query = MagicMock()
    query.where.return_value = query
    query.alias.return_value = query
    query.join.return_value = query
    query.select.return_value = query
    query.withColumn.return_value = query
    query.orderBy.return_value = query
    query.toPandas.return_value = joined
    run = MagicMock()
    run.where.return_value = run
    run.select.return_value = run
    run.first.return_value = SimpleNamespace(
        data_outlier_rate=0.0, prediction_mean_z=0.0
    )
    spark = MagicMock(spec=SparkSession)
    spark.catalog.tableExists.side_effect = lambda table: table != METRICS_TABLE
    spark.table.side_effect = {
        FORECAST_TABLE: query,
        GOLD_TABLE: query,
        SILVER_TABLE: query,
        FORECAST_RUN_TABLE: run,
    }.__getitem__
    update = spark.createDataFrame.return_value
    update.limit.return_value.count.return_value = 1
    update.replace.return_value = update
    client = create_autospec(MlflowClient, instance=True)
    client.get_model_version.return_value = SimpleNamespace(tags={"test_mae": "10.0"})
    monkeypatch.setattr("delu.ml.evaluate.MlflowClient", Mock(return_value=client))
    monkeypatch.setattr("delu.ml.evaluate.mlflow.set_registry_uri", Mock())
    return spark, joined, run


def test_operational_forecast_is_evaluated_and_monitored(evaluation) -> None:
    spark, _joined, _run = evaluation
    metrics = evaluate_day(date(2026, 9, 5), spark=spark)

    assert metrics is not None
    assert metrics["mae"] == pytest.approx(1.0)
    saved = spark.createDataFrame.call_args.args[0]
    assert saved.loc[0, "monitoring_status"] == "ok"
    assert saved.loc[0, "monitoring_reasons"] == ""
    assert saved.loc[0, "normalized_96_mae"] == pytest.approx(1.0)
    spark.createDataFrame.return_value.write.format.return_value.saveAsTable.assert_called_once_with(
        METRICS_TABLE
    )


def test_retrospective_forecast_is_excluded_from_monitoring(evaluation) -> None:
    spark, joined, _run = evaluation
    joined["forecast_kind"] = "retrospective"
    joined["predicted_at"] = datetime(2026, 9, 10, 18, tzinfo=UTC)

    metrics = evaluate_day(date(2026, 9, 5), spark=spark)

    assert metrics is not None
    saved = spark.createDataFrame.call_args.args[0]
    assert saved.loc[0, "monitoring_status"] == "not_applicable"
    assert saved.loc[0, "forecast_kind"] == "retrospective"


def test_actual_prices_can_arrive_on_a_later_run(evaluation, caplog) -> None:
    spark, joined, _run = evaluation
    joined.loc[0, "price_de_lu_sdac_eur_per_mwh"] = np.nan

    assert evaluate_day(date(2026, 9, 5), spark=spark) is None
    spark.createDataFrame.assert_not_called()

    joined.loc[0, "price_de_lu_sdac_eur_per_mwh"] = 51.0
    assert evaluate_day(date(2026, 9, 5), spark=spark) is not None
    spark.createDataFrame.assert_called_once()


def test_missing_run_metadata_is_reported_without_persisting_metrics(
    evaluation,
) -> None:
    spark, _joined, run = evaluation
    run.first.return_value = None

    with pytest.raises(ValueError, match="run metadata is missing"):
        evaluate_day(date(2026, 9, 5), spark=spark)

    spark.createDataFrame.assert_not_called()


def test_model_drift_is_recorded_without_failing_data_processing(evaluation) -> None:
    spark, _joined, run = evaluation
    run.first.return_value = SimpleNamespace(
        data_outlier_rate=0.3, prediction_mean_z=0.0
    )

    assert evaluate_day(date(2026, 9, 5), spark=spark) is not None

    saved = spark.createDataFrame.call_args.args[0]
    assert saved.loc[0, "monitoring_status"] == "alert"
    assert "20%" in saved.loc[0, "monitoring_reasons"]


@pytest.mark.parametrize(
    ("delivery_date", "quarters"),
    [
        (date(2026, 1, 15), 96),
        (date(2026, 3, 29), 92),
        (date(2026, 10, 25), 100),
    ],
)
def test_physical_evaluation_grid_follows_the_berlin_day(
    delivery_date, quarters
) -> None:
    assert _physical_quarter_count(delivery_date) == quarters


def test_autumn_physical_metrics_remain_distinct_from_normalized_metrics(
    evaluation,
) -> None:
    spark, joined, _run = evaluation
    day = date(2025, 10, 26)
    joined["delivery_date"] = day
    joined["predicted_at"] = datetime(2025, 10, 25, 9, 30, tzinfo=UTC)
    physical = pd.concat(
        [
            joined,
            joined.iloc[8:12].assign(price_de_lu_sdac_eur_per_mwh=100.0),
        ],
        ignore_index=True,
    )
    spark.table(FORECAST_TABLE).toPandas.side_effect = [joined, physical]

    metrics = evaluate_day(day, spark=spark)

    assert metrics is not None
    assert metrics["mae"] == pytest.approx(2.96)
    saved = spark.createDataFrame.call_args.args[0]
    assert saved.loc[0, "normalized_96_mae"] == pytest.approx(1.0)


def test_refresh_recomputes_an_existing_evaluation() -> None:
    day = date(2026, 9, 5)
    query = MagicMock()
    for method in ("where", "groupBy", "agg", "select", "join", "orderBy"):
        getattr(query, method).return_value = query
    query.first.return_value = SimpleNamespace(day=day)
    query.collect.return_value = [SimpleNamespace(delivery_date=day)]
    spark = MagicMock(spec=SparkSession)
    spark.catalog.tableExists.return_value = True
    spark.table.return_value = query

    assert _evaluation_dates(spark, start=day, end=day, refresh=True) == (day,)

    assert all(call.args[0] != METRICS_TABLE for call in spark.table.call_args_list)


def test_refresh_cli_requires_a_bounded_range(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["delu-evaluate", "--refresh", "true"])

    with pytest.raises(SystemExit) as error:
        evaluate.main()

    assert error.value.code == 2
