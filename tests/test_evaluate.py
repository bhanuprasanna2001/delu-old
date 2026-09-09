from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, create_autospec

import numpy as np
import pandas as pd
import pytest
from mlflow import MlflowClient
from pyspark.sql import SparkSession

from delu.ml.evaluate import METRICS_TABLE, evaluate_day
from delu.ml.predict import FORECAST_RUN_TABLE, FORECAST_TABLE
from delu.pipeline.gold import TABLE as GOLD_TABLE


@pytest.fixture
def evaluation(monkeypatch):
    joined = pd.DataFrame(
        {
            "delivery_date": [date(2026, 9, 5)] * 96,
            "quarter_of_day": range(96),
            "model_version": "1",
            "predicted_at": datetime(2026, 9, 4, 13, 36, tzinfo=UTC),
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
        FORECAST_RUN_TABLE: run,
    }.__getitem__
    spark.createDataFrame.return_value.limit.return_value.count.return_value = 1
    client = create_autospec(MlflowClient, instance=True)
    client.get_model_version.return_value = SimpleNamespace(tags={"test_mae": "10.0"})
    monkeypatch.setattr("delu.ml.evaluate.MlflowClient", Mock(return_value=client))
    monkeypatch.setattr("delu.ml.evaluate.mlflow.set_registry_uri", Mock())
    return spark, joined, run


@pytest.mark.parametrize(
    "published_at",
    [
        datetime(2026, 9, 4, 13, 36, tzinfo=UTC),
        datetime(2026, 9, 10, 18, tzinfo=UTC),
    ],
    ids=["after-1500", "five-days-later"],
)
def test_delayed_forecast_is_evaluated_and_monitored(evaluation, published_at) -> None:
    spark, joined, _run = evaluation
    joined["predicted_at"] = published_at

    metrics = evaluate_day(date(2026, 9, 5), spark=spark)

    assert metrics is not None
    assert metrics["mae"] == pytest.approx(1.0)
    saved = spark.createDataFrame.call_args.args[0]
    assert saved.loc[0, "monitoring_status"] == "ok"
    assert saved.loc[0, "monitoring_reasons"] == ""
    spark.createDataFrame.return_value.write.format.return_value.saveAsTable.assert_called_once_with(
        METRICS_TABLE
    )


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
