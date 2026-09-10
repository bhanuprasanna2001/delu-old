from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock, Mock, create_autospec

import numpy as np
import pandas as pd
import pytest
from mlflow import MlflowClient
from pyspark.sql import SparkSession

from delu.contracts import FEATURE_DATA_VERSION
from delu.ml import predict as predict_module
from delu.ml.data import BOOLEAN_FEATURES, FEATURE_NAMES, NUMERIC_FEATURES
from delu.ml.model import BoostingConfig, ConformalPriceForecaster
from delu.ml.predict import FORECAST_RUN_TABLE, forecast_day, publication_status
from delu.pipeline.gold import TABLE as GOLD_TABLE


@pytest.fixture
def prediction(monkeypatch):
    day = date(2026, 9, 5)
    gold = pd.DataFrame({column: np.full(96, 50.0) for column in NUMERIC_FEATURES})
    gold = gold.assign(
        **dict.fromkeys(BOOLEAN_FEATURES, False),
        delivery_date=day,
        delivery_start_local=pd.date_range("2026-09-05", periods=96, freq="15min"),
        feature_data_version=FEATURE_DATA_VERSION,
        quarter_of_day=np.arange(96),
        day_of_week=5,
        month=9,
        price_de_lu_sdac_eur_per_mwh=np.nan,
    )
    config = BoostingConfig(max_iter=2, max_leaf_nodes=3, min_samples_leaf=5)
    features = (
        np.random.default_rng(7)
        .normal(size=(8, 96, len(FEATURE_NAMES)))
        .astype(np.float32)
    )
    model = ConformalPriceForecaster(
        feature_count=len(FEATURE_NAMES),
        reference_feature_index=0,
        calibration_days=2,
        point_config=config,
        interval_config=config,
    ).fit(features, features[..., 0])
    client = create_autospec(MlflowClient, instance=True)
    client.get_model_version_by_alias.return_value = SimpleNamespace(version="1")
    monkeypatch.setattr("delu.ml.predict.MlflowClient", Mock(return_value=client))
    monkeypatch.setattr("delu.ml.predict.mlflow.set_registry_uri", Mock())
    monkeypatch.setattr(
        "delu.ml.predict.mlflow.sklearn.load_model", Mock(return_value=model)
    )
    query = MagicMock()
    query.where.return_value = query
    query.orderBy.return_value = query
    query.toPandas.return_value = gold
    spark = MagicMock(spec=SparkSession)
    spark.catalog.tableExists.side_effect = lambda table: table == GOLD_TABLE
    spark.table.return_value = query
    spark.createDataFrame.return_value.limit.return_value.count.return_value = 1
    return spark, query


@pytest.mark.parametrize(
    ("now", "expected_status"),
    [
        (datetime(2026, 9, 4, 9, 30, tzinfo=UTC), "on_time"),
        (datetime(2026, 9, 4, 13, 36, tzinfo=UTC), "late"),
        (datetime(2026, 9, 10, 18, tzinfo=UTC), "backfill"),
    ],
    ids=["auction-window", "after-cutoff", "five-days-later"],
)
def test_delayed_forecast_publishes_with_its_actual_creation_time(
    prediction, now, expected_status
) -> None:
    spark, _query = prediction

    assert forecast_day(date(2026, 9, 5), spark=spark, now=now) == "1"

    saved = spark.createDataFrame.call_args_list[0].args[0]
    assert len(saved) == 96
    assert saved["predicted_at"].eq(now.replace(tzinfo=None)).all()
    assert np.isfinite(saved["predicted_price_eur_per_mwh"]).all()
    run = spark.createDataFrame.call_args_list[1].args[0]
    assert run.loc[0, "publication_status"] == expected_status


@pytest.mark.parametrize(
    ("predicted_at", "expected"),
    [
        (datetime(2026, 9, 4, 8, 14, 59, tzinfo=UTC), "backfill"),
        (datetime(2026, 9, 4, 8, 15, tzinfo=UTC), "on_time"),
        (datetime(2026, 9, 4, 9, 59, 59, tzinfo=UTC), "on_time"),
        (datetime(2026, 9, 4, 10, 0, tzinfo=UTC), "late"),
        (datetime(2026, 9, 5, 9, 30, tzinfo=UTC), "backfill"),
    ],
)
def test_publication_status_uses_the_exaa_to_sdac_window(
    predicted_at: datetime,
    expected: str,
) -> None:
    assert publication_status(date(2026, 9, 5), predicted_at) == expected


def test_retry_preserves_an_existing_forecast(prediction) -> None:
    spark, query = prediction
    spark.catalog.tableExists.side_effect = lambda table: table == FORECAST_RUN_TABLE
    query.first.return_value = SimpleNamespace(model_version="1")

    assert forecast_day(date(2026, 9, 5), spark=spark) == "1"

    spark.createDataFrame.assert_not_called()
    query.toPandas.assert_not_called()


def test_missing_model_inputs_remain_pending(prediction) -> None:
    spark, query = prediction
    query.toPandas.return_value = query.toPandas.return_value.iloc[:80]

    assert forecast_day(date(2026, 9, 5), spark=spark) is None

    spark.createDataFrame.assert_not_called()


def test_old_feature_model_leaves_the_forecast_pending(prediction) -> None:
    spark, _query = prediction
    loader = cast(Mock, predict_module.mlflow.sklearn.load_model)
    loader.return_value.feature_data_version = FEATURE_DATA_VERSION - 1

    assert forecast_day(date(2026, 9, 5), spark=spark) is None

    spark.createDataFrame.assert_not_called()
