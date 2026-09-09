from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from delu.ml.evaluate import evaluate_day
from delu.ml.predict import FORECAST_RUN_TABLE, FORECAST_TABLE
from delu.pipeline.gold import TABLE as GOLD_TABLE


def test_failed_evaluation_does_not_persist_a_pending_metric(monkeypatch) -> None:
    delivery_date = date(2026, 9, 5)
    joined = pd.DataFrame(
        {
            "delivery_date": [delivery_date] * 96,
            "quarter_of_day": range(96),
            "model_version": ["1"] * 96,
            "predicted_price_eur_per_mwh": [50.0] * 96,
            "lower_price_eur_per_mwh": [40.0] * 96,
            "upper_price_eur_per_mwh": [60.0] * 96,
            "price_de_lu_sdac_eur_per_mwh": [51.0] * 96,
            "price_de_lu_exaa_eur_per_mwh": [49.0] * 96,
            "price_de_lu_sdac_lag_7d_eur_per_mwh": [48.0] * 96,
        }
    )
    forecast = MagicMock()
    actual = MagicMock()
    joined_query = MagicMock()
    run = MagicMock()
    forecast.where.return_value = forecast
    forecast.alias.return_value = forecast
    actual.where.return_value = actual
    actual.alias.return_value = actual
    forecast.join.return_value = joined_query
    joined_query.select.return_value = joined_query
    joined_query.orderBy.return_value = joined_query
    joined_query.toPandas.return_value = joined
    run.where.return_value = run
    run.select.return_value = run
    run.first.return_value = None

    spark = MagicMock(spec=SparkSession)
    spark.catalog.tableExists.return_value = False
    spark.table.side_effect = lambda table: {
        FORECAST_TABLE: forecast,
        GOLD_TABLE: actual,
        FORECAST_RUN_TABLE: run,
    }[table]
    merge = Mock()
    monkeypatch.setattr("delu.ml.evaluate.merge_delta", merge)

    with pytest.raises(ValueError, match="run metadata is missing"):
        evaluate_day(delivery_date, spark=spark)

    merge.assert_not_called()
