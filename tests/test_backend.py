from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, Mock

from databricks import sql

from app.delu_app.backend import Settings, SqlForecastStore


def settings() -> Settings:
    return Settings(
        warehouse_id="warehouse",
        gold_table="delu.gold.model_input",
        forecast_table="delu.gold.forecasts",
        forecast_run_table="delu.gold.forecast_runs",
        metrics_table="delu.gold.forecast_metrics",
        model_volume="/Volumes/delu/gold/model_exports",
    )


def test_successful_query_is_reused_within_its_cache_window(monkeypatch) -> None:
    now = [0.0]
    cursor = MagicMock()
    cursor.description = [("delivery_date",)]
    cursor.fetchall.return_value = [(date(2026, 9, 5),)]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connect = Mock(return_value=connection)
    monkeypatch.setattr("app.delu_app.backend.sql.connect", connect)
    config = MagicMock()
    config.host = "https://workspace.example"
    store = SqlForecastStore(settings(), config=config, clock=lambda: now[0])

    first = store.dates(10)
    now[0] = 60
    second = store.dates(10)

    assert first == second == [{"delivery_date": date(2026, 9, 5)}]
    connect.assert_called_once()


def test_expired_query_uses_stale_success_during_warehouse_outage(
    monkeypatch,
) -> None:
    now = [0.0]
    cursor = MagicMock()
    cursor.description = [("delivery_date",)]
    cursor.fetchall.return_value = [(date(2026, 9, 5),)]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connect = Mock(return_value=connection)
    monkeypatch.setattr("app.delu_app.backend.sql.connect", connect)
    config = MagicMock()
    config.host = "https://workspace.example"
    store = SqlForecastStore(settings(), config=config, clock=lambda: now[0])
    store.dates(10)
    now[0] = 16 * 60
    connect.side_effect = sql.Error("daily quota exhausted")

    rows = store.dates(10)

    assert rows == [{"delivery_date": date(2026, 9, 5)}]
    assert connect.call_count == 2
