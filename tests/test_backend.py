from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, Mock

from databricks.sql.exc import RequestError

from app.delu_app.backend import Settings, SqlForecastStore


def _store(monkeypatch, now: list[float]) -> tuple[SqlForecastStore, Mock]:
    cursor = MagicMock()
    cursor.description = [("delivery_date",)]
    cursor.fetchall.return_value = [(date(2026, 9, 5),)]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connect = Mock(return_value=connection)
    monkeypatch.setattr("app.delu_app.backend.sql.connect", connect)
    config = MagicMock()
    config.host = "https://workspace.example"
    settings = Settings(
        warehouse_id="warehouse",
        gold_table="delu.gold.model_input",
        forecast_table="delu.gold.forecasts",
        forecast_run_table="delu.gold.forecast_runs",
        metrics_table="delu.gold.forecast_metrics",
        model_volume="/Volumes/delu/gold/model_exports",
    )
    return SqlForecastStore(settings, config=config, clock=lambda: now[0]), connect


def test_successful_query_is_reused_without_sharing_mutable_rows(monkeypatch) -> None:
    now = [0.0]
    store, connect = _store(monkeypatch, now)

    first = store.dates(10)
    first[0]["delivery_date"] = date(2000, 1, 1)
    now[0] = 60
    second = store.dates(10)

    assert second == [{"delivery_date": date(2026, 9, 5)}]
    connect.assert_called_once()


def test_quota_error_serves_stale_data_and_delays_another_refresh(
    monkeypatch,
) -> None:
    now = [0.0]
    store, connect = _store(monkeypatch, now)
    expected = store.dates(10)
    now[0] = 16 * 60
    connect.side_effect = RequestError(
        "BAD_REQUEST: Sorry, cannot run the resource because you have hit your "
        "free daily limit."
    )

    first_stale = store.dates(10)
    now[0] = 17 * 60
    second_stale = store.dates(10)

    assert first_stale == second_stale == expected
    assert connect.call_count == 2
