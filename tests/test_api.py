from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import httpx2
from fastapi import FastAPI

from app.delu_app.api import create_app
from app.delu_app.backend import FileDownload, SqlForecastStore


async def get(app: FastAPI, *paths: str) -> list[httpx2.Response]:
    """Exercise the ASGI app with its lifespan, without a network server."""
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            return [await client.get(path) for path in paths]


def forecast_rows() -> list[dict[str, object]]:
    common: dict[str, object] = {
        "delivery_date": date(2026, 9, 5),
        "model_version": "1",
        "predicted_at": datetime(2026, 9, 4, 9, 30, tzinfo=UTC),
        "nominal_coverage": 0.9,
        "mae": 10.0,
        "rmse": 12.0,
        "picp": 0.9,
        "mpiw": 40.0,
        "interval_score": 50.0,
        "bias": 1.0,
        "baseline_exaa_mae": 11.0,
        "baseline_7d_mae": 20.0,
        "evaluated_at": datetime(2026, 9, 5, 13, 0, tzinfo=UTC),
        "monitoring_status": "ok",
        "monitoring_reasons": "",
        "rolling_7d_mae": 9.5,
        "rolling_7d_baseline_exaa_mae": 10.5,
        "rolling_28d_picp": 0.89,
    }
    return [
        {
            **common,
            "quarter_of_day": quarter,
            "predicted_price_eur_per_mwh": 50.0 + quarter,
            "lower_price_eur_per_mwh": 40.0 + quarter,
            "upper_price_eur_per_mwh": 60.0 + quarter,
            "actual_price_eur_per_mwh": 51.0 + quarter,
        }
        for quarter in range(96)
    ]


def test_forecast_endpoint_returns_complete_settled_day() -> None:
    store = MagicMock(spec=SqlForecastStore)
    store.forecast.return_value = forecast_rows()

    [response] = asyncio.run(get(create_app(store), "/api/forecasts/2026-09-05"))

    assert response.status_code == 200
    payload = response.json()
    assert payload["settled"] is True
    assert payload["metrics"]["mae"] == 10.0
    assert len(payload["quarters"]) == 96
    assert payload["quarters"][0]["delivery_start_local"] == "2026-09-05T00:00:00"
    assert response.headers["cache-control"] == (
        "public, max-age=1800, s-maxage=3600, stale-if-error=604800"
    )


def test_forecast_endpoint_rejects_partial_grid() -> None:
    store = MagicMock(spec=SqlForecastStore)
    store.forecast.return_value = forecast_rows()[:-1]

    [response] = asyncio.run(get(create_app(store), "/api/forecasts/2026-09-05"))

    assert response.status_code == 503
    assert response.json() == {"detail": "Stored forecast is incomplete"}


def test_model_endpoint_and_download_use_same_published_version() -> None:
    store = MagicMock(spec=SqlForecastStore)
    store.model_info.return_value = {
        "model_name": "delu.ml.sdac_cqr",
        "model_family": "hist_gradient_boosting_cqr",
        "version": "1",
        "training_run_id": "run-1",
        "training_through": "2026-08-31",
        "published_at": "2026-09-03T06:00:00+00:00",
        "target_coverage": 0.9,
        "point_shrinkage": 0.61,
        "test_metrics": {"mae": 8.57, "picp": 0.91},
        "baseline_exaa_mae": 8.81,
        "archive": "sdac-cqr-v1.zip",
    }
    store.model_download.return_value = FileDownload(
        name="sdac-cqr-v1.zip",
        stream=BytesIO(b"model"),
        size=5,
    )

    metadata, archive = asyncio.run(
        get(create_app(store), "/api/model", "/api/downloads/model.zip")
    )

    assert metadata.status_code == 200
    assert metadata.json()["version"] == "1"
    assert metadata.json()["download_url"] == "/api/downloads/model.zip"
    assert archive.status_code == 200
    assert archive.content == b"model"
    assert archive.headers["content-disposition"].endswith('"sdac-cqr-v1.zip"')


def test_dates_include_observations_without_forecast_metadata() -> None:
    store = MagicMock(spec=SqlForecastStore)
    store.dates.return_value = [
        {
            "delivery_date": date(2025, 10, 8),
            "model_version": None,
            "predicted_at": None,
            "has_forecast": False,
            "has_actual": True,
            "has_metrics": False,
            "settled": True,
            "mae": None,
            "picp": None,
            "monitoring_status": None,
        }
    ]

    [response] = asyncio.run(get(create_app(store), "/api/dates"))

    assert response.status_code == 200
    assert response.json()[0]["has_forecast"] is False
    assert response.json()[0]["predicted_at"] is None
    assert response.json()[0]["has_actual"] is True
    assert response.headers["cache-control"] == (
        "public, max-age=60, s-maxage=900, stale-if-error=604800"
    )


def test_observations_never_fabricate_forecasts() -> None:
    store = MagicMock(spec=SqlForecastStore)
    store.observations.return_value = [
        {
            "quarter_of_day": quarter,
            "actual_price_eur_per_mwh": 51.0,
            "price_de_lu_exaa_eur_per_mwh": 50.0,
            "price_at_exaa_eur_per_mwh": 49.0,
        }
        for quarter in range(96)
    ]

    [response] = asyncio.run(get(create_app(store), "/api/observations/2025-10-08"))

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["quarters"]) == 96
    assert payload["quarters"][-1]["delivery_start_local"] == "2025-10-08T23:45:00"
    assert "predicted_price_eur_per_mwh" not in payload["quarters"][0]
    assert "metrics" not in payload


def test_observations_reject_duplicate_quarters() -> None:
    store = MagicMock(spec=SqlForecastStore)
    store.observations.return_value = [{"quarter_of_day": 0}] * 96

    [response] = asyncio.run(get(create_app(store), "/api/observations/2025-10-08"))

    assert response.status_code == 503


def test_features_reject_duplicate_quarters() -> None:
    store = MagicMock(spec=SqlForecastStore)
    store.features.return_value = [{"quarter_of_day": 0}] * 96

    [response] = asyncio.run(
        get(create_app(store), "/api/forecasts/2025-10-08/features")
    )

    assert response.status_code == 503


def test_weather_endpoint_returns_complete_grid_mean() -> None:
    store = MagicMock(spec=SqlForecastStore)
    store.weather.return_value = [
        {
            "quarter_of_day": quarter,
            "temperature_2m_c": 17.5,
            "wind_speed_100m_m_s": 6.25,
            "shortwave_radiation_w_m2": 350.0,
            "cloud_cover_pct": 42.0,
        }
        for quarter in range(96)
    ]

    [response] = asyncio.run(get(create_app(store), "/api/weather/2026-09-08"))

    assert response.status_code == 200
    payload = response.json()
    assert payload["model_run_date"] == "2026-09-07"
    assert payload["model"] == "ecmwf_ifs"
    assert payload["location_count"] == 25
    assert payload["quarters"][0] == {
        "quarter_of_day": 0,
        "delivery_start_local": "2026-09-08T00:00:00",
        "temperature_2m_c": 17.5,
        "wind_speed_100m_m_s": 6.25,
        "shortwave_radiation_w_m2": 350.0,
        "cloud_cover_pct": 42.0,
    }


def test_unsettled_forecast_does_not_show_metrics_or_truth() -> None:
    store = MagicMock(spec=SqlForecastStore)
    rows = forecast_rows()
    for row in rows:
        row["actual_price_eur_per_mwh"] = None
        row["evaluated_at"] = None
    store.forecast.return_value = rows

    [response] = asyncio.run(get(create_app(store), "/api/forecasts/2026-09-05"))

    assert response.status_code == 200
    payload = response.json()
    assert payload["settled"] is False
    assert payload["metrics"] is None
    assert payload["quarters"][0]["actual_price_eur_per_mwh"] is None
    assert response.headers["cache-control"] == (
        "public, max-age=300, s-maxage=300, stale-if-error=604800"
    )


def test_website_mount_preserves_api_and_missing_asset_responses(
    tmp_path: Path,
) -> None:
    store = MagicMock(spec=SqlForecastStore)
    store.dates.return_value = []
    (tmp_path / "index.html").write_text("<html><body>DELU</body></html>")

    homepage, sources, dates, missing_api, missing_asset = asyncio.run(
        get(
            create_app(store, static_dir=tmp_path),
            "/",
            "/sources",
            "/api/dates",
            "/api/missing",
            "/assets/missing.js",
        )
    )

    assert homepage.status_code == 200
    assert "DELU" in homepage.text
    assert sources.status_code == 200
    assert sources.text == homepage.text
    assert dates.json() == []
    assert missing_api.status_code == 404
    assert missing_api.json() == {"detail": "API endpoint not found"}
    assert missing_asset.status_code == 404


def test_liveness_does_not_query_databricks() -> None:
    store = MagicMock(spec=SqlForecastStore)
    store.health.side_effect = RuntimeError("The SQL warehouse is asleep")

    [response] = asyncio.run(get(create_app(store), "/healthz"))

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    store.health.assert_not_called()
    store._query.assert_not_called()
