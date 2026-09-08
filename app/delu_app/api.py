"""DELU website and read-only market data API."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, cast

from databricks import sql
from databricks.sdk.errors import DatabricksError
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, FiniteFloat

from .backend import (
    WEATHER_LOCATION_COUNT,
    ArtifactNotFoundError,
    FileDownload,
    SqlForecastStore,
)

LOGGER = logging.getLogger(__name__)


class Health(BaseModel):
    status: str
    latest_forecast_date: date | None
    latest_settlement_date: date | None
    model_version: str | None
    monitoring_status: str | None


class DateSummary(BaseModel):
    delivery_date: date
    model_version: str | None
    predicted_at: datetime | None
    has_forecast: bool
    has_actual: bool
    has_metrics: bool
    settled: bool
    mae: float | None
    picp: float | None
    monitoring_status: str | None


class ForecastQuarter(BaseModel):
    quarter_of_day: int
    delivery_start_local: str
    predicted_price_eur_per_mwh: float
    lower_price_eur_per_mwh: float
    upper_price_eur_per_mwh: float
    actual_price_eur_per_mwh: float | None


class ForecastMetrics(BaseModel):
    mae: float
    rmse: float
    bias: float
    picp: float
    mpiw: float
    interval_score: float
    baseline_exaa_mae: float
    baseline_7d_mae: float
    rolling_7d_mae: float
    rolling_7d_baseline_exaa_mae: float
    rolling_28d_picp: float
    monitoring_status: str
    monitoring_reasons: list[str]
    evaluated_at: datetime


class ForecastDay(BaseModel):
    delivery_date: date
    model_version: str
    predicted_at: datetime
    nominal_coverage: float
    settled: bool
    metrics: ForecastMetrics | None
    quarters: list[ForecastQuarter]


class FeatureTable(BaseModel):
    delivery_date: date
    rows: list[dict[str, Any]]


class WeatherQuarter(BaseModel):
    quarter_of_day: int
    delivery_start_local: str
    temperature_2m_c: FiniteFloat
    wind_speed_100m_m_s: FiniteFloat
    shortwave_radiation_w_m2: FiniteFloat
    cloud_cover_pct: FiniteFloat


class WeatherDay(BaseModel):
    delivery_date: date
    model_run_date: date
    model: str
    location_count: int
    quarters: list[WeatherQuarter]


class ObservationQuarter(BaseModel):
    quarter_of_day: int
    delivery_start_local: str
    actual_price_eur_per_mwh: float | None
    price_de_lu_exaa_eur_per_mwh: float
    price_at_exaa_eur_per_mwh: float


class ObservationDay(BaseModel):
    delivery_date: date
    quarters: list[ObservationQuarter]


class ModelSummary(BaseModel):
    model_name: str
    model_family: str
    version: str
    training_run_id: str
    training_through: date
    published_at: datetime
    target_coverage: float
    point_shrinkage: float
    test_metrics: dict[str, float]
    baseline_exaa_mae: float
    download_url: str


def _local_start(day: date, quarter_of_day: int) -> str:
    hour, quarter = divmod(quarter_of_day, 4)
    return f"{day.isoformat()}T{hour:02d}:{quarter * 15:02d}:00"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _store(request: Request) -> SqlForecastStore:
    return cast(SqlForecastStore, request.app.state.store)


StoreDependency = Annotated[SqlForecastStore, Depends(_store)]
LimitQuery = Annotated[int, Query(ge=1, le=2_000)]


def _iter_download(download: FileDownload) -> Iterator[bytes]:
    try:
        while chunk := download.stream.read(1024 * 1024):
            yield chunk
    finally:
        download.stream.close()


def _forecast_response(day: date, rows: list[dict[str, Any]]) -> ForecastDay:
    if not rows:
        raise HTTPException(status_code=404, detail="Forecast day not found")
    if len(rows) != 96:
        raise HTTPException(status_code=503, detail="Stored forecast is incomplete")
    quarters = {int(row["quarter_of_day"]) for row in rows}
    if quarters != set(range(96)):
        raise HTTPException(status_code=503, detail="Stored forecast grid is invalid")

    versions = {str(row["model_version"]) for row in rows}
    predicted_at = {row["predicted_at"] for row in rows}
    coverage = {float(row["nominal_coverage"]) for row in rows}
    if len(versions) != 1 or len(predicted_at) != 1 or len(coverage) != 1:
        raise HTTPException(status_code=503, detail="Forecast metadata is inconsistent")

    actual_count = sum(row["actual_price_eur_per_mwh"] is not None for row in rows)
    if actual_count not in {0, 96}:
        raise HTTPException(status_code=503, detail="Settlement data is incomplete")
    first = rows[0]
    metrics = None
    if first["evaluated_at"] is not None:
        reasons = str(first["monitoring_reasons"] or "")
        first = {**first, "evaluated_at": _as_utc(first["evaluated_at"])}
        metric_values = {
            key: first[key]
            for key in ForecastMetrics.model_fields
            if key != "monitoring_reasons"
        }
        metric_values["monitoring_reasons"] = [
            reason for reason in reasons.split("; ") if reason
        ]
        metrics = ForecastMetrics.model_validate(metric_values)
    return ForecastDay(
        delivery_date=day,
        model_version=versions.pop(),
        predicted_at=_as_utc(predicted_at.pop()),
        nominal_coverage=coverage.pop(),
        settled=actual_count == 96,
        metrics=metrics,
        quarters=[
            ForecastQuarter(
                quarter_of_day=int(row["quarter_of_day"]),
                delivery_start_local=_local_start(day, int(row["quarter_of_day"])),
                predicted_price_eur_per_mwh=row["predicted_price_eur_per_mwh"],
                lower_price_eur_per_mwh=row["lower_price_eur_per_mwh"],
                upper_price_eur_per_mwh=row["upper_price_eur_per_mwh"],
                actual_price_eur_per_mwh=row["actual_price_eur_per_mwh"],
            )
            for row in rows
        ],
    )


def create_app(
    store: SqlForecastStore | None = None,
    *,
    static_dir: Path | None = None,
) -> FastAPI:
    """Create the API, optionally injecting a store for tests."""

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        application.state.store = store or SqlForecastStore.from_env()
        yield

    application = FastAPI(
        title="DELU API",
        version="1.0.0",
        description="Read-only SDAC forecasts, inputs, metrics, and exports.",
        lifespan=lifespan,
    )

    @application.exception_handler(sql.Error)
    @application.exception_handler(DatabricksError)
    async def databricks_error(_request: Request, error: Exception) -> JSONResponse:
        LOGGER.error(
            "Databricks dependency failed",
            exc_info=(type(error), error, error.__traceback__),
        )
        return JSONResponse(
            status_code=503,
            content={"detail": "Databricks dependency unavailable"},
        )

    @application.get("/healthz", include_in_schema=False)
    def liveness() -> dict[str, str]:
        """Check the web process without waking the SQL warehouse."""
        return {"status": "ok"}

    @application.get("/api/health", response_model=Health)
    def health(backend: StoreDependency) -> Health:
        return Health(status="ok", **backend.health())

    @application.get("/api/dates", response_model=list[DateSummary])
    def dates(
        backend: StoreDependency,
        limit: LimitQuery = 366,
    ) -> list[DateSummary]:
        return [
            DateSummary.model_validate(
                {
                    **row,
                    "predicted_at": (
                        _as_utc(row["predicted_at"]) if row["predicted_at"] else None
                    ),
                }
            )
            for row in backend.dates(limit)
        ]

    @application.get("/api/forecasts/{delivery_date}", response_model=ForecastDay)
    def forecast(
        delivery_date: date,
        backend: StoreDependency,
    ) -> ForecastDay:
        return _forecast_response(delivery_date, backend.forecast(delivery_date))

    @application.get("/api/observations/{delivery_date}", response_model=ObservationDay)
    def observations(delivery_date: date, backend: StoreDependency) -> ObservationDay:
        rows = backend.observations(delivery_date)
        if not rows:
            raise HTTPException(status_code=404, detail="Market day not found")
        if len(rows) != 96 or {int(row["quarter_of_day"]) for row in rows} != set(
            range(96)
        ):
            raise HTTPException(status_code=503, detail="Market day is incomplete")
        return ObservationDay(
            delivery_date=delivery_date,
            quarters=[
                ObservationQuarter(
                    **row,
                    delivery_start_local=_local_start(
                        day=delivery_date, quarter_of_day=int(row["quarter_of_day"])
                    ),
                )
                for row in rows
            ],
        )

    @application.get(
        "/api/forecasts/{delivery_date}/features", response_model=FeatureTable
    )
    def features(
        delivery_date: date,
        backend: StoreDependency,
    ) -> FeatureTable:
        rows = backend.features(delivery_date)
        if not rows:
            raise HTTPException(status_code=404, detail="Gold features not found")
        if (
            len(rows) != 96
            or {int(row["quarter_of_day"]) for row in rows} != set(range(96))
            or any(value is None for row in rows for value in row.values())
        ):
            raise HTTPException(status_code=503, detail="Gold features are incomplete")
        for row in rows:
            row["delivery_start_local"] = _local_start(
                delivery_date, int(row["quarter_of_day"])
            )
        return FeatureTable(delivery_date=delivery_date, rows=rows)

    @application.get("/api/weather/{delivery_date}", response_model=WeatherDay)
    def weather(delivery_date: date, backend: StoreDependency) -> WeatherDay:
        rows = backend.weather(delivery_date)
        if not rows:
            raise HTTPException(status_code=404, detail="Weather inputs not found")
        if (
            len(rows) != 96
            or {int(row["quarter_of_day"]) for row in rows} != set(range(96))
            or any(value is None for row in rows for value in row.values())
        ):
            raise HTTPException(status_code=503, detail="Weather inputs are incomplete")
        return WeatherDay(
            delivery_date=delivery_date,
            model_run_date=delivery_date - timedelta(days=1),
            model="ecmwf_ifs",
            location_count=WEATHER_LOCATION_COUNT,
            quarters=[
                WeatherQuarter(
                    **row,
                    delivery_start_local=_local_start(
                        delivery_date, int(row["quarter_of_day"])
                    ),
                )
                for row in rows
            ],
        )

    @application.get("/api/model", response_model=ModelSummary)
    def model(backend: StoreDependency) -> ModelSummary:
        try:
            info = backend.model_info()
        except ArtifactNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        info["download_url"] = "/api/downloads/model.zip"
        return ModelSummary.model_validate(info)

    @application.get("/api/downloads/gold.csv")
    def download_gold(
        backend: StoreDependency,
        through: date | None = None,
    ) -> StreamingResponse:
        suffix = "" if through is None else f"-through-{through.isoformat()}"
        return StreamingResponse(
            backend.gold_csv(through),
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="delu-gold{suffix}.csv"'
            },
        )

    @application.get("/api/downloads/model.zip")
    def download_model(
        backend: StoreDependency,
    ) -> StreamingResponse:
        try:
            download = backend.model_download()
        except ArtifactNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        headers = {
            "Content-Disposition": f'attachment; filename="{download.name}"',
        }
        if download.size is not None:
            headers["Content-Length"] = str(download.size)
        return StreamingResponse(
            _iter_download(download),
            media_type="application/zip",
            headers=headers,
        )

    @application.get("/api/{path:path}", include_in_schema=False)
    def missing_api(path: str) -> None:
        raise HTTPException(status_code=404, detail="API endpoint not found")

    assets = (
        static_dir if static_dir is not None else Path(__file__).parents[1] / "static"
    )
    if (assets / "index.html").is_file():
        application.mount("/", StaticFiles(directory=assets, html=True), name="website")

    return application


app = create_app()
