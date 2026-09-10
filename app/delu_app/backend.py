"""Databricks-backed reads for the DELU application."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import dataclass
from datetime import date
from threading import Lock
from time import monotonic
from typing import Any, BinaryIO

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import DatabricksError, NotFound

_TABLE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*\."
    r"[A-Za-z_][A-Za-z0-9_]*\."
    r"[A-Za-z_][A-Za-z0-9_]*$"
)
_VOLUME = re.compile(
    r"^/Volumes/[A-Za-z_][A-Za-z0-9_]*/"
    r"[A-Za-z_][A-Za-z0-9_]*/[A-Za-z_][A-Za-z0-9_]*$"
)
_ARCHIVE = re.compile(r"^sdac-cqr-v[0-9]+\.zip$")
_WEATHER_LOCATIONS = (
    "emden",
    "bremen",
    "hamburg",
    "kiel",
    "rostock",
    "hanover",
    "berlin",
    "muenster",
    "kassel",
    "leipzig",
    "dresden",
    "cologne",
    "frankfurt",
    "erfurt",
    "nuremberg",
    "luxembourg",
    "stuttgart",
    "freiburg",
    "munich",
    "passau",
    "north_sea_west",
    "north_sea_centre",
    "north_sea_east",
    "baltic_west",
    "baltic_east",
)
WEATHER_LOCATION_COUNT = len(_WEATHER_LOCATIONS)
_WEATHER_METRICS = (
    "temperature_2m_c",
    "wind_speed_100m_m_s",
    "shortwave_radiation_w_m2",
    "cloud_cover_pct",
)
_HEALTH_CACHE_SECONDS = 5 * 60
_DATES_CACHE_SECONDS = 15 * 60
_FORECAST_CACHE_SECONDS = 5 * 60
_SETTLED_DATA_CACHE_SECONDS = 12 * 60 * 60
_STALE_CACHE_SECONDS = 7 * 24 * 60 * 60
_MAX_CACHE_ENTRIES = 512
LOGGER = logging.getLogger(__name__)


def _weather_mean(metric: str) -> str:
    columns = " + ".join(
        f"weather_{location}_{metric}" for location in _WEATHER_LOCATIONS
    )
    return f"({columns}) / {WEATHER_LOCATION_COUNT} AS {metric}"


def _weather_select() -> str:
    return ",\n".join(_weather_mean(metric) for metric in _WEATHER_METRICS)


class ArtifactNotFoundError(FileNotFoundError):
    """Raised when no downloadable production model has been published."""


@dataclass(frozen=True)
class Settings:
    """Resource names injected by Databricks Apps."""

    warehouse_id: str
    gold_table: str
    forecast_table: str
    forecast_run_table: str
    metrics_table: str
    model_volume: str

    @classmethod
    def from_env(cls) -> Settings:
        """Load and validate the app resource environment."""
        values = {
            field: os.environ.get(env_name, "")
            for field, env_name in {
                "warehouse_id": "DATABRICKS_WAREHOUSE_ID",
                "gold_table": "DELU_GOLD_TABLE",
                "forecast_table": "DELU_FORECAST_TABLE",
                "forecast_run_table": "DELU_FORECAST_RUN_TABLE",
                "metrics_table": "DELU_METRICS_TABLE",
                "model_volume": "DELU_MODEL_VOLUME",
            }.items()
        }
        missing = sorted(name for name, value in values.items() if not value)
        if missing:
            raise RuntimeError(f"Missing Databricks app resources: {missing}")
        for name in (
            "gold_table",
            "forecast_table",
            "forecast_run_table",
            "metrics_table",
        ):
            if not _TABLE.fullmatch(values[name]):
                raise RuntimeError(f"Invalid table resource: {values[name]!r}")
        if not _VOLUME.fullmatch(values["model_volume"]):
            raise RuntimeError(f"Invalid volume resource: {values['model_volume']!r}")
        return cls(**values)


@dataclass
class FileDownload:
    """An open volume file ready for a streaming HTTP response."""

    name: str
    stream: BinaryIO
    size: int | None


@dataclass(frozen=True)
class _QueryCacheEntry:
    rows: tuple[dict[str, Any], ...]
    stored_at: float


class SqlForecastStore:
    """Read forecasts and features through a SQL warehouse."""

    def __init__(
        self,
        settings: Settings,
        *,
        config: Config | None = None,
        workspace: WorkspaceClient | None = None,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self.settings = settings
        self._config = config or Config()
        self._workspace = workspace or WorkspaceClient(config=self._config)
        self._clock = clock
        self._cache: dict[
            tuple[str, tuple[tuple[str, object], ...]], _QueryCacheEntry
        ] = {}
        self._cache_lock = Lock()

    @classmethod
    def from_env(cls) -> SqlForecastStore:
        """Build a store from Databricks-managed app resources."""
        return cls(Settings.from_env())

    def _connect(self) -> Any:
        host = self._config.host
        if not host:
            raise RuntimeError("Databricks authentication did not provide a host")
        return sql.connect(
            server_hostname=host.removeprefix("https://").rstrip("/"),
            http_path=f"/sql/1.0/warehouses/{self.settings.warehouse_id}",
            credentials_provider=lambda: self._config.authenticate,
        )

    def _query(
        self,
        statement: str,
        parameters: dict[str, object] | None = None,
        *,
        max_age_seconds: int,
    ) -> list[dict[str, Any]]:
        query_parameters = parameters or {}
        key = (statement, tuple(sorted(query_parameters.items())))
        now = self._clock()
        with self._cache_lock:
            cached = self._cache.get(key)
            if cached is not None and now - cached.stored_at <= max_age_seconds:
                return [row.copy() for row in cached.rows]

        try:
            with (
                closing(self._connect()) as connection,
                closing(connection.cursor()) as cursor,
            ):
                cursor.execute(statement, query_parameters)
                columns = [column[0] for column in cursor.description]
                rows = [
                    dict(zip(columns, row, strict=True)) for row in cursor.fetchall()
                ]
        except (sql.Error, DatabricksError):
            with self._cache_lock:
                stale = self._cache.get(key)
            if stale is None or now - stale.stored_at > _STALE_CACHE_SECONDS:
                raise
            LOGGER.warning(
                "Serving a cached Databricks response after a warehouse failure",
                exc_info=True,
            )
            return [row.copy() for row in stale.rows]

        entry = _QueryCacheEntry(tuple(row.copy() for row in rows), now)
        with self._cache_lock:
            if len(self._cache) >= _MAX_CACHE_ENTRIES and key not in self._cache:
                oldest = min(
                    self._cache,
                    key=lambda cached_key: self._cache[cached_key].stored_at,
                )
                del self._cache[oldest]
            self._cache[key] = entry
        return [row.copy() for row in rows]

    def health(self) -> dict[str, Any]:
        """Return the latest persisted forecast and settlement state."""
        rows = self._query(
            f"""
            SELECT
              (SELECT MAX(delivery_date) FROM {self.settings.forecast_table})
                AS latest_forecast_date,
              (SELECT MAX(delivery_date) FROM {self.settings.metrics_table})
                AS latest_settlement_date,
              (SELECT model_version FROM {self.settings.forecast_run_table}
                 ORDER BY delivery_date DESC LIMIT 1) AS model_version,
              (SELECT monitoring_status FROM {self.settings.metrics_table}
                 ORDER BY delivery_date DESC LIMIT 1) AS monitoring_status
            """,
            max_age_seconds=_HEALTH_CACHE_SECONDS,
        )
        if len(rows) != 1:
            raise RuntimeError("Health query returned an unexpected result")
        return rows[0]

    def dates(self, limit: int) -> list[dict[str, Any]]:
        """List complete market and forecast days without inventing forecasts."""
        return self._query(
            f"""
            WITH forecast_days AS (
              SELECT delivery_date, MAX(model_version) AS model_version,
                     MAX(predicted_at) AS predicted_at, COUNT(*) AS quarters
              FROM {self.settings.forecast_table}
              GROUP BY delivery_date
            ), actual_days AS (
              SELECT delivery_date,
                     COUNT(price_de_lu_sdac_eur_per_mwh) AS actual_quarters,
                     COUNT(*) AS quarters
              FROM {self.settings.gold_table}
              GROUP BY delivery_date
            )
            SELECT COALESCE(f.delivery_date, a.delivery_date) AS delivery_date,
                   f.model_version, f.predicted_at,
                   COALESCE(f.quarters, 0) = 96 AS has_forecast,
                   COALESCE(a.actual_quarters, 0) = 96 AS has_actual,
                   m.evaluated_at IS NOT NULL AS has_metrics,
                   COALESCE(a.actual_quarters, 0) = 96 AS settled,
                   m.mae, m.picp, m.monitoring_status
            FROM forecast_days f
            FULL OUTER JOIN actual_days a USING (delivery_date)
            LEFT JOIN {self.settings.metrics_table} m
              ON m.delivery_date = COALESCE(f.delivery_date, a.delivery_date)
            WHERE f.quarters = 96 OR a.quarters = 96
            ORDER BY delivery_date DESC
            LIMIT :limit
            """,
            {"limit": limit},
            max_age_seconds=_DATES_CACHE_SECONDS,
        )

    def forecast(self, delivery_date: date) -> list[dict[str, Any]]:
        """Return one forecast day, optional truth, and daily metrics."""
        return self._query(
            f"""
            SELECT f.delivery_date, f.quarter_of_day, f.model_version,
                   f.predicted_at, f.nominal_coverage,
                   f.predicted_price_eur_per_mwh,
                   f.lower_price_eur_per_mwh,
                   f.upper_price_eur_per_mwh,
                   g.price_de_lu_sdac_eur_per_mwh AS actual_price_eur_per_mwh,
                   m.mae, m.rmse, m.picp, m.mpiw, m.interval_score, m.bias,
                   m.baseline_exaa_mae, m.baseline_7d_mae, m.evaluated_at,
                   m.monitoring_status, m.monitoring_reasons,
                   m.rolling_7d_mae, m.rolling_7d_baseline_exaa_mae,
                   m.rolling_28d_picp
            FROM {self.settings.forecast_table} f
            LEFT JOIN {self.settings.gold_table} g
              USING (delivery_date, quarter_of_day)
            LEFT JOIN {self.settings.metrics_table} m USING (delivery_date)
            WHERE f.delivery_date = :delivery_date
            ORDER BY f.quarter_of_day
            """,
            {"delivery_date": delivery_date},
            max_age_seconds=_FORECAST_CACHE_SECONDS,
        )

    def features(self, delivery_date: date) -> list[dict[str, Any]]:
        """Return the human-readable Gold inputs used for one prediction."""
        return self._query(
            f"""
            SELECT delivery_date, quarter_of_day, hour, quarter, day_of_week,
                   month, season, is_weekend, is_holiday_de_nationwide,
                   is_holiday_lu, price_de_lu_exaa_eur_per_mwh,
                   price_at_exaa_eur_per_mwh,
                   price_de_lu_sdac_lag_1d_eur_per_mwh,
                   price_de_lu_sdac_lag_2d_eur_per_mwh,
                   price_de_lu_sdac_lag_7d_eur_per_mwh,
                   load_day_ahead_forecast_mw,
                   solar_day_ahead_forecast_mw,
                   wind_onshore_day_ahead_forecast_mw,
                   wind_offshore_day_ahead_forecast_mw,
                   residual_load_day_ahead_forecast_mw,
                   load_actual_d_minus_2_mw, solar_actual_d_minus_2_mw,
                   wind_onshore_actual_d_minus_2_mw,
                   wind_offshore_actual_d_minus_2_mw,
                   residual_load_actual_d_minus_2_mw
            FROM {self.settings.gold_table}
            WHERE delivery_date = :delivery_date
            ORDER BY quarter_of_day
            """,
            {"delivery_date": delivery_date},
            max_age_seconds=_SETTLED_DATA_CACHE_SECONDS,
        )

    def weather(self, delivery_date: date) -> list[dict[str, Any]]:
        """Return quarter-hour means across the 25 weather grid points."""
        return self._query(
            f"""
            SELECT quarter_of_day, {_weather_select()}
            FROM {self.settings.gold_table}
            WHERE delivery_date = :delivery_date
            ORDER BY quarter_of_day
            """,
            {"delivery_date": delivery_date},
            max_age_seconds=_SETTLED_DATA_CACHE_SECONDS,
        )

    def observations(self, delivery_date: date) -> list[dict[str, Any]]:
        """Read delivery-day market prices independently of model history."""
        return self._query(
            f"""
            SELECT quarter_of_day,
                   price_de_lu_sdac_eur_per_mwh AS actual_price_eur_per_mwh,
                   price_de_lu_exaa_eur_per_mwh, price_at_exaa_eur_per_mwh
            FROM {self.settings.gold_table}
            WHERE delivery_date = :delivery_date
            ORDER BY quarter_of_day
            """,
            {"delivery_date": delivery_date},
            max_age_seconds=_SETTLED_DATA_CACHE_SECONDS,
        )

    def gold_csv(self, through: date | None) -> Iterator[bytes]:
        """Stream the complete Gold table as CSV without loading it in app memory."""
        statement = f"SELECT * FROM {self.settings.gold_table}"
        parameters: dict[str, object] = {}
        if through is not None:
            statement += " WHERE delivery_date <= :through"
            parameters["through"] = through
        statement += " ORDER BY delivery_date, quarter_of_day"

        with (
            closing(self._connect()) as connection,
            closing(connection.cursor()) as cursor,
        ):
            cursor.execute(statement, parameters)
            buffer = io.StringIO(newline="")
            writer = csv.writer(buffer)
            writer.writerow(column[0] for column in cursor.description)
            yield buffer.getvalue().encode()
            while rows := cursor.fetchmany(1_000):
                buffer.seek(0)
                buffer.truncate(0)
                writer.writerows(rows)
                yield buffer.getvalue().encode()

    def model_info(self) -> dict[str, Any]:
        """Read metadata for the downloadable production model."""
        path = f"{self.settings.model_volume}/latest.json"
        try:
            response = self._workspace.files.download(path)
        except NotFound as exc:
            raise ArtifactNotFoundError(
                "No production model export is available"
            ) from exc
        if response.contents is None:
            raise RuntimeError("The model metadata download returned no content")
        with closing(response.contents) as stream:
            value = json.loads(stream.read())
        if not isinstance(value, dict):
            raise RuntimeError("The model metadata is malformed")
        return value

    def model_download(self) -> FileDownload:
        """Open the production model archive referenced by ``latest.json``."""
        info = self.model_info()
        name = info.get("archive")
        if not isinstance(name, str) or not _ARCHIVE.fullmatch(name):
            raise RuntimeError("The model archive name is malformed")
        try:
            response = self._workspace.files.download(
                f"{self.settings.model_volume}/{name}"
            )
        except NotFound as exc:
            raise ArtifactNotFoundError(
                "The production model archive is missing"
            ) from exc
        if response.contents is None:
            raise RuntimeError("The model archive download returned no content")
        return FileDownload(
            name=name, stream=response.contents, size=response.content_length
        )
