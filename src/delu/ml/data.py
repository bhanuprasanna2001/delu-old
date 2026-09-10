"""Turn the Gold table into leakage-safe daily model tensors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from delu.contracts import FEATURE_DATA_VERSION
from delu.pipeline.gold import WEATHER_FEATURES

QUARTERS_PER_DAY = 96
TARGET_COLUMN = "price_de_lu_sdac_eur_per_mwh"
BASELINE_COLUMN = "price_de_lu_exaa_eur_per_mwh"
NUMERIC_FEATURES = (
    BASELINE_COLUMN,
    "price_at_exaa_eur_per_mwh",
    "price_de_lu_sdac_lag_1d_eur_per_mwh",
    "price_de_lu_sdac_lag_2d_eur_per_mwh",
    "price_de_lu_sdac_lag_7d_eur_per_mwh",
    "load_actual_d_minus_2_mw",
    "solar_actual_d_minus_2_mw",
    "wind_onshore_actual_d_minus_2_mw",
    "wind_offshore_actual_d_minus_2_mw",
    "residual_load_actual_d_minus_2_mw",
    *WEATHER_FEATURES,
)
BOOLEAN_FEATURES = (
    "is_weekend",
    "is_holiday_de_nationwide",
    "is_holiday_lu",
)
CYCLICAL_FEATURES = (
    "quarter_sin",
    "quarter_cos",
    "weekday_sin",
    "weekday_cos",
    "month_sin",
    "month_cos",
)
FEATURE_NAMES = (*NUMERIC_FEATURES, *BOOLEAN_FEATURES, *CYCLICAL_FEATURES)


@dataclass(frozen=True)
class DailyData:
    """One feature and target sequence per local delivery day."""

    dates: tuple[date, ...]
    features: NDArray[np.float32]
    targets: NDArray[np.float32] | None
    baseline: NDArray[np.float32]

    def __len__(self) -> int:
        return len(self.dates)

    def take(self, start: int, stop: int) -> DailyData:
        """Return a contiguous temporal subset."""
        return DailyData(
            dates=self.dates[start:stop],
            features=self.features[start:stop],
            targets=None if self.targets is None else self.targets[start:stop],
            baseline=self.baseline[start:stop],
        )


@dataclass(frozen=True)
class TemporalSplit:
    """Chronological train, validation, and test partitions."""

    train: DailyData
    validation: DailyData
    test: DailyData


@dataclass(frozen=True)
class ValidationFold:
    """One expanding-window training partition and its following validation."""

    train: DailyData
    validation: DailyData


@dataclass(frozen=True)
class WalkForwardSplit:
    """Recent rolling-origin folds followed by one untouched test partition."""

    folds: tuple[ValidationFold, ...]
    test: DailyData


def _require_columns(frame: pd.DataFrame, columns: tuple[str, ...]) -> None:
    missing = sorted(set(columns).difference(frame.columns))
    if missing:
        raise ValueError(f"Gold is missing required columns: {missing}")


def _engineer_features(frame: pd.DataFrame) -> pd.DataFrame:
    features = frame.loc[:, NUMERIC_FEATURES].apply(pd.to_numeric, errors="raise")
    for column in BOOLEAN_FEATURES:
        if frame[column].isna().any():
            raise ValueError(f"Gold feature {column!r} contains missing values")
        features[column] = frame[column].astype("bool").astype("float32")

    quarter_angle = 2 * np.pi * frame["quarter_of_day"] / QUARTERS_PER_DAY
    weekday_angle = 2 * np.pi * frame["day_of_week"] / 7
    month_angle = 2 * np.pi * (frame["month"] - 1) / 12
    features["quarter_sin"] = np.sin(quarter_angle)
    features["quarter_cos"] = np.cos(quarter_angle)
    features["weekday_sin"] = np.sin(weekday_angle)
    features["weekday_cos"] = np.cos(weekday_angle)
    features["month_sin"] = np.sin(month_angle)
    features["month_cos"] = np.cos(month_angle)
    return features.loc[:, FEATURE_NAMES]


def prepare_daily_data(
    frame: pd.DataFrame,
    *,
    require_targets: bool,
) -> DailyData:
    """Validate Gold rows and reshape them to ``[day, 96, feature]``."""
    required = (
        "delivery_date",
        "quarter_of_day",
        "feature_data_version",
        "day_of_week",
        "month",
        TARGET_COLUMN,
        *NUMERIC_FEATURES,
        *BOOLEAN_FEATURES,
    )
    _require_columns(frame, required)
    if frame.empty:
        raise ValueError("Gold contains no rows")

    data = frame.loc[:, list(dict.fromkeys(required))].copy()
    versions = pd.to_numeric(data["feature_data_version"], errors="raise").unique()
    if len(versions) != 1 or versions[0] != FEATURE_DATA_VERSION:
        raise ValueError(
            "Gold feature data version is incompatible: "
            f"expected {FEATURE_DATA_VERSION}, found {versions.tolist()}"
        )
    data["delivery_date"] = pd.to_datetime(
        data["delivery_date"], errors="raise"
    ).dt.date
    data["quarter_of_day"] = pd.to_numeric(data["quarter_of_day"], errors="raise")
    data = data.sort_values(["delivery_date", "quarter_of_day"]).reset_index(drop=True)

    key = ["delivery_date", "quarter_of_day"]
    if data.duplicated(key).any():
        duplicate = data.loc[data.duplicated(key, keep=False), key].iloc[0].to_dict()
        raise ValueError(f"Gold contains a duplicate model interval: {duplicate}")

    counts = data.groupby("delivery_date", sort=False).size()
    if not counts.eq(QUARTERS_PER_DAY).all():
        invalid = counts.loc[~counts.eq(QUARTERS_PER_DAY)].to_dict()
        raise ValueError(f"Gold must contain 96 rows per delivery day: {invalid}")

    quarters = data["quarter_of_day"].to_numpy().reshape(-1, QUARTERS_PER_DAY)
    if not np.all(quarters == np.arange(QUARTERS_PER_DAY)):
        raise ValueError("Each Gold day must contain quarter_of_day 0 through 95")

    target = pd.to_numeric(data[TARGET_COLUMN], errors="coerce")
    target_counts = target.notna().groupby(data["delivery_date"], sort=False).sum()
    partial = target_counts.between(1, QUARTERS_PER_DAY - 1)
    if require_targets and partial.any():
        raise ValueError(
            "Gold contains partially observed target days: "
            f"{target_counts.loc[partial].to_dict()}"
        )
    if require_targets:
        complete_dates = target_counts.loc[target_counts.eq(QUARTERS_PER_DAY)].index
        data = data.loc[data["delivery_date"].isin(complete_dates)].reset_index(
            drop=True
        )
        if data.empty:
            raise ValueError("Gold contains no fully observed target days")

    feature_frame = _engineer_features(data)
    feature_values = feature_frame.to_numpy(dtype=np.float32)
    invalid_features = ~np.isfinite(feature_values)
    if invalid_features.any():
        invalid_columns = feature_frame.columns[invalid_features.any(axis=0)].tolist()
        raise ValueError(f"Gold contains non-finite model features: {invalid_columns}")

    features = feature_values.reshape(-1, QUARTERS_PER_DAY, len(FEATURE_NAMES))
    baseline = (
        data[BASELINE_COLUMN].to_numpy(dtype=np.float32).reshape(-1, QUARTERS_PER_DAY)
    )
    targets: NDArray[np.float32] | None = None
    if require_targets:
        targets = (
            data[TARGET_COLUMN].to_numpy(dtype=np.float32).reshape(-1, QUARTERS_PER_DAY)
        )
        if not np.isfinite(targets).all():
            raise ValueError("Gold target contains non-finite values")

    dates = tuple(data["delivery_date"].drop_duplicates())
    return DailyData(dates, features, targets, baseline)


def temporal_split(
    data: DailyData,
    *,
    validation_days: int = 28,
    test_days: int = 28,
    minimum_train_days: int = 120,
) -> TemporalSplit:
    """Split complete days chronologically without shuffling."""
    if data.targets is None:
        raise ValueError("Temporal training split requires targets")
    if min(validation_days, test_days, minimum_train_days) < 1:
        raise ValueError("Split sizes must be positive")

    train_stop = len(data) - validation_days - test_days
    validation_stop = len(data) - test_days
    if train_stop < minimum_train_days:
        required = minimum_train_days + validation_days + test_days
        raise ValueError(f"Training requires at least {required} complete days")

    return TemporalSplit(
        train=data.take(0, train_stop),
        validation=data.take(train_stop, validation_stop),
        test=data.take(validation_stop, len(data)),
    )


def walk_forward_split(
    data: DailyData,
    *,
    folds: int = 3,
    validation_days: int = 28,
    test_days: int = 28,
    minimum_train_days: int = 120,
) -> WalkForwardSplit:
    """Build expanding rolling-origin folds before an untouched final test."""
    if data.targets is None:
        raise ValueError("Walk-forward training requires targets")
    if min(folds, validation_days, test_days, minimum_train_days) < 1:
        raise ValueError("Walk-forward split sizes must be positive")

    test_start = len(data) - test_days
    first_validation_start = test_start - folds * validation_days
    if first_validation_start < minimum_train_days:
        required = minimum_train_days + folds * validation_days + test_days
        raise ValueError(f"Walk-forward training requires at least {required} days")

    validation_folds = tuple(
        ValidationFold(
            train=data.take(0, start),
            validation=data.take(start, start + validation_days),
        )
        for start in range(
            first_validation_start,
            test_start,
            validation_days,
        )
    )
    return WalkForwardSplit(
        folds=validation_folds,
        test=data.take(test_start, len(data)),
    )
