"""Leakage-safe fitting helpers for the conformal price model."""

from __future__ import annotations

import numpy as np
from numpy.typing import ArrayLike

from delu.ml.data import BASELINE_COLUMN, FEATURE_NAMES, DailyData
from delu.ml.model import (
    INTERVAL_CONFIG,
    POINT_CONFIG,
    BoostingConfig,
    ConformalPriceForecaster,
)

REFERENCE_FEATURE_INDEX = FEATURE_NAMES.index(BASELINE_COLUMN)


def fit_model(
    data: DailyData,
    *,
    coverage: float,
    calibration_days: int,
    point_shrinkage: float,
    point_config: BoostingConfig = POINT_CONFIG,
    interval_config: BoostingConfig = INTERVAL_CONFIG,
) -> ConformalPriceForecaster:
    """Fit one model using only the supplied chronological partition."""
    if data.targets is None:
        raise ValueError("Training data must contain targets")
    return ConformalPriceForecaster(
        feature_count=data.features.shape[-1],
        reference_feature_index=REFERENCE_FEATURE_INDEX,
        coverage=coverage,
        calibration_days=calibration_days,
        point_shrinkage=point_shrinkage,
        point_config=point_config,
        interval_config=interval_config,
    ).fit(data.features, data.targets)


def select_point_shrinkage(
    corrections: ArrayLike,
    residuals: ArrayLike,
    *,
    steps: int = 101,
) -> float:
    """Select EXAA correction strength from out-of-fold absolute error."""
    predicted = np.asarray(corrections, dtype=np.float64).ravel()
    actual = np.asarray(residuals, dtype=np.float64).ravel()
    if predicted.shape != actual.shape or predicted.size == 0:
        raise ValueError("corrections and residuals must have the same non-empty shape")
    if not np.isfinite(predicted).all() or not np.isfinite(actual).all():
        raise ValueError("corrections and residuals must be finite")
    if steps < 2:
        raise ValueError("steps must be at least two")

    candidates = np.linspace(0, 1, steps, dtype=np.float64)
    errors = np.mean(
        np.abs(candidates[:, np.newaxis] * predicted - actual),
        axis=1,
    )
    return float(candidates[np.argmin(errors)])
