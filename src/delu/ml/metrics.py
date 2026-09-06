"""Deterministic point and interval forecast metrics."""

from __future__ import annotations

import numpy as np
from numpy.typing import ArrayLike


def forecast_metrics(
    target: ArrayLike,
    point: ArrayLike,
    lower: ArrayLike,
    upper: ArrayLike,
    *,
    coverage: float,
) -> dict[str, float]:
    """Calculate point accuracy, interval coverage, width, and interval score."""
    if not 0 < coverage < 1:
        raise ValueError("coverage must be between 0 and 1")
    actual, prediction, low, high = (
        np.asarray(value, dtype=np.float64) for value in (target, point, lower, upper)
    )
    if not (actual.shape == prediction.shape == low.shape == high.shape):
        raise ValueError("target and predictions must have the same shape")
    if actual.size == 0 or not all(
        np.isfinite(value).all() for value in (actual, prediction, low, high)
    ):
        raise ValueError("target and predictions must be non-empty and finite")
    if np.any(low > high):
        raise ValueError("lower predictions cannot exceed upper predictions")

    error = prediction - actual
    width = high - low
    alpha = 1 - coverage
    interval_score = width.copy()
    interval_score += np.where(actual < low, 2 / alpha * (low - actual), 0)
    interval_score += np.where(actual > high, 2 / alpha * (actual - high), 0)
    return {
        "mae": float(np.mean(np.abs(error))),
        "rmse": float(np.sqrt(np.mean(np.square(error)))),
        "picp": float(np.mean((actual >= low) & (actual <= high))),
        "mpiw": float(np.mean(width)),
        "interval_score": float(np.mean(interval_score)),
    }
