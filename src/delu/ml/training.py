"""Leakage-safe fitting helpers for the conformal price model."""

from __future__ import annotations

import numpy as np
from numpy.typing import ArrayLike, NDArray

from delu.contracts import FEATURE_DATA_VERSION
from delu.ml.data import (
    BASELINE_COLUMN,
    FEATURE_NAMES,
    DailyData,
)
from delu.ml.metrics import forecast_metrics
from delu.ml.model import (
    INTERVAL_CONFIG,
    POINT_CONFIG,
    BoostingConfig,
    ConformalPriceForecaster,
    conformal_adjustment,
)

REFERENCE_FEATURE_INDEX = FEATURE_NAMES.index(BASELINE_COLUMN)
BOOTSTRAP_SAMPLES = 10_000
RANDOM_SEED = 42


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
        feature_data_version=FEATURE_DATA_VERSION,
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


def reference_forecast(
    training: DailyData,
    evaluation: DailyData,
    *,
    coverage: float,
    calibration_days: int,
) -> NDArray[np.float32]:
    """Build a quarter-specific spread baseline with calibrated intervals."""
    if training.targets is None or evaluation.targets is None:
        raise ValueError("Reference evaluation requires observed targets")
    if not 0 < coverage < 1:
        raise ValueError("coverage must be between zero and one")
    if calibration_days < 1 or len(training) <= calibration_days:
        raise ValueError("Reference training requires a calibration partition")

    spread = np.asarray(training.targets - training.baseline, dtype=np.float64)
    estimation = spread[:-calibration_days]
    calibration = spread[-calibration_days:]
    alpha = 1 - coverage
    point_spread = np.median(spread, axis=0)
    lower_spread = np.quantile(estimation, alpha / 2, axis=0)
    upper_spread = np.quantile(estimation, 1 - alpha / 2, axis=0)
    adjustment = conformal_adjustment(
        calibration,
        np.broadcast_to(lower_spread, calibration.shape),
        np.broadcast_to(upper_spread, calibration.shape),
        coverage=coverage,
    )
    lower_spread = np.minimum(lower_spread - adjustment, point_spread)
    upper_spread = np.maximum(upper_spread + adjustment, point_spread)
    shape = evaluation.baseline.shape
    return np.stack(
        (
            evaluation.baseline + np.broadcast_to(point_spread, shape),
            evaluation.baseline + np.broadcast_to(lower_spread, shape),
            evaluation.baseline + np.broadcast_to(upper_spread, shape),
        ),
        axis=-1,
    ).astype(np.float32, copy=False)


def reference_metrics(
    training: DailyData,
    evaluation: DailyData,
    *,
    coverage: float,
    calibration_days: int,
) -> dict[str, float]:
    """Measure unchanged EXAA and the empirical quarter-spread benchmark."""
    if evaluation.targets is None:
        raise ValueError("Reference evaluation requires observed targets")
    prediction = reference_forecast(
        training,
        evaluation,
        coverage=coverage,
        calibration_days=calibration_days,
    )
    metrics = forecast_metrics(
        evaluation.targets,
        prediction[..., 0],
        prediction[..., 1],
        prediction[..., 2],
        coverage=coverage,
    )
    metrics["exaa_mae"] = float(
        np.mean(np.abs(evaluation.baseline - evaluation.targets))
    )
    return metrics


def paired_mae_gain_interval(
    actual: ArrayLike,
    baseline: ArrayLike,
    candidate: ArrayLike,
    *,
    samples: int = BOOTSTRAP_SAMPLES,
    seed: int = RANDOM_SEED,
) -> tuple[float, float, float]:
    """Return mean candidate MAE gain and a paired 95% interval by day."""
    observed = np.asarray(actual, dtype=np.float64)
    reference = np.asarray(baseline, dtype=np.float64)
    predicted = np.asarray(candidate, dtype=np.float64)
    if (
        observed.shape != reference.shape
        or observed.shape != predicted.shape
        or observed.ndim != 2
        or observed.size == 0
    ):
        raise ValueError("MAE gain inputs must share a non-empty [day, quarter] shape")
    if not all(
        np.isfinite(values).all() for values in (observed, reference, predicted)
    ):
        raise ValueError("MAE gain inputs must be finite")
    if samples < 1:
        raise ValueError("Bootstrap samples must be positive")

    daily_gain = np.mean(
        np.abs(reference - observed) - np.abs(predicted - observed),
        axis=1,
    )
    rng = np.random.default_rng(seed)
    indices = rng.integers(0, len(daily_gain), size=(samples, len(daily_gain)))
    resampled_gain = daily_gain[indices].mean(axis=1)
    low, high = np.quantile(resampled_gain, [0.025, 0.975])
    return float(daily_gain.mean()), float(low), float(high)
