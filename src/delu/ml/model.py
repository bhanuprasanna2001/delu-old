"""EXAA-anchored price forecasts with conformal quantile intervals."""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from numpy.typing import ArrayLike, NDArray
from sklearn.base import BaseEstimator
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.utils.validation import check_is_fitted


@dataclass(frozen=True)
class BoostingConfig:
    """Small, reproducible configuration for one boosted residual model."""

    learning_rate: float = 0.05
    max_iter: int = 250
    max_leaf_nodes: int = 15
    min_samples_leaf: int = 40
    l2_regularization: float = 10.0

    def estimator(
        self,
        *,
        loss: str,
        quantile: float | None = None,
    ) -> HistGradientBoostingRegressor:
        """Build an unfitted estimator from this configuration."""
        return HistGradientBoostingRegressor(
            loss=loss,
            quantile=quantile,
            learning_rate=self.learning_rate,
            max_iter=self.max_iter,
            max_leaf_nodes=self.max_leaf_nodes,
            min_samples_leaf=self.min_samples_leaf,
            l2_regularization=self.l2_regularization,
            early_stopping=True,
            validation_fraction=0.1,
            n_iter_no_change=10,
            random_state=42,
        )


POINT_CONFIG = BoostingConfig()
INTERVAL_CONFIG = BoostingConfig(max_leaf_nodes=7)


def conformal_adjustment(
    target: ArrayLike,
    lower: ArrayLike,
    upper: ArrayLike,
    *,
    coverage: float,
) -> float:
    """Return the finite-sample CQR adjustment for a calibration set."""
    if not 0 < coverage < 1:
        raise ValueError("coverage must be between 0 and 1")
    actual, low, high = (
        np.asarray(value, dtype=np.float64) for value in (target, lower, upper)
    )
    if not (actual.shape == low.shape == high.shape):
        raise ValueError("target, lower, and upper must have the same shape")
    if actual.size == 0 or not all(
        np.isfinite(value).all() for value in (actual, low, high)
    ):
        raise ValueError("calibration values must be non-empty and finite")
    if np.any(low > high):
        raise ValueError("lower calibration bounds cannot exceed upper bounds")

    scores = np.maximum(low - actual, actual - high).ravel()
    rank = min(scores.size - 1, math.ceil((scores.size + 1) * coverage) - 1)
    return float(np.partition(scores, rank)[rank])


class ConformalPriceForecaster(BaseEstimator):
    """Predict the SDAC-minus-EXAA spread and calibrated price intervals."""

    def __init__(
        self,
        feature_count: int,
        reference_feature_index: int,
        *,
        horizon: int = 96,
        coverage: float = 0.9,
        calibration_days: int = 28,
        point_shrinkage: float = 1.0,
        point_config: BoostingConfig = POINT_CONFIG,
        interval_config: BoostingConfig = INTERVAL_CONFIG,
    ) -> None:
        self.feature_count = feature_count
        self.reference_feature_index = reference_feature_index
        self.horizon = horizon
        self.coverage = coverage
        self.calibration_days = calibration_days
        self.point_shrinkage = point_shrinkage
        self.point_config = point_config
        self.interval_config = interval_config

    def _validated_features(self, features: ArrayLike) -> NDArray[np.float32]:
        values = np.asarray(features, dtype=np.float32)
        expected = (self.horizon, self.feature_count)
        if values.ndim != 3 or values.shape[1:] != expected:
            raise ValueError(
                f"features must have shape [day, {self.horizon}, {self.feature_count}]"
            )
        if values.shape[0] == 0 or not np.isfinite(values).all():
            raise ValueError("features must be non-empty and finite")
        return values

    def fit(
        self,
        features: ArrayLike,
        targets: ArrayLike,
    ) -> ConformalPriceForecaster:
        """Fit point and quantile residual models, then conformalize the bounds."""
        values = self._validated_features(features)
        actual = np.asarray(targets, dtype=np.float32)
        if actual.shape != values.shape[:2] or not np.isfinite(actual).all():
            raise ValueError("targets must be finite with shape [day, quarter]")
        if min(self.feature_count, self.horizon, self.calibration_days) < 1:
            raise ValueError("model dimensions and calibration_days must be positive")
        if not 0 <= self.reference_feature_index < self.feature_count:
            raise ValueError("reference_feature_index is outside the feature array")
        if not 0 < self.coverage < 1:
            raise ValueError("coverage must be between 0 and 1")
        if not 0 <= self.point_shrinkage <= 1:
            raise ValueError("point_shrinkage must be between 0 and 1")
        if len(values) <= self.calibration_days:
            raise ValueError("training requires more days than calibration_days")

        flattened = values.reshape(-1, self.feature_count)
        reference = values[..., self.reference_feature_index]
        spread = actual - reference
        interval_stop = len(values) - self.calibration_days
        interval_features = values[:interval_stop].reshape(-1, self.feature_count)
        interval_spread = spread[:interval_stop].ravel()
        calibration_features = values[interval_stop:].reshape(-1, self.feature_count)
        calibration_spread = spread[interval_stop:].ravel()

        self.point_model_ = self.point_config.estimator(loss="absolute_error").fit(
            flattened, spread.ravel()
        )
        alpha = 1 - self.coverage
        self.lower_model_ = self.interval_config.estimator(
            loss="quantile", quantile=alpha / 2
        ).fit(interval_features, interval_spread)
        self.upper_model_ = self.interval_config.estimator(
            loss="quantile", quantile=1 - alpha / 2
        ).fit(interval_features, interval_spread)

        raw_lower = self.lower_model_.predict(calibration_features)
        raw_upper = self.upper_model_.predict(calibration_features)
        lower = np.minimum(raw_lower, raw_upper)
        upper = np.maximum(raw_lower, raw_upper)
        self.conformal_adjustment_ = conformal_adjustment(
            calibration_spread,
            lower,
            upper,
            coverage=self.coverage,
        )
        self.feature_mean_ = flattened.mean(axis=0, dtype=np.float64).astype(np.float32)
        self.feature_scale_ = flattened.std(axis=0, dtype=np.float64).astype(np.float32)
        self.feature_scale_[self.feature_scale_ < 1e-6] = 1.0
        self.target_mean_ = float(actual.mean(dtype=np.float64))
        self.target_scale_ = float(actual.std(dtype=np.float64))
        if self.target_scale_ < 1e-6:
            self.target_scale_ = 1.0
        return self

    def set_point_shrinkage(self, shrinkage: float) -> None:
        """Shrink the learned correction toward the known EXAA price."""
        if not math.isfinite(shrinkage) or not 0 <= shrinkage <= 1:
            raise ValueError("point shrinkage must be finite and between 0 and 1")
        self.point_shrinkage = shrinkage

    def predict_correction(
        self,
        features: ArrayLike,
        *,
        apply_shrinkage: bool = True,
    ) -> NDArray[np.float32]:
        """Predict the SDAC-minus-EXAA point correction."""
        check_is_fitted(self, "point_model_")
        values = self._validated_features(features)
        correction = self.point_model_.predict(
            values.reshape(-1, self.feature_count)
        ).reshape(values.shape[:2])
        if apply_shrinkage:
            correction *= self.point_shrinkage
        return correction.astype(np.float32, copy=False)

    def predict(self, features: ArrayLike) -> NDArray[np.float32]:
        """Return point, lower, and upper prices with shape ``[day, 96, 3]``."""
        check_is_fitted(
            self,
            ("point_model_", "lower_model_", "upper_model_", "conformal_adjustment_"),
        )
        values = self._validated_features(features)
        flattened = values.reshape(-1, self.feature_count)
        shape = values.shape[:2]
        reference = values[..., self.reference_feature_index]
        point_spread = self.predict_correction(values)
        raw_lower = self.lower_model_.predict(flattened).reshape(shape)
        raw_upper = self.upper_model_.predict(flattened).reshape(shape)
        lower_spread = np.minimum(raw_lower, raw_upper) - self.conformal_adjustment_
        upper_spread = np.maximum(raw_lower, raw_upper) + self.conformal_adjustment_
        lower_spread = np.minimum(lower_spread, point_spread)
        upper_spread = np.maximum(upper_spread, point_spread)
        return np.stack(
            (
                reference + point_spread,
                reference + lower_spread,
                reference + upper_spread,
            ),
            axis=-1,
        ).astype(np.float32, copy=False)
