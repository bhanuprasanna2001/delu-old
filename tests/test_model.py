from __future__ import annotations

from pathlib import Path

import mlflow.sklearn
import numpy as np
import pytest

from delu.ml.model import (
    BoostingConfig,
    ConformalPriceForecaster,
    conformal_adjustment,
)

FAST_CONFIG = BoostingConfig(
    learning_rate=0.1,
    max_iter=5,
    max_leaf_nodes=3,
    min_samples_leaf=5,
    l2_regularization=1.0,
)


def training_arrays(days: int = 8) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(7)
    features = rng.normal(size=(days, 96, 3)).astype(np.float32)
    features[..., 0] += 50
    targets = (
        features[..., 0] + 2 * features[..., 1] + rng.normal(scale=0.5, size=(days, 96))
    )
    return features, targets.astype(np.float32)


def fitted_model() -> ConformalPriceForecaster:
    features, targets = training_arrays()
    return ConformalPriceForecaster(
        feature_count=3,
        reference_feature_index=0,
        calibration_days=2,
        point_config=FAST_CONFIG,
        interval_config=FAST_CONFIG,
    ).fit(features, targets)


def test_model_returns_ordered_price_intervals() -> None:
    features, _ = training_arrays()
    output = fitted_model().predict(features[:2])

    assert output.shape == (2, 96, 3)
    assert np.all(output[..., 1] <= output[..., 0])
    assert np.all(output[..., 0] <= output[..., 2])
    assert np.isfinite(output).all()


def test_conformal_adjustment_uses_finite_sample_rank() -> None:
    target = np.arange(10, dtype=float)
    lower = np.full(10, -1.0)
    upper = np.full(10, 1.0)

    adjustment = conformal_adjustment(target, lower, upper, coverage=0.8)
    covered = np.mean((target >= lower - adjustment) & (target <= upper + adjustment))

    assert adjustment == pytest.approx(7.0)
    assert covered >= 0.8


def test_model_round_trips_through_mlflow(tmp_path: Path) -> None:
    features, _ = training_arrays()
    model = fitted_model()
    expected = model.predict(features[:1])
    path = tmp_path / "model"

    mlflow.sklearn.save_model(
        model,
        str(path),
        serialization_format="cloudpickle",
        pip_requirements=[],
    )
    loaded = mlflow.sklearn.load_model(str(path))

    assert isinstance(loaded, ConformalPriceForecaster)
    np.testing.assert_allclose(loaded.predict(features[:1]), expected)
