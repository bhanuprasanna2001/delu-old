from __future__ import annotations

import numpy as np
import pytest

from delu.ml.loss_experiment import _sample_weights, mae_gain_interval, shift_metrics


def test_sample_weights_emphasize_only_material_shifts() -> None:
    weights = _sample_weights(
        np.array([-20.0, -5.0, 0.0, 5.0, 20.0]),
        threshold=10.0,
        material_weight=5.0,
    )

    np.testing.assert_array_equal(weights, [5.0, 1.0, 1.0, 1.0, 5.0])


def test_shift_metrics_measure_direction_tail_detection_and_exaa_value() -> None:
    metrics = shift_metrics(
        actual_shift=np.array([-20.0, -5.0, 0.0, 5.0, 20.0]),
        predicted_shift=np.array([-12.0, -4.0, 0.0, -3.0, 25.0]),
        material_shift=10.0,
    )

    assert metrics == pytest.approx(
        {
            "mae": 4.4,
            "rmse": np.sqrt(30.8),
            "direction_accuracy_pct": 75.0,
            "material_mae": 6.5,
            "material_recall_pct": 100.0,
            "beat_exaa_pct": 60.0,
            "mean_absolute_correction": 8.8,
        }
    )


def test_mae_gain_interval_resamples_whole_days() -> None:
    gain, low, high = mae_gain_interval(
        actual_shift=np.array([[2.0, -2.0], [4.0, -4.0]]),
        predicted_shift=np.array([[1.0, -1.0], [1.0, -1.0]]),
        samples=100,
        seed=7,
    )

    assert (gain, low, high) == pytest.approx((1.0, 1.0, 1.0))
