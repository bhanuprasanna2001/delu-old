import pytest

from delu.ml.metrics import forecast_metrics


def test_forecast_metrics_include_point_and_interval_quality() -> None:
    metrics = forecast_metrics(
        [1.0, 3.0],
        [2.0, 2.0],
        [0.0, 1.0],
        [4.0, 3.0],
        coverage=0.9,
    )

    assert metrics["mae"] == pytest.approx(1.0)
    assert metrics["rmse"] == pytest.approx(1.0)
    assert metrics["picp"] == pytest.approx(1.0)
    assert metrics["mpiw"] == pytest.approx(3.0)
    assert metrics["interval_score"] == pytest.approx(3.0)


def test_forecast_metrics_penalise_missed_intervals() -> None:
    metrics = forecast_metrics([5.0], [2.0], [1.0], [3.0], coverage=0.8)

    assert metrics["picp"] == 0.0
    assert metrics["interval_score"] == pytest.approx(22.0)
