from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from delu.ml.monitoring import assess_drift


def history(
    days: int,
    *,
    mae: float = 10.0,
    baseline_exaa_mae: float = 11.0,
    picp: float = 0.9,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "delivery_date": [
                date(2026, 1, 1) + timedelta(days=i) for i in range(days)
            ],
            "mae": [mae] * days,
            "baseline_exaa_mae": [baseline_exaa_mae] * days,
            "picp": [picp] * days,
        }
    )


def test_monitoring_stays_quiet_for_healthy_forecasts() -> None:
    result = assess_drift(
        history(14),
        data_outlier_rate=0.01,
        prediction_mean_z=0.5,
        reference_mae=9.0,
        target_coverage=0.9,
    )

    assert result.status == "ok"
    assert result.reasons == ()


def test_monitoring_reports_data_and_performance_drift() -> None:
    result = assess_drift(
        history(14, mae=20.0, baseline_exaa_mae=10.0, picp=0.7),
        data_outlier_rate=0.3,
        prediction_mean_z=5.0,
        reference_mae=10.0,
        target_coverage=0.9,
    )

    assert result.status == "alert"
    assert len(result.reasons) == 5
