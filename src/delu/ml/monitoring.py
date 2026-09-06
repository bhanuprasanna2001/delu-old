"""Small, explicit drift rules for the daily batch workflow."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class MonitoringResult:
    status: str
    reasons: tuple[str, ...]
    rolling_7d_mae: float
    rolling_7d_baseline_exaa_mae: float
    rolling_28d_picp: float


def assess_drift(
    history: pd.DataFrame,
    *,
    data_outlier_rate: float,
    prediction_mean_z: float,
    reference_mae: float | None,
    target_coverage: float,
) -> MonitoringResult:
    """Assess input, output, performance, and interval-coverage drift."""
    required = {"delivery_date", "mae", "baseline_exaa_mae", "picp"}
    missing = sorted(required.difference(history.columns))
    if missing or history.empty:
        raise ValueError(f"Metric history is missing data: {missing}")

    ordered = history.sort_values("delivery_date")
    rolling_7d_mae = float(ordered.tail(7)["mae"].mean())
    rolling_7d_baseline_exaa_mae = float(ordered.tail(7)["baseline_exaa_mae"].mean())
    rolling_28d_picp = float(ordered.tail(28)["picp"].mean())
    reasons = []
    if data_outlier_rate > 0.2:
        reasons.append(
            "more than 20% of input values are outside four training "
            "standard deviations"
        )
    if abs(prediction_mean_z) > 4:
        reasons.append(
            "the daily mean prediction is outside four training target "
            "standard deviations"
        )
    if (
        len(ordered) >= 7
        and reference_mae is not None
        and rolling_7d_mae > 1.5 * reference_mae
    ):
        reasons.append("seven-day MAE is more than 50% above the model test MAE")
    if len(ordered) >= 7 and rolling_7d_mae > rolling_7d_baseline_exaa_mae:
        reasons.append("seven-day model MAE is worse than the EXAA baseline")
    if len(ordered) >= 14 and rolling_28d_picp < target_coverage - 0.1:
        reasons.append("rolling prediction-interval coverage is below tolerance")

    return MonitoringResult(
        status="alert" if reasons else "ok",
        reasons=tuple(reasons),
        rolling_7d_mae=rolling_7d_mae,
        rolling_7d_baseline_exaa_mae=rolling_7d_baseline_exaa_mae,
        rolling_28d_picp=rolling_28d_picp,
    )
