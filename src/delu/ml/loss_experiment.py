"""Compare point losses for the SDAC-minus-EXAA correction model."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from databricks.connect import DatabricksSession
from numpy.typing import ArrayLike, NDArray
from pyspark.sql import functions as F
from sklearn._loss.loss import HuberLoss
from sklearn.ensemble import HistGradientBoostingRegressor

from delu.ml.data import DailyData, prepare_daily_data, walk_forward_split
from delu.ml.model import POINT_CONFIG
from delu.ml.training import paired_mae_gain_interval
from delu.pipeline.gold import TABLE as GOLD_TABLE

DEFAULT_THROUGH = date(2026, 8, 31)
DEFAULT_DATA_PATH = Path("data/gold/model_input.parquet")
DEFAULT_RESULTS_PATH = Path("data/research/loss_comparison.csv")
MATERIAL_SHIFT = 10.0
MATERIAL_WEIGHT = 5.0
HUBER_DELTA = 10.0
BOOTSTRAP_SAMPLES = 10_000
RANDOM_SEED = 42


@dataclass(frozen=True)
class LossSpec:
    """One point-loss treatment with every other model setting held fixed."""

    label: str
    loss: str
    weight_threshold: float | None = None


LOSS_SPECS = (
    LossSpec("MAE", "absolute_error"),
    LossSpec("MSE", "squared_error"),
    LossSpec(f"Huber (delta={HUBER_DELTA:g})", "huber"),
    LossSpec(
        f"Weighted MAE (abs shift >= {MATERIAL_SHIFT:g})",
        "absolute_error",
        MATERIAL_SHIFT,
    ),
)


def _validated_snapshot(frame: pd.DataFrame, through: date) -> DailyData:
    """Return complete daily tensors and reject a partial or stale snapshot."""
    data = prepare_daily_data(frame, require_targets=True)
    if data.dates[-1] != through:
        raise ValueError(
            f"Gold ends on {data.dates[-1]}, not the requested cutoff {through}"
        )
    if len(frame) != len(data) * 96:
        raise ValueError("Gold contains rows outside its fully observed delivery days")
    expected_dates = tuple(
        timestamp.date()
        for timestamp in pd.date_range(data.dates[0], through, freq="D")
    )
    if data.dates != expected_dates:
        raise ValueError("Gold contains a missing delivery day")
    return data


def refresh_snapshot(*, profile: str, path: Path, through: date) -> DailyData:
    """Read a cutoff of governed Gold without changing the Databricks table."""
    spark = DatabricksSession.builder.profile(profile).serverless().getOrCreate()
    frame = (
        spark.table(GOLD_TABLE)
        .where(F.col("delivery_date") <= F.lit(through))
        .orderBy("delivery_date", "quarter_of_day")
        .toPandas()
    )
    data = _validated_snapshot(frame, through)

    path.parent.mkdir(parents=True, exist_ok=True)
    frame.attrs.clear()
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        frame.to_parquet(temporary, index=False)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return data


def load_snapshot(path: Path, through: date) -> DailyData:
    """Load and validate the local Gold cutoff used by the experiment."""
    frame = pd.read_parquet(path)
    if "delivery_date" not in frame:
        raise ValueError("Gold is missing required column: delivery_date")
    delivery_dates = pd.to_datetime(frame["delivery_date"], errors="raise").dt.date
    return _validated_snapshot(frame.loc[delivery_dates <= through], through)


def _spread(data: DailyData) -> NDArray[np.float64]:
    if data.targets is None:
        raise ValueError("Loss evaluation requires observed SDAC targets")
    return np.asarray(data.targets - data.baseline, dtype=np.float64)


def _sample_weights(
    spread: ArrayLike,
    *,
    threshold: float,
    material_weight: float = MATERIAL_WEIGHT,
) -> NDArray[np.float64]:
    values = np.asarray(spread, dtype=np.float64)
    if threshold <= 0 or material_weight < 1:
        raise ValueError("Weight threshold must be positive and weight at least one")
    if values.size == 0 or not np.isfinite(values).all():
        raise ValueError("Training spread must be non-empty and finite")
    return np.where(np.abs(values) >= threshold, material_weight, 1.0)


def _estimator(spec: LossSpec) -> HistGradientBoostingRegressor:
    loss = "absolute_error" if spec.loss == "huber" else spec.loss
    estimator = POINT_CONFIG.estimator(loss=loss)
    if spec.loss == "huber":
        # HistGradientBoosting has no public Huber option in scikit-learn 1.9.
        # The project lockfile pins the private BaseLoss implementation used here.
        estimator.set_params(loss=HuberLoss(delta=HUBER_DELTA))
    return estimator


def _fit_predict(
    train: DailyData,
    evaluation: DailyData,
    spec: LossSpec,
) -> NDArray[np.float64]:
    training_spread = _spread(train).ravel()
    weights = (
        None
        if spec.weight_threshold is None
        else _sample_weights(training_spread, threshold=spec.weight_threshold)
    )
    model = _estimator(spec).fit(
        train.features.reshape(-1, train.features.shape[-1]),
        training_spread,
        sample_weight=weights,
    )
    prediction = model.predict(
        evaluation.features.reshape(-1, evaluation.features.shape[-1])
    )
    return np.asarray(prediction, dtype=np.float64).reshape(
        len(evaluation), evaluation.features.shape[1]
    )


def shift_metrics(
    actual_shift: ArrayLike,
    predicted_shift: ArrayLike,
    *,
    material_shift: float = MATERIAL_SHIFT,
) -> dict[str, float]:
    """Measure correction magnitude, direction, and value over unchanged EXAA."""
    actual = np.asarray(actual_shift, dtype=np.float64).ravel()
    predicted = np.asarray(predicted_shift, dtype=np.float64).ravel()
    if actual.shape != predicted.shape or actual.size == 0:
        raise ValueError(
            "Actual and predicted shifts must have the same non-empty shape"
        )
    if not np.isfinite(actual).all() or not np.isfinite(predicted).all():
        raise ValueError("Actual and predicted shifts must be finite")
    if material_shift <= 0:
        raise ValueError("Material-shift threshold must be positive")

    absolute_error = np.abs(predicted - actual)
    nonzero = actual != 0
    material = np.abs(actual) >= material_shift
    if not nonzero.any() or not material.any():
        raise ValueError("Evaluation requires nonzero and material observed shifts")
    correct_direction = np.sign(predicted) == np.sign(actual)
    correct_material_detection = correct_direction & (
        np.abs(predicted) >= material_shift
    )

    return {
        "mae": float(absolute_error.mean()),
        "rmse": float(np.sqrt(np.mean(np.square(predicted - actual)))),
        "direction_accuracy_pct": float(100 * correct_direction[nonzero].mean()),
        "material_mae": float(absolute_error[material].mean()),
        "material_recall_pct": float(100 * correct_material_detection[material].mean()),
        "beat_exaa_pct": float(100 * (absolute_error < np.abs(actual)).mean()),
        "mean_absolute_correction": float(np.abs(predicted).mean()),
    }


def mae_gain_interval(
    actual_shift: ArrayLike,
    predicted_shift: ArrayLike,
    *,
    samples: int = BOOTSTRAP_SAMPLES,
    seed: int = RANDOM_SEED,
) -> tuple[float, float, float]:
    """Return mean MAE gain and a paired 95% bootstrap interval by day."""
    actual = np.asarray(actual_shift, dtype=np.float64)
    return paired_mae_gain_interval(
        actual,
        np.zeros_like(actual),
        np.asarray(predicted_shift, dtype=np.float64),
        samples=samples,
        seed=seed,
    )


def run_experiment(data: DailyData) -> pd.DataFrame:
    """Evaluate all losses on identical folds and one final holdout."""
    split = walk_forward_split(data, folds=3)
    validation_actual = np.concatenate(
        [_spread(fold.validation).ravel() for fold in split.folds]
    )
    test_actual = _spread(split.test)
    pretest = data.take(0, len(data) - len(split.test))

    rows: list[dict[str, float | str]] = []

    def add_result(
        label: str,
        validation_prediction: NDArray[np.float64],
        test_prediction: NDArray[np.float64],
    ) -> None:
        validation = shift_metrics(validation_actual, validation_prediction)
        test = shift_metrics(test_actual, test_prediction)
        gain, gain_low, gain_high = mae_gain_interval(test_actual, test_prediction)
        rows.append(
            {
                "point_loss": label,
                "cv_mae": validation["mae"],
                "holdout_sdac_mae": test["mae"],
                "holdout_rmse": test["rmse"],
                "direction_accuracy_pct": test["direction_accuracy_pct"],
                "material_mae": test["material_mae"],
                "material_recall_pct": test["material_recall_pct"],
                "beat_exaa_pct": test["beat_exaa_pct"],
                "mean_absolute_correction": test["mean_absolute_correction"],
                "mae_gain_vs_exaa": gain,
                "mae_gain_ci_low": gain_low,
                "mae_gain_ci_high": gain_high,
            }
        )

    add_result(
        "EXAA unchanged",
        np.zeros_like(validation_actual),
        np.zeros_like(test_actual),
    )
    for spec in LOSS_SPECS:
        validation_prediction = np.concatenate(
            [
                _fit_predict(fold.train, fold.validation, spec).ravel()
                for fold in split.folds
            ]
        )
        add_result(
            spec.label,
            validation_prediction,
            _fit_predict(pretest, split.test, spec),
        )
    return pd.DataFrame.from_records(rows)


def markdown_table(results: pd.DataFrame) -> str:
    """Render the README subset without requiring a table-format dependency."""
    lines = [
        "| Point loss | CV MAE | Holdout MAE | MAE gain vs EXAA (95% CI) | "
        "RMSE | Direction | Material MAE | Material recall | Beats EXAA |",
        "| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in results.to_dict(orient="records"):
        lines.append(
            f"| {row['point_loss']} | {float(row['cv_mae']):.3f} | "
            f"{float(row['holdout_sdac_mae']):.3f} | "
            f"{float(row['mae_gain_vs_exaa']):+.3f} "
            f"[{float(row['mae_gain_ci_low']):+.3f}, "
            f"{float(row['mae_gain_ci_high']):+.3f}] | "
            f"{float(row['holdout_rmse']):.3f} | "
            f"{float(row['direction_accuracy_pct']):.1f}% | "
            f"{float(row['material_mae']):.3f} | "
            f"{float(row['material_recall_pct']):.1f}% | "
            f"{float(row['beat_exaa_pct']):.1f}% |"
        )
    return "\n".join(lines)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through", type=date.fromisoformat, default=DEFAULT_THROUGH)
    parser.add_argument("--data-path", type=Path, default=DEFAULT_DATA_PATH)
    parser.add_argument("--results-path", type=Path, default=DEFAULT_RESULTS_PATH)
    parser.add_argument(
        "--refresh-data",
        action="store_true",
        help="replace the local snapshot from read-only Databricks Gold",
    )
    parser.add_argument(
        "--profile",
        help="explicit Databricks CLI profile, required with --refresh-data",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Refresh Gold when requested, run the comparison, and save its raw results."""
    parser = _parser()
    args = parser.parse_args(argv)
    if args.refresh_data and not args.profile:
        parser.error("--profile is required with --refresh-data")

    data = (
        refresh_snapshot(
            profile=args.profile,
            path=args.data_path,
            through=args.through,
        )
        if args.refresh_data
        else load_snapshot(args.data_path, args.through)
    )
    results = run_experiment(data)
    args.results_path.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(args.results_path, index=False)

    split = walk_forward_split(data, folds=3)
    print(
        f"Gold: {len(data) * 96:,} rows, {len(data)} days, "
        f"{data.dates[0]} through {data.dates[-1]}"
    )
    print(
        f"Holdout: {split.test.dates[0]} through {split.test.dates[-1]}; "
        f"material shift: {MATERIAL_SHIFT:g} EUR/MWh"
    )
    print(markdown_table(results))
    print(f"Raw results: {args.results_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
