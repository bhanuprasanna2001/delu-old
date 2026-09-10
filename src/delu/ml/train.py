"""Evaluate locally, then register and promote the monthly price model."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from databricks.connect import DatabricksSession
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from delu.ml.data import (
    FEATURE_NAMES,
    DailyData,
    prepare_daily_data,
    walk_forward_split,
)
from delu.ml.metrics import forecast_metrics
from delu.ml.model import (
    INTERVAL_CONFIG,
    POINT_CONFIG,
    ConformalPriceForecaster,
)
from delu.ml.training import fit_model, select_point_shrinkage
from delu.pipeline.bronze import BERLIN
from delu.pipeline.gold import TABLE as GOLD_TABLE

MODEL_NAME = "delu.ml.sdac_cqr"
EXPERIMENT_NAME = "/Shared/delu"
TARGET_COVERAGE = 0.9
CALIBRATION_DAYS = 28
WALK_FORWARD_FOLDS = 3
LOCAL_MODEL_PATH = Path("artifacts/sdac_cqr")
MODEL_CODE_PATH = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CandidateResult:
    """A fully evaluated candidate and the model safe to persist."""

    model: ConformalPriceForecaster
    fold_metrics: tuple[dict[str, float], ...]
    test_metrics: dict[str, float]
    baseline_mae: float
    point_shrinkage: float
    promotion_reasons: tuple[str, ...]


def previous_month_end(today: date) -> date:
    """Return the final day of the month before ``today``."""
    return today.replace(day=1) - timedelta(days=1)


def _metrics(
    model: ConformalPriceForecaster,
    data: DailyData,
) -> dict[str, float]:
    if data.targets is None:
        raise ValueError("Evaluation data must contain targets")
    prediction = model.predict(data.features)
    return forecast_metrics(
        data.targets,
        prediction[..., 0],
        prediction[..., 1],
        prediction[..., 2],
        coverage=TARGET_COVERAGE,
    )


def _load_production_metrics(
    client: MlflowClient,
    test: DailyData,
) -> tuple[str, dict[str, float]] | None:
    try:
        version = client.get_model_version_by_alias(MODEL_NAME, "prod")
    except MlflowException:
        return None
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@prod")
    if not isinstance(model, ConformalPriceForecaster):
        raise TypeError("The production model has an incompatible Python type")
    if model.feature_count != test.features.shape[-1]:
        LOGGER.warning("Skipping production comparison after a feature schema change")
        return None
    return version.version, _metrics(model, test)


def _promotion_reasons(
    candidate: dict[str, float],
    baseline_mae: float,
    production: tuple[str, dict[str, float]] | None,
) -> tuple[str, ...]:
    reasons = []
    if candidate["mae"] >= baseline_mae:
        reasons.append("candidate did not beat the delivery-day EXAA baseline")
    if candidate["picp"] < TARGET_COVERAGE - 0.02:
        reasons.append("candidate interval coverage is below tolerance")
    if production is not None:
        _, metrics = production
        if candidate["mae"] >= metrics["mae"]:
            reasons.append("candidate did not beat production MAE")
        if candidate["interval_score"] >= metrics["interval_score"]:
            reasons.append("candidate did not beat production interval score")
    return tuple(reasons)


def evaluate_candidate(
    data: DailyData,
    *,
    production: tuple[str, dict[str, float]] | None = None,
) -> CandidateResult:
    """Select correction shrinkage on rolling folds and evaluate once on test."""
    if data.targets is None:
        raise ValueError("Candidate evaluation requires targets")
    split = walk_forward_split(data, folds=WALK_FORWARD_FOLDS)
    fold_models = [
        fit_model(
            fold.train,
            coverage=TARGET_COVERAGE,
            calibration_days=CALIBRATION_DAYS,
            point_shrinkage=1.0,
        )
        for fold in split.folds
    ]
    corrections = np.concatenate(
        [
            model.predict_correction(
                fold.validation.features,
                apply_shrinkage=False,
            ).ravel()
            for model, fold in zip(fold_models, split.folds, strict=True)
        ]
    )
    residuals = np.concatenate(
        [
            (fold.validation.targets - fold.validation.baseline).ravel()
            for fold in split.folds
            if fold.validation.targets is not None
        ]
    )
    point_shrinkage = select_point_shrinkage(corrections, residuals)
    for model in fold_models:
        model.set_point_shrinkage(point_shrinkage)
    fold_metrics = tuple(
        _metrics(model, fold.validation)
        for model, fold in zip(fold_models, split.folds, strict=True)
    )

    pretest = data.take(0, len(data) - len(split.test))
    evaluation_model = fit_model(
        pretest,
        coverage=TARGET_COVERAGE,
        calibration_days=CALIBRATION_DAYS,
        point_shrinkage=point_shrinkage,
    )
    test_metrics = _metrics(evaluation_model, split.test)
    if split.test.targets is None:
        raise AssertionError("Test targets disappeared after temporal split")
    baseline_mae = float(np.mean(np.abs(split.test.baseline - split.test.targets)))
    reasons = _promotion_reasons(test_metrics, baseline_mae, production)
    persisted_model = evaluation_model
    if not reasons:
        persisted_model = fit_model(
            data,
            coverage=TARGET_COVERAGE,
            calibration_days=CALIBRATION_DAYS,
            point_shrinkage=point_shrinkage,
        )
    return CandidateResult(
        model=persisted_model,
        fold_metrics=fold_metrics,
        test_metrics=test_metrics,
        baseline_mae=baseline_mae,
        point_shrinkage=point_shrinkage,
        promotion_reasons=reasons,
    )


def _metadata(result: CandidateResult, through: date) -> dict[str, object]:
    return {
        "feature_names": list(FEATURE_NAMES),
        "target_coverage": TARGET_COVERAGE,
        "calibration_days": CALIBRATION_DAYS,
        "training_through": through.isoformat(),
        "point_reference": "price_de_lu_exaa_eur_per_mwh",
        "point_shrinkage": result.point_shrinkage,
        "refit_on_all_data": not result.promotion_reasons,
        "fold_metrics": list(result.fold_metrics),
        "test_metrics": result.test_metrics,
        "baseline_exaa_mae": result.baseline_mae,
        "promotion_reasons": list(result.promotion_reasons),
    }


def _export_model_archive(
    *,
    model_uri: str,
    output_dir: Path,
    version: str,
) -> str:
    """Publish one immutable MLflow model archive to a Unity Catalog volume."""
    archive_name = f"sdac-cqr-v{version}.zip"
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        download_dir = root / "model"
        download_dir.mkdir()
        model_path = Path(
            mlflow.artifacts.download_artifacts(
                artifact_uri=model_uri,
                dst_path=str(download_dir),
            )
        )
        archive = Path(
            shutil.make_archive(
                str(root / archive_name.removesuffix(".zip")),
                "zip",
                root_dir=model_path,
            )
        )
        temporary_destination = output_dir / f".{archive_name}.tmp"
        shutil.copyfile(archive, temporary_destination)
        temporary_destination.replace(output_dir / archive_name)
    return archive_name


def _publish_model_manifest(
    *,
    output_dir: Path,
    archive_name: str,
    version: str,
    run_id: str,
    through: date,
    result: CandidateResult,
) -> None:
    """Point the application at the newly promoted immutable archive."""
    manifest = {
        "model_name": MODEL_NAME,
        "model_family": "hist_gradient_boosting_cqr",
        "version": version,
        "training_run_id": run_id,
        "training_through": through.isoformat(),
        "published_at": datetime.now(UTC).isoformat(),
        "target_coverage": TARGET_COVERAGE,
        "point_shrinkage": result.point_shrinkage,
        "test_metrics": result.test_metrics,
        "baseline_exaa_mae": result.baseline_mae,
        "archive": archive_name,
    }
    temporary_manifest = output_dir / ".latest.json.tmp"
    temporary_manifest.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary_manifest.replace(output_dir / "latest.json")


def _load_local_data(path: Path, through: date) -> DailyData:
    frame = pd.read_parquet(path)
    if "delivery_date" not in frame:
        raise ValueError("Gold is missing required column: delivery_date")
    delivery_date = pd.to_datetime(frame["delivery_date"], errors="raise").dt.date
    frame = frame.loc[delivery_date <= through]
    return prepare_daily_data(frame, require_targets=True)


def train_local(
    *,
    data_path: Path,
    output_path: Path,
    through: date,
) -> CandidateResult:
    """Train and save the production-identical model without Databricks compute."""
    data = _load_local_data(data_path, through)
    result = evaluate_candidate(data)
    if output_path.exists():
        raise FileExistsError(f"local model path already exists: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    example = data.features[-1:]
    mlflow.sklearn.save_model(
        result.model,
        str(output_path),
        serialization_format="cloudpickle",
        signature=infer_signature(example, result.model.predict(example)),
        input_example=example,
        metadata=_metadata(result, through),
        code_paths=[str(MODEL_CODE_PATH)],
        pip_requirements=mlflow.sklearn.get_default_pip_requirements(
            include_cloudpickle=True
        ),
    )
    LOGGER.info(
        "Saved local model through %s to %s with test metrics %s",
        through,
        output_path,
        result.test_metrics,
    )
    return result


def train_monthly(
    *,
    through: date,
    model_export_dir: Path | None = None,
    spark: SparkSession | None = None,
) -> str:
    """Evaluate, register, and conditionally promote one monthly model."""
    spark = (
        spark
        or SparkSession.getActiveSession()
        or DatabricksSession.builder.serverless().getOrCreate()
    )
    spark.sql("CREATE SCHEMA IF NOT EXISTS delu.ml")
    frame = (
        spark.table(GOLD_TABLE)
        .where(F.col("delivery_date") <= F.lit(through))
        .orderBy("delivery_date", "quarter_of_day")
        .toPandas()
    )
    data = prepare_daily_data(frame, require_targets=True)

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(os.getenv("MLFLOW_EXPERIMENT_NAME", EXPERIMENT_NAME))
    client = MlflowClient()
    split = walk_forward_split(data, folds=WALK_FORWARD_FOLDS)
    production = _load_production_metrics(client, split.test)
    with mlflow.start_run(run_name=f"monthly-{through.isoformat()}") as run:
        result = evaluate_candidate(data, production=production)
        mlflow.set_tags(
            {
                "model_family": "hist_gradient_boosting_cqr",
                "training_through": through.isoformat(),
                "train_start": data.dates[0].isoformat(),
                "test_end": split.test.dates[-1].isoformat(),
            }
        )
        mlflow.log_params(
            {
                **{
                    f"point_{key}": value for key, value in asdict(POINT_CONFIG).items()
                },
                **{
                    f"interval_{key}": value
                    for key, value in asdict(INTERVAL_CONFIG).items()
                },
                "point_shrinkage": result.point_shrinkage,
                "feature_count": len(FEATURE_NAMES),
                "target_coverage": TARGET_COVERAGE,
                "calibration_days": CALIBRATION_DAYS,
                "walk_forward_folds": WALK_FORWARD_FOLDS,
            }
        )
        mlflow.log_metrics(
            {
                **{
                    f"fold_{index}_{key}": value
                    for index, metrics in enumerate(result.fold_metrics)
                    for key, value in metrics.items()
                },
                **{f"test_{key}": value for key, value in result.test_metrics.items()},
                "baseline_exaa_mae": result.baseline_mae,
                "conformal_adjustment": result.model.conformal_adjustment_,
                "promoted_to_prod": float(not result.promotion_reasons),
            }
        )

        example = split.test.features[:1]
        model_info = mlflow.sklearn.log_model(
            result.model,
            name="model",
            registered_model_name=MODEL_NAME,
            serialization_format="cloudpickle",
            signature=infer_signature(example, result.model.predict(example)),
            input_example=example,
            metadata=_metadata(result, through),
            code_paths=[str(MODEL_CODE_PATH)],
            pip_requirements=mlflow.sklearn.get_default_pip_requirements(
                include_cloudpickle=True
            ),
        )
        version = model_info.registered_model_version
        if version is None:
            raise RuntimeError("MLflow did not return a registered model version")
        client.set_registered_model_alias(MODEL_NAME, "candidate", version)
        client.set_model_version_tag(
            MODEL_NAME, version, "test_mae", result.test_metrics["mae"]
        )
        client.set_model_version_tag(
            MODEL_NAME,
            version,
            "test_interval_score",
            result.test_metrics["interval_score"],
        )
        client.set_model_version_tag(
            MODEL_NAME, version, "training_run_id", run.info.run_id
        )
        if result.promotion_reasons:
            client.set_model_version_tag(
                MODEL_NAME,
                version,
                "promotion_blocked",
                "; ".join(result.promotion_reasons),
            )
            LOGGER.warning(
                "Registered candidate %s but did not promote: %s",
                version,
                result.promotion_reasons,
            )
        else:
            archive_name = None
            if model_export_dir is not None:
                archive_name = _export_model_archive(
                    model_uri=model_info.model_uri,
                    output_dir=model_export_dir,
                    version=version,
                )
            client.set_registered_model_alias(MODEL_NAME, "prod", version)
            if model_export_dir is not None and archive_name is not None:
                _publish_model_manifest(
                    output_dir=model_export_dir,
                    archive_name=archive_name,
                    version=version,
                    run_id=run.info.run_id,
                    through=through,
                    result=result,
                )
            LOGGER.info("Promoted model version %s to @prod", version)
        return version


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through", type=date.fromisoformat)
    parser.add_argument(
        "--data-path",
        type=Path,
        help="Local Gold Parquet snapshot. Omit only for the Databricks monthly job.",
    )
    parser.add_argument("--output-path", type=Path, default=LOCAL_MODEL_PATH)
    parser.add_argument(
        "--model-export-dir",
        type=Path,
        help="Unity Catalog volume directory for downloadable production models.",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    through = args.through or previous_month_end(datetime.now(BERLIN).date())
    if args.data_path is not None:
        train_local(
            data_path=args.data_path,
            output_path=args.output_path,
            through=through,
        )
        return
    train_monthly(through=through, model_export_dir=args.model_export_dir)


if __name__ == "__main__":
    main()
