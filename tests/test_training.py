from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock
from zipfile import ZipFile

import numpy as np

from delu.ml.data import DailyData
from delu.ml.model import BoostingConfig, ConformalPriceForecaster
from delu.ml.train import _export_model_archive, _load_production_metrics
from delu.ml.training import fit_model, select_point_shrinkage

FAST_CONFIG = BoostingConfig(
    learning_rate=0.1,
    max_iter=5,
    max_leaf_nodes=3,
    min_samples_leaf=5,
    l2_regularization=1.0,
)


def daily_data(days: int, features: int) -> DailyData:
    rng = np.random.default_rng(7)
    values = rng.normal(size=(days, 96, features)).astype(np.float32)
    values[..., 0] += 20
    targets = (values[..., 0] + 2 * values[..., 1]).astype(np.float32)
    return DailyData(
        dates=(),
        features=values,
        targets=targets,
        baseline=values[..., 0],
    )


def test_fit_model_uses_training_statistics_and_predicts_intervals() -> None:
    data = daily_data(5, 3)

    model = fit_model(
        data,
        coverage=0.8,
        calibration_days=2,
        point_shrinkage=0.5,
        point_config=FAST_CONFIG,
        interval_config=FAST_CONFIG,
    )
    output = model.predict(data.features)

    expected_mean = data.features.reshape(-1, 3).mean(axis=0)
    np.testing.assert_allclose(model.feature_mean_, expected_mean, rtol=1e-6)
    assert model.point_shrinkage == 0.5
    assert output.shape == (5, 96, 3)
    assert np.all(output[..., 1] <= output[..., 2])


def test_select_point_shrinkage_minimizes_out_of_fold_mae() -> None:
    corrections = np.array([2.0, 2.0, -2.0, -2.0])
    residuals = np.array([1.0, 1.0, -1.0, -1.0])

    shrinkage = select_point_shrinkage(corrections, residuals)

    assert shrinkage == 0.5


def test_incompatible_production_model_is_skipped_during_migration(monkeypatch) -> None:
    client = MagicMock()
    client.get_model_version_by_alias.return_value.version = "1"
    old_model = ConformalPriceForecaster(feature_count=2, reference_feature_index=0)
    monkeypatch.setattr("delu.ml.train.mlflow.sklearn.load_model", lambda _: old_model)

    production = _load_production_metrics(client, daily_data(5, 3))

    assert production is None


def test_model_export_is_a_versioned_mlflow_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "MLmodel").write_text("model", encoding="utf-8")
    monkeypatch.setattr(
        "delu.ml.train.mlflow.artifacts.download_artifacts",
        lambda **_kwargs: str(source),
    )

    export_dir = tmp_path / "exports"
    name = _export_model_archive(
        model_uri="models:/delu.ml.sdac_cqr/1",
        output_dir=export_dir,
        version="1",
    )

    assert name == "sdac-cqr-v1.zip"
    with ZipFile(export_dir / name) as archive:
        assert archive.read("MLmodel") == b"model"
