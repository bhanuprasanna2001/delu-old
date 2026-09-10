from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from delu.contracts import FEATURE_DATA_VERSION
from delu.ml.data import (
    BOOLEAN_FEATURES,
    FEATURE_NAMES,
    NUMERIC_FEATURES,
    TARGET_COLUMN,
    prepare_daily_data,
    temporal_split,
    walk_forward_split,
)


def gold_frame(days: int = 8, *, include_future: bool = False) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    start = date(2026, 1, 1)
    total_days = days + int(include_future)
    for day_index in range(total_days):
        delivery_date = start + timedelta(days=day_index)
        for quarter in range(96):
            row: dict[str, object] = {
                "delivery_date": delivery_date,
                "quarter_of_day": quarter,
                "feature_data_version": FEATURE_DATA_VERSION,
                "day_of_week": delivery_date.weekday(),
                "month": delivery_date.month,
                TARGET_COLUMN: float(day_index + quarter),
            }
            row.update(
                {
                    column: float(index + day_index + quarter)
                    for index, column in enumerate(NUMERIC_FEATURES)
                }
            )
            row.update({column: False for column in BOOLEAN_FEATURES})
            if include_future and day_index == total_days - 1:
                row[TARGET_COLUMN] = np.nan
            rows.append(row)
    return pd.DataFrame(rows)


def test_prepare_daily_data_builds_ordered_daily_tensors() -> None:
    data = prepare_daily_data(
        gold_frame(3).sample(frac=1, random_state=7), require_targets=True
    )

    assert data.features.shape == (3, 96, len(FEATURE_NAMES))
    assert data.targets is not None and data.targets.shape == (3, 96)
    assert data.baseline.shape == (3, 96)
    assert data.dates == (date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3))
    assert np.isfinite(data.features).all()


def test_training_ignores_a_wholly_unlabelled_future_day() -> None:
    data = prepare_daily_data(gold_frame(2, include_future=True), require_targets=True)

    assert len(data) == 2
    assert data.dates[-1] == date(2026, 1, 2)


def test_prepare_daily_data_rejects_partial_targets_and_bad_day_shapes() -> None:
    partial = gold_frame(2)
    partial.loc[0, TARGET_COLUMN] = np.nan
    with pytest.raises(ValueError, match="partially observed"):
        prepare_daily_data(partial, require_targets=True)

    incomplete = gold_frame(2).iloc[:-1]
    with pytest.raises(ValueError, match="96 rows"):
        prepare_daily_data(incomplete, require_targets=True)


def test_prepare_daily_data_rejects_an_old_feature_contract() -> None:
    frame = gold_frame(1)
    frame["feature_data_version"] = FEATURE_DATA_VERSION - 1

    with pytest.raises(ValueError, match="feature data version is incompatible"):
        prepare_daily_data(frame, require_targets=True)


def test_temporal_split_never_shuffles_days() -> None:
    data = prepare_daily_data(gold_frame(8), require_targets=True)
    split = temporal_split(data, validation_days=2, test_days=2, minimum_train_days=4)

    assert split.train.dates == data.dates[:4]
    assert split.validation.dates == data.dates[4:6]
    assert split.test.dates == data.dates[6:]


def test_walk_forward_split_expands_training_before_untouched_test() -> None:
    data = prepare_daily_data(gold_frame(12), require_targets=True)
    split = walk_forward_split(
        data,
        folds=3,
        validation_days=2,
        test_days=2,
        minimum_train_days=4,
    )

    assert [fold.train.dates for fold in split.folds] == [
        data.dates[:4],
        data.dates[:6],
        data.dates[:8],
    ]
    assert [fold.validation.dates for fold in split.folds] == [
        data.dates[4:6],
        data.dates[6:8],
        data.dates[8:10],
    ]
    assert split.test.dates == data.dates[10:]


def test_forecasting_does_not_wait_for_partially_published_targets() -> None:
    frame = gold_frame(1)
    frame.loc[0, TARGET_COLUMN] = np.nan
    daily = prepare_daily_data(frame, require_targets=False)
    assert len(daily) == 1
    assert daily.targets is None
    assert np.isfinite(daily.features).all()
