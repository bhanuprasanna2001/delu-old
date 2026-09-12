from __future__ import annotations

from unittest.mock import MagicMock, Mock, create_autospec

import pytest

from delu.pipeline import gold
from delu.pipeline.gold import _covered_dates, _shift_date


def test_cli_accepts_forwarded_backfill_parameters(monkeypatch) -> None:
    build = Mock()
    monkeypatch.setattr(gold, "build", build)
    monkeypatch.setattr(
        "sys.argv",
        ["delu-gold", "--start=2026-09-01", "--end=2026-09-05"],
    )

    gold.main()

    build.assert_called_once_with(through=None)


def test_reversed_holiday_range_is_invalid() -> None:
    with pytest.raises(ValueError, match="startDate cannot be after endDate"):
        _covered_dates([{"startDate": "2026-12-26", "endDate": "2026-12-25"}])


def test_forecast_source_day_maps_to_the_following_model_day(monkeypatch) -> None:
    frame = MagicMock()
    shifted = object()
    date_column = object()
    frame.withColumn.return_value = shifted
    date_add = create_autospec(gold.F.date_add, return_value=date_column)
    monkeypatch.setattr(gold.F, "date_add", date_add)

    assert _shift_date(frame, 1) is shifted

    date_add.assert_called_once_with("delivery_date", 1)
    frame.withColumn.assert_called_once_with("delivery_date", date_column)
