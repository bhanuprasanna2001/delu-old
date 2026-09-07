from __future__ import annotations

from datetime import date

import pytest

from delu.ml.predict import _dates_to_forecast


def test_dates_to_forecast_prioritizes_latest_and_refills_gaps() -> None:
    complete = {
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 4),
        date(2026, 9, 5),
        date(2026, 9, 7),
    }

    missing = _dates_to_forecast(
        date(2026, 9, 1),
        date(2026, 9, 8),
        complete,
    )

    assert missing == (date(2026, 9, 8), date(2026, 9, 3), date(2026, 9, 6))


def test_dates_to_forecast_rejects_reversed_window() -> None:
    with pytest.raises(ValueError, match="start cannot be after end"):
        _dates_to_forecast(date(2026, 9, 2), date(2026, 9, 1), set())
