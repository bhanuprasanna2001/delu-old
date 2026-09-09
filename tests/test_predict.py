from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from delu.ml.predict import forecast_day


def test_production_forecast_rejects_a_historical_delivery_date() -> None:
    with pytest.raises(ValueError, match="only allowed for tomorrow"):
        forecast_day(
            date(2026, 9, 9),
            now=datetime(2026, 9, 9, 9, 30, tzinfo=UTC),
        )


def test_production_forecast_rejects_tomorrow_after_publication_cutoff() -> None:
    with pytest.raises(ValueError, match="cutoff has passed"):
        forecast_day(
            date(2026, 9, 10),
            now=datetime(2026, 9, 9, 13, 0, tzinfo=UTC),
        )
