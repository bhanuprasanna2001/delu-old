import json
from datetime import UTC, date, datetime, timedelta
from math import isfinite
from unittest import TestCase

import pytest

from delu.pipeline.bronze import WEATHER_FIELDS, WEATHER_LOCATIONS
from delu.pipeline.silver import (
    _parse_rows,
    _parse_weather_payload,
    _weather_value,
    parse_payload,
    validate_payload,
)


def payload(
    start: str,
    end: str,
    *,
    curve: str = "A03",
    points: tuple[tuple[int, float], ...] = ((1, 10.0), (3, 20.0)),
    price: bool = False,
) -> str:
    value_tag = "price.amount" if price else "quantity"
    unit_tag = "price_Measure_Unit.name" if price else "quantity_Measure_Unit.name"
    unit = "MWH" if price else "MAW"
    currency = "<currency_Unit.name>EUR</currency_Unit.name>" if price else ""
    xml_points = "".join(
        f"<Point><position>{position}</position>"
        f"<{value_tag}>{value}</{value_tag}></Point>"
        for position, value in points
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<GL_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
  <TimeSeries>
    {currency}
    <{unit_tag}>{unit}</{unit_tag}>
    <curveType>{curve}</curveType>
    <Period>
      <timeInterval><start>{start}</start><end>{end}</end></timeInterval>
      <resolution>PT15M</resolution>
      {xml_points}
    </Period>
  </TimeSeries>
</GL_MarketDocument>"""


class ParsePayloadTest(TestCase):
    def test_a03_forward_fills_every_interval(self) -> None:
        intervals = parse_payload(
            payload("2025-12-31T23:00Z", "2026-01-01T23:00Z"),
            "de_lu.load.actual",
            date(2026, 1, 1),
        )

        self.assertEqual(len(intervals), 96)
        self.assertEqual(
            [value for _, value in intervals[:4]], [10.0, 10.0, 20.0, 20.0]
        )
        self.assertTrue(all(isfinite(value) for _, value in intervals))

    def test_a01_and_price_units(self) -> None:
        points = tuple((position, float(position)) for position in range(1, 97))
        intervals = parse_payload(
            payload(
                "2025-12-31T23:00Z",
                "2026-01-01T23:00Z",
                curve="A01",
                points=points,
                price=True,
            ),
            "de_lu.price.sdac",
            date(2026, 1, 1),
        )

        self.assertEqual(len(intervals), 96)
        self.assertEqual(intervals[-1][1], 96.0)

    def test_forward_fill_respects_dst_days(self) -> None:
        spring = parse_payload(
            payload(
                "2026-03-28T23:00Z",
                "2026-03-29T22:00Z",
                points=((1, 1.0),),
            ),
            "de_lu.load.actual",
            date(2026, 3, 29),
        )
        autumn = parse_payload(
            payload(
                "2025-10-25T22:00Z",
                "2025-10-26T23:00Z",
                points=((1, 1.0),),
            ),
            "de_lu.load.actual",
            date(2025, 10, 26),
        )

        self.assertEqual(len(spring), 92)
        self.assertEqual(len(autumn), 100)
        self.assertTrue(all(value is not None for _, value in spring + autumn))

    def test_a03_fills_a_leading_dst_gap(self) -> None:
        intervals = parse_payload(
            payload(
                "2025-10-25T23:00Z",
                "2025-10-26T23:00Z",
                points=((1, 1.0),),
            ),
            "de_lu.generation.solar.forecast",
            date(2025, 10, 26),
        )

        self.assertEqual(len(intervals), 100)
        self.assertEqual(intervals[0][0].isoformat(), "2025-10-25T22:00:00+00:00")
        self.assertTrue(all(value == 1.0 for _, value in intervals))

    def test_a03_requires_a_value_for_the_first_interval(self) -> None:
        with self.assertRaisesRegex(ValueError, "A03 curve must start at position 1"):
            parse_payload(
                payload(
                    "2025-12-31T23:00Z",
                    "2026-01-01T23:00Z",
                    points=((2, 10.0),),
                ),
                "de_lu.load.actual",
                date(2026, 1, 1),
            )


@pytest.mark.parametrize(
    ("ingested_at", "expected_rows"),
    [
        (datetime(2026, 9, 4, 9, 30, tzinfo=UTC), 96),
        (datetime(2026, 9, 4, 10, 0, tzinfo=UTC), 0),
        (datetime(2026, 9, 6, 10, 0, tzinfo=UTC), 0),
    ],
    ids=["captured-before-sdac", "at-sdac", "backfilled"],
)
def test_forecast_fundamentals_require_a_point_in_time_capture(
    ingested_at: datetime,
    expected_rows: int,
) -> None:
    points = tuple((position, float(position)) for position in range(1, 97))
    raw = payload(
        "2026-09-04T22:00Z",
        "2026-09-05T22:00Z",
        curve="A01",
        points=points,
    )

    rows = _parse_rows([(date(2026, 9, 5), "de_lu.load.forecast", raw, ingested_at)])

    assert len(rows) == expected_rows


def test_non_finite_entsoe_value_is_rejected_before_storage() -> None:
    with pytest.raises(ValueError, match="non-finite value"):
        validate_payload(
            payload(
                "2025-12-31T23:00Z",
                "2026-01-01T23:00Z",
                points=((1, float("nan")),),
            ),
            "de_lu.load.actual",
            date(2026, 1, 1),
        )


def test_weather_uses_temperature_fallback_and_keeps_only_next_day() -> None:
    run_date = date(2026, 6, 23)
    run_start = datetime(2026, 6, 23, tzinfo=UTC)
    times = [
        (run_start + timedelta(hours=offset))
        .replace(tzinfo=None)
        .isoformat(timespec="minutes")
        for offset in range(48)
    ]
    units = {"time": "iso8601"}
    for field, _, unit in WEATHER_FIELDS:
        units[f"{field}_ecmwf_ifs"] = unit
        units[f"{field}_ecmwf_ifs025"] = unit
    locations = [location for location in WEATHER_LOCATIONS if location[3] == "sea"]
    payload = json.dumps(
        [
            {
                "latitude": latitude,
                "longitude": longitude,
                "timezone": "GMT",
                "utc_offset_seconds": 0,
                "hourly_units": units,
                "hourly": {
                    "time": times,
                    **{
                        f"{field}_ecmwf_ifs": [
                            None if field == "temperature_2m" else float(offset)
                            for offset in range(48)
                        ]
                        for field, _, _ in WEATHER_FIELDS
                    },
                    **{
                        f"{field}_ecmwf_ifs025": [
                            float(offset + 100) for offset in range(48)
                        ]
                        for field, _, _ in WEATHER_FIELDS
                    },
                },
            }
            for _, latitude, longitude, _ in locations
        ]
    )

    rows = _parse_weather_payload(payload, run_date, "sea")

    combined = json.loads(payload)
    primary = []
    fallback = []
    for raw in combined:
        metadata = {
            key: raw[key]
            for key in ("latitude", "longitude", "timezone", "utc_offset_seconds")
        }
        primary.append(
            {
                **metadata,
                "hourly_units": {
                    "time": "iso8601",
                    **{field: unit for field, _, unit in WEATHER_FIELDS},
                },
                "hourly": {
                    "time": times,
                    **{
                        field: raw["hourly"][f"{field}_ecmwf_ifs"]
                        for field, _, _ in WEATHER_FIELDS
                    },
                },
            }
        )
        fallback.append(
            {
                **metadata,
                "hourly_units": {"time": "iso8601", "temperature_2m": "°C"},
                "hourly": {
                    "time": times,
                    "temperature_2m": raw["hourly"]["temperature_2m_ecmwf_ifs025"],
                },
            }
        )
    separate_rows = _parse_weather_payload(
        json.dumps({"primary": primary, "temperature_fallback": fallback}),
        run_date,
        "sea",
    )

    assert len(rows) == len(locations) * len(WEATHER_FIELDS) * 24
    assert separate_rows == rows
    assert {row[0] for row in rows} == {date(2026, 6, 24)}
    temperature = next(
        row for row in rows if row[2] == "weather.north_sea_west.temperature_2m"
    )
    assert temperature[3] == 122.0


def test_weather_repairs_only_the_known_null_shapes() -> None:
    run_start = datetime(2026, 6, 23, tzinfo=UTC)

    assert _weather_value("temperature_2m", None, 17.5, run_start, run_start) == 17.5
    assert (
        _weather_value("shortwave_radiation", None, None, run_start, run_start) == 0.0
    )
    with pytest.raises(ValueError, match="unexpected null"):
        _weather_value("cloud_cover", None, None, run_start, run_start)
