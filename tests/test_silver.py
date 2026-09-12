import json
from datetime import UTC, date, datetime, timedelta
from math import isfinite
from unittest import TestCase

import pytest

from delu.pipeline.bronze import (
    WEATHER_FIELDS,
    WEATHER_LOCATIONS,
    IncompletePublication,
)
from delu.pipeline.silver import (
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

    def test_a03_does_not_fill_outside_its_period(self) -> None:
        with self.assertRaisesRegex(
            IncompletePublication, "Expected 100 delivery intervals"
        ):
            parse_payload(
                payload(
                    "2025-10-25T23:00Z",
                    "2025-10-26T23:00Z",
                    points=((1, 1.0),),
                ),
                "de_lu.generation.wind_offshore.forecast",
                date(2025, 10, 26),
            )

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

    def test_structurally_empty_document_is_invalid(self) -> None:
        with self.assertRaisesRegex(ValueError, "contains no TimeSeries"):
            parse_payload("<root/>", "de_lu.load.actual", date(2026, 1, 1))

    def test_duplicate_period_interval_is_invalid(self) -> None:
        document = payload(
            "2025-12-31T23:00Z",
            "2026-01-01T23:00Z",
            points=((1, 1.0),),
        )
        start = document.index("    <Period>")
        end = document.index("    </Period>") + len("    </Period>")
        period = document[start:end]

        with self.assertRaisesRegex(ValueError, "Duplicate interval"):
            parse_payload(
                document[:end] + period + document[end:],
                "de_lu.load.actual",
                date(2026, 1, 1),
            )

    def test_identical_duplicate_time_series_is_ignored(self) -> None:
        document = payload(
            "2025-12-31T23:00Z",
            "2026-01-01T23:00Z",
            points=((1, 1.0),),
        )
        start = document.index("  <TimeSeries>")
        end = document.index("  </TimeSeries>") + len("  </TimeSeries>")
        time_series = document[start:end]

        intervals = parse_payload(
            document[:end] + time_series + document[end:],
            "de_lu.load.actual",
            date(2026, 1, 1),
        )

        self.assertEqual(len(intervals), 96)

    def test_conflicting_duplicate_time_series_is_invalid(self) -> None:
        document = payload(
            "2025-12-31T23:00Z",
            "2026-01-01T23:00Z",
            points=((1, 1.0),),
        )
        start = document.index("  <TimeSeries>")
        end = document.index("  </TimeSeries>") + len("  </TimeSeries>")
        conflicting = document[start:end].replace(
            "<quantity>1.0</quantity>", "<quantity>2.0</quantity>"
        )

        with self.assertRaisesRegex(ValueError, "Duplicate interval"):
            parse_payload(
                document[:end] + conflicting + document[end:],
                "de_lu.load.actual",
                date(2026, 1, 1),
            )

    def test_non_finite_entsoe_value_is_rejected_before_storage(self) -> None:
        with self.assertRaisesRegex(ValueError, "non-finite value"):
            validate_payload(
                payload(
                    "2025-12-31T23:00Z",
                    "2026-01-01T23:00Z",
                    points=((1, float("nan")),),
                ),
                "de_lu.load.actual",
                date(2026, 1, 1),
            )


@pytest.mark.parametrize(
    ("series", "positions"),
    [
        pytest.param(
            "de_lu.generation.solar.forecast",
            (1, *range(28, 75), 77),
            id="solar-sparse-a03",
        ),
        pytest.param(
            "de_lu.generation.wind_onshore.forecast",
            tuple(range(1, 97)),
            id="onshore-complete-a03",
        ),
    ],
)
def test_verified_autumn_forecasts_preserve_their_published_96_intervals(
    series: str,
    positions: tuple[int, ...],
) -> None:
    intervals = parse_payload(
        payload(
            "2025-10-25T23:00Z",
            "2025-10-26T23:00Z",
            points=tuple((position, float(position)) for position in positions),
        ),
        series,
        date(2025, 10, 26),
    )

    assert len(intervals) == 96
    assert intervals[0][0] == datetime(2025, 10, 25, 23, tzinfo=UTC)
    assert intervals[-1][1] == float(positions[-1])


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
    radiation = [
        row for row in rows if row[2] == "weather.north_sea_west.shortwave_radiation"
    ]
    assert len(radiation) == 24
    assert radiation[0][1] == datetime(2026, 6, 23, 22, tzinfo=UTC)
    assert radiation[0][3] == 23.0


def test_weather_repairs_only_the_known_null_shapes() -> None:
    run_start = datetime(2026, 6, 23, tzinfo=UTC)

    assert _weather_value("temperature_2m", None, 17.5, run_start, run_start) == 17.5
    assert (
        _weather_value("shortwave_radiation", None, None, run_start, run_start) == 0.0
    )
    with pytest.raises(IncompletePublication, match="not fully published"):
        _weather_value("cloud_cover", None, None, run_start, run_start)
