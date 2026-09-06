from datetime import date
from math import isfinite
from unittest import TestCase

from delu.pipeline.silver import parse_payload


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
