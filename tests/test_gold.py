from __future__ import annotations

from unittest.mock import Mock

from delu.pipeline import gold


def test_cli_accepts_forwarded_backfill_parameters(monkeypatch) -> None:
    build = Mock()
    monkeypatch.setattr(gold, "build", build)
    monkeypatch.setattr(
        "sys.argv",
        ["delu-gold", "--start=2026-09-01", "--end=2026-09-05"],
    )

    gold.main()

    build.assert_called_once_with(through=None)
