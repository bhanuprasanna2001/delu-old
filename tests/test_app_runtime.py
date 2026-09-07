from pathlib import Path
from runpy import run_path
from unittest.mock import patch

import pytest


@pytest.mark.parametrize(
    ("render_port", "databricks_port", "expected"),
    [
        pytest.param("10000", "8000", 10000, id="render"),
        pytest.param(None, "8123", 8123, id="databricks"),
        pytest.param(None, None, 8000, id="local"),
    ],
)
def test_runtime_binds_the_platform_port(
    monkeypatch: pytest.MonkeyPatch,
    render_port: str | None,
    databricks_port: str | None,
    expected: int,
) -> None:
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
    if render_port is not None:
        monkeypatch.setenv("PORT", render_port)
    if databricks_port is not None:
        monkeypatch.setenv("DATABRICKS_APP_PORT", databricks_port)

    with patch("uvicorn.run", autospec=True) as run:
        run_path(str(Path(__file__).parents[1] / "app" / "run.py"), run_name="__main__")

    run.assert_called_once_with("delu_app.api:app", host="0.0.0.0", port=expected)
