from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.noconfig
def test_catalog_changes(pytester: pytest.Pytester, tmp_path: Path) -> None:
    """Fail if the catalog has changed."""
    # TODO: Discovery should be able to run any config
    dummy_config = {
        "repositories": [
            "meltano/meltano",
            "meltano/sdk",
        ],
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(dummy_config))
    result = pytester.run(
        "tap-github",
        "--discover",
        f"--config={config_path.as_posix()}",
    )
    assert result.ret == 0, "Tap discovery failed"

    catalog = json.loads("".join(result.outlines))
    assert "streams" in catalog
    assert isinstance(catalog["streams"], list)
    assert len(catalog["streams"]) > 0
