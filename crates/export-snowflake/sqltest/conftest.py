import json
import os
import subprocess
from pathlib import Path

import pytest

CRATE = Path(__file__).resolve().parent.parent
RANDOM_COUNT = os.environ.get("EXPORT_SQLTEST_RANDOM", "24")


@pytest.fixture(scope="session")
def scenarios():
    """Synthetic records and what the reference consumer derives from them,
    from the crate's `scenarios` example."""
    out = subprocess.run(
        ["cargo", "run", "-q", "-p", "celld-export-snowflake", "--example", "scenarios", RANDOM_COUNT],
        check=True,
        capture_output=True,
        cwd=CRATE,
    ).stdout
    return {s["name"]: s for s in json.loads(out)["scenarios"]}
