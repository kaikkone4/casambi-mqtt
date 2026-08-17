import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(request):
    """Enable custom integrations when running the HA-specific test environment."""
    try:
        request.getfixturevalue("enable_custom_integrations")
    except pytest.FixtureLookupError:
        pass
