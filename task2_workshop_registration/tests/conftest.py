"""
tests/conftest.py
-----------------
Shared pytest configuration and fixtures.

asyncio_mode = "auto"
    Every async test function / fixture is automatically treated as an
    asyncio coroutine without needing @pytest.mark.asyncio on each one.

The session-scoped compose_up_down fixture is defined in test_resilience.py
because it is only used by that module.  If other test modules need it,
move it here and add it to __all__.
"""

import pytest


def pytest_configure(config):
    """Register custom markers so pytest doesn't warn about unknown marks."""
    config.addinivalue_line(
        "markers",
        "asyncio: mark test as an asyncio coroutine",
    )
