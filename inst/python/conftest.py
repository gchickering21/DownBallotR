"""
conftest.py — top-level pytest configuration for inst/python/.

Registers custom marks so pytest doesn't warn about unknown marks.
Run any test suite from inst/python/ with:

    pytest tests/test_all_scrapers_smoke.py -v
    pytest tests/test_all_scrapers_smoke.py -v -m "not playwright"
    pytest tests/test_all_scrapers_smoke.py -v -m playwright
"""

import sys
from pathlib import Path

# Ensure inst/python/ is on sys.path for all tests
sys.path.insert(0, str(Path(__file__).parent))


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "playwright: test requires Playwright/Chromium (playwright install chromium)",
    )
    config.addinivalue_line(
        "markers",
        "classic: test uses requests-based scraping (no browser required)",
    )
