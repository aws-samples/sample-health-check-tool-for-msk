"""Basic test to verify project setup."""

import msk_health_check


def test_package_version():
    """Verify package version is defined."""
    assert hasattr(msk_health_check, '__version__')
    assert msk_health_check.__version__ == "0.1.0"
