"""Property-based tests for validators module."""

from hypothesis import given, strategies as st
from msk_health_check.validators import validate_region, validate_arn


# Property 1: Invalid region rejection
@given(st.text().filter(lambda x: not _is_valid_region_format(x)))
def test_property_invalid_region_rejection(invalid_region):
    """Property: Any string not matching AWS region format should be rejected."""
    result = validate_region(invalid_region)
    assert not result.is_valid
    assert result.error_message is not None


# Property 2: Invalid ARN rejection
@given(st.text().filter(lambda x: not _is_valid_arn_format(x)))
def test_property_invalid_arn_rejection(invalid_arn):
    """Property: Any string not matching MSK ARN format should be rejected."""
    result = validate_arn(invalid_arn)
    assert not result.is_valid
    assert result.error_message is not None


def _is_valid_region_format(s: str) -> bool:
    """Check if string matches valid AWS region format."""
    import re
    return bool(re.match(r'^[a-z]{2}-[a-z]+-\d{1}$', s))


def _is_valid_arn_format(s: str) -> bool:
    """Check if string matches valid MSK ARN format."""
    import re
    return bool(re.match(
        r'^arn:aws:kafka:[a-z]{2}-[a-z]+-\d{1}:\d{12}:cluster/[a-zA-Z0-9_-]+/[a-f0-9-]+$',
        s
    ))
