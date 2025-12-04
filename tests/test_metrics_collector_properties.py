"""Property-based tests for metrics collector module."""

from datetime import datetime, timedelta
from hypothesis import given, strategies as st
from msk_health_check.metrics_collector import collect_metrics, STANDARD_METRICS
from tests.test_metrics_collector import MockCloudWatchClient


# Property 3: Time period documentation
@given(st.integers(min_value=1, max_value=90))
def test_property_time_period_documentation(days_back):
    """Property: Time period should always be documented in the result."""
    mock_client = MockCloudWatchClient(success=True)
    cluster_arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid'
    
    result = collect_metrics(mock_client, cluster_arn, broker_count=3, cluster_type='PROVISIONED', days_back=days_back)
    
    # Time period must be documented
    assert result.start_time is not None
    assert result.end_time is not None
    assert isinstance(result.start_time, datetime)
    assert isinstance(result.end_time, datetime)
    
    # Time range should match requested days
    actual_days = (result.end_time - result.start_time).days
    assert actual_days == days_back


# Property 4: Complete metric retrieval attempt
def test_property_complete_metric_retrieval_attempt():
    """Property: System should attempt to retrieve all 16 defined metrics."""
    mock_client = MockCloudWatchClient(success=True)
    cluster_arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid'
    
    result = collect_metrics(mock_client, cluster_arn, days_back=7)
    
    # Should attempt all metrics
    total_attempted = len(result.metrics) + len(result.missing_metrics)
    assert total_attempted == len(METRIC_DEFINITIONS)
    assert total_attempted == 16


# Property 5: Exponential backoff retry
def test_property_exponential_backoff_retry():
    """Property: Retry delays should follow exponential backoff pattern."""
    from unittest.mock import patch
    from msk_health_check.metrics_collector import query_metric_with_retry
    
    with patch('msk_health_check.metrics_collector.time.sleep') as mock_sleep:
        mock_client = MockCloudWatchClient(fail_count=2)
        start_time = datetime.utcnow() - timedelta(days=1)
        end_time = datetime.utcnow()
        
        query_metric_with_retry(
            mock_client, 'CpuUser',
            'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid',
            start_time, end_time, max_retries=3
        )
        
        # Verify exponential backoff pattern: 2^0, 2^1, 2^2, ...
        if mock_sleep.call_count > 0:
            for i, call in enumerate(mock_sleep.call_args_list):
                expected_delay = 2 ** i
                actual_delay = call[0][0]
                assert actual_delay == expected_delay


# Property 6: Graceful degradation on retry exhaustion
def test_property_graceful_degradation():
    """Property: System should continue with available metrics when some fail."""
    # Test with a few specific combinations instead of hypothesis
    test_cases = [
        ['CpuUser'],
        ['CpuUser', 'CpuSystem'],
        ['ActiveControllerCount', 'OfflinePartitionsCount', 'CpuUser'],
    ]
    
    for failing_metrics in test_cases:
        mock_client = MockCloudWatchClient(fail_metrics=failing_metrics)
        cluster_arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid'
        
        result = collect_metrics(mock_client, cluster_arn, days_back=7)
        
        # Should not crash, should return a result
        assert result is not None
        
        # Failed metrics should be in missing_metrics
        for metric in failing_metrics:
            assert metric in result.missing_metrics
        
        # Should have collected remaining metrics
        expected_successful = len(METRIC_DEFINITIONS) - len(failing_metrics)
        assert len(result.metrics) == expected_successful
