"""Tests for metrics collector module."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from msk_health_check.metrics_collector import (
    collect_metrics, query_metric_with_retry, MetricData, MetricsCollection, STANDARD_METRICS
)


class TestMetricDefinitions:
    """Tests for STANDARD_METRICS."""
    
    def test_all_14_metrics_defined(self):
        """Test that required metrics are defined."""
        assert len(STANDARD_METRICS) >= 13
        
        required_metrics = [
            'ActiveControllerCount', 'OfflinePartitionsCount', 'KafkaDataLogsDiskUsed',
            'ClientConnectionCount', 'PartitionCount', 'CpuUser', 'CpuSystem',
            'UnderMinIsrPartitionCount', 'BytesInPerSec', 'BytesOutPerSec',
            'LeaderCount', 'MemoryUsed', 'MemoryFree', 'HeapMemoryAfterGC'
        ]
        
        for metric in required_metrics:
            assert metric in STANDARD_METRICS


class TestQueryMetricWithRetry:
    """Tests for query_metric_with_retry function."""
    
    def test_successful_query(self):
        """Test successful metric query."""
        mock_client = MockCloudWatchClient(success=True)
        start_time = datetime.utcnow() - timedelta(days=1)
        end_time = datetime.utcnow()
        
        result = query_metric_with_retry(
            mock_client, 'CpuUser', 
            'arn:aws:kafka:us-east-1:123456789012:cluster/test-cluster/uuid',
            start_time, end_time
        )
        
        assert result is not None
        assert isinstance(result, MetricData)
        assert result.metric_name == 'CpuUser'
        assert len(result.values) > 0
        assert 'min' in result.statistics
        assert 'max' in result.statistics
        assert 'avg' in result.statistics
        assert 'p95' in result.statistics
        assert 'p99' in result.statistics
    
    def test_no_datapoints(self):
        """Test when no data points are returned."""
        mock_client = MockCloudWatchClient(datapoints=[])
        start_time = datetime.utcnow() - timedelta(days=1)
        end_time = datetime.utcnow()
        
        result = query_metric_with_retry(
            mock_client, 'CpuUser',
            'arn:aws:kafka:us-east-1:123456789012:cluster/test-cluster/uuid',
            start_time, end_time
        )
        
        assert result is None
    
    @patch('msk_health_check.metrics_collector.time.sleep')
    def test_retry_with_exponential_backoff(self, mock_sleep):
        """Test retry logic with exponential backoff."""
        mock_client = MockCloudWatchClient(fail_count=2)
        start_time = datetime.utcnow() - timedelta(days=1)
        end_time = datetime.utcnow()
        
        result = query_metric_with_retry(
            mock_client, 'CpuUser',
            'arn:aws:kafka:us-east-1:123456789012:cluster/test-cluster/uuid',
            start_time, end_time, max_retries=3
        )
        
        # Should succeed on third attempt
        assert result is not None
        assert mock_client.call_count == 3
        
        # Verify exponential backoff: 1s, 2s
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0][0][0] == 1  # 2^0
        assert mock_sleep.call_args_list[1][0][0] == 2  # 2^1
    
    @patch('msk_health_check.metrics_collector.time.sleep')
    def test_all_retries_exhausted(self, mock_sleep):
        """Test when all retries are exhausted."""
        mock_client = MockCloudWatchClient(fail_count=5)
        start_time = datetime.utcnow() - timedelta(days=1)
        end_time = datetime.utcnow()
        
        result = query_metric_with_retry(
            mock_client, 'CpuUser',
            'arn:aws:kafka:us-east-1:123456789012:cluster/test-cluster/uuid',
            start_time, end_time, max_retries=3
        )
        
        assert result is None
        assert mock_client.call_count == 3


class TestCollectMetrics:
    """Tests for collect_metrics function."""
    
    def test_successful_collection(self):
        """Test successful collection of all metrics."""
        mock_client = MockCloudWatchClient(success=True)
        cluster_arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/test-cluster/uuid'
        
        result = collect_metrics(mock_client, cluster_arn, broker_count=3, cluster_type='PROVISIONED', days_back=7)
        
        assert isinstance(result, MetricsCollection)
        assert result.cluster_arn == cluster_arn
        assert len(result.metrics) > 0
        assert len(result.missing_metrics) == 0
        assert result.end_time > result.start_time
        assert (result.end_time - result.start_time).days == 7
    
    def test_partial_failure_graceful_degradation(self):
        """Test graceful handling of partial metric collection failures."""
        mock_client = MockCloudWatchClient(fail_metrics=['CpuUser', 'CpuSystem'])
        cluster_arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/test-cluster/uuid'
        
        result = collect_metrics(mock_client, cluster_arn, broker_count=3, cluster_type='PROVISIONED', days_back=7)
        
        assert isinstance(result, MetricsCollection)
        assert 'CpuUser' in result.missing_metrics
        assert 'CpuSystem' in result.missing_metrics
        assert len(result.metrics) == len(STANDARD_METRICS) - 2
    
    def test_time_period_documentation(self):
        """Test that time period is properly documented."""
        mock_client = MockCloudWatchClient(success=True)
        cluster_arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/test-cluster/uuid'
        
        result = collect_metrics(mock_client, cluster_arn, broker_count=3, cluster_type='PROVISIONED', days_back=30)
        
        assert result.start_time is not None
        assert result.end_time is not None
        assert isinstance(result.start_time, datetime)
        assert isinstance(result.end_time, datetime)


class MockCloudWatchClient:
    """Mock CloudWatch client for testing."""
    
    def __init__(self, success=True, datapoints=None, fail_count=0, fail_metrics=None):
        self.success = success
        self.datapoints = datapoints
        self.fail_count = fail_count
        self.fail_metrics = fail_metrics or []
        self.call_count = 0
        self._call_counts = {}  # Track per-metric call counts for thread safety
    
    def get_metric_statistics(self, **kwargs):
        """Mock get_metric_statistics call."""
        self.call_count += 1
        
        metric_name = kwargs.get('MetricName')
        
        # Track per-metric calls
        if metric_name not in self._call_counts:
            self._call_counts[metric_name] = 0
        self._call_counts[metric_name] += 1
        
        # Check if this metric should fail
        if metric_name in self.fail_metrics:
            raise ClientError(
                {'Error': {'Code': 'Throttling', 'Message': 'Rate exceeded'}},
                'GetMetricStatistics'
            )
        
        # Simulate retries based on per-metric count
        if self._call_counts[metric_name] <= self.fail_count:
            raise ClientError(
                {'Error': {'Code': 'Throttling', 'Message': 'Rate exceeded'}},
                'GetMetricStatistics'
            )
        
        if self.datapoints is not None:
            return {'Datapoints': self.datapoints, 'Label': 'Test'}
        
        if self.success:
            # Generate sample data
            start = kwargs['StartTime']
            end = kwargs['EndTime']
            period = kwargs['Period']
            stat = kwargs['Statistics'][0]
            
            num_points = min(int((end - start).total_seconds() / period), 24)
            datapoints = []
            for i in range(num_points):
                datapoints.append({
                    'Timestamp': start + timedelta(seconds=i * period),
                    stat: 50.0 + i
                })
            
            return {'Datapoints': datapoints, 'Label': 'Test'}
        
        return {'Datapoints': [], 'Label': 'Test'}
