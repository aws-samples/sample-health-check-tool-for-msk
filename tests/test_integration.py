"""Integration tests for end-to-end workflow."""

import os
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from msk_health_check.cluster_info import ClusterInfo
from msk_health_check.metrics_collector import MetricData, MetricsCollection
from msk_health_check.analyzer import analyze_metrics
from msk_health_check.recommendations import generate_recommendations
from msk_health_check.visualizations import create_charts
from msk_health_check.pdf_builder import ReportContent, generate_output_filename


def test_end_to_end_workflow_without_pdf():
    """Test complete workflow from analysis to recommendations."""
    # Create test data
    cluster_info = ClusterInfo(
        arn='arn:aws:kafka:us-east-1:123456789012:cluster/test-cluster/abc123',
        name='test-cluster',
        cluster_type='PROVISIONED',
        instance_type='kafka.m5.large',
        instance_family='intel',
        broker_count=3,
        availability_zones=3,
        authentication_methods=['IAM'],
        encryption_in_transit=True,
        encryption_at_rest=True,
        kafka_version='2.8.1',
        storage_auto_scaling_enabled=True,
        logging_enabled=True,
        logging_destinations=['CloudWatch'],
        available_kafka_versions=['3.8.0', '3.7.0', '2.8.1'],
        intelligent_rebalancing_enabled=False
    )
    
    # Create test metrics
    now = datetime.utcnow()
    test_metric = MetricData(
        metric_name='CpuUser',
        broker_id='1',
        timestamps=[now - timedelta(hours=i) for i in range(10)],
        values=[50.0] * 10,
        unit='Percent',
        statistics={'min': 50.0, 'max': 50.0, 'avg': 50.0, 'p95': 50.0, 'p99': 50.0}
    )
    
    metrics = MetricsCollection(
        cluster_arn=cluster_info.arn,
        start_time=now - timedelta(days=7),
        end_time=now,
        metrics={'CpuUser': [test_metric]},
        missing_metrics=[]
    )
    
    # Run analysis
    analysis = analyze_metrics(cluster_info, metrics)
    assert analysis is not None
    assert analysis.overall_health_score >= 0
    assert len(analysis.findings) > 0
    
    # Generate recommendations
    recommendations = generate_recommendations(analysis)
    assert isinstance(recommendations, list)
    
    # Mock CloudWatch client for charts
    mock_cw_client = MagicMock()
    mock_cw_client.get_metric_widget_image.return_value = {
        'MetricWidgetImage': b'fake_image_data'
    }
    
    # Create charts
    charts = create_charts(mock_cw_client, cluster_info, metrics)
    assert isinstance(charts, list)
    
    # Verify report content can be created
    report_content = ReportContent(
        cluster_info=cluster_info,
        analysis=analysis,
        recommendations=recommendations,
        charts=charts,
        generation_time=datetime.utcnow()
    )
    
    assert report_content.cluster_info == cluster_info
    assert report_content.analysis == analysis


def test_generate_output_filename():
    """Test output filename generation."""
    arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc123'
    timestamp = datetime(2025, 11, 25, 15, 30, 0)
    
    filename = generate_output_filename(arn, timestamp)
    
    assert 'my-cluster' in filename
    assert '20251125' in filename
    assert filename.endswith('.pdf')
