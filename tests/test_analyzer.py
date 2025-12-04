"""Tests for analyzer module."""

import pytest
from datetime import datetime, timedelta

from msk_health_check.analyzer import (
    analyze_metrics, Severity, Category, Finding, AnalysisResult, _calculate_health_score
)
from msk_health_check.cluster_info import ClusterInfo
from msk_health_check.metrics_collector import MetricData, MetricsCollection


class TestHealthScoreCalculation:
    """Tests for health score calculation."""
    
    def test_perfect_score_no_findings(self):
        """Test perfect score with no findings."""
        score = _calculate_health_score([])
        assert score == 100.0
    
    def test_score_with_critical_finding(self):
        """Test score deduction for critical finding in reliability (35% weight)."""
        findings = [
            Finding(
                metric_name='test',
                severity=Severity.CRITICAL,
                category=Category.RELIABILITY,
                title='Test',
                description='Test',
                current_value=1.0,
                threshold_value=0.0,
                evidence={}
            )
        ]
        score = _calculate_health_score(findings)
        # Reliability: 100 * 0.6 = 60, weighted: 60 * 0.35 = 21
        # Other categories: 100 * (0.30 + 0.20 + 0.15) = 65
        # Total: 21 + 65 = 86
        assert score == 86.0
    
    def test_score_with_warning_finding(self):
        """Test score deduction for warning finding in performance (30% weight)."""
        findings = [
            Finding(
                metric_name='test',
                severity=Severity.WARNING,
                category=Category.PERFORMANCE,
                title='Test',
                description='Test',
                current_value=1.0,
                threshold_value=0.0,
                evidence={}
            )
        ]
        score = _calculate_health_score(findings)
        # Performance: 100 * 0.85 = 85, weighted: 85 * 0.30 = 25.5
        # Other categories: 100 * (0.35 + 0.20 + 0.15) = 70
        # Total: 25.5 + 70 = 95.5
        assert score == 95.5
    
    def test_score_with_multiple_findings(self):
        """Test score with multiple findings across categories."""
        findings = [
            Finding('test1', Severity.CRITICAL, Category.RELIABILITY, 'T1', 'D1', 1.0, 0.0, {}),
            Finding('test2', Severity.WARNING, Category.PERFORMANCE, 'T2', 'D2', 1.0, 0.0, {}),
            Finding('test3', Severity.INFORMATIONAL, Category.COST, 'T3', 'D3', 1.0, 0.0, {}),
        ]
        score = _calculate_health_score(findings)
        # Reliability: 100 * 0.6 = 60, weighted: 60 * 0.35 = 21
        # Performance: 100 * 0.85 = 85, weighted: 85 * 0.30 = 25.5
        # Security: 100 * 1.0 = 100, weighted: 100 * 0.20 = 20
        # Cost: 100 * 0.95 = 95, weighted: 95 * 0.15 = 14.25
        # Total: 21 + 25.5 + 20 + 14.25 = 80.75
        assert score == 80.8  # Rounded
    
    def test_score_minimum_zero(self):
        """Test score doesn't go below zero with many critical findings."""
        findings = [Finding('test', Severity.CRITICAL, Category.RELIABILITY, 'T', 'D', 1.0, 0.0, {})] * 10
        score = _calculate_health_score(findings)
        # With multiplicative deductions (0.6^10 = 0.006), reliability category approaches 0
        # Reliability: ~0.6, weighted: ~0.2
        # Other categories: 65 (unchanged)
        # Total: ~65.2
        assert score >= 0.0
        assert score < 70.0  # Should be low due to reliability issues


class TestAnalyzeMetrics:
    """Tests for analyze_metrics orchestrator."""
    
    def test_analyze_with_empty_metrics(self):
        """Test analysis with no metrics."""
        cluster_info = create_test_cluster_info()
        metrics = MetricsCollection(
            cluster_arn='arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid',
            start_time=datetime.utcnow() - timedelta(days=7),
            end_time=datetime.utcnow(),
            metrics={},
            missing_metrics=[]
        )
        
        result = analyze_metrics(cluster_info, metrics)
        
        assert isinstance(result, AnalysisResult)
        assert result.cluster_info == cluster_info
        assert result.metrics == metrics
        assert isinstance(result.findings, list)
        assert 0 <= result.overall_health_score <= 100
    
    def test_analyze_returns_analysis_result(self):
        """Test that analyze_metrics returns proper AnalysisResult."""
        cluster_info = create_test_cluster_info()
        metrics = create_test_metrics()
        
        result = analyze_metrics(cluster_info, metrics)
        
        assert isinstance(result, AnalysisResult)
        assert result.cluster_info == cluster_info
        assert result.metrics == metrics
        assert isinstance(result.findings, list)
        assert len(result.findings) > 0
        assert 0 <= result.overall_health_score <= 100
    
    def test_analyze_with_standard_cluster(self):
        """Test analysis with standard cluster type."""
        cluster_info = create_test_cluster_info()
        cluster_info.cluster_type = 'PROVISIONED'
        metrics = create_test_metrics()
        
        result = analyze_metrics(cluster_info, metrics)
        
        assert isinstance(result, AnalysisResult)
        assert len(result.findings) > 0
    
    def test_analyze_with_express_cluster(self):
        """Test analysis with express cluster type."""
        cluster_info = create_test_cluster_info()
        cluster_info.cluster_type = 'EXPRESS'
        metrics = create_test_metrics()
        
        result = analyze_metrics(cluster_info, metrics)
        
        assert isinstance(result, AnalysisResult)
        assert len(result.findings) > 0


def create_test_cluster_info():
    """Create test cluster info."""
    from datetime import datetime, timezone, timedelta
    return ClusterInfo(
        arn='arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid',
        name='test-cluster',
        instance_type='kafka.m5.large',
        instance_family='intel',
        broker_count=3,
        authentication_methods=['IAM'],
        encryption_in_transit=True,
        encryption_at_rest=True,
        kafka_version='2.8.1',
        cluster_type='PROVISIONED',
        availability_zones=3,
        encryption_in_transit_type='TLS',
        storage_auto_scaling_enabled=True,
        logging_enabled=False,
        logging_destinations=[],
        available_kafka_versions=['2.8.1', '3.8.x'],
        intelligent_rebalancing_enabled=False,
        ebs_volume_size=100,
        enhanced_monitoring_level='DEFAULT',
        cluster_state='ACTIVE',
        creation_time=datetime.now(timezone.utc) - timedelta(days=60)
    )


def create_test_metrics():
    """Create test metrics collection."""
    now = datetime.utcnow()
    
    # Create sample metric data
    metric_data = MetricData(
        metric_name='ActiveControllerCount',
        broker_id=None,
        values=[1.0] * 100,
        timestamps=[now - timedelta(hours=i) for i in range(100)],
        statistics={'avg': 1.0, 'min': 1.0, 'max': 1.0, 'p95': 1.0},
        unit='Count'
    )
    
    return MetricsCollection(
        cluster_arn='arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid',
        start_time=now - timedelta(days=7),
        end_time=now,
        metrics={'ActiveControllerCount': [metric_data]},
        missing_metrics=[]
    )
