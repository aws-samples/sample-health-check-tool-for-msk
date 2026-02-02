"""Metrics analysis module for MSK Health Check Report."""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import numpy as np

from .cluster_info import ClusterInfo
from .metrics_collector import MetricData, MetricsCollection

logger = logging.getLogger(__name__)


def get_cluster_metric(metrics_list: List[MetricData]) -> Optional[MetricData]:
    """Get cluster-level metric (broker_id is None)."""
    for metric in metrics_list:
        if metric.broker_id is None:
            return metric
    return None


def get_broker_metrics(metrics_list: List[MetricData]) -> List[MetricData]:
    """Get all per-broker metrics."""
    return [m for m in metrics_list if m.broker_id is not None]


class Severity(Enum):
    """Severity levels for findings."""
    CRITICAL = "critical"
    WARNING = "warning"
    INFORMATIONAL = "informational"
    HEALTHY = "healthy"


class Category(Enum):
    """Optimization categories for findings."""
    RELIABILITY = "reliability"
    SECURITY = "security"
    PERFORMANCE = "performance"
    COST = "cost"


@dataclass
class Finding:
    """Individual analysis result."""
    metric_name: str
    severity: Severity
    category: Category
    title: str
    description: str
    current_value: Optional[float]
    threshold_value: Optional[float]
    evidence: Dict[str, Any]  # Supporting data for the finding


@dataclass
class AnalysisResult:
    """Complete analysis output."""
    cluster_info: ClusterInfo
    metrics: MetricsCollection
    findings: List[Finding]
    overall_health_score: float  # 0-100


def analyze_metrics(
    cluster_info: ClusterInfo,
    metrics: MetricsCollection
) -> AnalysisResult:
    """
    Analyze all metrics against best practices.
    Metrics are collected per-broker to detect imbalances.
    
    Args:
        cluster_info: Cluster configuration
        metrics: Collected metrics (per-broker and cluster-level)
        
    Returns:
        AnalysisResult with findings and health score
    """
    findings = []
    
    # Analyze cluster-level metrics
    if 'ActiveControllerCount' in metrics.metrics:
        metric = get_cluster_metric(metrics.metrics['ActiveControllerCount'])
        if metric:
            findings.extend(analyze_active_controller_count(metric))
    
    if 'GlobalPartitionCount' in metrics.metrics:
        metric = get_cluster_metric(metrics.metrics['GlobalPartitionCount'])
        if metric:
            findings.extend(analyze_partition_count(metric, cluster_info))
    
    if 'GlobalTopicCount' in metrics.metrics:
        metric = get_cluster_metric(metrics.metrics['GlobalTopicCount'])
        if metric:
            findings.extend(analyze_topic_count(metric, cluster_info))
    
    if 'OfflinePartitionsCount' in metrics.metrics:
        metric = get_cluster_metric(metrics.metrics['OfflinePartitionsCount'])
        if metric:
            findings.extend(analyze_offline_partitions(metric))
    
    if 'ClientConnectionCount' in metrics.metrics:
        # Can be cluster or per-broker
        cluster_metric = get_cluster_metric(metrics.metrics['ClientConnectionCount'])
        broker_metrics_list = get_broker_metrics(metrics.metrics['ClientConnectionCount'])
        if cluster_metric:
            findings.extend(analyze_connection_count(cluster_metric, cluster_info))
        elif broker_metrics_list:
            findings.extend(analyze_per_broker_metrics(broker_metrics_list, 'ClientConnectionCount', cluster_info))
    
    # Analyze per-broker metrics
    for metric_name in ['CpuUser', 'CpuSystem', 'MemoryUsed', 'MemoryFree', 'HeapMemoryAfterGC',
                        'KafkaDataLogsDiskUsed', 'LeaderCount', 'PartitionCount', 
                        'UnderMinIsrPartitionCount', 'BytesInPerSec', 'BytesOutPerSec', 'MessagesInPerSec',
                        'ConnectionCount', 'ConnectionCreationRate']:
        if metric_name in metrics.metrics:
            broker_metrics_list = get_broker_metrics(metrics.metrics[metric_name])
            if broker_metrics_list:
                findings.extend(analyze_per_broker_metrics(broker_metrics_list, metric_name, cluster_info))
    
    # Analyze CPU total (User + System) - must be < 60%
    cpu_user_list = metrics.metrics.get('CpuUser', [])
    cpu_system_list = metrics.metrics.get('CpuSystem', [])
    if cpu_user_list and cpu_system_list:
        findings.extend(analyze_cpu_total(cpu_user_list, cpu_system_list))
    
    # Analyze throughput against network limits
    bytes_in_list = metrics.metrics.get('BytesInPerSec', [])
    bytes_out_list = metrics.metrics.get('BytesOutPerSec', [])
    bytes_in_metric = bytes_in_list[0] if bytes_in_list else None
    bytes_out_metric = bytes_out_list[0] if bytes_out_list else None
    if bytes_in_metric or bytes_out_metric:
        findings.extend(analyze_throughput(bytes_in_metric, bytes_out_metric, cluster_info))
    
    # Analyze cluster configuration
    findings.extend(analyze_authentication_methods(cluster_info))
    findings.extend(analyze_instance_type(cluster_info, metrics))
    findings.extend(analyze_kafka_version(cluster_info))
    findings.extend(analyze_availability_zones(cluster_info))
    findings.extend(analyze_storage_auto_scaling(cluster_info))
    findings.extend(analyze_logging_configuration(cluster_info))
    # findings.extend(analyze_intelligent_rebalancing(cluster_info))  # Disabled: API doesn't return Rebalancing field
    
    # Calculate overall health score
    health_score = _calculate_health_score(findings)
    
    logger.info(f"Analysis complete: {len(findings)} findings, health score: {health_score}")
    
    return AnalysisResult(
        cluster_info=cluster_info,
        metrics=metrics,
        findings=findings,
        overall_health_score=health_score
    )


def _calculate_health_score(findings: List[Finding]) -> float:
    """
    Calculate overall health score from findings using category-based approach.
    
    Each category starts at 100 and is reduced based on severity of issues:
    - Reliability: 35% weight (critical for availability)
    - Performance: 30% weight (impacts user experience)
    - Security: 20% weight (compliance and protection)
    - Cost Optimization: 15% weight (efficiency)
    
    Args:
        findings: List of findings
        
    Returns:
        Health score from 0-100
    """
    if not findings:
        return 100.0
    
    # Group findings by category
    category_findings = {
        Category.RELIABILITY: [],
        Category.PERFORMANCE: [],
        Category.SECURITY: [],
        Category.COST: []
    }
    
    for finding in findings:
        if finding.category in category_findings:
            category_findings[finding.category].append(finding)
    
    # Category weights (must sum to 1.0)
    category_weights = {
        Category.RELIABILITY: 0.35,
        Category.PERFORMANCE: 0.30,
        Category.SECURITY: 0.20,
        Category.COST: 0.15
    }
    
    # Calculate score for each category
    category_scores = {}
    for category, weight in category_weights.items():
        category_scores[category] = _calculate_category_score(category_findings[category])
    
    # Weighted average
    total_score = sum(category_scores[cat] * weight for cat, weight in category_weights.items())
    
    return round(total_score, 1)


def _calculate_category_score(findings: List[Finding]) -> float:
    """
    Calculate score for a specific category.
    
    Uses percentage-based deduction to prevent negative scores:
    - CRITICAL: -40% of current score
    - WARNING: -15% of current score
    - INFORMATIONAL: -5% of current score
    - HEALTHY: +0%
    
    Args:
        findings: List of findings for this category
        
    Returns:
        Category score from 0-100
    """
    if not findings:
        return 100.0
    
    score = 100.0
    
    # Count by severity
    critical_count = sum(1 for f in findings if f.severity == Severity.CRITICAL)
    warning_count = sum(1 for f in findings if f.severity == Severity.WARNING)
    info_count = sum(1 for f in findings if f.severity == Severity.INFORMATIONAL)
    
    # Apply percentage-based deductions (multiplicative to prevent negative)
    for _ in range(critical_count):
        score *= 0.60  # -40% per critical issue
    
    for _ in range(warning_count):
        score *= 0.85  # -15% per warning
    
    for _ in range(info_count):
        score *= 0.95  # -5% per informational
    
    return max(0.0, score)


def analyze_active_controller_count(metric: MetricData) -> List[Finding]:
    """
    Should be exactly 1. Only alert if minimum value in the period was < 1.
    This indicates the cluster lost its controller at some point.
    """
    findings = []
    min_val = metric.statistics['min']
    max_val = metric.statistics['max']
    
    if min_val < 1.0:
        findings.append(Finding(
            metric_name='ActiveControllerCount',
            severity=Severity.CRITICAL,
            category=Category.RELIABILITY,
            title='Active Controller Count Dropped Below 1',
            description=f'Active controller count dropped to {min_val:.0f} during the monitoring period. This indicates a cluster stability issue.',
            current_value=min_val,
            threshold_value=1.0,
            evidence={'statistics': metric.statistics}
        ))
    elif max_val > 1.0:
        findings.append(Finding(
            metric_name='ActiveControllerCount',
            severity=Severity.WARNING,
            category=Category.RELIABILITY,
            title='Multiple Active Controllers Detected',
            description=f'Active controller count reached {max_val:.0f}. Expected exactly 1.',
            current_value=max_val,
            threshold_value=1.0,
            evidence={'statistics': metric.statistics}
        ))
    else:
        findings.append(Finding(
            metric_name='ActiveControllerCount',
            severity=Severity.HEALTHY,
            category=Category.RELIABILITY,
            title='Active Controller Count Normal',
            description='Active controller count is healthy at 1 throughout the monitoring period.',
            current_value=max_val,
            threshold_value=1.0,
            evidence={'statistics': metric.statistics}
        ))
    
    return findings


def analyze_topic_count(metric: MetricData, cluster_info: ClusterInfo) -> List[Finding]:
    """Analyze if topic count is appropriate for cluster."""
    findings = []
    
    current_topics = int(metric.statistics['avg'])
    
    # Topic limits are generally not a hard constraint, but good practice
    # Recommend keeping under 1000 topics for operational simplicity
    if current_topics > 1000:
        findings.append(Finding(
            metric_name='GlobalTopicCount',
            severity=Severity.WARNING,
            category=Category.PERFORMANCE,
            title='High Topic Count',
            description=f'Cluster has {current_topics} topics. High topic counts can increase operational complexity.',
            current_value=float(current_topics),
            threshold_value=1000.0,
            evidence={'current_topics': current_topics}
        ))
    else:
        findings.append(Finding(
            metric_name='GlobalTopicCount',
            severity=Severity.HEALTHY,
            category=Category.PERFORMANCE,
            title='Topic Count OK',
            description=f'Cluster has {current_topics} topics, within recommended range.',
            current_value=float(current_topics),
            threshold_value=1000.0,
            evidence={'current_topics': current_topics}
        ))
    
    return findings


def analyze_offline_partitions(metric: MetricData) -> List[Finding]:
    """Should be 0."""
    findings = []
    max_val = metric.statistics['max']
    
    if max_val > 0:
        findings.append(Finding(
            metric_name='OfflinePartitionsCount',
            severity=Severity.CRITICAL,
            category=Category.RELIABILITY,
            title='Offline Partitions Detected',
            description=f'Detected up to {int(max_val)} offline partitions. This indicates data unavailability.',
            current_value=max_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics}
        ))
    else:
        findings.append(Finding(
            metric_name='OfflinePartitionsCount',
            severity=Severity.HEALTHY,
            category=Category.RELIABILITY,
            title='No Offline Partitions',
            description='All partitions are online and available.',
            current_value=max_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics}
        ))
    
    return findings


def analyze_disk_usage(metric: MetricData, cluster_info: ClusterInfo) -> List[Finding]:
    """Analyze disk usage (STANDARD only - Express has serverless storage)."""
    findings = []
    
    # Skip for EXPRESS clusters
    if cluster_info.cluster_type == 'EXPRESS':
        return findings
    
    max_val = metric.statistics['max']
    avg_val = metric.statistics['avg']
    
    # Calculate storage growth rate if we have time series data
    growth_rate_per_day = None
    days_until_full = None
    
    if len(metric.values) > 1 and len(metric.timestamps) > 1:
        # Calculate linear growth rate
        first_val = metric.values[0]
        last_val = metric.values[-1]
        time_diff_days = (metric.timestamps[-1] - metric.timestamps[0]).total_seconds() / 86400
        
        if time_diff_days > 0 and last_val > first_val:
            growth_rate_per_day = (last_val - first_val) / time_diff_days
            
            # Project days until 80% full
            if growth_rate_per_day > 0 and last_val < 80:
                days_until_full = (80 - last_val) / growth_rate_per_day
    
    if max_val >= 80.0:
        findings.append(Finding(
            metric_name='KafkaDataLogsDiskUsed',
            severity=Severity.CRITICAL,
            category=Category.RELIABILITY,
            title='High Disk Usage',
            description=f'Disk usage reached {max_val:.1f}%, exceeding 80% threshold. Risk of broker failures.',
            current_value=max_val,
            threshold_value=80.0,
            evidence={'statistics': metric.statistics, 'growth_rate_per_day': growth_rate_per_day}
        ))
    elif max_val >= 70.0:
        desc = f'Disk usage at {max_val:.1f}%. Consider increasing storage capacity.'
        if days_until_full and days_until_full < 30:
            desc += f' Projected to reach 80% in ~{int(days_until_full)} days at current growth rate.'
        
        findings.append(Finding(
            metric_name='KafkaDataLogsDiskUsed',
            severity=Severity.WARNING,
            category=Category.RELIABILITY,
            title='Elevated Disk Usage',
            description=desc,
            current_value=max_val,
            threshold_value=80.0,
            evidence={'statistics': metric.statistics, 'growth_rate_per_day': growth_rate_per_day, 'days_until_full': days_until_full}
        ))
    else:
        desc = f'Disk usage at {max_val:.1f}%, well below 80% threshold.'
        if days_until_full and days_until_full < 90:
            desc += f' Projected to reach 80% in ~{int(days_until_full)} days at current growth rate.'
        
        findings.append(Finding(
            metric_name='KafkaDataLogsDiskUsed',
            severity=Severity.HEALTHY,
            category=Category.RELIABILITY,
            title='Disk Usage Normal',
            description=desc,
            current_value=max_val,
            threshold_value=80.0,
            evidence={'statistics': metric.statistics, 'growth_rate_per_day': growth_rate_per_day, 'days_until_full': days_until_full}
        ))
    
    return findings


def analyze_leader_count(metric: MetricData, cluster_info: ClusterInfo) -> List[Finding]:
    """Check if leaders are balanced across brokers."""
    findings = []
    avg_val = metric.statistics['avg']
    
    findings.append(Finding(
        metric_name='LeaderCount',
        severity=Severity.INFORMATIONAL,
        category=Category.PERFORMANCE,
        title='Leader Distribution',
        description=f'Average leader count per broker: {avg_val:.1f}. Leaders should be balanced across brokers for optimal performance.',
        current_value=avg_val,
        threshold_value=None,
        evidence={'statistics': metric.statistics}
    ))
    
    return findings


def analyze_broker_partition_count(metric: MetricData, cluster_info: ClusterInfo) -> List[Finding]:
    """Analyze partition count per broker."""
    findings = []
    avg_val = metric.statistics['avg']
    max_val = metric.statistics['max']
    
    findings.append(Finding(
        metric_name='PartitionCount',
        severity=Severity.INFORMATIONAL,
        category=Category.PERFORMANCE,
        title='Partition Count Per Broker',
        description=f'Average partitions per broker: {avg_val:.1f}, Max: {max_val:.1f}. Includes replicas.',
        current_value=avg_val,
        threshold_value=None,
        evidence={'statistics': metric.statistics}
    ))
    
    return findings


def analyze_cpu_total(cpu_user_list: List[MetricData], cpu_system_list: List[MetricData]) -> List[Finding]:
    """
    Analyze total CPU usage (User + System) per broker.
    Best practice: Total CPU should stay below 60%.
    - CRITICAL if P95 >= 60% (sustained high usage)
    - Ignores imbalance if all brokers have low CPU usage (<30%)
    """
    findings = []
    
    # Calculate total CPU for all brokers
    broker_cpu_data = []
    for cpu_user in cpu_user_list:
        broker_id = cpu_user.broker_id
        cpu_system = next((m for m in cpu_system_list if m.broker_id == broker_id), None)
        
        if not cpu_system:
            continue
        
        # Calculate total CPU for this broker
        avg_total = cpu_user.statistics['avg'] + cpu_system.statistics['avg']
        max_total = cpu_user.statistics['max'] + cpu_system.statistics['max']
        p95_total = cpu_user.statistics['p95'] + cpu_system.statistics['p95']
        
        broker_cpu_data.append({
            'broker_id': broker_id,
            'avg_total': avg_total,
            'max_total': max_total,
            'p95_total': p95_total,
            'cpu_user_avg': cpu_user.statistics['avg'],
            'cpu_system_avg': cpu_system.statistics['avg']
        })
    
    # Check for sustained high CPU usage (P95 >= 60%)
    high_cpu_brokers = [b for b in broker_cpu_data if b['p95_total'] >= 60.0]
    
    for broker in high_cpu_brokers:
        findings.append(Finding(
            metric_name='CpuTotal',
            severity=Severity.CRITICAL,
            category=Category.PERFORMANCE,
            title=f'High CPU Usage - Broker {broker["broker_id"]}',
            description=f'Total CPU (User+System) P95 at {broker["p95_total"]:.1f}%, exceeding 60% threshold. Avg: {broker["avg_total"]:.1f}%, Max: {broker["max_total"]:.1f}%',
            current_value=broker['p95_total'],
            threshold_value=60.0,
            evidence=broker
        ))
    
    # If no critical findings, create a healthy summary
    if not findings and broker_cpu_data:
        cluster_avg_total = np.mean([b['avg_total'] for b in broker_cpu_data])
        cluster_max_p95 = max([b['p95_total'] for b in broker_cpu_data])
        
        findings.append(Finding(
            metric_name='CpuTotal',
            severity=Severity.HEALTHY,
            category=Category.PERFORMANCE,
            title='CPU Usage Normal',
            description=f'Total CPU usage (User+System) below 60% threshold. Cluster average: {cluster_avg_total:.1f}%, Max P95: {cluster_max_p95:.1f}%',
            current_value=cluster_avg_total,
            threshold_value=60.0,
            evidence={'cluster_avg_total': cluster_avg_total, 'cluster_max_p95': cluster_max_p95}
        ))
    
    return findings


def analyze_cpu_usage(cpu_user: MetricData, cpu_system: MetricData) -> List[Finding]:
    """Combined should be < 60% sustained."""
    findings = []
    
    # Calculate combined CPU
    combined_avg = cpu_user.statistics['avg'] + cpu_system.statistics['avg']
    combined_p95 = cpu_user.statistics['p95'] + cpu_system.statistics['p95']
    
    if combined_p95 >= 60.0:
        findings.append(Finding(
            metric_name='CpuUsage',
            severity=Severity.WARNING,
            category=Category.PERFORMANCE,
            title='High CPU Usage',
            description=f'Combined CPU usage (P95) at {combined_p95:.1f}%, exceeding 60% threshold.',
            current_value=combined_p95,
            threshold_value=60.0,
            evidence={
                'cpu_user_stats': cpu_user.statistics,
                'cpu_system_stats': cpu_system.statistics,
                'combined_avg': combined_avg
            }
        ))
    else:
        findings.append(Finding(
            metric_name='CpuUsage',
            severity=Severity.HEALTHY,
            category=Category.PERFORMANCE,
            title='CPU Usage Normal',
            description=f'Combined CPU usage (P95) at {combined_p95:.1f}%, below 60% threshold.',
            current_value=combined_p95,
            threshold_value=60.0,
            evidence={
                'cpu_user_stats': cpu_user.statistics,
                'cpu_system_stats': cpu_system.statistics,
                'combined_avg': combined_avg
            }
        ))
    
    return findings


def analyze_memory_usage(memory_used: MetricData, memory_free: MetricData) -> List[Finding]:
    """Utilization should be < 85%."""
    findings = []
    
    # Calculate memory utilization percentage
    total_memory = memory_used.statistics['avg'] + memory_free.statistics['avg']
    if total_memory > 0:
        utilization = (memory_used.statistics['avg'] / total_memory) * 100
        max_utilization = (memory_used.statistics['max'] / total_memory) * 100
        
        if max_utilization >= 85.0:
            findings.append(Finding(
                metric_name='MemoryUsage',
                severity=Severity.WARNING,
                category=Category.PERFORMANCE,
                title='High Memory Usage',
                description=f'Memory utilization reached {max_utilization:.1f}%, exceeding 85% threshold.',
                current_value=max_utilization,
                threshold_value=85.0,
                evidence={
                    'memory_used_stats': memory_used.statistics,
                    'memory_free_stats': memory_free.statistics,
                    'avg_utilization': utilization
                }
            ))
    
    return findings


def analyze_heap_memory(metric: MetricData) -> List[Finding]:
    """After GC should be < 60%."""
    findings = []
    max_val = metric.statistics['max']
    
    if max_val >= 60.0:
        findings.append(Finding(
            metric_name='HeapMemoryAfterGC',
            severity=Severity.WARNING,
            category=Category.PERFORMANCE,
            title='High Heap Memory After GC',
            description=f'Heap memory after GC at {max_val:.1f}%, exceeding 60% threshold. Indicates memory pressure.',
            current_value=max_val,
            threshold_value=60.0,
            evidence={'statistics': metric.statistics}
        ))
    
    return findings


def analyze_under_min_isr(metric: MetricData) -> List[Finding]:
    """Should be 0. CRITICAL if current > 0, WARNING if historical > 0."""
    findings = []
    max_val = metric.statistics['max']
    current_val = metric.values[-1] if metric.values else 0  # Most recent value
    
    if current_val > 0:
        findings.append(Finding(
            metric_name='UnderMinIsrPartitionCount',
            severity=Severity.CRITICAL,
            category=Category.RELIABILITY,
            title='Partitions Under Min ISR Now',
            description=f'Currently {int(current_val)} partitions under minimum ISR. Immediate risk of data loss.',
            current_value=current_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics, 'current': current_val}
        ))
    elif max_val > 0:
        findings.append(Finding(
            metric_name='UnderMinIsrPartitionCount',
            severity=Severity.WARNING,
            category=Category.RELIABILITY,
            title='Partitions Under Min ISR (Historical)',
            description=f'Detected up to {int(max_val)} partitions under minimum ISR in last 30 days. Monitor replication health.',
            current_value=max_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics}
        ))
    else:
        findings.append(Finding(
            metric_name='UnderMinIsrPartitionCount',
            severity=Severity.HEALTHY,
            category=Category.RELIABILITY,
            title='All Partitions Meet Min ISR',
            description='All partitions meet minimum in-sync replica requirements.',
            current_value=max_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics}
        ))
    
    return findings


def analyze_under_replicated_partitions(metric: MetricData) -> List[Finding]:
    """Should be 0."""
    findings = []
    max_val = metric.statistics['max']
    
    if max_val > 0:
        findings.append(Finding(
            metric_name='UnderReplicatedPartitions',
            severity=Severity.WARNING,
            category=Category.RELIABILITY,
            title='Under-Replicated Partitions Detected',
            description=f'Detected up to {int(max_val)} under-replicated partitions. Replication is lagging.',
            current_value=max_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics}
        ))
    else:
        findings.append(Finding(
            metric_name='UnderReplicatedPartitions',
            severity=Severity.HEALTHY,
            category=Category.RELIABILITY,
            title='All Partitions Fully Replicated',
            description='All partitions are fully replicated.',
            current_value=max_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics}
        ))
    
    return findings


def analyze_throughput(
    bytes_in: Optional[MetricData],
    bytes_out: Optional[MetricData],
    cluster_info: ClusterInfo
) -> List[Finding]:
    """
    Analyze cluster throughput patterns and check against network limits.
    
    Network limits based on EC2 instance specifications.
    Data from: msk-limits-hardware-bottleneck.png
    """
    findings = []
    
    # Network bandwidth limits based on AWS MSK documentation
    # Standard: Based on EC2 instance network performance
    # Express: Based on AWS MSK Express throttle limits (Maximum quota)
    # Format: instance_type: (ingress_mb_per_sec, egress_mb_per_sec)
    network_limits = {
        # M5 Standard instances
        'kafka.m5.large': (9, 9),
        'kafka.m5.xlarge': (16, 16),
        'kafka.m5.2xlarge': (31, 31),
        'kafka.m5.4xlarge': (63, 63),
        'kafka.m5.8xlarge': (106, 106),
        'kafka.m5.12xlarge': (125, 125),
        'kafka.m5.16xlarge': (125, 125),
        'kafka.m5.24xlarge': (125, 125),
        # M7g Standard instances (Graviton3)
        'kafka.m7g.large': (10, 10),
        'kafka.m7g.xlarge': (20, 20),
        'kafka.m7g.2xlarge': (39, 39),
        'kafka.m7g.4xlarge': (78, 78),
        'kafka.m7g.8xlarge': (125, 125),
        'kafka.m7g.12xlarge': (125, 125),
        'kafka.m7g.16xlarge': (125, 125),
        # M7g Express instances (Maximum quota from AWS docs)
        'express.m7g.large': (23.4, 58.5),
        'express.m7g.xlarge': (46.8, 117),
        'express.m7g.2xlarge': (93.7, 234.2),
        'express.m7g.4xlarge': (187.5, 468.7),
        'express.m7g.8xlarge': (375, 937.5),
        'express.m7g.12xlarge': (562.5, 1406.2),
        'express.m7g.16xlarge': (750, 1875),
    }
    
    ingress_limit_mb, egress_limit_mb = network_limits.get(cluster_info.instance_type, (10, 10))
    ingress_limit_bytes = ingress_limit_mb * 1024 * 1024  # Convert MB/s to bytes/s
    egress_limit_bytes = egress_limit_mb * 1024 * 1024
    threshold_70_ingress = ingress_limit_bytes * 0.7
    threshold_70_egress = egress_limit_bytes * 0.7
    
    if bytes_in:
        max_in_bytes = bytes_in.statistics['max']
        avg_in_bytes = bytes_in.statistics['avg']
        avg_in_mb = avg_in_bytes / (1024 * 1024)
        max_in_mb = max_in_bytes / (1024 * 1024)
        utilization_in = (max_in_bytes / ingress_limit_bytes) * 100
        
        if max_in_bytes >= threshold_70_ingress:
            findings.append(Finding(
                metric_name='BytesInPerSec',
                severity=Severity.WARNING,
                category=Category.PERFORMANCE,
                title='High Inbound Network Utilization',
                description=f'Peak inbound throughput at {utilization_in:.1f}% of network capacity ({max_in_mb:.1f} MB/s / {ingress_limit_mb} MB/s). Consider upgrading instance type to avoid throttling.',
                current_value=max_in_mb,
                threshold_value=float(ingress_limit_mb),
                evidence={'max_mb_per_sec': max_in_mb, 'avg_mb_per_sec': avg_in_mb, 'limit_mb_per_sec': ingress_limit_mb, 'utilization_pct': utilization_in}
            ))
        else:
            findings.append(Finding(
                metric_name='BytesInPerSec',
                severity=Severity.HEALTHY,
                category=Category.PERFORMANCE,
                title='Inbound Throughput OK',
                description=f'Inbound throughput: Avg {avg_in_mb:.2f} MB/s, Peak {max_in_mb:.2f} MB/s ({utilization_in:.1f}% of {ingress_limit_mb} MB/s capacity)',
                current_value=avg_in_mb,
                threshold_value=None,
                evidence={'max_mb_per_sec': max_in_mb, 'avg_mb_per_sec': avg_in_mb, 'limit_mb_per_sec': ingress_limit_mb}
            ))
    
    if bytes_out:
        max_out_bytes = bytes_out.statistics['max']
        avg_out_bytes = bytes_out.statistics['avg']
        avg_out_mb = avg_out_bytes / (1024 * 1024)
        max_out_mb = max_out_bytes / (1024 * 1024)
        utilization_out = (max_out_bytes / egress_limit_bytes) * 100
        
        if max_out_bytes >= threshold_70_egress:
            findings.append(Finding(
                metric_name='BytesOutPerSec',
                severity=Severity.WARNING,
                category=Category.PERFORMANCE,
                title='High Outbound Network Utilization',
                description=f'Peak outbound throughput at {utilization_out:.1f}% of network capacity ({max_out_mb:.1f} MB/s / {egress_limit_mb} MB/s). Consider upgrading instance type to avoid throttling.',
                current_value=max_out_mb,
                threshold_value=float(egress_limit_mb),
                evidence={'max_mb_per_sec': max_out_mb, 'avg_mb_per_sec': avg_out_mb, 'limit_mb_per_sec': egress_limit_mb, 'utilization_pct': utilization_out}
            ))
        else:
            findings.append(Finding(
                metric_name='BytesOutPerSec',
                severity=Severity.HEALTHY,
                category=Category.PERFORMANCE,
                title='Outbound Throughput OK',
                description=f'Outbound throughput: Avg {avg_out_mb:.2f} MB/s, Peak {max_out_mb:.2f} MB/s ({utilization_out:.1f}% of {egress_limit_mb} MB/s capacity)',
                current_value=avg_out_mb,
                threshold_value=None,
                evidence={'max_mb_per_sec': max_out_mb, 'avg_mb_per_sec': avg_out_mb, 'limit_mb_per_sec': egress_limit_mb}
            ))
    
    return findings


def analyze_storage_used(metric: MetricData, cluster_info: ClusterInfo) -> List[Finding]:
    """Analyze storage usage."""
    findings = []
    avg_bytes = metric.statistics['avg']
    max_bytes = metric.statistics['max']
    avg_gb = avg_bytes / (1024 ** 3)
    max_gb = max_bytes / (1024 ** 3)
    
    findings.append(Finding(
        metric_name='StorageUsed',
        severity=Severity.INFORMATIONAL,
        category=Category.COST,
        title='Storage Usage',
        description=f'Average: {avg_gb:.2f} GB, Peak: {max_gb:.2f} GB across {cluster_info.broker_count} brokers',
        current_value=avg_gb,
        threshold_value=None,
        evidence={'statistics': metric.statistics}
    ))
    
    return findings


def analyze_rebalance_status(metric: MetricData) -> List[Finding]:
    """Check if rebalancing is happening frequently."""
    findings = []
    max_val = metric.statistics['max']
    
    if max_val > 0:
        findings.append(Finding(
            metric_name='RebalanceInProgress',
            severity=Severity.WARNING,
            category=Category.PERFORMANCE,
            title='Consumer Group Rebalancing Detected',
            description='Consumer group rebalancing detected during the monitoring period. This can impact performance.',
            current_value=max_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics}
        ))
    else:
        findings.append(Finding(
            metric_name='RebalanceInProgress',
            severity=Severity.HEALTHY,
            category=Category.PERFORMANCE,
            title='No Rebalancing Activity',
            description='No consumer group rebalancing detected.',
            current_value=max_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics}
        ))
    
    return findings


def analyze_under_provisioned(metric: MetricData) -> List[Finding]:
    """Check if cluster is under-provisioned."""
    findings = []
    max_val = metric.statistics['max']
    
    if max_val > 0:
        findings.append(Finding(
            metric_name='UnderProvisioned',
            severity=Severity.CRITICAL,
            category=Category.PERFORMANCE,
            title='Cluster Under-Provisioned',
            description='Cluster is under-provisioned. Consider scaling up broker instance types or adding more brokers.',
            current_value=max_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics}
        ))
    else:
        findings.append(Finding(
            metric_name='UnderProvisioned',
            severity=Severity.HEALTHY,
            category=Category.PERFORMANCE,
            title='Cluster Properly Provisioned',
            description='Cluster capacity is adequate for current workload.',
            current_value=max_val,
            threshold_value=0.0,
            evidence={'statistics': metric.statistics}
        ))
    
    return findings


def analyze_connection_count(
    metric: MetricData,
    cluster_info: ClusterInfo
) -> List[Finding]:
    """
    Analyze client connection count against cluster capacity.
    ClientConnectionCount is a cluster-level metric (sum across all brokers).
    Based on AWS MSK documentation:
    https://docs.aws.amazon.com/msk/latest/developerguide/broker-instance-sizes.html
    """
    findings = []
    
    # Connection limits per broker based on AWS documentation
    connection_limits_per_broker = {
        # T3 instances
        'kafka.t3.small': 300,
        
        # M5 instances
        'kafka.m5.large': 1000,
        'kafka.m5.xlarge': 1500,
        'kafka.m5.2xlarge': 2000,
        'kafka.m5.4xlarge': 4000,
        'kafka.m5.8xlarge': 8000,
        'kafka.m5.12xlarge': 12000,
        'kafka.m5.16xlarge': 16000,
        'kafka.m5.24xlarge': 24000,
        
        # M7g instances (Graviton3)
        'kafka.m7g.large': 1000,
        'kafka.m7g.xlarge': 1500,
        'kafka.m7g.2xlarge': 2000,
        'kafka.m7g.4xlarge': 4000,
        'kafka.m7g.8xlarge': 8000,
        'kafka.m7g.12xlarge': 12000,
        'kafka.m7g.16xlarge': 16000,
    }
    
    limit_per_broker = connection_limits_per_broker.get(cluster_info.instance_type, 1000)
    cluster_limit = limit_per_broker * cluster_info.broker_count
    max_connections = metric.statistics['max']
    utilization = (max_connections / cluster_limit) * 100
    
    if utilization >= 90.0:
        findings.append(Finding(
            metric_name='ClientConnectionCount',
            severity=Severity.CRITICAL,
            category=Category.PERFORMANCE,
            title='Critical Connection Count',
            description=f'Connection count at {utilization:.1f}% of cluster limit ({int(max_connections)}/{cluster_limit}). Risk of connection exhaustion.',
            current_value=utilization,
            threshold_value=80.0,
            evidence={'max_connections': max_connections, 'cluster_limit': cluster_limit, 'broker_count': cluster_info.broker_count}
        ))
    elif utilization >= 80.0:
        findings.append(Finding(
            metric_name='ClientConnectionCount',
            severity=Severity.WARNING,
            category=Category.PERFORMANCE,
            title='High Connection Count',
            description=f'Connection count at {utilization:.1f}% of cluster limit ({int(max_connections)}/{cluster_limit}). Consider connection pooling.',
            current_value=utilization,
            threshold_value=80.0,
            evidence={'max_connections': max_connections, 'cluster_limit': cluster_limit, 'broker_count': cluster_info.broker_count}
        ))
    else:
        findings.append(Finding(
            metric_name='ClientConnectionCount',
            severity=Severity.HEALTHY,
            category=Category.PERFORMANCE,
            title='Connection Count Normal',
            description=f'Connection count at {utilization:.1f}% of cluster limit ({int(max_connections)}/{cluster_limit}).',
            current_value=utilization,
            threshold_value=80.0,
            evidence={'max_connections': max_connections, 'cluster_limit': cluster_limit, 'broker_count': cluster_info.broker_count}
        ))
    
    return findings


def analyze_connection_creation_rate(
    broker_metrics: List[MetricData],
    cluster_info: ClusterInfo
) -> List[Finding]:
    """
    Analyze connection creation rate to detect excessive reconnections.
    
    High connection creation rates indicate:
    - Missing connection pooling
    - Short connection timeouts
    - Network instability
    - Client restart loops
    
    Reference: AWS MSK Best Practices
    - IAM auth supports up to 100 new connections/sec per cluster
    - New connections are expensive (CPU overhead)
    - Sustained high rates indicate client configuration issues
    """
    findings = []
    
    if not broker_metrics:
        return findings
    
    # Calculate cluster-wide statistics
    all_avgs = [m.statistics['avg'] for m in broker_metrics]
    all_p95s = [m.statistics['p95'] for m in broker_metrics]
    all_maxs = [m.statistics['max'] for m in broker_metrics]
    
    cluster_avg = sum(all_avgs)  # Sum across brokers for cluster-wide rate
    cluster_p95 = sum(all_p95s)
    cluster_max = sum(all_maxs)
    
    # Thresholds based on AWS documentation and best practices
    # IAM auth limit: 100 connections/sec per cluster
    # General recommendation: Keep creation rate low for stable workloads
    
    # Check if IAM authentication is enabled (more restrictive limits)
    is_iam_auth = 'IAM' in cluster_info.authentication_methods
    
    if is_iam_auth:
        critical_threshold = 80.0  # 80% of IAM limit (100/sec)
        warning_threshold = 50.0   # 50% of IAM limit
    else:
        critical_threshold = 50.0  # Arbitrary threshold for non-IAM
        warning_threshold = 20.0
    
    # Analyze P95 (sustained high rate is more concerning than spikes)
    if cluster_p95 >= critical_threshold:
        auth_note = ' (approaching IAM auth limit of 100/sec)' if is_iam_auth else ''
        findings.append(Finding(
            metric_name='ConnectionCreationRate',
            severity=Severity.CRITICAL,
            category=Category.PERFORMANCE,
            title='Excessive Connection Creation Rate',
            description=(
                f'High connection creation rate detected: P95={cluster_p95:.1f} conn/sec, '
                f'avg={cluster_avg:.1f} conn/sec, max={cluster_max:.1f} conn/sec{auth_note}. '
                f'This indicates missing connection pooling, short timeouts, or client instability. '
                f'New connections are expensive and impact CPU performance.'
            ),
            current_value=cluster_p95,
            threshold_value=critical_threshold,
            evidence={
                'cluster_avg': cluster_avg,
                'cluster_p95': cluster_p95,
                'cluster_max': cluster_max,
                'broker_count': len(broker_metrics),
                'iam_auth_enabled': is_iam_auth
            }
        ))
    elif cluster_p95 >= warning_threshold:
        auth_note = ' (IAM auth limit is 100/sec)' if is_iam_auth else ''
        findings.append(Finding(
            metric_name='ConnectionCreationRate',
            severity=Severity.WARNING,
            category=Category.PERFORMANCE,
            title='Elevated Connection Creation Rate',
            description=(
                f'Elevated connection creation rate: P95={cluster_p95:.1f} conn/sec, '
                f'avg={cluster_avg:.1f} conn/sec, max={cluster_max:.1f} conn/sec{auth_note}. '
                f'Consider implementing connection pooling and reviewing client timeout configurations.'
            ),
            current_value=cluster_p95,
            threshold_value=warning_threshold,
            evidence={
                'cluster_avg': cluster_avg,
                'cluster_p95': cluster_p95,
                'cluster_max': cluster_max,
                'broker_count': len(broker_metrics),
                'iam_auth_enabled': is_iam_auth
            }
        ))
    elif cluster_avg >= 5.0:
        # Informational: Moderate rate, worth monitoring
        findings.append(Finding(
            metric_name='ConnectionCreationRate',
            severity=Severity.INFORMATIONAL,
            category=Category.PERFORMANCE,
            title='Moderate Connection Creation Rate',
            description=(
                f'Connection creation rate: P95={cluster_p95:.1f} conn/sec, '
                f'avg={cluster_avg:.1f} conn/sec, max={cluster_max:.1f} conn/sec. '
                f'Rate is within acceptable range. Monitor for increases that may indicate client issues.'
            ),
            current_value=cluster_avg,
            threshold_value=None,
            evidence={
                'cluster_avg': cluster_avg,
                'cluster_p95': cluster_p95,
                'cluster_max': cluster_max,
                'broker_count': len(broker_metrics),
                'iam_auth_enabled': is_iam_auth
            }
        ))
    else:
        # Healthy: Low connection creation rate
        findings.append(Finding(
            metric_name='ConnectionCreationRate',
            severity=Severity.HEALTHY,
            category=Category.PERFORMANCE,
            title='Low Connection Creation Rate',
            description=(
                f'Connection creation rate is low: P95={cluster_p95:.1f} conn/sec, '
                f'avg={cluster_avg:.1f} conn/sec. This indicates stable client connections.'
            ),
            current_value=cluster_avg,
            threshold_value=None,
            evidence={
                'cluster_avg': cluster_avg,
                'cluster_p95': cluster_p95,
                'cluster_max': cluster_max,
                'broker_count': len(broker_metrics),
                'iam_auth_enabled': is_iam_auth
            }
        ))
    
    return findings


def analyze_connection_churn(
    creation_rate: MetricData,
    close_rate: MetricData
) -> List[Finding]:
    """
    Analyze connection creation and close rates to detect instability.
    
    Note: Connection patterns vary significantly by workload type:
    - Batch jobs: Higher churn is normal
    - Streaming apps: Should have stable connections
    - Serverless: High churn expected
    
    These metrics should be monitored continuously and interpreted in context.
    """
    findings = []
    
    avg_creation = creation_rate.statistics['avg']
    avg_close = close_rate.statistics['avg']
    max_creation = creation_rate.statistics['max']
    max_close = close_rate.statistics['max']
    
    # Calculate churn rate (average of creation and close rates)
    churn_rate = (avg_creation + avg_close) / 2
    
    # High churn detection
    if churn_rate >= 20.0:
        findings.append(Finding(
            metric_name='ConnectionChurn',
            severity=Severity.CRITICAL,
            category=Category.PERFORMANCE,
            title='Excessive Connection Churn',
            description=f'High connection churn rate ({churn_rate:.1f} connections/sec). This may indicate missing connection pooling, short timeouts, or network issues. Note: Connection patterns vary by workload - batch jobs and serverless applications naturally have higher churn.',
            current_value=churn_rate,
            threshold_value=20.0,
            evidence={
                'avg_creation_rate': avg_creation,
                'avg_close_rate': avg_close,
                'max_creation_rate': max_creation,
                'max_close_rate': max_close,
                'churn_rate': churn_rate
            }
        ))
    elif churn_rate >= 10.0:
        findings.append(Finding(
            metric_name='ConnectionChurn',
            severity=Severity.WARNING,
            category=Category.PERFORMANCE,
            title='Elevated Connection Churn',
            description=f'Moderate connection churn rate ({churn_rate:.1f} connections/sec). Monitor for patterns - this may be normal for your workload or indicate optimization opportunities. Consider connection pooling if using short-lived clients.',
            current_value=churn_rate,
            threshold_value=10.0,
            evidence={
                'avg_creation_rate': avg_creation,
                'avg_close_rate': avg_close,
                'max_creation_rate': max_creation,
                'max_close_rate': max_close,
                'churn_rate': churn_rate
            }
        ))
    
    # Imbalanced creation/close ratio
    if avg_close > 0:
        ratio = avg_creation / avg_close
        if ratio > 1.5 or ratio < 0.7:
            findings.append(Finding(
                metric_name='ConnectionChurn',
                severity=Severity.INFORMATIONAL,
                category=Category.PERFORMANCE,
                title='Imbalanced Connection Creation/Close Ratio',
                description=f'Connection creation/close ratio is {ratio:.2f} (creation: {avg_creation:.1f}/sec, close: {avg_close:.1f}/sec). Ratio should be close to 1.0 for stable workloads. Variations are normal for dynamic workloads and should be monitored in context.',
                current_value=ratio,
                threshold_value=1.0,
                evidence={
                    'avg_creation_rate': avg_creation,
                    'avg_close_rate': avg_close,
                    'ratio': ratio
                }
            ))
    
    return findings


def analyze_authentication_methods(cluster_info: ClusterInfo) -> List[Finding]:
    """Check for security best practices."""
    findings = []
    
    if 'unauthenticated' in cluster_info.authentication_methods:
        findings.append(Finding(
            metric_name='Authentication',
            severity=Severity.CRITICAL,
            category=Category.SECURITY,
            title='Unauthenticated Access Enabled',
            description='Cluster allows unauthenticated access. This is a critical security risk.',
            current_value=None,
            threshold_value=None,
            evidence={'methods': cluster_info.authentication_methods}
        ))
    
    if len(cluster_info.authentication_methods) == 1:
        findings.append(Finding(
            metric_name='Authentication',
            severity=Severity.INFORMATIONAL,
            category=Category.SECURITY,
            title='Single Authentication Method',
            description='Only one authentication method enabled. Consider enabling multiple methods for flexibility.',
            current_value=None,
            threshold_value=None,
            evidence={'methods': cluster_info.authentication_methods}
        ))
    
    if 'SASL/SCRAM' in cluster_info.authentication_methods:
        findings.append(Finding(
            metric_name='Authentication',
            severity=Severity.INFORMATIONAL,
            category=Category.SECURITY,
            title='SASL/SCRAM Authentication',
            description='SASL/SCRAM enabled. Ensure regular credential rotation practices.',
            current_value=None,
            threshold_value=None,
            evidence={'methods': cluster_info.authentication_methods}
        ))
    
    return findings


def analyze_instance_type(
    cluster_info: ClusterInfo,
    metrics: MetricsCollection
) -> List[Finding]:
    """Check for cost optimization opportunities."""
    findings = []
    
    if cluster_info.instance_family == 'intel':
        # Calculate potential savings (Graviton is ~20% cheaper)
        savings_percentage = 20
        
        findings.append(Finding(
            metric_name='InstanceType',
            severity=Severity.INFORMATIONAL,
            category=Category.COST,
            title='Graviton Migration Opportunity',
            description=f'Cluster uses Intel instances ({cluster_info.instance_type}). Migrating to Graviton could save ~{savings_percentage}% on compute costs.',
            current_value=None,
            threshold_value=None,
            evidence={
                'current_type': cluster_info.instance_type,
                'instance_family': cluster_info.instance_family,
                'estimated_savings': savings_percentage
            }
        ))
    else:
        findings.append(Finding(
            metric_name='InstanceType',
            severity=Severity.HEALTHY,
            category=Category.COST,
            title='Cost-Optimized Instance Type',
            description=f'Cluster uses Graviton instances ({cluster_info.instance_type}), which are cost-optimized.',
            current_value=None,
            threshold_value=None,
            evidence={
                'current_type': cluster_info.instance_type,
                'instance_family': cluster_info.instance_family
            }
        ))
    
    return findings


def analyze_partition_count(
    metric: MetricData,
    cluster_info: ClusterInfo
) -> List[Finding]:
    """
    Analyze if partition count is appropriate for cluster capacity.
    
    Based on AWS MSK documentation:
    https://docs.aws.amazon.com/msk/latest/developerguide/broker-instance-sizes.html
    """
    findings = []
    
    # Partition limits per broker based on AWS documentation
    partition_limits = {
        # T3 instances
        'kafka.t3.small': 300,
        # M5 instances
        'kafka.m5.large': 1000,
        'kafka.m5.xlarge': 1500,
        'kafka.m5.2xlarge': 2000,
        'kafka.m5.4xlarge': 4000,
        'kafka.m5.8xlarge': 8000,
        'kafka.m5.12xlarge': 12000,
        'kafka.m5.16xlarge': 16000,
        'kafka.m5.24xlarge': 24000,
        # M7g instances (Graviton3)
        'kafka.m7g.large': 1000,
        'kafka.m7g.xlarge': 1500,
        'kafka.m7g.2xlarge': 2000,
        'kafka.m7g.4xlarge': 4000,
        'kafka.m7g.8xlarge': 8000,
        'kafka.m7g.12xlarge': 12000,
        'kafka.m7g.16xlarge': 16000,
        # Express instances (maximum limits)
        'express.m7g.large': 1500,
        'express.m7g.xlarge': 2000,
        'express.m7g.2xlarge': 4000,
        'express.m7g.4xlarge': 8000,
        'express.m7g.8xlarge': 16000,
        'express.m7g.12xlarge': 24000,
        'express.m7g.16xlarge': 32000,
    }
    
    limit_per_broker = partition_limits.get(cluster_info.instance_type, 1000)
    max_recommended = limit_per_broker * cluster_info.broker_count
    
    # Use current value (last collected) instead of average
    current_partitions = int(metric.values[-1]) if metric.values else int(metric.statistics['avg'])
    utilization = (current_partitions / max_recommended) * 100
    
    if utilization > 100.0:
        findings.append(Finding(
            metric_name='GlobalPartitionCount',
            severity=Severity.CRITICAL,
            category=Category.PERFORMANCE,
            title='Partition Count Exceeded Capacity',
            description=f'Partition count ({current_partitions}) exceeds cluster capacity ({max_recommended}). Immediate action required.',
            current_value=float(current_partitions),
            threshold_value=float(max_recommended),
            evidence={
                'current_partitions': current_partitions,
                'max_recommended': max_recommended,
                'limit_per_broker': limit_per_broker,
                'broker_count': cluster_info.broker_count,
                'instance_type': cluster_info.instance_type
            }
        ))
    elif utilization >= 90.0:
        findings.append(Finding(
            metric_name='GlobalPartitionCount',
            severity=Severity.WARNING,
            category=Category.PERFORMANCE,
            title='Partition Count Near Capacity',
            description=f'Partition count ({current_partitions}) at {utilization:.1f}% of cluster capacity ({max_recommended}). Plan capacity expansion.',
            current_value=float(current_partitions),
            threshold_value=float(max_recommended),
            evidence={
                'current_partitions': current_partitions,
                'max_recommended': max_recommended,
                'limit_per_broker': limit_per_broker,
                'broker_count': cluster_info.broker_count,
                'instance_type': cluster_info.instance_type
            }
        ))
    else:
        findings.append(Finding(
            metric_name='GlobalPartitionCount',
            severity=Severity.HEALTHY,
            category=Category.PERFORMANCE,
            title='Partition Count OK',
            description=f'Partition count ({current_partitions}) at {utilization:.1f}% of cluster capacity ({max_recommended}). Within healthy range.',
            current_value=float(current_partitions),
            threshold_value=float(max_recommended),
            evidence={
                'current_partitions': current_partitions,
                'max_recommended': max_recommended,
                'limit_per_broker': limit_per_broker,
                'broker_count': cluster_info.broker_count,
                'instance_type': cluster_info.instance_type
            }
        ))
    
    return findings


def get_recommended_kafka_version() -> str:
    """
    Get recommended Kafka version from AWS documentation.
    Scrapes https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html
    
    Returns:
        Recommended version string (e.g., "3.8") or "3.8" as fallback
    """
    try:
        import urllib.request
        import re
        
        # Static AWS documentation URL - hardcoded to prevent file:// or custom schemes
        url = "https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html"
        
        # Validate URL scheme to prevent file:// access
        if not url.startswith("https://"):
            logger.warning("Invalid URL scheme, using fallback version: 3.8")
            return "3.8"
        
        request = urllib.request.Request(url, headers={'User-Agent': 'MSK-Health-Check/1.0'})
        # nosemgrep: dynamic-urllib-use-detected
        with urllib.request.urlopen(request, timeout=5) as response:  # nosec B310
            html = response.read().decode('utf-8')
            
        # Look for pattern: "version 3.8.x (Recommended)" or "Amazon MSK version 3.8.x (Recommended)"
        pattern = r'version\s+(\d+\.\d+)\.x\s*\(Recommended\)'
        match = re.search(pattern, html, re.IGNORECASE)
        
        if match:
            recommended = match.group(1)
            logger.info(f"Found recommended Kafka version from AWS docs: {recommended}")
            return recommended
        
        logger.warning("Could not find recommended version in AWS docs, using fallback: 3.8")
        return "3.8"
        
    except Exception as e:
        logger.warning(f"Error fetching recommended version from AWS docs: {e}, using fallback: 3.8")
        return "3.8"


def analyze_kafka_version(cluster_info: ClusterInfo) -> List[Finding]:
    """
    Analyze Kafka version against AWS recommended version.
    Fetches recommended version from AWS documentation in real-time.
    """
    findings = []
    current_version = cluster_info.kafka_version
    
    # Get recommended version from AWS docs
    recommended_version = get_recommended_kafka_version()
    
    try:
        # Parse versions for comparison
        current_parts = current_version.split('.')
        rec_parts = recommended_version.split('.')
        
        if len(current_parts) >= 2 and len(rec_parts) >= 2:
            current_major = int(current_parts[0])
            current_minor = int(current_parts[1])
            rec_major = int(rec_parts[0])
            rec_minor = int(rec_parts[1])
            
            # Calculate version gap
            version_gap = (rec_major - current_major) * 10 + (rec_minor - current_minor)
            
            if version_gap >= 5:
                findings.append(Finding(
                    metric_name='KafkaVersion',
                    severity=Severity.CRITICAL,
                    category=Category.RELIABILITY,
                    title='Kafka Version Severely Outdated',
                    description=f'Cluster running Kafka {current_version}. AWS recommends version {recommended_version}.x for latest features and security patches.',
                    current_value=None,
                    threshold_value=None,
                    evidence={
                        'current_version': current_version,
                        'recommended_version': f'{recommended_version}.x',
                        'version_gap': version_gap,
                        'docs_url': 'https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html'
                    }
                ))
            elif version_gap > 0:
                findings.append(Finding(
                    metric_name='KafkaVersion',
                    severity=Severity.WARNING,
                    category=Category.RELIABILITY,
                    title='Kafka Version Upgrade Available',
                    description=f'Cluster running Kafka {current_version}. AWS recommends upgrading to {recommended_version}.x for latest features and security patches.',
                    current_value=None,
                    threshold_value=None,
                    evidence={
                        'current_version': current_version,
                        'recommended_version': f'{recommended_version}.x',
                        'version_gap': version_gap,
                        'docs_url': 'https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html'
                    }
                ))
            elif version_gap == 0:
                findings.append(Finding(
                    metric_name='KafkaVersion',
                    severity=Severity.HEALTHY,
                    category=Category.RELIABILITY,
                    title='Kafka Version Up-to-Date',
                    description=f'Cluster running AWS recommended Kafka version {current_version}.',
                    current_value=None,
                    threshold_value=None,
                    evidence={
                        'current_version': current_version,
                        'recommended_version': f'{recommended_version}.x'
                    }
                ))
            else:
                # Current version is newer than recommended
                findings.append(Finding(
                    metric_name='KafkaVersion',
                    severity=Severity.INFORMATIONAL,
                    category=Category.RELIABILITY,
                    title='Kafka Version Newer Than Recommended',
                    description=f'Cluster running Kafka {current_version}, which is newer than AWS recommended {recommended_version}.x.',
                    current_value=None,
                    threshold_value=None,
                    evidence={
                        'current_version': current_version,
                        'recommended_version': f'{recommended_version}.x'
                    }
                ))
    except (ValueError, IndexError) as e:
        logger.warning(f"Could not parse Kafka versions: {e}")
        findings.append(Finding(
            metric_name='KafkaVersion',
            severity=Severity.INFORMATIONAL,
            category=Category.RELIABILITY,
            title='Kafka Version Check',
            description=f'Cluster running Kafka {current_version}. AWS recommends {recommended_version}.x.',
            current_value=None,
            threshold_value=None,
            evidence={'current_version': current_version, 'recommended_version': f'{recommended_version}.x'}
        ))
    
    return findings


def analyze_availability_zones(cluster_info: ClusterInfo) -> List[Finding]:
    """Check if cluster is deployed across multiple AZs."""
    findings = []
    az_count = cluster_info.availability_zones
    
    if az_count < 2:
        findings.append(Finding(
            metric_name='AvailabilityZones',
            severity=Severity.CRITICAL,
            category=Category.RELIABILITY,
            title='Single AZ Deployment',
            description=f'Cluster deployed in only {az_count} AZ. MSK requires at least 2 AZs for high availability.',
            current_value=float(az_count),
            threshold_value=2.0,
            evidence={'az_count': az_count}
        ))
    elif az_count == 2:
        findings.append(Finding(
            metric_name='AvailabilityZones',
            severity=Severity.WARNING,
            category=Category.RELIABILITY,
            title='Two AZ Deployment',
            description=f'Cluster deployed across {az_count} AZs. For critical/production workloads, 3 AZs is recommended for better fault tolerance.',
            current_value=float(az_count),
            threshold_value=3.0,
            evidence={'az_count': az_count}
        ))
    else:  # 3 or more AZs
        findings.append(Finding(
            metric_name='AvailabilityZones',
            severity=Severity.HEALTHY,
            category=Category.RELIABILITY,
            title='Multi-AZ Deployment',
            description=f'Cluster deployed across {az_count} AZs, providing excellent fault tolerance.',
            current_value=float(az_count),
            threshold_value=3.0,
            evidence={'az_count': az_count}
        ))
    
    return findings


def analyze_storage_auto_scaling(cluster_info: ClusterInfo) -> List[Finding]:
    """Check if storage auto-scaling is enabled (PROVISIONED/STANDARD only)."""
    findings = []
    
    # Skip for EXPRESS clusters (serverless storage)
    if cluster_info.cluster_type == 'EXPRESS':
        return findings
    
    if not cluster_info.storage_auto_scaling_enabled:
        findings.append(Finding(
            metric_name='StorageAutoScaling',
            severity=Severity.WARNING,
            category=Category.RELIABILITY,
            title='Storage Auto-Scaling Disabled',
            description='Storage auto-scaling is not enabled. Enable it to automatically increase storage capacity and prevent disk space issues.',
            current_value=0.0,
            threshold_value=1.0,
            evidence={'enabled': False}
        ))
    else:
        findings.append(Finding(
            metric_name='StorageAutoScaling',
            severity=Severity.HEALTHY,
            category=Category.RELIABILITY,
            title='Storage Auto-Scaling Enabled',
            description='Storage auto-scaling is enabled, providing automatic capacity management.',
            current_value=1.0,
            threshold_value=1.0,
            evidence={'enabled': True}
        ))
    
    return findings


def analyze_logging_configuration(cluster_info: ClusterInfo) -> List[Finding]:
    """Check if logging is enabled."""
    findings = []
    
    if not cluster_info.logging_enabled:
        findings.append(Finding(
            metric_name='Logging',
            severity=Severity.WARNING,
            category=Category.SECURITY,
            title='Logging Disabled',
            description='Broker logs are not being sent to CloudWatch, S3, or Firehose. Enable logging for production/critical environments for troubleshooting and compliance.',
            current_value=0.0,
            threshold_value=1.0,
            evidence={'enabled': False, 'destinations': []}
        ))
    else:
        destinations_str = ', '.join(cluster_info.logging_destinations)
        findings.append(Finding(
            metric_name='Logging',
            severity=Severity.HEALTHY,
            category=Category.SECURITY,
            title='Logging Enabled',
            description=f'Broker logs are being sent to: {destinations_str}.',
            current_value=1.0,
            threshold_value=1.0,
            evidence={'enabled': True, 'destinations': cluster_info.logging_destinations}
        ))
    
    return findings


def analyze_consumer_lag(
    estimated_lag: Optional[MetricData],
    max_offset_lag: Optional[MetricData],
    sum_offset_lag: Optional[MetricData]
) -> List[Finding]:
    """Analyze consumer lag metrics."""
    findings = []
    
    if estimated_lag:
        max_time_lag = estimated_lag.statistics['max']
        if max_time_lag > 0:
            findings.append(Finding(
                metric_name='EstimatedMaxTimeLag',
                severity=Severity.WARNING,
                category=Category.PERFORMANCE,
                title='Consumer Lag Detected',
                description=f'Maximum estimated time lag: {max_time_lag:.0f} seconds. Consumers may be falling behind producers. Review consumer performance and scaling.',
                current_value=max_time_lag,
                threshold_value=0.0,
                evidence={'statistics': estimated_lag.statistics}
            ))
    
    if max_offset_lag:
        max_offset = max_offset_lag.statistics['max']
        if max_offset > 0:
            findings.append(Finding(
                metric_name='MaxOffsetLag',
                severity=Severity.INFORMATIONAL,
                category=Category.PERFORMANCE,
                title='Offset Lag Present',
                description=f'Maximum offset lag: {max_offset:.0f} messages. Monitor consumer group performance.',
                current_value=max_offset,
                threshold_value=None,
                evidence={'statistics': max_offset_lag.statistics}
            ))
    
    return findings


def analyze_intelligent_rebalancing(cluster_info: ClusterInfo) -> List[Finding]:
    """Check if intelligent rebalancing is enabled for EXPRESS clusters."""
    findings = []
    
    # Only applicable to EXPRESS clusters
    if cluster_info.cluster_type != 'EXPRESS':
        return findings
    
    if not cluster_info.intelligent_rebalancing_enabled:
        findings.append(Finding(
            metric_name='IntelligentRebalancing',
            severity=Severity.WARNING,
            category=Category.RELIABILITY,
            title='Intelligent Rebalancing Disabled',
            description='Intelligent rebalancing is not enabled. For production workloads, enabling this feature helps maintain balanced partition distribution and optimal performance.',
            current_value=0.0,
            threshold_value=1.0,
            evidence={'enabled': False, 'cluster_type': 'EXPRESS'}
        ))
    else:
        findings.append(Finding(
            metric_name='IntelligentRebalancing',
            severity=Severity.HEALTHY,
            category=Category.RELIABILITY,
            title='Intelligent Rebalancing Enabled',
            description='Intelligent rebalancing is enabled, ensuring optimal partition distribution across brokers.',
            current_value=1.0,
            threshold_value=1.0,
            evidence={'enabled': True, 'cluster_type': 'EXPRESS'}
        ))
    
    return findings


def analyze_per_broker_metrics(broker_metrics: List[MetricData], metric_name: str, cluster_info: ClusterInfo) -> List[Finding]:
    """
    Analyze per-broker metrics and detect imbalances.
    For LeaderCount and PartitionCount, uses current values instead of averages.
    """
    findings = []
    
    if not broker_metrics:
        return findings
    
    # Skip storage metrics for EXPRESS clusters (serverless storage)
    if cluster_info.cluster_type == 'EXPRESS' and metric_name == 'KafkaDataLogsDiskUsed':
        return findings
    
    # Special handling for ConnectionCreationRate - use dedicated analysis
    if metric_name == 'ConnectionCreationRate':
        return analyze_connection_creation_rate(broker_metrics, cluster_info)
    
    # For LeaderCount and PartitionCount, use current values (last collected)
    # For other metrics, use averages
    if metric_name in ['LeaderCount', 'PartitionCount']:
        all_values = [m.values[-1] if m.values else m.statistics['avg'] for m in broker_metrics]
        cluster_avg = np.mean(all_values)
        cluster_max = max(all_values)
        cluster_min = min(all_values)
    else:
        # Calculate statistics across all brokers using averages
        all_avgs = [m.statistics['avg'] for m in broker_metrics]
        all_maxs = [m.statistics['max'] for m in broker_metrics]
        cluster_avg = np.mean(all_avgs)
        cluster_max = max(all_maxs)
        cluster_min = min([m.statistics['min'] for m in broker_metrics])
        all_values = all_avgs
    
    # Check per-broker partition capacity limits
    if metric_name == 'PartitionCount':
        partition_limits = {
            'kafka.t3.small': 300, 'kafka.m5.large': 1000, 'kafka.m5.xlarge': 1500,
            'kafka.m5.2xlarge': 2000, 'kafka.m5.4xlarge': 4000, 'kafka.m5.8xlarge': 8000,
            'kafka.m5.12xlarge': 12000, 'kafka.m5.16xlarge': 16000, 'kafka.m5.24xlarge': 24000,
            'kafka.m7g.large': 1000, 'kafka.m7g.xlarge': 1500, 'kafka.m7g.2xlarge': 2000,
            'kafka.m7g.4xlarge': 4000, 'kafka.m7g.8xlarge': 8000, 'kafka.m7g.12xlarge': 12000,
            'kafka.m7g.16xlarge': 16000, 'express.m7g.large': 1500, 'express.m7g.xlarge': 2000,
            'express.m7g.2xlarge': 4000, 'express.m7g.4xlarge': 8000, 'express.m7g.8xlarge': 16000,
            'express.m7g.12xlarge': 24000, 'express.m7g.16xlarge': 32000,
        }
        limit_per_broker = partition_limits.get(cluster_info.instance_type, 1000)
        max_broker_partitions = int(cluster_max)
        utilization = (max_broker_partitions / limit_per_broker) * 100
        
        if utilization > 100.0:
            findings.append(Finding(
                metric_name='PartitionCount',
                severity=Severity.CRITICAL,
                category=Category.PERFORMANCE,
                title='Broker Partition Count Exceeded',
                description=f'At least one broker has {max_broker_partitions} partitions, exceeding the {limit_per_broker} limit for {cluster_info.instance_type}.',
                current_value=float(max_broker_partitions),
                threshold_value=float(limit_per_broker),
                evidence={'max_partitions': max_broker_partitions, 'limit': limit_per_broker, 'instance_type': cluster_info.instance_type}
            ))
        elif utilization >= 90.0:
            findings.append(Finding(
                metric_name='PartitionCount',
                severity=Severity.WARNING,
                category=Category.PERFORMANCE,
                title='Broker Partition Count Near Limit',
                description=f'At least one broker has {max_broker_partitions} partitions ({utilization:.1f}% of {limit_per_broker} limit for {cluster_info.instance_type}).',
                current_value=float(max_broker_partitions),
                threshold_value=float(limit_per_broker),
                evidence={'max_partitions': max_broker_partitions, 'limit': limit_per_broker, 'instance_type': cluster_info.instance_type}
            ))
    
    # Detect imbalance with different thresholds
    max_deviation = max(abs(val - cluster_avg) / cluster_avg * 100 if cluster_avg > 0 else 0 for val in all_values)
    
    # Different thresholds for different metrics
    if metric_name in ['MessagesInPerSec', 'PartitionCount', 'LeaderCount']:
        imbalance_threshold = 10.0  # Stricter threshold
    elif metric_name == 'ConnectionCount':
        imbalance_threshold = 15.0  # Medium threshold
    else:
        imbalance_threshold = 20.0  # Default threshold
    
    # For network metrics (BytesIn/Out), ignore imbalance if total volume is low (< 10MB/s per broker)
    ignore_imbalance = False
    if metric_name in ['BytesInPerSec', 'BytesOutPerSec']:
        avg_mb_per_sec = cluster_avg / (1024 * 1024)
        if avg_mb_per_sec < 10:
            ignore_imbalance = True
    
    # For MessagesInPerSec, ignore if very low volume (< 100 msg/s per broker)
    if metric_name == 'MessagesInPerSec' and cluster_avg < 100:
        ignore_imbalance = True
    
    # For CPU metrics, ignore imbalance if all brokers have low usage (< 30%)
    if metric_name in ['CpuUser', 'CpuSystem', 'CpuIdle'] and cluster_max < 30:
        ignore_imbalance = True
    
    # Create summary finding
    if max_deviation > imbalance_threshold and not ignore_imbalance:
        # Special recommendation for PartitionCount imbalance
        if metric_name == 'PartitionCount':
            partition_limits = {
                'kafka.t3.small': 300, 'kafka.m5.large': 1000, 'kafka.m5.xlarge': 1500,
                'kafka.m5.2xlarge': 2000, 'kafka.m5.4xlarge': 4000, 'kafka.m5.8xlarge': 8000,
                'kafka.m5.12xlarge': 12000, 'kafka.m5.16xlarge': 16000, 'kafka.m5.24xlarge': 24000,
                'kafka.m7g.large': 1000, 'kafka.m7g.xlarge': 1500, 'kafka.m7g.2xlarge': 2000,
                'kafka.m7g.4xlarge': 4000, 'kafka.m7g.8xlarge': 8000, 'kafka.m7g.12xlarge': 12000,
                'kafka.m7g.16xlarge': 16000, 'express.m7g.large': 1500, 'express.m7g.xlarge': 2000,
                'express.m7g.2xlarge': 4000, 'express.m7g.4xlarge': 8000, 'express.m7g.8xlarge': 16000,
                'express.m7g.12xlarge': 24000, 'express.m7g.16xlarge': 32000,
            }
            limit_per_broker = partition_limits.get(cluster_info.instance_type, 1000)
            total_capacity = limit_per_broker * len(broker_metrics)
            total_partitions = sum(all_values)
            capacity_utilization = (total_partitions / total_capacity) * 100
            
            # Check if there's available capacity for rebalancing
            if capacity_utilization < 80:
                tools = 'Use Cruise Control or kafka-reassign-partitions tool'
                if cluster_info.cluster_type == 'EXPRESS':
                    tools += ' or enable intelligent rebalancing'
                
                rebalance_action = (
                    f'Rebalance partitions across all {len(broker_metrics)} brokers. '
                    f'Cluster has {capacity_utilization:.1f}% capacity utilization ({int(total_partitions)}/{int(total_capacity)} partitions). '
                    f'{tools}.'
                )
            else:
                rebalance_action = (
                    f'Cluster capacity at {capacity_utilization:.1f}% ({int(total_partitions)}/{int(total_capacity)} partitions). '
                    f'Consider adding more brokers or upgrading instance type.'
                )
            
            description = (
                f'Partition distribution imbalance detected ({max_deviation:.1f}% deviation exceeds {imbalance_threshold}% threshold). '
                f'Cluster avg: {cluster_avg:.0f}, min: {cluster_min:.0f}, max: {cluster_max:.0f}. {rebalance_action}'
            )
        # Special recommendation for MessagesInPerSec imbalance
        elif metric_name == 'MessagesInPerSec':
            rebalance_action = (
                'Automatic rebalancing is enabled and will handle this.' if cluster_info.intelligent_rebalancing_enabled 
                else 'Consider rebalancing partitions using Cruise Control or manual reassignment.'
            )
            description = (
                f'Message distribution imbalance detected ({max_deviation:.1f}% deviation exceeds {imbalance_threshold}% threshold). '
                f'Cluster avg: {cluster_avg:.0f} msg/s, min: {cluster_min:.0f}, max: {cluster_max:.0f}. {rebalance_action}'
            )
        else:
            description = f'Significant imbalance detected ({max_deviation:.1f}% deviation). Cluster avg: {cluster_avg:.2f}, min: {cluster_min:.2f}, max: {cluster_max:.2f}'
        
        findings.append(Finding(
            metric_name=metric_name,
            severity=Severity.WARNING,
            category=Category.PERFORMANCE,
            title=f'{metric_name} - Broker Imbalance Detected',
            description=description,
            current_value=max_deviation,
            threshold_value=imbalance_threshold,
            evidence={'broker_count': len(broker_metrics), 'deviation': max_deviation}
        ))
    else:
        findings.append(Finding(
            metric_name=metric_name,
            severity=Severity.HEALTHY,
            category=Category.PERFORMANCE,
            title=f'{metric_name} - Balanced Across Brokers',
            description=f'Metric is balanced across {len(broker_metrics)} brokers. Avg: {cluster_avg:.2f}, min: {cluster_min:.2f}, max: {cluster_max:.2f}',
            current_value=cluster_avg,
            threshold_value=None,
            evidence={'broker_count': len(broker_metrics)}
        ))
    
    return findings
