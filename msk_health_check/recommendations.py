"""Recommendation generation module."""

from dataclasses import dataclass
from typing import Dict, List, Optional

from .analyzer import AnalysisResult, Finding, Severity, Category


@dataclass
class Recommendation:
    """Actionable recommendation with context."""
    finding: Finding
    action: str
    rationale: str
    impact: str  # Impact of NOT following the recommendation
    documentation_links: List[str]
    priority: int  # 1 (highest) to 5 (lowest)
    estimated_impact: str  # Description of expected improvement


# Recommendation templates
RECOMMENDATION_TEMPLATES = {
    'ActiveControllerCount': {
        'action': 'Investigate cluster controller stability and restart brokers if needed',
        'rationale': 'Controller instability can lead to cluster-wide issues',
        'impact': 'Cluster cannot process metadata changes, leading to service disruption',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/troubleshooting.html'],
        'estimated_impact': 'Restore cluster stability and prevent data loss'
    },
    'OfflinePartitionsCount': {
        'action': 'Investigate and recover offline partitions immediately',
        'rationale': 'Offline partitions indicate data unavailability',
        'impact': 'Data unavailability and potential data loss',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/troubleshooting.html'],
        'estimated_impact': 'Restore data availability'
    },
    'KafkaDataLogsDiskUsed': {
        'action': 'Increase broker storage or enable auto-scaling storage',
        'rationale': 'High disk usage can lead to broker failures',
        'impact': 'Broker failures and data loss when disk space is exhausted',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/msk-storage.html'],
        'estimated_impact': 'Prevent broker failures and data loss'
    },
    'CpuUsage': {
        'action': 'Scale up to larger instance type or add more brokers',
        'rationale': 'High CPU usage impacts throughput and latency',
        'impact': 'Reduced throughput, increased latency, and potential message loss',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/msk-scaling.html'],
        'estimated_impact': 'Improve throughput and reduce latency'
    },
    'CpuTotal': {
        'action': 'Scale up to larger instance type or add more brokers to distribute load',
        'rationale': 'Total CPU usage above 60% indicates broker is under heavy load',
        'impact': 'Reduced throughput, increased latency, and potential message loss',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html',
                 'https://docs.aws.amazon.com/msk/latest/developerguide/msk-scaling.html'],
        'estimated_impact': 'Improve throughput, reduce latency, and increase cluster capacity'
    },
    'MemoryUsage': {
        'action': 'Scale up to instance type with more memory',
        'rationale': 'High memory usage can cause performance degradation',
        'impact': 'Performance degradation and potential out-of-memory errors',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html'],
        'estimated_impact': 'Improve broker stability and performance'
    },
    'HeapMemoryAfterGC': {
        'action': 'Scale up to instance type with more memory',
        'rationale': 'High heap memory after GC indicates memory pressure',
        'impact': 'Frequent garbage collection pauses affecting performance',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html'],
        'estimated_impact': 'Reduce GC pauses and improve performance'
    },
    'UnderMinIsrPartitionCount': {
        'action': 'Investigate broker health and network connectivity',
        'rationale': 'Partitions under min ISR risk data loss',
        'impact': 'Risk of data loss if broker fails',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/troubleshooting.html'],
        'estimated_impact': 'Ensure data durability and prevent data loss'
    },
    'ClientConnectionCount': {
        'action': 'Implement connection pooling or scale up instance type',
        'rationale': 'High connection count can exhaust broker resources',
        'impact': 'Connection exhaustion preventing new clients from connecting',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html'],
        'estimated_impact': 'Prevent connection exhaustion and improve stability'
    },
    'ConnectionCreationRate': {
        'action': 'Implement connection pooling, increase client timeouts, and add exponential backoff with circuit breaker pattern',
        'rationale': 'High connection creation rate indicates missing connection pooling, short timeouts, or client instability. New connections are expensive (CPU overhead) and IAM auth is limited to 100 connections/sec',
        'impact': 'Increased CPU usage, potential connection throttling (IAM auth), and reduced cluster performance',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html',
                 'https://docs.aws.amazon.com/msk/latest/developerguide/client-access.html'],
        'estimated_impact': 'Reduce CPU overhead, prevent connection throttling, and improve overall cluster stability'
    },
    'ConnectionChurn': {
        'action': 'Implement connection pooling and review client timeout configurations',
        'rationale': 'High connection churn creates unnecessary overhead and may indicate missing connection pooling or network issues',
        'impact': 'Increased CPU overhead, higher latency, and reduced cluster stability',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html'],
        'estimated_impact': 'Reduce CPU overhead, improve latency, and increase cluster stability'
    },
    'PartitionCount': {
        'action': 'Add more brokers or upgrade to larger instance type to handle partition load',
        'rationale': 'Excessive partitions per broker degrades performance and increases latency',
        'impact': 'Performance degradation, increased latency, and potential broker overload',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html#partitions-per-broker'],
        'estimated_impact': 'Improve throughput, reduce latency, and prevent broker overload'
    },
    'Authentication': {
        'action': 'Enable IAM, SASL/SCRAM, or mTLS authentication and disable unauthenticated access',
        'rationale': 'Unauthenticated access is a critical security risk',
        'impact': 'Unauthorized access to cluster data and potential data breaches',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/msk-authentication.html'],
        'estimated_impact': 'Secure cluster access and meet compliance requirements'
    },
    'InstanceType': {
        'action': 'Migrate to Graviton-based instances for cost savings',
        'rationale': 'Graviton instances offer better price-performance',
        'impact': 'Higher costs and reduced performance compared to Graviton',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/graviton.html'],
        'estimated_impact': 'Reduce compute costs by ~20%'
    },
    'KafkaVersion': {
        'action': 'Upgrade to AWS recommended Kafka version',
        'rationale': 'Newer versions include critical security patches, bug fixes, and performance improvements',
        'impact': 'Security vulnerabilities, missing features, and limited support',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html'],
        'estimated_impact': 'Improve security, stability, and access to latest Kafka features'
    },
    'AvailabilityZones': {
        'action': 'Deploy cluster across 3 availability zones for production workloads',
        'rationale': 'Multi-AZ deployment provides fault tolerance and high availability',
        'impact': 'Reduced fault tolerance and potential service disruption during AZ failures',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html#multi-az'],
        'estimated_impact': 'Improve fault tolerance and ensure business continuity'
    },
    'StorageAutoScaling': {
        'action': 'Enable storage auto-scaling to automatically manage disk capacity',
        'rationale': 'Prevents disk space exhaustion and broker failures',
        'impact': 'Manual intervention required and risk of disk space exhaustion',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/msk-autoexpand.html'],
        'estimated_impact': 'Prevent disk-related outages and reduce operational overhead'
    },
    'Logging': {
        'action': 'Enable broker logs to CloudWatch, S3, or Firehose',
        'rationale': 'Logging is essential for troubleshooting, compliance, and security auditing',
        'impact': 'Limited troubleshooting capabilities and compliance issues',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/msk-logging.html'],
        'estimated_impact': 'Enable troubleshooting, meet compliance requirements, and improve security posture'
    },
    'IntelligentRebalancing': {
        'action': 'Enable intelligent rebalancing for MSK Express cluster',
        'rationale': 'Intelligent rebalancing automatically maintains balanced partition distribution',
        'impact': 'Unbalanced load distribution and suboptimal performance',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/msk-express.html'],
        'estimated_impact': 'Improve cluster performance and maintain balanced load distribution'
    },
    'MessagesInPerSec': {
        'action': 'Rebalance partitions using Cruise Control or manual reassignment',
        'rationale': 'Message distribution imbalance indicates uneven partition distribution across brokers',
        'impact': 'Uneven load distribution leading to hotspots and suboptimal performance',
        'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/cruise-control.html',
                 'https://kafka.apache.org/documentation/#basic_ops_cluster_expansion'],
        'estimated_impact': 'Improve cluster performance and prevent broker hotspots'
    },
}


def generate_recommendations(analysis: AnalysisResult) -> List[Recommendation]:
    """
    Generate prioritized recommendations from analysis.
    
    Args:
        analysis: Complete analysis result
        
    Returns:
        List of prioritized recommendations
    """
    recommendations = []
    
    for finding in analysis.findings:
        if finding.severity in [Severity.CRITICAL, Severity.WARNING, Severity.INFORMATIONAL]:
            rec = create_recommendation_for_finding(finding)
            if rec:
                recommendations.append(rec)
    
    # Sort by priority (1 = highest)
    recommendations.sort(key=lambda r: r.priority)
    
    return recommendations


def create_recommendation_for_finding(finding: Finding) -> Optional[Recommendation]:
    """
    Create specific recommendation for a finding.
    
    Args:
        finding: Analysis finding
        
    Returns:
        Recommendation with action and context, or None if not actionable
    """
    # Skip recommendations for low-impact imbalances
    if finding.metric_name in ['ClientConnectionCount', 'ConnectionCount', 'MemoryUsed']:
        if 'Imbalance' in finding.title and finding.current_value < 10:
            return None  # Too low to be actionable
    
    # Special handling for partition count based on finding title/description
    if finding.metric_name == 'PartitionCount':
        if 'Low Partition Utilization' in finding.title:
            template = {
                'action': 'Consider downsizing cluster by reducing broker count or using smaller instance types',
                'rationale': 'Low partition utilization indicates over-provisioning, leading to unnecessary costs',
                'impact': 'Higher operational costs than necessary',
                'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html#right-sizing'],
                'estimated_impact': 'Reduce operational costs while maintaining adequate capacity'
            }
        elif 'Imbalance' in finding.title and 'Rebalance partitions' in finding.description:
            # Extract the rebalancing recommendation from the description
            template = {
                'action': finding.description.split('. ')[2] if len(finding.description.split('. ')) > 2 else 'Rebalance partitions across all brokers',
                'rationale': 'Uneven partition distribution causes performance hotspots and inefficient resource utilization',
                'impact': 'Performance degradation on overloaded brokers and underutilization of available capacity',
                'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html#partitions-per-broker'],
                'estimated_impact': 'Improve cluster balance, optimize resource utilization, and enhance overall performance'
            }
        else:  # High/Excessive partition count
            template = {
                'action': 'Add more brokers or upgrade to larger instance type to handle partition load',
                'rationale': 'Excessive partitions per broker degrades performance and increases latency',
                'impact': 'Performance degradation, increased latency, and potential broker overload',
                'docs': ['https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html#partitions-per-broker'],
                'estimated_impact': 'Improve throughput, reduce latency, and prevent broker overload'
            }
    else:
        template = RECOMMENDATION_TEMPLATES.get(finding.metric_name)
    
    if not template:
        # Generic recommendation
        template = {
            'action': f'Review and address {finding.title}',
            'rationale': finding.description,
            'impact': 'May affect cluster performance or reliability',
            'docs': ['https://docs.aws.amazon.com/msk/'],
            'estimated_impact': 'Improve cluster health'
        }
    
    # Determine priority based on severity and context
    priority_map = {
        Severity.CRITICAL: 1,
        Severity.WARNING: 2,
        Severity.INFORMATIONAL: 3,
        Severity.HEALTHY: 5
    }
    
    base_priority = priority_map.get(finding.severity, 3)
    
    # Adjust priority based on context
    # Partition imbalance with available capacity is high priority
    if finding.metric_name == 'PartitionCount' and 'Imbalance' in finding.title:
        if 'Rebalance partitions' in finding.description:
            base_priority = 1  # Highest priority - actionable with available capacity
    
    # High CPU usage is critical priority
    if finding.metric_name == 'CpuTotal' and finding.severity == Severity.CRITICAL:
        base_priority = 1  # Highest priority - performance impact
    
    # Memory/Connection imbalance with low usage is lower priority
    if finding.metric_name in ['MemoryUsed', 'ClientConnectionCount', 'ConnectionCount']:
        if 'Imbalance' in finding.title and finding.current_value < 50:
            base_priority = min(base_priority + 1, 4)  # Lower priority if usage is low
    
    # Authentication warnings are important but not urgent if cluster is stable
    if finding.metric_name == 'Authentication' and finding.severity == Severity.WARNING:
        base_priority = 2  # Important but not critical
    
    return Recommendation(
        finding=finding,
        action=template['action'],
        rationale=template['rationale'],
        impact=template.get('impact', 'May affect cluster performance or reliability'),
        documentation_links=template['docs'],
        priority=base_priority,
        estimated_impact=template['estimated_impact']
    )
