"""Visualization engine using CloudWatch GetMetricWidgetImage."""

import json
import logging
from dataclasses import dataclass
from typing import List

from .cluster_info import ClusterInfo
from .metrics_collector import MetricsCollection, STANDARD_METRICS, EXPRESS_METRICS

logger = logging.getLogger(__name__)


@dataclass
class ChartImage:
    """Container for chart image data."""
    metric_name: str
    image_data: bytes
    title: str


def create_charts(
    cloudwatch_client,
    cluster_info: ClusterInfo,
    metrics: MetricsCollection
) -> List[ChartImage]:
    """
    Create charts using CloudWatch GetMetricWidgetImage API.
    For per-broker metrics, creates a single chart with multiple lines (one per broker).
    
    Args:
        cloudwatch_client: Boto3 CloudWatch client
        cluster_info: Cluster information
        metrics: Collected metrics
        
    Returns:
        List of ChartImage objects
    """
    charts = []
    region = cluster_info.arn.split(':')[3]
    metric_defs = EXPRESS_METRICS if cluster_info.cluster_type == 'EXPRESS' else STANDARD_METRICS
    
    # Group metrics by name (to handle per-broker metrics)
    for metric_name in metrics.metrics.keys():
        try:
            metric_def = metric_defs.get(metric_name)
            if not metric_def:
                continue
            
            widget_def = _create_widget_definition(
                metric_name=metric_name,
                cluster_name=cluster_info.name,
                broker_count=cluster_info.broker_count,
                metric_level=metric_def['level'],
                stat=metric_def['stat'],
                region=region,
                start_time=metrics.start_time,
                end_time=metrics.end_time
            )
            
            response = cloudwatch_client.get_metric_widget_image(
                MetricWidget=json.dumps(widget_def)
            )
            
            charts.append(ChartImage(
                metric_name=metric_name,
                image_data=response['MetricWidgetImage'],
                title=_get_metric_title(metric_name)
            ))
            
            logger.info(f"Created chart for {metric_name}")
            
        except Exception as e:
            logger.warning(f"Failed to create chart for {metric_name}: {e}")
    
    return charts


def _create_widget_definition(
    metric_name: str,
    cluster_name: str,
    broker_count: int,
    metric_level: str,
    stat: str,
    region: str,
    start_time,
    end_time
):
    """Create CloudWatch widget definition with multiple lines for per-broker metrics."""
    
    metrics_array = []
    
    if metric_level == 'broker':
        # Create one line per broker
        for broker_id in range(1, broker_count + 1):
            metrics_array.append([
                "AWS/Kafka", 
                metric_name, 
                "Cluster Name", cluster_name,
                "Broker ID", str(broker_id),
                {"stat": stat, "label": f"Broker {broker_id}"}
            ])
    else:
        # Cluster-level metric
        metrics_array.append([
            "AWS/Kafka", 
            metric_name, 
            "Cluster Name", cluster_name,
            {"stat": stat}
        ])
    
    return {
        "width": 600,
        "height": 300,
        "metrics": metrics_array,
        "period": 3600,
        "stat": stat,
        "region": region,
        "title": _get_metric_title(metric_name),
        "yAxis": {
            "left": {
                "label": _get_metric_unit(metric_name)
            }
        },
        "start": start_time.isoformat(),
        "end": end_time.isoformat(),
        "legend": {
            "position": "bottom"
        }
    }


def _get_metric_title(metric_name: str) -> str:
    """Get human-readable title for metric."""
    titles = {
        # Standard metrics
        'ActiveControllerCount': 'Active Controller Count',
        'GlobalPartitionCount': 'Global Partition Count',
        'GlobalTopicCount': 'Global Topic Count',
        'OfflinePartitionsCount': 'Offline Partitions Count',
        'HeapMemoryAfterGC': 'Heap Memory After GC',
        'CpuUser': 'CPU User',
        'CpuSystem': 'CPU System',
        'CpuIdle': 'CPU Idle',
        'MemoryUsed': 'Memory Used',
        'MemoryFree': 'Memory Free',
        'KafkaDataLogsDiskUsed': 'Disk Used',
        'LeaderCount': 'Leader Count',
        'PartitionCount': 'Partition Count',
        'ClientConnectionCount': 'Client Connections',
        'ConnectionCount': 'Total Connections',
        'ConnectionCreationRate': 'Connection Creation Rate',
        'UnderMinIsrPartitionCount': 'Under Min ISR Partitions',
        'BytesInPerSec': 'Bytes In Per Second',
        'BytesOutPerSec': 'Bytes Out Per Second',
        # Express metrics
        'ClusterActiveConnectionCount': 'Cluster Active Connections',
        'ClusterBytesInPerSec': 'Cluster Bytes In Per Second',
        'ClusterBytesOutPerSec': 'Cluster Bytes Out Per Second',
        'ClusterMessagesInPerSec': 'Cluster Messages In Per Second',
    }
    return titles.get(metric_name, metric_name.replace('_', ' ').title())


def _get_metric_unit(metric_name: str) -> str:
    """Get unit label for metric."""
    units = {
        'ActiveControllerCount': 'Count',
        'GlobalPartitionCount': 'Partitions',
        'GlobalTopicCount': 'Topics',
        'OfflinePartitionsCount': 'Partitions',
        'HeapMemoryAfterGC': 'Percent',
        'CpuUser': 'Percent',
        'CpuSystem': 'Percent',
        'CpuIdle': 'Percent',
        'MemoryUsed': 'Bytes',
        'MemoryFree': 'Bytes',
        'KafkaDataLogsDiskUsed': 'Percent',
        'LeaderCount': 'Count',
        'PartitionCount': 'Count',
        'ClientConnectionCount': 'Count',
        'ConnectionCount': 'Count',
        'ConnectionCreationRate': 'Connections/sec',
        'UnderMinIsrPartitionCount': 'Count',
        'BytesInPerSec': 'Bytes/sec',
        'BytesOutPerSec': 'Bytes/sec',
        'ClusterActiveConnectionCount': 'Count',
        'ClusterBytesInPerSec': 'Bytes/sec',
        'ClusterBytesOutPerSec': 'Bytes/sec',
        'ClusterMessagesInPerSec': 'Messages/sec',
    }
    return units.get(metric_name, 'Value')
