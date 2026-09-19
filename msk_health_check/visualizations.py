"""Chart generation with the CloudWatch GetMetricWidgetImage API.

Charts carry the threshold each check compares against as a horizontal annotation, so the
reader sees why a finding was raised without cross-referencing tables. Derived charts (CPU
User + System, connection totals per minute) use CloudWatch metric math.
"""

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .cluster_info import ClusterInfo
from .metrics_collector import MetricsCollection, METRIC_CATALOG, SUM_PER_MINUTE_METRICS, metric_title, metric_unit
from . import reference as ref

logger = logging.getLogger(__name__)

WIDGET_WIDTH = 800
WIDGET_HEIGHT = 400


@dataclass
class ChartImage:
    """Container for chart image data."""
    metric_name: str
    image_data: bytes
    title: str
    width: int = WIDGET_WIDTH
    height: int = WIDGET_HEIGHT
    caption: str = ''


def _annotations(metric_name: str, cluster_info: ClusterInfo) -> List[Dict[str, Any]]:
    limits = ref.get_instance_limits(cluster_info.instance_type)
    iam = 'IAM' in cluster_info.authentication_methods
    mb = 1024 * 1024
    ann: List[Dict[str, Any]] = []

    def line(value: float, label: str, color: str = '#d62728') -> None:
        ann.append({'value': value, 'label': label, 'color': color})

    if metric_name == 'CpuTotal':
        line(ref.CPU_TOTAL_MAX_PCT, 'AWS recommended limit 60%')
    elif metric_name == 'HeapMemoryAfterGC':
        line(ref.HEAP_AFTER_GC_MAX_PCT, 'AWS recommended limit 60%')
    elif metric_name == 'KafkaDataLogsDiskUsed':
        line(ref.DISK_ACTION_PCT, 'action threshold 85%')
        line(ref.DISK_WARNING_PCT, 'warning 75%', '#ff7f0e')
    elif metric_name in ('BytesInPerSec', 'BytesOutPerSec') and limits:
        sustained = limits.ingress_sustained_mbps if metric_name == 'BytesInPerSec' else limits.egress_sustained_mbps
        maximum = limits.ingress_max_mbps if metric_name == 'BytesInPerSec' else limits.egress_max_mbps
        basis = 'sustained limit' if limits.throughput_confidence == 'official' else 'tool guideline'
        if sustained:
            line(sustained * mb, f'{basis} {sustained:g} MB/s', '#ff7f0e')
        if maximum:
            line(maximum * mb, f'throttle quota {maximum:g} MB/s')
    elif metric_name == 'PartitionCount' and limits:
        line(limits.partitions_recommended, f'recommended {limits.partitions_recommended}', '#ff7f0e')
        line(limits.partitions_max, f'maximum {limits.partitions_max}')
    elif metric_name == 'ClientConnectionCount' and iam:
        quota = limits.iam_connections_per_broker if limits else 3000
        line(quota, f'IAM quota {quota} per broker')
    elif metric_name == 'ConnectionCreationRate' and iam:
        quota = limits.iam_connection_rate_per_sec if limits else 100
        line(quota, f'IAM quota {quota:g}/s per broker')
    elif metric_name in ('OfflinePartitionsCount', 'UnderMinIsrPartitionCount', 'UnderReplicatedPartitions'):
        line(0, 'expected 0', '#2ca02c')
    elif metric_name == 'ActiveControllerCount':
        line(1, 'expected 1', '#2ca02c')
    return ann


def _broker_metric(metric_name: str, cluster_name: str, broker_id: int, stat: str, label: str,
                   visible: bool = True, mid: Optional[str] = None) -> List[Any]:
    opts: Dict[str, Any] = {'stat': stat, 'label': label}
    if not visible:
        opts['visible'] = False
    if mid:
        opts['id'] = mid
    return ['AWS/Kafka', metric_name, 'Cluster Name', cluster_name, 'Broker ID', str(broker_id), opts]


def _create_widget_definition(metric_name: str, cluster_info: ClusterInfo, metrics: MetricsCollection) -> Dict[str, Any]:
    cluster = cluster_info.name
    brokers = range(1, cluster_info.broker_count + 1)
    period = metrics.period_seconds
    minutes = max(period // 60, 1)
    metrics_array: List[Any] = []
    y_label = metric_unit(metric_name)

    if metric_name == 'CpuTotal':
        for b in brokers:
            metrics_array.append(_broker_metric('CpuUser', cluster, b, 'Average', f'user {b}', False, f'u{b}'))
            metrics_array.append(_broker_metric('CpuSystem', cluster, b, 'Average', f'system {b}', False, f's{b}'))
            metrics_array.append([{'expression': f'u{b} + s{b}', 'label': f'Broker {b} (User + System)', 'id': f'c{b}'}])
        y_label = 'Percent'
        title = 'CPU User + System per broker (average per bucket)'
    elif metric_name in SUM_PER_MINUTE_METRICS:
        for b in brokers:
            metrics_array.append(_broker_metric(metric_name, cluster, b, 'Sum', f'sum {b}', False, f'm{b}'))
            metrics_array.append([{'expression': f'm{b} / {minutes}', 'label': f'Broker {b}', 'id': f't{b}'}])
        title = f'{metric_title(metric_name)} per broker (total per minute, mean per bucket)'
    else:
        spec = METRIC_CATALOG.get(metric_name, {'level': 'broker', 'stat': 'Average'})
        stat = spec['stat']
        if spec['level'] == 'cluster':
            metrics_array.append(['AWS/Kafka', metric_name, 'Cluster Name', cluster, {'stat': stat, 'label': metric_title(metric_name)}])
        else:
            for b in brokers:
                metrics_array.append(_broker_metric(metric_name, cluster, b, stat, f'Broker {b}'))
        stat_label = {'Average': 'average per bucket', 'Maximum': 'maximum per bucket', 'Minimum': 'minimum per bucket'}.get(stat, stat)
        title = f'{metric_title(metric_name)} ({stat_label})'

    widget: Dict[str, Any] = {
        'width': WIDGET_WIDTH,
        'height': WIDGET_HEIGHT,
        'metrics': metrics_array,
        'period': period,
        'region': cluster_info.region,
        'title': title,
        'view': 'timeSeries',
        'stacked': False,
        'yAxis': {'left': {'label': y_label, 'min': 0, 'showUnits': False}},
        'start': metrics.start_time.isoformat(),
        'end': metrics.end_time.isoformat(),
        'legend': {'position': 'bottom'},
        'timezone': '+0000',
    }
    annotations = _annotations(metric_name, cluster_info)
    if annotations:
        widget['annotations'] = {'horizontal': annotations}
    return widget


def chart_names_for(metrics: MetricsCollection) -> List[str]:
    """Which charts to render: every collected metric plus the derived CPU total."""
    names = [n for n in METRIC_CATALOG if n in metrics.metrics]
    if 'CpuUser' in metrics.metrics and 'CpuSystem' in metrics.metrics:
        names.insert(0, 'CpuTotal')
    for skip in ('CpuIdle', 'MemoryFree'):
        if skip in names:
            names.remove(skip)
    return names


def create_charts(cloudwatch_client, cluster_info: ClusterInfo, metrics: MetricsCollection) -> List[ChartImage]:
    """Render one chart per collected metric with the CloudWatch widget image API."""
    charts: List[ChartImage] = []
    for metric_name in chart_names_for(metrics):
        try:
            widget = _create_widget_definition(metric_name, cluster_info, metrics)
            response = cloudwatch_client.get_metric_widget_image(MetricWidget=json.dumps(widget))
            charts.append(ChartImage(metric_name=metric_name, image_data=response['MetricWidgetImage'],
                                     title=widget['title'], caption=_caption(metric_name)))
            logger.info(f"Created chart for {metric_name}")
        except Exception as e:
            logger.warning(f"Failed to create chart for {metric_name}: {e}")
    return charts


def _caption(metric_name: str) -> str:
    if metric_name == 'CpuTotal':
        return 'Series are CloudWatch metric math (CpuUser + CpuSystem) per broker; the red line is the AWS recommended limit.'
    if metric_name in SUM_PER_MINUTE_METRICS:
        return ('Broker total estimated as Sum of the per-network-processor samples divided by the minutes in the period; '
                'red line is the IAM quota where applicable.')
    spec = METRIC_CATALOG.get(metric_name)
    if spec and spec['stat'] in ('Maximum', 'Minimum'):
        return f'Plotted with the {spec["stat"]} statistic so short events inside an hour remain visible.'
    return 'Averages per bucket; peaks inside a bucket are summarised in the statistics table.'


# Backwards-compatible helpers used by older callers/tests
def _get_metric_title(metric_name: str) -> str:
    return metric_title(metric_name)


def _get_metric_unit(metric_name: str) -> str:
    return metric_unit(metric_name) or 'Value'
