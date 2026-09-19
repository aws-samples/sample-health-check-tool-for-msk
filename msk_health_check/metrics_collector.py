"""CloudWatch metrics collection module.

Every metric is queried once per broker (or once per cluster) with all five CloudWatch
statistics, so the analysis can pick the statistic that answers each question:

* ``Average`` of the time buckets for utilisation-style gauges (CPU, disk, heap, bytes/s);
* ``Maximum``/``Minimum`` for "did it ever happen" gauges (offline partitions, controller count);
* ``Sum`` per minute for connection metrics, which MSK publishes as one datapoint per network
  processor per minute - averaging those samples would under-count the broker total.

The collector also records which metrics were not published (monitoring level, no traffic) and
which queries failed, so the report can say "not assessed" instead of silently passing a check.
"""

import logging
import statistics as pystats
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

NAMESPACE = 'AWS/Kafka'
ALL_STATISTICS = ['Average', 'Maximum', 'Minimum', 'Sum', 'SampleCount']
DEFAULT_PERIOD_SECONDS = 3600


@dataclass
class MetricData:
    """Time-series data for a single metric (one broker, or the cluster)."""
    metric_name: str
    broker_id: Optional[str]              # None for cluster-level metrics
    timestamps: List[datetime]
    values: List[float]                   # effective series used by the analysis (see effective_stat)
    unit: str
    statistics: Dict[str, float]          # min, max, avg, p95, p99, last, peak, samples
    series: Dict[str, List[float]] = field(default_factory=dict)   # raw CloudWatch statistics
    period_seconds: int = DEFAULT_PERIOD_SECONDS
    effective_stat: str = 'Average'       # Average | Maximum | Minimum | SumPerMinute
    expected_points: int = 0
    coverage_pct: float = 0.0
    emitters_per_minute: float = 1.0      # datapoints published per minute (network processors)
    breakdown: Dict[str, float] = field(default_factory=dict)      # e.g. per Client Authentication
    breakdown_peak: Dict[str, float] = field(default_factory=dict)  # peak per breakdown key


@dataclass
class MetricsCollection:
    """Collection of all metrics for a cluster."""
    cluster_arn: str
    start_time: datetime
    end_time: datetime
    metrics: Dict[str, List[MetricData]]  # metric_name -> one MetricData per broker (or one for cluster)
    missing_metrics: List[str]            # metric names that returned no data at all
    period_seconds: int = DEFAULT_PERIOD_SECONDS
    collection_errors: Dict[str, List[str]] = field(default_factory=dict)  # metric -> error descriptions
    not_published: Dict[str, str] = field(default_factory=dict)            # metric -> reason
    partial_metrics: Dict[str, List[str]] = field(default_factory=dict)    # metric -> brokers without data
    attempted_metrics: List[str] = field(default_factory=list)
    monitoring_level: str = 'DEFAULT'
    discovery_available: bool = False

    def get(self, metric_name: str) -> List[MetricData]:
        return self.metrics.get(metric_name, [])

    def cluster_metric(self, metric_name: str) -> Optional[MetricData]:
        for m in self.metrics.get(metric_name, []):
            if m.broker_id is None:
                return m
        return None

    def broker_metrics(self, metric_name: str) -> List[MetricData]:
        return sorted(
            [m for m in self.metrics.get(metric_name, []) if m.broker_id is not None],
            key=lambda m: int(m.broker_id) if str(m.broker_id).isdigit() else 0,
        )


# Metric catalog. 'stat' is the statistic the analysis reads; 'monitoring' is the lowest
# enhanced-monitoring level at which MSK publishes the metric; 'kinds' says which broker types
# publish it. References: metrics-details.html (Standard) and metrics-details-express.html.
METRIC_CATALOG: Dict[str, Dict[str, Any]] = {
    # Cluster level
    # One sample per broker per minute (1 on the controller, 0 elsewhere): the cluster value is the Sum per minute
    'ActiveControllerCount': {'level': 'cluster', 'stat': 'Sum', 'unit': 'Count', 'monitoring': 'DEFAULT',
                              'kinds': ('standard', 'express'), 'title': 'Active Controller Count'},
    'OfflinePartitionsCount': {'level': 'cluster', 'stat': 'Maximum', 'unit': 'Count', 'monitoring': 'DEFAULT',
                               'kinds': ('standard',), 'title': 'Offline Partitions Count'},
    'GlobalPartitionCount': {'level': 'cluster', 'stat': 'Average', 'unit': 'Count', 'monitoring': 'DEFAULT',
                             'kinds': ('standard', 'express'), 'title': 'Global Partition Count'},
    'GlobalTopicCount': {'level': 'cluster', 'stat': 'Average', 'unit': 'Count', 'monitoring': 'DEFAULT',
                         'kinds': ('standard', 'express'), 'title': 'Global Topic Count'},
    # Per broker - utilisation gauges
    'CpuUser': {'level': 'broker', 'stat': 'Average', 'unit': 'Percent', 'monitoring': 'DEFAULT',
                'kinds': ('standard', 'express'), 'title': 'CPU User'},
    'CpuSystem': {'level': 'broker', 'stat': 'Average', 'unit': 'Percent', 'monitoring': 'DEFAULT',
                  'kinds': ('standard', 'express'), 'title': 'CPU System'},
    'CpuIdle': {'level': 'broker', 'stat': 'Average', 'unit': 'Percent', 'monitoring': 'DEFAULT',
                'kinds': ('standard', 'express'), 'title': 'CPU Idle'},
    'MemoryUsed': {'level': 'broker', 'stat': 'Average', 'unit': 'Bytes', 'monitoring': 'DEFAULT',
                   'kinds': ('standard', 'express'), 'title': 'Memory Used'},
    'MemoryFree': {'level': 'broker', 'stat': 'Average', 'unit': 'Bytes', 'monitoring': 'DEFAULT',
                   'kinds': ('standard', 'express'), 'title': 'Memory Free'},
    'HeapMemoryAfterGC': {'level': 'broker', 'stat': 'Average', 'unit': 'Percent', 'monitoring': 'DEFAULT',
                          'kinds': ('standard',), 'title': 'Heap Memory After GC'},
    'KafkaDataLogsDiskUsed': {'level': 'broker', 'stat': 'Average', 'unit': 'Percent', 'monitoring': 'DEFAULT',
                              'kinds': ('standard',), 'title': 'Data Logs Disk Used'},
    # Per broker - counts
    'LeaderCount': {'level': 'broker', 'stat': 'Average', 'unit': 'Count', 'monitoring': 'DEFAULT',
                    'kinds': ('standard', 'express'), 'title': 'Leader Count'},
    'PartitionCount': {'level': 'broker', 'stat': 'Average', 'unit': 'Count', 'monitoring': 'DEFAULT',
                       'kinds': ('standard', 'express'), 'title': 'Partition Count (including replicas)'},
    'UnderMinIsrPartitionCount': {'level': 'broker', 'stat': 'Maximum', 'unit': 'Count', 'monitoring': 'DEFAULT',
                                  'kinds': ('standard',), 'title': 'Under Min ISR Partitions'},
    'UnderReplicatedPartitions': {'level': 'broker', 'stat': 'Maximum', 'unit': 'Count', 'monitoring': 'DEFAULT',
                                  'kinds': ('standard',), 'title': 'Under-Replicated Partitions'},
    'StorageUsed': {'level': 'cluster', 'stat': 'Average', 'unit': 'Bytes', 'monitoring': 'DEFAULT',
                    'kinds': ('express',), 'title': 'Storage Used (cluster, excluding replicas)'},
    # Per broker - traffic
    'BytesInPerSec': {'level': 'broker', 'stat': 'Average', 'unit': 'Bytes/Second', 'monitoring': 'DEFAULT',
                      'kinds': ('standard', 'express'), 'title': 'Bytes In Per Second'},
    'BytesOutPerSec': {'level': 'broker', 'stat': 'Average', 'unit': 'Bytes/Second', 'monitoring': 'DEFAULT',
                       'kinds': ('standard', 'express'), 'title': 'Bytes Out Per Second'},
    'MessagesInPerSec': {'level': 'broker', 'stat': 'Average', 'unit': 'Count/Second', 'monitoring': 'DEFAULT',
                         'kinds': ('standard', 'express'), 'title': 'Messages In Per Second'},
    # Per broker - connections (one datapoint per network processor per minute -> Sum per minute)
    'ClientConnectionCount': {'level': 'broker', 'stat': 'Sum', 'unit': 'Count', 'monitoring': 'DEFAULT',
                              'kinds': ('standard', 'express'), 'title': 'Client Connections',
                              'optional_dimension': 'Client Authentication'},
    'ConnectionCount': {'level': 'broker', 'stat': 'Sum', 'unit': 'Count', 'monitoring': 'DEFAULT',
                        'kinds': ('standard', 'express'), 'title': 'Total Connections'},
    'ConnectionCreationRate': {'level': 'broker', 'stat': 'Sum', 'unit': 'Count/Second', 'monitoring': 'PER_BROKER',
                               'kinds': ('standard', 'express'), 'title': 'Connection Creation Rate'},
    'IAMTooManyConnections': {'level': 'broker', 'stat': 'Maximum', 'unit': 'Count', 'monitoring': 'PER_BROKER',
                              'kinds': ('standard', 'express'), 'title': 'IAM Too Many Connections',
                              'requires_auth': 'IAM'},
}

# Connection metrics are read as "Sum per minute" (broker total), everything else as its stat.
SUM_PER_MINUTE_METRICS = {'ClientConnectionCount', 'ConnectionCount', 'ConnectionCreationRate', 'ActiveControllerCount'}


def _catalog_view(kind: str) -> Dict[str, Dict[str, str]]:
    return {
        name: {'namespace': NAMESPACE, 'stat': spec['stat'], 'level': spec['level']}
        for name, spec in METRIC_CATALOG.items() if kind in spec['kinds']
    }


# Backwards-compatible views of the catalog
STANDARD_METRICS = _catalog_view('standard')
EXPRESS_METRICS = _catalog_view('express')


def metric_title(metric_name: str) -> str:
    spec = METRIC_CATALOG.get(metric_name)
    return spec['title'] if spec else metric_name


def metric_unit(metric_name: str) -> str:
    spec = METRIC_CATALOG.get(metric_name)
    return spec['unit'] if spec else ''


def metrics_for_cluster(cluster_type: str, monitoring_level: Optional[str] = None,
                        auth_methods: Optional[List[str]] = None) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """Split the catalog into metrics to query and metrics that cannot be published for this cluster.

    Returns (to_query, not_published) where not_published maps metric name -> reason.
    """
    from .reference import monitoring_level_rank

    kind = 'express' if cluster_type == 'EXPRESS' else 'standard'
    level_rank = monitoring_level_rank(monitoring_level)
    to_query: Dict[str, Dict[str, Any]] = {}
    not_published: Dict[str, str] = {}
    for name, spec in METRIC_CATALOG.items():
        if kind not in spec['kinds']:
            continue
        if monitoring_level is not None and monitoring_level_rank(spec['monitoring']) > level_rank:
            not_published[name] = f"requires enhanced monitoring level {spec['monitoring']} (cluster uses {monitoring_level})"
            continue
        required_auth = spec.get('requires_auth')
        if required_auth and auth_methods is not None and required_auth not in auth_methods:
            not_published[name] = f'only published when {required_auth} authentication is enabled'
            continue
        to_query[name] = spec
    return to_query, not_published


def _floor_to_period(ts: datetime, period_seconds: int) -> datetime:
    epoch = ts.timestamp()
    return datetime.fromtimestamp(epoch - (epoch % period_seconds), tz=timezone.utc)


def choose_period(days_back: float) -> int:
    """Bucket size for a window: 5 min up to 1 day, 15 min up to 5 days, 1 hour beyond (<= 1440 points)."""
    if days_back <= 1:
        return 300
    if days_back <= 5:
        return 900
    return DEFAULT_PERIOD_SECONDS


def compute_window(days_back: int, period_seconds: int = DEFAULT_PERIOD_SECONDS,
                   now: Optional[datetime] = None, not_before: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """Return an aligned [start, end) window ending at the last complete period.

    ``not_before`` (typically the cluster creation time) clamps the start so that coverage is
    measured against the time the cluster existed, not against the requested number of days.
    """
    now = now or datetime.now(timezone.utc)
    end_time = _floor_to_period(now, period_seconds)
    start_time = end_time - timedelta(days=days_back)
    if not_before is not None:
        if not_before.tzinfo is None:
            not_before = not_before.replace(tzinfo=timezone.utc)
        floor_nb = _floor_to_period(not_before, period_seconds)
        if floor_nb > start_time:
            start_time = floor_nb
    if start_time >= end_time:
        start_time = end_time - timedelta(seconds=period_seconds)
    return start_time, end_time


def percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    return float(np.percentile(values, pct))


def summarize(values: List[float]) -> Dict[str, float]:
    if not values:
        return {'min': 0.0, 'max': 0.0, 'avg': 0.0, 'p95': 0.0, 'p99': 0.0, 'last': 0.0, 'samples': 0}
    return {
        'min': float(np.min(values)),
        'max': float(np.max(values)),
        'avg': float(np.mean(values)),
        'p95': percentile(values, 95),
        'p99': percentile(values, 99),
        'last': float(values[-1]),
        'samples': len(values),
    }


def align_series(a: MetricData, b: MetricData) -> Tuple[List[datetime], List[float], List[float]]:
    """Return the timestamps present in both series and the two value lists aligned on them."""
    index_b = {ts: v for ts, v in zip(b.timestamps, b.values)}
    timestamps, va, vb = [], [], []
    for ts, v in zip(a.timestamps, a.values):
        if ts in index_b:
            timestamps.append(ts)
            va.append(v)
            vb.append(index_b[ts])
    return timestamps, va, vb


def sum_aligned(metrics: List[MetricData]) -> Tuple[List[datetime], List[float]]:
    """Point-wise sum of several series over the timestamps common to all of them."""
    if not metrics:
        return [], []
    common = set(metrics[0].timestamps)
    for m in metrics[1:]:
        common &= set(m.timestamps)
    timestamps = sorted(common)
    totals = []
    for ts in timestamps:
        totals.append(sum(dict(zip(m.timestamps, m.values))[ts] for m in metrics))
    return timestamps, totals


def _build_metric_data(metric_name: str, broker_id: Optional[str], datapoints: List[Dict[str, Any]],
                       primary_stat: str, period_seconds: int, expected_points: int) -> MetricData:
    datapoints = sorted(datapoints, key=lambda dp: dp['Timestamp'])
    timestamps = [dp['Timestamp'] for dp in datapoints]
    series: Dict[str, List[float]] = {}
    for stat in ALL_STATISTICS:
        if all(stat in dp for dp in datapoints):
            series[stat] = [float(dp[stat]) for dp in datapoints]

    minutes_per_period = max(period_seconds / 60.0, 1.0)
    emitters = 1.0
    if 'SampleCount' in series and series['SampleCount']:
        emitters = max(1.0, float(pystats.median(series['SampleCount'])) / minutes_per_period)

    if metric_name in SUM_PER_MINUTE_METRICS and 'Sum' in series:
        values = [v / minutes_per_period for v in series['Sum']]
        effective = 'SumPerMinute'
    elif primary_stat in series:
        values = list(series[primary_stat])
        effective = primary_stat
    elif 'Average' in series:
        values = list(series['Average'])
        effective = 'Average'
    else:
        # Fallback for partial responses (e.g. tests returning a single statistic)
        first_key = next((k for k in ALL_STATISTICS if k in datapoints[0]), None)
        values = [float(dp.get(first_key, 0.0)) for dp in datapoints]
        effective = first_key or 'Average'

    stats = summarize(values)
    if 'Maximum' in series and metric_name not in SUM_PER_MINUTE_METRICS:
        stats['peak'] = float(np.max(series['Maximum']))
    else:
        stats['peak'] = stats['max']
    if 'Minimum' in series:
        stats['floor'] = float(np.min(series['Minimum']))

    unit = datapoints[0].get('Unit', '') if datapoints else ''
    coverage = (len(values) / expected_points * 100.0) if expected_points else 0.0
    return MetricData(
        metric_name=metric_name, broker_id=broker_id, timestamps=timestamps, values=values,
        unit=unit or metric_unit(metric_name), statistics=stats, series=series,
        period_seconds=period_seconds, effective_stat=effective, expected_points=expected_points,
        coverage_pct=min(100.0, coverage), emitters_per_minute=emitters,
    )


def query_metric_with_retry(
    cloudwatch_client,
    metric_name: str,
    cluster_name: str,
    broker_id: Optional[str],
    start_time: datetime,
    end_time: datetime,
    max_retries: int = 3,
    period_seconds: int = DEFAULT_PERIOD_SECONDS,
    extra_dimensions: Optional[List[Dict[str, str]]] = None,
) -> Optional[MetricData]:
    """Query one metric with exponential backoff. Returns None on no data or when retries fail."""
    try:
        return _query_metric(cloudwatch_client, metric_name, cluster_name, broker_id, start_time, end_time,
                             max_retries, period_seconds, extra_dimensions)
    except ClientError as e:
        logger.error(f"Giving up on {metric_name}: {e.response.get('Error', {}).get('Code', 'ClientError')}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error querying {metric_name}: {e}")
        return None


def _query_metric(
    cloudwatch_client,
    metric_name: str,
    cluster_name: str,
    broker_id: Optional[str],
    start_time: datetime,
    end_time: datetime,
    max_retries: int = 3,
    period_seconds: int = DEFAULT_PERIOD_SECONDS,
    extra_dimensions: Optional[List[Dict[str, str]]] = None,
) -> Optional[MetricData]:
    """Query one metric with all statistics and exponential backoff on throttling.

    Returns None when the metric has no datapoints. Raises the last ClientError when every
    retry fails, so the collector can record the failure instead of treating it as "no data".
    """
    spec = METRIC_CATALOG.get(metric_name)
    if not spec:
        logger.warning(f"Metric {metric_name} not found in catalog")
        return None

    dimensions = [{'Name': 'Cluster Name', 'Value': cluster_name}]
    if broker_id:
        dimensions.append({'Name': 'Broker ID', 'Value': str(broker_id)})
    if extra_dimensions:
        dimensions.extend(extra_dimensions)

    expected_points = int((end_time - start_time).total_seconds() // period_seconds)
    last_error: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            response = cloudwatch_client.get_metric_statistics(
                Namespace=NAMESPACE,
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start_time,
                EndTime=end_time,
                Period=period_seconds,
                Statistics=ALL_STATISTICS,
            )
            datapoints = response.get('Datapoints', [])
            if not datapoints:
                return None
            return _build_metric_data(metric_name, broker_id, datapoints, spec['stat'],
                                      period_seconds, expected_points)
        except ClientError as e:
            last_error = e
            code = e.response.get('Error', {}).get('Code', '')
            if code in ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation'):
                raise
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    if last_error:
        raise last_error
    return None


def _merge_breakdown(parts: Dict[str, MetricData], metric_name: str, broker_id: str) -> MetricData:
    """Sum per-dimension series (e.g. per Client Authentication) into one broker series."""
    metrics = list(parts.values())
    timestamps, totals = sum_aligned(metrics)
    base = metrics[0]
    stats = summarize(totals)
    stats['peak'] = stats['max']
    merged = MetricData(
        metric_name=metric_name, broker_id=broker_id, timestamps=timestamps, values=totals,
        unit=base.unit, statistics=stats, series={}, period_seconds=base.period_seconds,
        effective_stat=base.effective_stat, expected_points=base.expected_points,
        coverage_pct=(len(totals) / base.expected_points * 100.0) if base.expected_points else 0.0,
        emitters_per_minute=sum(m.emitters_per_minute for m in metrics),
        breakdown={key: m.statistics['avg'] for key, m in parts.items()},
        breakdown_peak={key: m.statistics['max'] for key, m in parts.items()},
    )
    return merged


def discover_dimension_sets(cloudwatch_client, cluster_name: str, metric_names: List[str]) -> Optional[Dict[str, List[Tuple[str, ...]]]]:
    """Use ListMetrics to learn which dimension sets exist for each metric of this cluster.

    Returns None when ListMetrics is not permitted, so the collector can fall back to blind queries.
    """
    found: Dict[str, List[Tuple[str, ...]]] = {name: [] for name in metric_names}
    try:
        paginator = cloudwatch_client.get_paginator('list_metrics')
        for name in metric_names:
            for page in paginator.paginate(Namespace=NAMESPACE, MetricName=name,
                                           Dimensions=[{'Name': 'Cluster Name', 'Value': cluster_name}]):
                for metric in page.get('Metrics', []):
                    dims = tuple(sorted(d['Name'] for d in metric.get('Dimensions', [])))
                    if dims not in found[name]:
                        found[name].append(dims)
        return found
    except ClientError as e:
        logger.warning(f"ListMetrics not available ({e.response.get('Error', {}).get('Code')}); "
                       f"metric discovery skipped")
        return None
    except Exception as e:  # pragma: no cover - defensive
        logger.warning(f"Metric discovery failed: {e}")
        return None


def _list_dimension_values(cloudwatch_client, cluster_name: str, metric_name: str, dimension: str) -> List[str]:
    values: List[str] = []
    paginator = cloudwatch_client.get_paginator('list_metrics')
    for page in paginator.paginate(Namespace=NAMESPACE, MetricName=metric_name,
                                   Dimensions=[{'Name': 'Cluster Name', 'Value': cluster_name}]):
        for metric in page.get('Metrics', []):
            for d in metric.get('Dimensions', []):
                if d['Name'] == dimension and d['Value'] not in values:
                    values.append(d['Value'])
    return values


def collect_metrics(
    cloudwatch_client,
    cluster_arn: str,
    broker_count: int = 3,
    cluster_type: str = 'PROVISIONED',
    days_back: int = 30,
    monitoring_level: Optional[str] = None,
    auth_methods: Optional[List[str]] = None,
    period_seconds: Optional[int] = None,
    not_before: Optional[datetime] = None,
) -> MetricsCollection:
    """Collect all catalog metrics for a cluster.

    Args:
        cloudwatch_client: boto3 CloudWatch client
        cluster_arn: cluster ARN (the cluster name is derived from it)
        broker_count: number of brokers (per-broker metrics are queried for IDs 1..n)
        cluster_type: 'PROVISIONED' (Standard) or 'EXPRESS'
        days_back: window length in days
        monitoring_level: enhanced monitoring level, used to skip metrics MSK cannot publish
        auth_methods: authentication methods enabled on the cluster
        period_seconds: CloudWatch period per datapoint
    """
    if not_before is not None:
        nb = not_before if not_before.tzinfo else not_before.replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - nb).total_seconds() / 86400
        days_back = max(min(days_back, age_days), 1 / 24)
    period_seconds = period_seconds or choose_period(days_back)
    start_time, end_time = compute_window(days_back, period_seconds, not_before=not_before)
    cluster_name = cluster_arn.split('/')[-2]
    to_query, not_published = metrics_for_cluster(cluster_type, monitoring_level, auth_methods)

    logger.info(f"Collecting {len(to_query)} {cluster_type} metrics from {start_time.isoformat()} "
                f"to {end_time.isoformat()} ({days_back} days, period {period_seconds}s)")

    discovery = discover_dimension_sets(cloudwatch_client, cluster_name, list(to_query.keys()))
    if discovery is not None:
        for name in list(to_query.keys()):
            if not discovery.get(name):
                spec = to_query.pop(name)
                if spec['monitoring'] != 'DEFAULT':
                    not_published[name] = (f"not published by this cluster (requires enhanced monitoring "
                                           f"level {spec['monitoring']})")
                else:
                    not_published[name] = 'no datapoints published for this cluster in the last two weeks'

    metrics: Dict[str, List[MetricData]] = {}
    errors: Dict[str, List[str]] = {}
    partial: Dict[str, List[str]] = {}
    missing: List[str] = []

    def dimension_sets_for(name: str) -> List[Tuple[str, ...]]:
        return discovery.get(name, []) if discovery else []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for name, spec in to_query.items():
            if spec['level'] == 'cluster':
                futures.append((executor.submit(_query_metric, cloudwatch_client, name, cluster_name,
                                                None, start_time, end_time, 3, period_seconds), name, None, None))
                continue

            optional_dim = spec.get('optional_dimension')
            dim_sets = dimension_sets_for(name)
            plain = ('Broker ID', 'Cluster Name')
            has_plain = (not dim_sets) or plain in dim_sets
            use_breakdown = bool(optional_dim) and any(optional_dim in ds for ds in dim_sets)
            breakdown_values: List[str] = []
            if use_breakdown:
                try:
                    breakdown_values = _list_dimension_values(cloudwatch_client, cluster_name, name, optional_dim)
                except Exception as e:  # pragma: no cover - defensive
                    logger.warning(f"Could not list {optional_dim} values for {name}: {e}")
                    use_breakdown = False

            for broker_id in range(1, broker_count + 1):
                if has_plain:
                    futures.append((executor.submit(_query_metric, cloudwatch_client, name, cluster_name,
                                                    str(broker_id), start_time, end_time, 3, period_seconds),
                                    name, str(broker_id), None))
                if use_breakdown and breakdown_values:
                    for value in breakdown_values:
                        extra = [{'Name': optional_dim, 'Value': value}]
                        futures.append((executor.submit(_query_metric, cloudwatch_client, name,
                                                        cluster_name, str(broker_id), start_time, end_time, 3,
                                                        period_seconds, extra), name, str(broker_id), value))

        pending_breakdowns: Dict[Tuple[str, str], Dict[str, MetricData]] = {}
        for future, name, broker_id, breakdown_key in futures:
            label = f"broker {broker_id}" if broker_id else "cluster"
            try:
                data = future.result()
            except ClientError as e:
                code = e.response.get('Error', {}).get('Code', 'ClientError')
                errors.setdefault(name, []).append(f"{label}: {code}")
                logger.error(f"Error collecting {name} ({label}): {code}")
                continue
            except Exception as e:
                errors.setdefault(name, []).append(f"{label}: {e}")
                logger.error(f"Error collecting {name} ({label}): {e}")
                continue

            if data is None:
                if breakdown_key is None:
                    partial.setdefault(name, []).append(label)
                continue
            if breakdown_key is not None:
                pending_breakdowns.setdefault((name, broker_id), {})[breakdown_key] = data
            else:
                metrics.setdefault(name, []).append(data)
                logger.info(f"Collected {name} ({label}): {len(data.values)} data points, "
                            f"coverage {data.coverage_pct:.0f}%")

        for (name, broker_id), parts in pending_breakdowns.items():
            existing = next((m for m in metrics.get(name, []) if m.broker_id == broker_id), None)
            if existing is not None:
                # plain per-broker series already collected: attach the per-listener breakdown to it
                existing.breakdown = {key: m.statistics['avg'] for key, m in parts.items()}
                existing.breakdown_peak = {key: m.statistics['max'] for key, m in parts.items()}
                continue
            merged = _merge_breakdown(parts, name, broker_id)
            metrics.setdefault(name, []).append(merged)
            logger.info(f"Collected {name} (broker {broker_id}) from {len(parts)} authentication listeners")

    for name in to_query:
        if name not in metrics:
            missing.append(name)
            partial.pop(name, None)
            if name not in not_published and name not in errors:
                not_published[name] = 'no datapoints in the analysis window'

    for name, values in metrics.items():
        values.sort(key=lambda m: (m.broker_id is not None,
                                   int(m.broker_id) if m.broker_id and m.broker_id.isdigit() else 0))

    logger.info(f"Collected {len(metrics)}/{len(to_query)} metric types; "
                f"{len(not_published)} not published, {len(errors)} with errors")

    return MetricsCollection(
        cluster_arn=cluster_arn,
        start_time=start_time,
        end_time=end_time,
        metrics=metrics,
        missing_metrics=sorted(set(missing)),
        period_seconds=period_seconds,
        collection_errors=errors,
        not_published=not_published,
        partial_metrics=partial,
        attempted_metrics=sorted(to_query.keys()),
        monitoring_level=monitoring_level or 'DEFAULT',
        discovery_available=discovery is not None,
    )
