"""Metrics and configuration analysis.

Each check produces one Finding with an explicit status. A check that cannot run (metric not
published, permission missing, broker size not in the catalog) produces a NOT_ASSESSED finding
instead of silently passing, so the report can show what was and was not evaluated.
"""

import logging
import re
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from .cluster_info import ClusterInfo, parse_version
from .metrics_collector import METRIC_CATALOG, MetricData, MetricsCollection, align_series, summarize, metric_title
from . import reference as ref

logger = logging.getLogger(__name__)


class Severity(Enum):
    """Severity levels for findings."""
    CRITICAL = "critical"
    WARNING = "warning"
    INFORMATIONAL = "informational"
    HEALTHY = "healthy"
    NOT_ASSESSED = "not_assessed"


class Category(Enum):
    """Optimization categories for findings."""
    RELIABILITY = "reliability"
    SECURITY = "security"
    PERFORMANCE = "performance"
    COST = "cost"


CATEGORY_WEIGHTS = {
    Category.RELIABILITY: 0.35,
    Category.PERFORMANCE: 0.30,
    Category.SECURITY: 0.20,
    Category.COST: 0.15,
}

SEVERITY_ORDER = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFORMATIONAL: 2,
                  Severity.HEALTHY: 3, Severity.NOT_ASSESSED: 4}


@dataclass
class Finding:
    """Result of one check."""
    metric_name: str                      # metric or check key (kept for compatibility)
    severity: Severity
    category: Category
    title: str
    description: str
    current_value: Optional[float]
    threshold_value: Optional[float]
    evidence: Dict[str, Any]
    check_id: str = ''
    confidence: str = 'high'              # high | medium | low
    source: str = ''                      # key in reference.DOCS or a label
    affected_brokers: List[str] = field(default_factory=list)
    observed: str = ''                    # human-readable observed value
    threshold: str = ''                   # human-readable threshold
    section: str = 'metric'               # metric | configuration | derived
    chart_metric: Optional[str] = None    # chart to attach in the report
    reason: str = ''                      # why the check was not assessed

    @property
    def source_url(self) -> str:
        return ref.DOCS.get(self.source, self.source if self.source.startswith('http') else '')


@dataclass
class AnalysisResult:
    """Complete analysis output."""
    cluster_info: ClusterInfo
    metrics: MetricsCollection
    findings: List[Finding]
    overall_health_score: float           # 0-100
    category_scores: Dict[str, float] = field(default_factory=dict)
    overall_status: str = 'Healthy'       # Healthy | Needs Attention | Critical
    checks_total: int = 0
    checks_assessed: int = 0
    rules_version: str = ref.RULES_VERSION
    workload: str = 'production'
    version_reference: Dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------- helpers

def get_cluster_metric(metric_list: List[MetricData]) -> Optional[MetricData]:
    for m in metric_list or []:
        if m.broker_id is None:
            return m
    return None


def get_broker_metrics(metric_list: List[MetricData]) -> List[MetricData]:
    return [m for m in metric_list or [] if m.broker_id is not None]


def _fmt(value: float, unit: str = '') -> str:
    if unit in ('Percent', '%'):
        return f'{value:.1f}%'
    if unit in ('Bytes/Second', 'MB/s'):
        return f'{value / (1024 * 1024):.2f} MB/s' if unit == 'Bytes/Second' else f'{value:.2f} MB/s'
    if unit == 'Bytes':
        return f'{value / (1024 ** 3):.2f} GiB'
    if abs(value) >= 100 or float(value).is_integer():
        return f'{value:,.0f}'
    return f'{value:.2f}'


def _brokers(metrics: List[MetricData]) -> List[str]:
    return [str(m.broker_id) for m in metrics if m.broker_id is not None]


def _missing_reason(metrics: MetricsCollection, metric_name: str) -> str:
    if metric_name in metrics.not_published:
        return f'{metric_name}: {metrics.not_published[metric_name]}'
    if metric_name in metrics.collection_errors:
        return f'{metric_name}: collection failed ({"; ".join(metrics.collection_errors[metric_name][:3])})'
    if metric_name not in metrics.attempted_metrics:
        return f'{metric_name}: not applicable to this cluster type'
    return f'{metric_name}: no datapoints in the analysis window'


def not_assessed(check_id: str, metric_name: str, category: Category, title: str, reason: str,
                 section: str = 'metric', source: str = '') -> Finding:
    return Finding(metric_name=metric_name, severity=Severity.NOT_ASSESSED, category=category, title=title,
                   description=f'This check could not be evaluated. {reason}', current_value=None,
                   threshold_value=None, evidence={'reason': reason}, check_id=check_id, confidence='high',
                   source=source, section=section, reason=reason)


def _finding(check_id: str, metric_name: str, severity: Severity, category: Category, title: str,
             description: str, *, value: Optional[float] = None, threshold: Optional[float] = None,
             evidence: Optional[Dict[str, Any]] = None, confidence: str = 'high', source: str = '',
             brokers: Optional[List[str]] = None, observed: str = '', threshold_text: str = '',
             section: str = 'metric', chart: Optional[str] = None) -> Finding:
    return Finding(metric_name=metric_name, severity=severity, category=category, title=title,
                   description=description, current_value=value, threshold_value=threshold,
                   evidence=evidence or {}, check_id=check_id, confidence=confidence, source=source,
                   affected_brokers=brokers or [], observed=observed, threshold=threshold_text,
                   section=section, chart_metric=chart)


def _imbalance(values: Dict[str, float], threshold_pct: float, min_activity: float) -> Dict[str, Any]:
    """Deviation of the most loaded broker from the mean, or a reason why it is not relevant."""
    if len(values) < 2:
        return {'relevant': False, 'reason': 'fewer than two brokers'}
    mean = float(np.mean(list(values.values())))
    if mean <= 0 or mean < min_activity:
        return {'relevant': False, 'reason': f'activity too low to matter (mean {mean:.1f} < {min_activity:g})',
                'mean': mean}
    hottest = max(values, key=values.get)
    coldest = min(values, key=values.get)
    deviation = (values[hottest] - mean) / mean * 100.0
    return {'relevant': True, 'mean': mean, 'max': values[hottest], 'min': values[coldest],
            'hottest': hottest, 'coldest': coldest, 'deviation_pct': deviation,
            'imbalanced': deviation > threshold_pct, 'threshold_pct': threshold_pct}


def _linear_growth_per_day(metric: MetricData) -> Optional[float]:
    """Slope (units per day) of a least-squares fit over the metric series."""
    if len(metric.values) < 24:
        return None
    t0 = metric.timestamps[0]
    days = np.array([(ts - t0).total_seconds() / 86400.0 for ts in metric.timestamps])
    if days[-1] - days[0] < 1.0:
        return None
    slope, _ = np.polyfit(days, np.array(metric.values, dtype=float), 1)
    return float(slope)


# --------------------------------------------------------------------------- scoring

def _calculate_category_score(findings: List[Finding]) -> float:
    """100 reduced multiplicatively: x0.60 per CRITICAL, x0.85 per WARNING. Informational findings
    and checks that were not assessed do not change the score."""
    score = 100.0
    for f in findings:
        if f.severity == Severity.CRITICAL:
            score *= 0.60
        elif f.severity == Severity.WARNING:
            score *= 0.85
    return max(0.0, score)


def _calculate_category_scores(findings: List[Finding]) -> Dict[Category, float]:
    grouped: Dict[Category, List[Finding]] = {c: [] for c in CATEGORY_WEIGHTS}
    for f in findings:
        if f.category in grouped:
            grouped[f.category].append(f)
    return {c: _calculate_category_score(fs) for c, fs in grouped.items()}


def _calculate_health_score(findings: List[Finding]) -> float:
    """Weighted average of the category scores (Reliability 35%, Performance 30%, Security 20%, Cost 15%)."""
    if not findings:
        return 100.0
    scores = _calculate_category_scores(findings)
    return round(sum(scores[c] * w for c, w in CATEGORY_WEIGHTS.items()), 1)


def overall_status(findings: List[Finding]) -> str:
    """Status label bounded by the worst severity: a critical finding can never read as Healthy."""
    severities = {f.severity for f in findings}
    if Severity.CRITICAL in severities:
        return 'Critical'
    if Severity.WARNING in severities:
        return 'Needs Attention'
    return 'Healthy'


# --------------------------------------------------------------------------- reliability checks

def analyze_active_controller_count(metric: MetricData) -> List[Finding]:
    """Exactly one controller must be active. MSK publishes one sample per broker per minute (1 on
    the controller, 0 elsewhere), so the series is the Sum per minute averaged over each bucket:
    1.0 when a controller was present every minute, below 1 when it was missing for part of the bucket."""
    # Normalise by the samples actually reported in each bucket, so publication jitter at bucket
    # boundaries (observed: 58-60 sums over 178-181 samples per hour) does not look like a gap.
    sums, counts = metric.series.get('Sum'), metric.series.get('SampleCount')
    emitters = max(1, round(metric.emitters_per_minute))
    if sums and counts and len(sums) == len(counts):
        # ignore sparse buckets (cluster creation, window edges) where a few samples distort the ratio
        full = 0.5 * float(np.median(counts)) if counts else 0.0
        values = [(sm * emitters / c) for sm, c in zip(sums, counts) if c and c >= full] or list(metric.values)
    else:
        values = list(metric.values)
    floor_val = min(values) if values else 0.0
    peak_val = max(values) if values else 0.0
    hours_without = sum(1 for v in values if v < 0.95)
    ev = {'statistics': metric.statistics, 'buckets_with_controller_gap': hours_without,
          'controller_fraction_min': floor_val, 'controller_fraction_max': peak_val, 'brokers_reporting': emitters}
    common = dict(check_id='active_controller', metric_name='ActiveControllerCount', category=Category.RELIABILITY,
                  source='best_practices', chart='ActiveControllerCount', threshold=1.0, threshold_text='exactly 1')
    if floor_val < 0.95:
        return [_finding(severity=Severity.CRITICAL, title='Cluster lost its active controller during the window',
                         description=(f'A controller was reported in only {floor_val * 100:.0f}% of the minutes of the worst bucket '
                                      f'({hours_without} bucket(s) below 95%), meaning no broker held the controller role for part of that time. '
                                      'Without a controller, partition leadership changes and topic operations stall. '
                                      'Broker restarts or maintenance can explain short gaps; repeated gaps suggest '
                                      'controller instability.'),
                         value=floor_val, evidence=ev, observed=f'controller present {floor_val * 100:.0f}% of minutes', **common)]
    if peak_val >= 1.5:
        return [_finding(severity=Severity.WARNING, title='More than one active controller reported',
                         description=(f'ActiveControllerCount reached {peak_val:.0f}. Two controllers can appear briefly '
                                      'during a controller move; a persistent value above 1 indicates a split view '
                                      'of the cluster that needs investigation.'),
                         value=peak_val, evidence=ev, observed=f'maximum {peak_val:.2f}', **common)]
    return [_finding(severity=Severity.HEALTHY, title='Exactly one active controller throughout the window',
                     description=('One broker reported itself as controller in every minute of the window (gaps shorter '
                                  'than 5% of a bucket are below the resolution of this check).'),
                     value=1.0, evidence=ev, observed='1', **common)]


def analyze_offline_partitions(metric: MetricData) -> List[Finding]:
    """Any offline partition means data unavailability. Uses the Maximum statistic."""
    peak_val = metric.statistics.get('peak', metric.statistics['max'])
    hours_affected = sum(1 for v in metric.values if v > 0)
    last_ts = next((ts for ts, v in zip(reversed(metric.timestamps), reversed(metric.values)) if v > 0), None)
    ev = {'statistics': metric.statistics, 'hours_affected': hours_affected,
          'last_occurrence': last_ts.isoformat() if last_ts else None}
    common = dict(check_id='offline_partitions', metric_name='OfflinePartitionsCount', category=Category.RELIABILITY,
                  source='best_practices', chart='OfflinePartitionsCount', threshold=0.0, threshold_text='0')
    if peak_val > 0:
        current = metric.values[-1] if metric.values else 0
        sev = Severity.CRITICAL
        when = f'last seen {last_ts.strftime("%Y-%m-%d %H:%M UTC")}' if last_ts else ''
        desc = (f'Up to {peak_val:.0f} partition(s) were offline in {hours_affected} bucket(s) ({when}). '
                'Offline partitions have no leader, so producers and consumers of those partitions fail. '
                'Typical causes are a broker outage with replication factor 1, or all replicas of a partition '
                'unavailable at once. ')
        desc += ('Partitions are still offline now.' if current > 0 else
                 'No partition is offline at the end of the window; confirm the root cause to prevent recurrence.')
        return [_finding(severity=sev, title='Offline partitions detected', description=desc, value=peak_val,
                         evidence=ev, observed=f'peak {peak_val:.0f}', **common)]
    return [_finding(severity=Severity.HEALTHY, title='No offline partitions',
                     description='OfflinePartitionsCount was 0 in every bucket.',
                     value=0.0, evidence=ev, observed='0', **common)]


def analyze_under_min_isr(brokers: List[MetricData]) -> List[Finding]:
    """UnderMinIsrPartitionCount per broker (Maximum statistic). Current > 0 is critical."""
    current = {m.broker_id: (m.values[-1] if m.values else 0.0) for m in brokers}
    historical = {m.broker_id: m.statistics.get('peak', m.statistics['max']) for m in brokers}
    now_affected = [b for b, v in current.items() if v > 0]
    hist_affected = [b for b, v in historical.items() if v > 0]
    ev = {'current_per_broker': current, 'peak_per_broker': historical}
    common = dict(check_id='under_min_isr', metric_name='UnderMinIsrPartitionCount', category=Category.RELIABILITY,
                  source='best_practices', chart='UnderMinIsrPartitionCount', threshold=0.0, threshold_text='0')
    if now_affected:
        total = sum(current[b] for b in now_affected)
        return [_finding(severity=Severity.CRITICAL, title='Partitions currently below min.insync.replicas',
                         description=(f'{total:.0f} partition(s) on broker(s) {", ".join(now_affected)} are below the '
                                      'configured minimum in-sync replicas in the latest bucket. Producers with acks=all '
                                      'receive NotEnoughReplicas errors and durability is reduced until replicas catch up.'),
                         value=total, evidence=ev, brokers=now_affected, observed=f'{total:.0f} now', **common)]
    if hist_affected:
        peak = max(historical.values())
        return [_finding(severity=Severity.WARNING, title='Partitions were below min.insync.replicas earlier in the window',
                         description=(f'Up to {peak:.0f} partition(s) fell below min ISR on broker(s) '
                                      f'{", ".join(hist_affected)}; none are affected in the latest bucket. Broker restarts '
                                      '(patching, size updates) explain short episodes; recurring episodes indicate a '
                                      'follower that cannot keep up.'),
                         value=peak, evidence=ev, brokers=hist_affected, observed=f'peak {peak:.0f}', confidence='medium',
                         **common)]
    return [_finding(severity=Severity.HEALTHY, title='All partitions met min.insync.replicas',
                     description='UnderMinIsrPartitionCount was 0 on every broker throughout the window.',
                     value=0.0, evidence=ev, observed='0', **common)]


def analyze_under_replicated(brokers: List[MetricData]) -> List[Finding]:
    """UnderReplicatedPartitions per broker. Sustained URP indicates replication lag."""
    peaks = {m.broker_id: m.statistics.get('peak', m.statistics['max']) for m in brokers}
    hours_total = max((len(m.values) for m in brokers), default=0)
    hours_affected = {m.broker_id: sum(1 for v in m.values if v > 0) for m in brokers}
    affected = [b for b, v in peaks.items() if v > 0]
    worst_hours = max(hours_affected.values(), default=0)
    current = any((m.values[-1] if m.values else 0) > 0 for m in brokers)
    ev = {'peak_per_broker': peaks, 'hours_affected_per_broker': hours_affected, 'hours_in_window': hours_total}
    common = dict(check_id='under_replicated', metric_name='UnderReplicatedPartitions', category=Category.RELIABILITY,
                  source='best_practices', chart='UnderReplicatedPartitions', threshold=0.0, threshold_text='0')
    if not affected:
        return [_finding(severity=Severity.HEALTHY, title='No under-replicated partitions',
                         description='UnderReplicatedPartitions was 0 on every broker throughout the window.',
                         value=0.0, evidence=ev, observed='0', **common)]
    share = (worst_hours / hours_total * 100.0) if hours_total else 0.0
    peak = max(peaks.values())
    if current or share >= 5.0:
        sev, title = Severity.WARNING, 'Under-replicated partitions persist'
        conf = 'high'
    else:
        sev, title = Severity.INFORMATIONAL, 'Brief under-replication episodes'
        conf = 'medium'
    desc = (f'Up to {peak:.0f} under-replicated partition(s) on broker(s) {", ".join(affected)}, present in '
            f'{worst_hours} of {hours_total} buckets ({share:.0f}%). ')
    desc += ('Under-replication is still present in the latest bucket. ' if current else '')
    desc += ('Short episodes are expected during broker restarts and rolling updates; sustained under-replication '
             'means a follower cannot keep up (network, disk, or CPU pressure on the follower broker).')
    return [_finding(severity=sev, title=title, description=desc, value=peak, evidence=ev, brokers=affected,
                     observed=f'peak {peak:.0f}, {share:.0f}% of hours', confidence=conf, **common)]


def analyze_disk_usage(brokers: List[MetricData], cluster_info: ClusterInfo) -> List[Finding]:
    """KafkaDataLogsDiskUsed per broker (Standard only). Action threshold 85% per AWS guidance."""
    peaks = {m.broker_id: m.statistics.get('peak', m.statistics['max']) for m in brokers}
    lasts = {m.broker_id: (m.values[-1] if m.values else 0.0) for m in brokers}
    worst_broker = max(peaks, key=peaks.get)
    worst_metric = next(m for m in brokers if m.broker_id == worst_broker)
    growth = _linear_growth_per_day(worst_metric)
    days_to_action = None
    if growth and growth > 0 and lasts[worst_broker] < ref.DISK_ACTION_PCT:
        days_to_action = (ref.DISK_ACTION_PCT - lasts[worst_broker]) / growth
    ev = {'peak_per_broker': peaks, 'current_per_broker': lasts, 'growth_pct_per_day': growth,
          'days_until_85pct': days_to_action, 'volume_gib': cluster_info.ebs_volume_size}
    proj = ''
    if days_to_action is not None and days_to_action < 90:
        proj = f' At the observed growth of {growth:.2f} percentage points per day, broker {worst_broker} reaches 85% in about {days_to_action:.0f} days.'
    common = dict(check_id='disk_usage', metric_name='KafkaDataLogsDiskUsed', category=Category.RELIABILITY,
                  source='best_practices', chart='KafkaDataLogsDiskUsed', threshold=ref.DISK_ACTION_PCT,
                  threshold_text=f'action at {ref.DISK_ACTION_PCT:.0f}%, warning at {ref.DISK_WARNING_PCT:.0f}%')
    peak = peaks[worst_broker]
    over = [b for b, v in peaks.items() if v >= ref.DISK_ACTION_PCT]
    warn = [b for b, v in peaks.items() if ref.DISK_WARNING_PCT <= v < ref.DISK_ACTION_PCT]
    if over:
        return [_finding(severity=Severity.CRITICAL, title='Data log disk usage reached the action threshold',
                         description=(f'Disk usage peaked at {peak:.1f}% on broker {worst_broker} (brokers at or above '
                                      f'85%: {", ".join(over)}). A full data volume stops the broker and can take '
                                      f'partitions offline.{proj}'),
                         value=peak, evidence=ev, brokers=over, observed=f'peak {peak:.1f}%', **common)]
    if warn:
        return [_finding(severity=Severity.WARNING, title='Data log disk usage approaching the action threshold',
                         description=(f'Disk usage peaked at {peak:.1f}% on broker {worst_broker}. AWS recommends acting '
                                      f'at 85%.{proj}'),
                         value=peak, evidence=ev, brokers=warn, observed=f'peak {peak:.1f}%', **common)]
    return [_finding(severity=Severity.HEALTHY, title='Data log disk usage within limits',
                     description=(f'Highest disk usage was {peak:.1f}% (broker {worst_broker}); the 85% action '
                                  f'threshold was not approached.{proj}'),
                     value=peak, evidence=ev, observed=f'peak {peak:.1f}%', **common)]


def analyze_availability_zones(cluster_info: ClusterInfo, workload: str) -> List[Finding]:
    az = cluster_info.availability_zones
    common = dict(check_id='availability_zones', metric_name='AvailabilityZones', category=Category.RELIABILITY,
                  source='best_practices', section='configuration', threshold=3.0, threshold_text='3 AZs')
    if az <= 0:
        return [not_assessed('availability_zones', 'AvailabilityZones', Category.RELIABILITY, 'Availability zones',
                             'The cluster description did not include client subnets.', section='configuration')]
    ev = {'az_count': az, 'client_subnets': cluster_info.client_subnets}
    if az == 1:
        return [_finding(severity=Severity.CRITICAL, title='Single availability zone',
                         description='All brokers are in one AZ; an AZ event takes the whole cluster offline.',
                         value=float(az), evidence=ev, observed='1 AZ', **common)]
    if az == 2:
        sev = Severity.WARNING if workload == 'production' else Severity.INFORMATIONAL
        return [_finding(severity=sev, title='Two availability zones',
                         description=('Brokers span 2 AZs. AWS recommends 3 AZs for production so that the loss of '
                                      'one AZ leaves a majority of replicas available; with 2 AZs and replication '
                                      'factor 3, an AZ failure can leave partitions with a single in-sync replica.'),
                         value=float(az), evidence=ev, observed='2 AZs', **common)]
    return [_finding(severity=Severity.HEALTHY, title='Three availability zones',
                     description=f'Brokers span {az} AZs, matching the AWS recommendation.',
                     value=float(az), evidence=ev, observed=f'{az} AZs', **common)]


def analyze_storage_auto_scaling(cluster_info: ClusterInfo, workload: str) -> List[Finding]:
    if cluster_info.is_express:
        return []
    common = dict(check_id='storage_autoscaling', metric_name='StorageAutoScaling', category=Category.RELIABILITY,
                  source='storage_autoscaling', section='configuration')
    if cluster_info.storage_autoscaling is None:
        return [not_assessed('storage_autoscaling', 'StorageAutoScaling', Category.RELIABILITY,
                             'Storage auto scaling', cluster_info.storage_autoscaling_detail,
                             section='configuration', source='storage_autoscaling')]
    ev = {'enabled': cluster_info.storage_autoscaling, 'detail': cluster_info.storage_autoscaling_detail,
          'target_pct': cluster_info.storage_autoscaling_target_pct}
    if cluster_info.storage_autoscaling:
        return [_finding(severity=Severity.HEALTHY, title='Storage auto scaling enabled',
                         description=f'Application Auto Scaling manages broker storage: {cluster_info.storage_autoscaling_detail}.',
                         value=1.0, evidence=ev, observed='enabled', **common)]
    sev = Severity.WARNING if workload == 'production' else Severity.INFORMATIONAL
    return [_finding(severity=sev, title='Storage auto scaling not configured',
                     description=(f'{cluster_info.storage_autoscaling_detail}. Without a scaling policy, disk growth '
                                  'requires a manual storage update; a full volume stops the broker.'),
                     value=0.0, evidence=ev, observed='disabled', **common)]


# --------------------------------------------------------------------------- performance checks

def cpu_total_series(cpu_user: List[MetricData], cpu_system: List[MetricData]) -> Dict[str, Dict[str, Any]]:
    """Per broker: CpuUser + CpuSystem summed point-wise on common timestamps."""
    result: Dict[str, Dict[str, Any]] = {}
    system_by_broker = {m.broker_id: m for m in cpu_system}
    for user in cpu_user:
        system = system_by_broker.get(user.broker_id)
        if not system:
            continue
        timestamps, vu, vs = align_series(user, system)
        totals = [a + b for a, b in zip(vu, vs)]
        if not totals:
            continue
        stats = summarize(totals)
        peak_u = user.series.get('Maximum'); peak_s = system.series.get('Maximum')
        stats['peak'] = (max(peak_u) + max(peak_s)) if peak_u and peak_s else stats['max']
        result[str(user.broker_id)] = {'timestamps': timestamps, 'values': totals, 'stats': stats}
    return result


def analyze_cpu_total(cpu_user: List[MetricData], cpu_system: List[MetricData]) -> List[Finding]:
    """CPU User + CPU System must stay under 60% (AWS best practice). Series are aligned by
    timestamp before summing, so percentiles describe the real combined load."""
    per_broker = cpu_total_series(cpu_user, cpu_system)
    if not per_broker:
        return [not_assessed('cpu_total', 'CpuTotal', Category.PERFORMANCE, 'CPU utilisation',
                             'CpuUser and CpuSystem series had no common timestamps.', section='derived')]
    limit = ref.CPU_TOTAL_MAX_PCT
    p95 = {b: d['stats']['p95'] for b, d in per_broker.items()}
    avg = {b: d['stats']['avg'] for b, d in per_broker.items()}
    hours_over = {b: sum(1 for v in d['values'] if v >= limit) for b, d in per_broker.items()}
    sustained = [b for b, v in p95.items() if v >= limit]
    episodic = [b for b, h in hours_over.items() if h > 0 and b not in sustained]
    worst = max(p95, key=p95.get)
    ev = {'p95_per_broker': p95, 'avg_per_broker': avg, 'buckets_at_or_above_60_per_broker': hours_over,
          'peak_per_broker': {b: d['stats']['peak'] for b, d in per_broker.items()}}
    common = dict(check_id='cpu_total', metric_name='CpuTotal', category=Category.PERFORMANCE, source='best_practices',
                  chart='CpuTotal', section='derived', threshold=limit, threshold_text=f'< {limit:.0f}% (User + System, P95)')
    if sustained:
        return [_finding(severity=Severity.CRITICAL, title='Sustained CPU utilisation above 60%',
                         description=(f'P95 of CPU (User + System) per bucket is {p95[worst]:.1f}% on broker {worst}; brokers '
                                      f'above the limit: {", ".join(sustained)}. Below 40% headroom, Kafka cannot absorb '
                                      'the extra load of a broker restart, patching or a leadership move without latency '
                                      'impact. AWS recommends moving to the next broker size, or adding brokers when '
                                      'topics are written round-robin.'),
                         value=p95[worst], evidence=ev, brokers=sustained, observed=f'P95 {p95[worst]:.1f}%', **common)]
    if episodic:
        hours = max(hours_over.values())
        return [_finding(severity=Severity.WARNING, title='CPU utilisation exceeded 60% in some periods',
                         description=(f'CPU (User + System) reached or exceeded 60% in up to {hours} bucket(s) on '
                                      f'broker(s) {", ".join(episodic)}; P95 stays at {p95[worst]:.1f}%. Identify whether '
                                      'the peaks follow a batch schedule or a rebalance; if they grow, plan a size change.'),
                         value=p95[worst], evidence=ev, brokers=episodic, observed=f'P95 {p95[worst]:.1f}%, {hours} buckets >= 60%',
                         confidence='medium', **common)]
    return [_finding(severity=Severity.HEALTHY, title='CPU utilisation within the recommended limit',
                     description=(f'Highest P95 of CPU (User + System) is {p95[worst]:.1f}% (broker {worst}); '
                                  f'cluster average {np.mean(list(avg.values())):.1f}%.'),
                     value=p95[worst], evidence=ev, observed=f'P95 {p95[worst]:.1f}%', **common)]


def analyze_heap_memory(brokers: List[MetricData]) -> List[Finding]:
    """HeapMemoryAfterGC should stay under 60% (AWS best practice)."""
    limit = ref.HEAP_AFTER_GC_MAX_PCT
    p95 = {m.broker_id: m.statistics['p95'] for m in brokers}
    hours_over = {m.broker_id: sum(1 for v in m.values if v >= limit) for m in brokers}
    worst = max(p95, key=p95.get)
    sustained = [b for b, v in p95.items() if v >= limit]
    episodic = [b for b, h in hours_over.items() if h > 0 and b not in sustained]
    ev = {'p95_per_broker': p95, 'buckets_at_or_above_60_per_broker': hours_over,
          'peak_per_broker': {m.broker_id: m.statistics.get('peak', m.statistics['max']) for m in brokers}}
    common = dict(check_id='heap_after_gc', metric_name='HeapMemoryAfterGC', category=Category.PERFORMANCE,
                  source='best_practices', chart='HeapMemoryAfterGC', threshold=limit, threshold_text=f'< {limit:.0f}%')
    if sustained:
        return [_finding(severity=Severity.CRITICAL, title='Heap memory after GC stays above 60%',
                         description=(f'P95 of HeapMemoryAfterGC is {p95[worst]:.1f}% on broker {worst} (brokers above the '
                                      f'limit: {", ".join(sustained)}). Heap that stays full after collection means '
                                      'the JVM is close to its limit: expect long GC pauses and, ultimately, broker '
                                      'restarts. Larger brokers add heap; reducing transactional.id.expiration.ms or '
                                      'the number of partitions per broker reduces demand.'),
                         value=p95[worst], evidence=ev, brokers=sustained, observed=f'P95 {p95[worst]:.1f}%', **common)]
    if episodic:
        return [_finding(severity=Severity.WARNING, title='Heap memory after GC exceeded 60% in some periods',
                         description=(f'HeapMemoryAfterGC reached 60% in up to {max(hours_over.values())} bucket(s) on '
                                      f'broker(s) {", ".join(episodic)}; P95 is {p95[worst]:.1f}%.'),
                         value=p95[worst], evidence=ev, brokers=episodic, observed=f'P95 {p95[worst]:.1f}%',
                         confidence='medium', **common)]
    return [_finding(severity=Severity.HEALTHY, title='Heap memory after GC within the recommended limit',
                     description=f'Highest P95 of HeapMemoryAfterGC is {p95[worst]:.1f}% (broker {worst}).',
                     value=p95[worst], evidence=ev, observed=f'P95 {p95[worst]:.1f}%', **common)]


def analyze_throughput(bytes_in: List[MetricData], bytes_out: List[MetricData], cluster_info: ClusterInfo) -> List[Finding]:
    """Per-broker BytesIn/BytesOut against the broker size limits. Express limits are published
    (sustained and throttle quota); Standard values are tool guidelines."""
    limits = ref.get_instance_limits(cluster_info.instance_type)
    findings: List[Finding] = []
    for direction, metrics, sustained, maximum in (
        ('in', bytes_in, limits.ingress_sustained_mbps if limits else None, limits.ingress_max_mbps if limits else None),
        ('out', bytes_out, limits.egress_sustained_mbps if limits else None, limits.egress_max_mbps if limits else None),
    ):
        metric_name = 'BytesInPerSec' if direction == 'in' else 'BytesOutPerSec'
        check_id = f'throughput_{direction}'
        label = 'Inbound' if direction == 'in' else 'Outbound'
        if not metrics:
            continue
        if not limits or sustained is None:
            findings.append(not_assessed(check_id, metric_name, Category.PERFORMANCE, f'{label} throughput vs broker size',
                                         f'Broker size {cluster_info.instance_type} is not in the limits catalog.',
                                         source='quotas'))
            continue
        mb = 1024 * 1024
        p95 = {m.broker_id: m.statistics['p95'] / mb for m in metrics}
        peak = {m.broker_id: m.statistics.get('peak', m.statistics['max']) / mb for m in metrics}
        avg = {m.broker_id: m.statistics['avg'] / mb for m in metrics}
        worst = max(p95, key=p95.get)
        total_avg = sum(avg.values())
        conf = 'high' if limits.throughput_confidence == 'official' else 'low'
        basis = ('published sustained limit' if limits.throughput_confidence == 'official'
                 else 'tool guideline (no published per-broker quota for Standard brokers)')
        ev = {'p95_mbps_per_broker': p95, 'peak_mbps_per_broker': peak, 'avg_mbps_per_broker': avg,
              'sustained_limit_mbps': sustained, 'max_quota_mbps': maximum, 'cluster_avg_mbps': total_avg,
              'limit_basis': basis}
        common = dict(check_id=check_id, metric_name=metric_name, category=Category.PERFORMANCE,
                      source='quotas' if conf == 'high' else 'best_practices', chart=metric_name,
                      threshold=sustained, confidence=conf,
                      threshold_text=f'{sustained:g} MB/s sustained' + (f', throttle at {maximum:g} MB/s' if maximum else ''))
        over_sustained = [b for b, v in p95.items() if v >= sustained]
        near_quota = [b for b, v in peak.items() if maximum and v >= maximum * ref.THROUGHPUT_MAX_WARNING_RATIO]
        if near_quota:
            findings.append(_finding(severity=Severity.CRITICAL, title=f'{label} throughput close to the throttle quota',
                                     description=(f'Peak {label.lower()} throughput reached {max(peak.values()):.1f} MB/s on '
                                                  f'broker(s) {", ".join(near_quota)}, within 10% of the {maximum:g} MB/s '
                                                  'quota at which MSK throttles client traffic. Add brokers or move to a '
                                                  'larger size before clients see throttling.'),
                                     value=max(peak.values()), evidence=ev, brokers=near_quota,
                                     observed=f'peak {max(peak.values()):.1f} MB/s', **common))
        elif over_sustained:
            findings.append(_finding(severity=Severity.WARNING, title=f'{label} throughput above the sustained limit',
                                     description=(f'P95 {label.lower()} throughput is {p95[worst]:.1f} MB/s on broker {worst} '
                                                  f'(brokers above {sustained:g} MB/s: {", ".join(over_sustained)}). Above '
                                                  f'the {basis}, latency degrades before throttling starts.'),
                                     value=p95[worst], evidence=ev, brokers=over_sustained,
                                     observed=f'P95 {p95[worst]:.1f} MB/s', **common))
        else:
            findings.append(_finding(severity=Severity.HEALTHY, title=f'{label} throughput within the broker size limit',
                                     description=(f'Highest P95 {label.lower()} throughput is {p95[worst]:.1f} MB/s (broker '
                                                  f'{worst}), {p95[worst] / sustained * 100:.0f}% of the {basis}; cluster '
                                                  f'average {total_avg:.2f} MB/s.'),
                                     value=p95[worst], evidence=ev, observed=f'P95 {p95[worst]:.1f} MB/s', **common))
    return findings


def analyze_partition_capacity(brokers: List[MetricData], cluster_info: ClusterInfo,
                               global_partitions: Optional[MetricData]) -> List[Finding]:
    """PartitionCount per broker (includes replicas, latest bucket) against the recommended and maximum values."""
    limits = ref.get_instance_limits(cluster_info.instance_type)
    current = {m.broker_id: (m.values[-1] if m.values else m.statistics['avg']) for m in brokers}
    worst = max(current, key=current.get)
    total_replicas = sum(current.values())
    global_count = global_partitions.values[-1] if global_partitions and global_partitions.values else None
    ev = {'current_per_broker': current, 'total_partition_replicas': total_replicas, 'global_partitions': global_count}
    if not limits:
        return [not_assessed('partition_capacity', 'PartitionCount', Category.PERFORMANCE, 'Partitions per broker',
                             f'Broker size {cluster_info.instance_type} is not in the limits catalog.', source='best_practices')]
    rec, mx = limits.partitions_recommended, limits.partitions_max
    ev.update({'recommended_per_broker': rec, 'maximum_per_broker': mx})
    common = dict(check_id='partition_capacity', metric_name='PartitionCount', category=Category.PERFORMANCE,
                  source='best_practices', chart='PartitionCount', threshold=float(rec),
                  threshold_text=f'recommended {rec}, maximum {mx} per broker')
    value = current[worst]
    over_max = [b for b, v in current.items() if v > mx]
    over_rec = [b for b, v in current.items() if rec < v <= mx]
    if over_max:
        return [_finding(severity=Severity.CRITICAL, title='Partition replicas per broker above the maximum',
                         description=(f'Broker {worst} hosts {value:.0f} partition replicas; the maximum for '
                                      f'{cluster_info.instance_type} is {mx}. Above it MSK blocks configuration updates and '
                                      'size reductions, and metrics can go missing. Add brokers or move to a larger size, '
                                      'then reassign partitions.'),
                         value=value, evidence=ev, brokers=over_max, observed=f'{value:.0f} replicas', **common)]
    if over_rec:
        return [_finding(severity=Severity.WARNING, title='Partition replicas per broker above the recommended value',
                         description=(f'Broker {worst} hosts {value:.0f} partition replicas; AWS recommends up to {rec} '
                                      f'for {cluster_info.instance_type} when traffic spans all partitions (maximum {mx}). '
                                      'Higher counts are acceptable for low-throughput partitions if validated by testing.'),
                         value=value, evidence=ev, brokers=over_rec, observed=f'{value:.0f} replicas', **common)]
    return [_finding(severity=Severity.HEALTHY, title='Partition replicas per broker within the recommended value',
                     description=(f'Most loaded broker ({worst}) hosts {value:.0f} partition replicas, '
                                  f'{value / rec * 100:.0f}% of the recommended {rec}.' +
                                  (f' The cluster has {global_count:.0f} partitions (leaders).' if global_count else '')),
                     value=value, evidence=ev, observed=f'{value:.0f} replicas', **common)]


def analyze_balance(check_id: str, metric_name: str, values: Dict[str, float], unit: str,
                    cluster_info: ClusterInfo, chart: Optional[str] = None, extra: Optional[Dict[str, Any]] = None,
                    title_noun: str = '') -> List[Finding]:
    """Generic per-broker balance check with an activity floor."""
    threshold = ref.IMBALANCE_THRESHOLDS_PCT.get(metric_name, 20.0)
    floor = ref.IMBALANCE_MIN_ACTIVITY.get(metric_name, 0.0)
    res = _imbalance(values, threshold, floor)
    noun = title_noun or metric_title(metric_name).lower()
    ev = {'per_broker': values, **res, **(extra or {})}
    common = dict(check_id=check_id, metric_name=metric_name, category=Category.PERFORMANCE, source='best_practices',
                  chart=chart or metric_name, section='derived', threshold=threshold,
                  threshold_text=f'hottest broker within {threshold:g}% of the mean', confidence='medium')
    if not res.get('relevant'):
        return [_finding(severity=Severity.HEALTHY, title=f'{noun.capitalize()}: distribution not assessed for imbalance',
                         description=f'Imbalance not evaluated: {res.get("reason")}.', value=None, evidence=ev,
                         observed=_fmt(res.get('mean', 0.0), unit) + ' mean', **common)]
    dev = res['deviation_pct']
    obs = (f'hottest broker {res["hottest"]} at {_fmt(res["max"], unit)} vs mean {_fmt(res["mean"], unit)} '
           f'(+{dev:.0f}%)')
    if res['imbalanced']:
        hint = ('Rebalance partitions with Cruise Control or kafka-reassign-partitions.sh so that leaders and '
                'replicas are spread evenly.')
        if cluster_info.is_express and cluster_info.intelligent_rebalancing_enabled:
            hint = 'Intelligent rebalancing is active on this Express cluster and should correct the skew over time.'
        return [_finding(severity=Severity.WARNING, title=f'Uneven {noun} across brokers',
                         description=(f'Broker {res["hottest"]} carries {_fmt(res["max"], unit)} against a mean of '
                                      f'{_fmt(res["mean"], unit)} (+{dev:.0f}%, threshold {threshold:g}%); the least loaded '
                                      f'broker ({res["coldest"]}) has {_fmt(res["min"], unit)}. Uneven {noun} means one '
                                      f'broker reaches its limits first. {hint}'),
                         value=dev, evidence=ev, brokers=[res['hottest']], observed=obs, **common)]
    return [_finding(severity=Severity.HEALTHY, title=f'{noun.capitalize()} balanced across brokers',
                     description=f'Hottest broker is within {dev:.0f}% of the mean ({_fmt(res["mean"], unit)}).',
                     value=dev, evidence=ev, observed=obs, **common)]


# --------------------------------------------------------------------------- connections

def analyze_client_connections(brokers: List[MetricData], cluster_info: ClusterInfo) -> List[Finding]:
    """ClientConnectionCount per broker (Sum per minute = broker total). IAM listeners have a
    published quota of 3000 connections per broker; other listeners have no enforced limit."""
    limits = ref.get_instance_limits(cluster_info.instance_type)
    quota = limits.iam_connections_per_broker if limits else 3000
    iam = 'IAM' in cluster_info.authentication_methods
    peak = {m.broker_id: (m.breakdown_peak.get('IAM', m.statistics['max']) if iam and m.breakdown_peak
                          else m.statistics['max']) for m in brokers}
    avg = {m.broker_id: m.statistics['avg'] for m in brokers}
    worst = max(peak, key=peak.get)
    ev = {'peak_per_broker': peak, 'avg_per_broker': avg, 'iam_quota_per_broker': quota,
          'listener_breakdown_avg': {m.broker_id: m.breakdown for m in brokers if m.breakdown},
          'estimation': 'broker total = Sum of per-network-processor samples per minute'}
    common = dict(check_id='client_connections', metric_name='ClientConnectionCount', category=Category.PERFORMANCE,
                  source='quotas', chart='ClientConnectionCount', threshold=float(quota) if iam else None,
                  threshold_text=f'{quota} per broker (IAM quota)' if iam else 'no enforced quota (non-IAM listeners)')
    if not iam:
        return [_finding(severity=Severity.INFORMATIONAL, title='Client connections (no enforced quota)',
                         description=(f'Peak client connections per broker: {", ".join(f"{b}: {v:.0f}" for b, v in peak.items())}. '
                                      'MSK enforces a connection quota only on IAM listeners; keep watching CPU and memory '
                                      'as connections grow.'),
                         value=peak[worst], evidence=ev, observed=f'peak {peak[worst]:.0f}', confidence='medium', **common)]
    ratio = peak[worst] / quota
    over = [b for b, v in peak.items() if v >= quota]
    near = [b for b, v in peak.items() if quota * ref.IAM_CONNECTIONS_WARNING_RATIO <= v < quota]
    if over:
        return [_finding(severity=Severity.CRITICAL, title='IAM client connections reached the per-broker quota',
                         description=(f'Broker {worst} peaked at {peak[worst]:.0f} client connections (quota {quota}). '
                                      'New IAM connections are refused at the quota. Pool connections in clients or raise '
                                      'listener.name.client_iam.max.connections after assessing broker memory.'),
                         value=peak[worst], evidence=ev, brokers=over, observed=f'peak {peak[worst]:.0f}', **common)]
    if near:
        return [_finding(severity=Severity.WARNING, title='IAM client connections approaching the per-broker quota',
                         description=(f'Broker {worst} peaked at {peak[worst]:.0f} client connections, {ratio * 100:.0f}% of '
                                      f'the {quota} quota.'),
                         value=peak[worst], evidence=ev, brokers=near, observed=f'peak {peak[worst]:.0f}', **common)]
    return [_finding(severity=Severity.HEALTHY, title='Client connections within the IAM quota',
                     description=f'Highest per-broker peak is {peak[worst]:.0f} connections (broker {worst}), {ratio * 100:.0f}% of the quota.',
                     value=peak[worst], evidence=ev, observed=f'peak {peak[worst]:.0f}', **common)]


def analyze_connection_creation_rate(brokers: List[MetricData], cluster_info: ClusterInfo,
                                     too_many: Optional[List[MetricData]] = None) -> List[Finding]:
    """ConnectionCreationRate per broker (new connections per second). IAM quota: 100/s per broker
    (4/s on kafka.t3.small)."""
    limits = ref.get_instance_limits(cluster_info.instance_type)
    quota = limits.iam_connection_rate_per_sec if limits else 100.0
    iam = 'IAM' in cluster_info.authentication_methods
    mixed = iam and any(m != 'IAM' for m in cluster_info.authentication_methods)
    p95 = {m.broker_id: m.statistics['p95'] for m in brokers}
    peak = {m.broker_id: m.statistics['max'] for m in brokers}
    avg = {m.broker_id: m.statistics['avg'] for m in brokers}
    worst = max(p95, key=p95.get)
    throttled = {m.broker_id: m.statistics.get('peak', m.statistics['max']) for m in (too_many or [])}
    throttled_brokers = [b for b, v in throttled.items() if v > 0]
    ev = {'p95_per_broker': p95, 'peak_per_broker': peak, 'avg_per_broker': avg, 'iam_quota_per_sec': quota,
          'iam_too_many_connections_peak': throttled,
          'note': 'ConnectionCreationRate aggregates every client listener; the quota applies to IAM listeners' if mixed else ''}
    common = dict(check_id='connection_creation_rate', metric_name='ConnectionCreationRate', category=Category.PERFORMANCE,
                  source='quotas', chart='ConnectionCreationRate', threshold=quota if iam else None,
                  confidence='medium' if mixed else 'high',
                  threshold_text=(f'{quota:g} new connections/s per broker (IAM quota' + (', all listeners counted)' if mixed else ')'))
                  if iam else 'no enforced quota')
    if throttled_brokers:
        return [_finding(severity=Severity.CRITICAL, title='IAM connection attempts were throttled',
                         description=(f'IAMTooManyConnections is above 0 on broker(s) {", ".join(throttled_brokers)}: clients '
                                      f'exceeded the {quota:g} new connections per second quota and were refused. P95 creation '
                                      f'rate is {p95[worst]:.1f}/s on broker {worst}. Clients that reconnect on every request '
                                      'or restart in loops are the usual cause; use long-lived producers/consumers and '
                                      'reconnect.backoff.ms.'),
                         value=p95[worst], evidence=ev, brokers=throttled_brokers, observed=f'P95 {p95[worst]:.1f}/s', **common)]
    if not iam:
        return [_finding(severity=Severity.INFORMATIONAL, title='Connection creation rate (no enforced quota)',
                         description=(f'P95 new connections per second per broker: '
                                      f'{", ".join(f"{b}: {v:.1f}" for b, v in p95.items())}. Each new connection costs CPU '
                                      '(TLS handshake, authentication); a sustained high rate usually points to clients '
                                      'without connection reuse.'),
                         value=p95[worst], evidence=ev, observed=f'P95 {p95[worst]:.1f}/s', confidence='medium', **common)]
    over = [b for b, v in p95.items() if v >= quota]
    near = [b for b, v in p95.items() if quota * ref.IAM_CONNECTION_RATE_WARNING_RATIO <= v < quota]
    if over:
        return [_finding(severity=Severity.CRITICAL, title='Connection creation rate at the IAM quota',
                         description=(f'P95 of new connections per second is {p95[worst]:.1f} on broker {worst} (quota {quota:g}); '
                                      f'brokers at or above the quota: {", ".join(over)}. Connections beyond the quota are refused.'),
                         value=p95[worst], evidence=ev, brokers=over, observed=f'P95 {p95[worst]:.1f}/s', **common)]
    if near:
        return [_finding(severity=Severity.WARNING, title='Connection creation rate approaching the IAM quota',
                         description=(f'P95 of new connections per second is {p95[worst]:.1f} on broker {worst}, '
                                      f'{p95[worst] / quota * 100:.0f}% of the {quota:g}/s quota (peak {peak[worst]:.1f}/s).'),
                         value=p95[worst], evidence=ev, brokers=near, observed=f'P95 {p95[worst]:.1f}/s', **common)]
    return [_finding(severity=Severity.HEALTHY, title='Connection creation rate within the IAM quota',
                     description=f'Highest P95 is {p95[worst]:.1f} new connections/s (broker {worst}), quota {quota:g}/s.',
                     value=p95[worst], evidence=ev, observed=f'P95 {p95[worst]:.1f}/s', **common)]


# --------------------------------------------------------------------------- configuration checks

_VERSION_CACHE: Dict[str, Any] = {}


def get_recommended_kafka_version(allow_network: bool = True) -> Dict[str, Any]:
    """Recommended Kafka version from the AWS documentation page, with provenance.

    Returns {'version': '3.8' | None, 'source': url | None, 'fetched_at': iso | None, 'error': str | None}.
    """
    if 'result' in _VERSION_CACHE:
        return _VERSION_CACHE['result']
    result: Dict[str, Any] = {'version': None, 'source': None, 'fetched_at': None, 'error': None}
    url = ref.DOCS['kafka_versions']
    if not allow_network:
        result['error'] = 'network lookup disabled'
    else:
        try:
            request = urllib.request.Request(url, headers={'User-Agent': 'MSK-Health-Check/1.1'})
            with urllib.request.urlopen(request, timeout=5) as response:  # nosec B310 - fixed https URL
                html = response.read().decode('utf-8', errors='ignore')
            match = re.search(r'(\d+\.\d+)(?:\.x|\.\d+)?\s*\(\s*recommended\s*\)', html, re.IGNORECASE)
            if match:
                result.update(version=match.group(1), source=url,
                              fetched_at=datetime.now(timezone.utc).isoformat(timespec='seconds'))
            else:
                result['error'] = 'recommended version marker not found on the documentation page'
        except Exception as e:  # network failures are expected in restricted environments
            result['error'] = f'{type(e).__name__}: {e}'
    _VERSION_CACHE['result'] = result
    return result


def analyze_kafka_version(cluster_info: ClusterInfo, allow_network: bool = True) -> List[Finding]:
    """Kafka version against MSK's version catalog (ListKafkaVersions status) and the documented
    recommended version when it can be fetched."""
    current = cluster_info.kafka_version
    status = (cluster_info.kafka_version_status or 'unknown').upper()
    recommended = get_recommended_kafka_version(allow_network)
    active = [v['version'] for v in cluster_info.kafka_versions_catalog if (v.get('status') or '').upper() == 'ACTIVE']
    latest_active = active[0] if active else None
    reference_version = recommended['version']
    reference_source = recommended['source']
    if not reference_version and latest_active:
        reference_version = '.'.join(str(x) for x in parse_version(latest_active)[:2])
        reference_source = 'ListKafkaVersions API (latest ACTIVE version)'
    ev = {'current_version': current, 'version_status': status, 'reference_version': reference_version,
          'reference_source': reference_source, 'reference_fetched_at': recommended.get('fetched_at'),
          'reference_error': recommended.get('error'), 'latest_active_version': latest_active}
    common = dict(check_id='kafka_version', metric_name='KafkaVersion', category=Category.RELIABILITY,
                  source='kafka_versions', section='configuration')
    if status == 'DEPRECATED':
        return [_finding(severity=Severity.WARNING, title='Kafka version is deprecated on MSK',
                         description=(f'The cluster runs Kafka {current}, which MSK lists as DEPRECATED. Deprecated versions '
                                      'reach end of support and stop receiving fixes; plan an in-place upgrade to a '
                                      f'supported version{f" (reference: {reference_version})" if reference_version else ""}.'),
                         value=None, evidence=ev, observed=f'{current} (DEPRECATED)', threshold_text='supported version', **common)]
    if not reference_version:
        return [not_assessed('kafka_version', 'KafkaVersion', Category.RELIABILITY, 'Kafka version',
                             'Neither the documentation page nor the MSK version catalog could be consulted '
                             f'({recommended.get("error")}).', section='configuration', source='kafka_versions')]
    cur = parse_version(current)[:2]
    rec = parse_version(reference_version)[:2]
    conf = 'high' if recommended['version'] else 'medium'
    src = 'AWS documentation' if recommended['version'] else 'MSK version catalog'
    if cur < rec:
        gap = (rec[0] - cur[0]) * 10 + (rec[1] - cur[1]) if len(cur) > 1 and len(rec) > 1 else 1
        sev = Severity.WARNING if gap >= 2 else Severity.INFORMATIONAL
        return [_finding(severity=sev, title='Newer Kafka version available',
                         description=(f'The cluster runs Kafka {current}; the {src} indicates {reference_version}.x as the '
                                      'reference version. Newer versions bring fixes and features; MSK performs in-place '
                                      'rolling upgrades.'),
                         value=None, evidence=ev, observed=current, threshold_text=f'{reference_version}.x', confidence=conf, **common)]
    return [_finding(severity=Severity.HEALTHY, title='Kafka version is current',
                     description=f'The cluster runs Kafka {current}; reference version from the {src} is {reference_version}.x.',
                     value=None, evidence=ev, observed=current, threshold_text=f'{reference_version}.x', confidence=conf, **common)]


def analyze_authentication_methods(cluster_info: ClusterInfo) -> List[Finding]:
    methods = cluster_info.authentication_methods
    ev = {'methods': methods, 'public_access': cluster_info.public_access}
    common = dict(check_id='authentication', metric_name='Authentication', category=Category.SECURITY,
                  source='authentication', section='configuration')
    if 'unauthenticated' in methods:
        others = [m for m in methods if m != 'unauthenticated']
        return [_finding(severity=Severity.CRITICAL, title='Unauthenticated client access is enabled',
                         description=('The cluster accepts unauthenticated connections' +
                                      (f' alongside {", ".join(others)}' if others else '') +
                                      '. Any client with network access to the brokers can produce and consume. '
                                      'Migrate clients to IAM, SASL/SCRAM or mTLS and disable the unauthenticated listener.'),
                         value=None, evidence=ev, observed='unauthenticated enabled', threshold_text='authenticated listeners only', **common)]
    if not methods:
        return [not_assessed('authentication', 'Authentication', Category.SECURITY, 'Client authentication',
                             'No authentication block was returned for the cluster.', section='configuration')]
    return [_finding(severity=Severity.HEALTHY, title='Only authenticated client access',
                     description=f'Enabled authentication: {", ".join(methods)}.', value=None, evidence=ev,
                     observed=', '.join(methods), threshold_text='authenticated listeners only', **common)]


def analyze_encryption(cluster_info: ClusterInfo) -> List[Finding]:
    findings: List[Finding] = []
    mode = cluster_info.encryption_in_transit_type
    ev = {'client_broker': mode, 'in_cluster': cluster_info.in_cluster_encryption, 'kms_key': cluster_info.kms_key_arn}
    common = dict(check_id='encryption_in_transit', metric_name='EncryptionInTransit', category=Category.SECURITY,
                  source='encryption', section='configuration', threshold_text='TLS')
    if mode == 'PLAINTEXT':
        findings.append(_finding(severity=Severity.CRITICAL, title='Client traffic is not encrypted',
                                 description='Client-broker encryption is PLAINTEXT; data and credentials cross the network unencrypted.',
                                 value=None, evidence=ev, observed='PLAINTEXT', **common))
    elif mode == 'TLS_PLAINTEXT':
        findings.append(_finding(severity=Severity.WARNING, title='Plaintext client listener still enabled',
                                 description=('Client-broker encryption is TLS_PLAINTEXT: TLS is available but a plaintext listener '
                                              'remains open. Move remaining clients to TLS and switch the setting to TLS.'),
                                 value=None, evidence=ev, observed='TLS_PLAINTEXT', **common))
    else:
        findings.append(_finding(severity=Severity.HEALTHY, title='Client traffic encrypted with TLS',
                                 description='Client-broker encryption is TLS only.', value=None, evidence=ev, observed='TLS', **common))
    if cluster_info.in_cluster_encryption is False:
        findings.append(_finding(check_id='encryption_in_cluster', metric_name='EncryptionInCluster', severity=Severity.WARNING,
                                 category=Category.SECURITY, title='Broker-to-broker traffic is not encrypted',
                                 description='In-cluster (replication) encryption is disabled.', value=None, evidence=ev,
                                 observed='disabled', threshold_text='enabled', source='encryption', section='configuration'))
    key = cluster_info.kms_key_arn or ''
    findings.append(_finding(check_id='encryption_at_rest', metric_name='EncryptionAtRest', severity=Severity.HEALTHY,
                             category=Category.SECURITY, title='Data encrypted at rest',
                             description=('Broker volumes are encrypted with KMS key ' + (key if key else '(AWS managed)') + '.'),
                             value=None, evidence=ev, observed='KMS', threshold_text='enabled', source='encryption',
                             section='configuration'))
    if cluster_info.public_access != 'DISABLED':
        findings.append(_finding(check_id='public_access', metric_name='PublicAccess', severity=Severity.INFORMATIONAL,
                                 category=Category.SECURITY, title='Brokers reachable from the internet',
                                 description=('Public access is enabled (service-provided elastic IPs). MSK requires TLS and '
                                              'authentication for public listeners; review security groups and IAM/ACL policies '
                                              'so that only intended principals can connect.'),
                                 value=None, evidence=ev, observed=cluster_info.public_access, source='public_access',
                                 section='configuration', confidence='medium'))
    return findings


def analyze_logging_configuration(cluster_info: ClusterInfo, workload: str) -> List[Finding]:
    ev = {'enabled': cluster_info.logging_enabled, 'destinations': cluster_info.logging_destinations}
    common = dict(check_id='broker_logging', metric_name='Logging', category=Category.SECURITY, source='logging',
                  section='configuration', threshold_text='broker logs delivered to at least one destination')
    if cluster_info.logging_enabled:
        return [_finding(severity=Severity.HEALTHY, title='Broker logs delivered',
                         description=f'Broker logs are sent to: {", ".join(cluster_info.logging_destinations)}.',
                         value=1.0, evidence=ev, observed=', '.join(cluster_info.logging_destinations), **common)]
    sev = Severity.WARNING if workload == 'production' else Severity.INFORMATIONAL
    return [_finding(severity=sev, title='Broker logs not delivered',
                     description=('Broker logs are not sent to CloudWatch Logs, S3 or Firehose. Without them, incidents '
                                  '(authentication failures, leader elections, disk errors) cannot be investigated after the fact.'),
                     value=0.0, evidence=ev, observed='disabled', **common)]


def analyze_enhanced_monitoring(cluster_info: ClusterInfo, metrics: MetricsCollection) -> List[Finding]:
    level = cluster_info.enhanced_monitoring_level
    skipped = sorted(n for n, r in metrics.not_published.items() if 'enhanced monitoring' in r)
    ev = {'level': level, 'checks_limited_by_level': skipped}
    common = dict(check_id='enhanced_monitoring', metric_name='EnhancedMonitoring', category=Category.PERFORMANCE,
                  source='monitoring', section='configuration', threshold_text='PER_BROKER or higher')
    if ref.monitoring_level_rank(level) >= 1:
        return [_finding(severity=Severity.HEALTHY, title=f'Enhanced monitoring at {level}',
                         description='Per-broker connection rate and throttling metrics are available to this analysis.',
                         value=1.0, evidence=ev, observed=level, **common)]
    return [_finding(severity=Severity.INFORMATIONAL, title='Enhanced monitoring at DEFAULT level',
                     description=('DEFAULT-level metrics (free) already include per-broker CPU, memory, disk, partitions and '
                                  'connections. PER_BROKER adds ConnectionCreationRate, IAMTooManyConnections and throttle '
                                  'metrics; the following checks were limited by the current level: ' +
                                  (', '.join(skipped) if skipped else 'none') + '.'),
                     value=0.0, evidence=ev, observed=level, **common)]


def analyze_intelligent_rebalancing(cluster_info: ClusterInfo) -> List[Finding]:
    if not cluster_info.is_express:
        return []
    common = dict(check_id='intelligent_rebalancing', metric_name='IntelligentRebalancing', category=Category.RELIABILITY,
                  source='best_practices_express', section='configuration', threshold_text='ACTIVE')
    status = cluster_info.rebalancing_status
    if status is None:
        return [not_assessed('intelligent_rebalancing', 'IntelligentRebalancing', Category.RELIABILITY,
                             'Intelligent rebalancing', 'DescribeClusterV2 did not return the Rebalancing field '
                             '(requires boto3/botocore with the 2025 Kafka API model).', section='configuration')]
    ev = {'status': status}
    if status == 'ACTIVE':
        return [_finding(severity=Severity.HEALTHY, title='Intelligent rebalancing active',
                         description='MSK redistributes partitions automatically on this Express cluster.',
                         value=1.0, evidence=ev, observed=status, **common)]
    return [_finding(severity=Severity.INFORMATIONAL, title='Intelligent rebalancing not active',
                     description=f'Rebalancing status is {status}. Partition skew has to be corrected manually until it is enabled.',
                     value=0.0, evidence=ev, observed=status, **common)]


# --------------------------------------------------------------------------- cost checks

def analyze_instance_type(cluster_info: ClusterInfo) -> List[Finding]:
    common = dict(check_id='graviton', metric_name='InstanceType', category=Category.COST, source='graviton',
                  section='configuration')
    ev = {'instance_type': cluster_info.instance_type, 'family': cluster_info.instance_family}
    if cluster_info.instance_family == 'graviton':
        return [_finding(severity=Severity.HEALTHY, title='Graviton broker size in use',
                         description=f'{cluster_info.instance_type} is Graviton-based.', value=None, evidence=ev,
                         observed=cluster_info.instance_type, **common)]
    equivalent = ref.has_graviton_equivalent(cluster_info.instance_type)
    if cluster_info.instance_family == 'intel' and equivalent:
        return [_finding(severity=Severity.INFORMATIONAL, title='Graviton equivalent available',
                         description=(f'{cluster_info.instance_type} has a Graviton counterpart ({equivalent}). AWS positions M7g '
                                      'brokers as better price-performance; compare hourly prices for this Region on the MSK '
                                      'pricing page and validate with a load test before a size update.'),
                         value=None, evidence={**ev, 'graviton_equivalent': equivalent}, observed=cluster_info.instance_type,
                         confidence='medium', **common)]
    if cluster_info.instance_family == 'intel':
        return [_finding(severity=Severity.INFORMATIONAL, title='No Graviton counterpart for this size',
                         description=f'{cluster_info.instance_type} has no Graviton equivalent in MSK.', value=None,
                         evidence=ev, observed=cluster_info.instance_type, **common)]
    return [not_assessed('graviton', 'InstanceType', Category.COST, 'Broker family',
                         f'Broker size {cluster_info.instance_type} is not in the catalog.', section='configuration')]


def analyze_right_sizing(cluster_info: ClusterInfo, findings: List[Finding]) -> List[Finding]:
    """Cost signal built from the utilisation checks already computed."""
    by_id = {f.check_id: f for f in findings}
    cpu = by_id.get('cpu_total'); tin = by_id.get('throughput_in'); tout = by_id.get('throughput_out')
    parts = by_id.get('partition_capacity')
    signals = []
    ev: Dict[str, Any] = {}
    if cpu and cpu.severity != Severity.NOT_ASSESSED and cpu.current_value is not None:
        ev['cpu_p95_max'] = cpu.current_value
        signals.append(cpu.current_value < 20.0)
    for t in (tin, tout):
        if t and t.severity != Severity.NOT_ASSESSED and t.threshold_value:
            ev[t.check_id + '_p95_ratio'] = t.current_value / t.threshold_value
            signals.append(t.current_value / t.threshold_value < 0.2)
    if parts and parts.severity != Severity.NOT_ASSESSED and parts.threshold_value:
        ev['partition_ratio'] = parts.current_value / parts.threshold_value
        signals.append(parts.current_value / parts.threshold_value < 0.3)
    common = dict(check_id='right_sizing', metric_name='RightSizing', category=Category.COST, source='broker_sizes',
                  section='derived', confidence='low')
    if len(signals) < 2:
        return [not_assessed('right_sizing', 'RightSizing', Category.COST, 'Right-sizing signal',
                             'Not enough utilisation checks were assessed to judge headroom.', section='derived')]
    if all(signals):
        return [_finding(severity=Severity.INFORMATIONAL, title='Large capacity headroom',
                         description=('CPU P95, network throughput and partition replicas per broker are all far below the '
                                      f'limits of {cluster_info.instance_type} over the window. If this is the steady state and '
                                      'not a seasonal low, a smaller broker size or fewer brokers may serve the workload; '
                                      'keep at least 3 brokers and replication factor 3.'),
                         value=None, evidence=ev, observed='all utilisation signals < 20-30% of limits', **common)]
    return [_finding(severity=Severity.HEALTHY, title='Utilisation consistent with the broker size',
                     description='At least one utilisation dimension uses a meaningful share of the broker size limits.',
                     value=None, evidence=ev, observed='no right-sizing signal', **common)]


# --------------------------------------------------------------------------- orchestration

def _applicable(name: str, cluster_info: ClusterInfo) -> bool:
    spec = METRIC_CATALOG.get(name)
    kind = 'express' if cluster_info.is_express else 'standard'
    return bool(spec) and kind in spec['kinds']


def _require_brokers(metrics: MetricsCollection, name: str, check_id: str, category: Category, title: str,
                     findings: List[Finding], chart: Optional[str] = None) -> Optional[List[MetricData]]:
    brokers = metrics.broker_metrics(name)
    if brokers:
        return brokers
    f = not_assessed(check_id, name, category, title, _missing_reason(metrics, name))
    f.chart_metric = chart
    findings.append(f)
    return None


def analyze_metrics(cluster_info: ClusterInfo, metrics: MetricsCollection, workload: str = 'production',
                    allow_network: bool = True) -> AnalysisResult:
    """Run every check and compute the health score.

    Args:
        cluster_info: cluster configuration
        metrics: collected metrics
        workload: 'production' or 'non-production' (adjusts severity of resilience recommendations)
        allow_network: allow the documentation lookup for the recommended Kafka version
    """
    findings: List[Finding] = []
    express = cluster_info.is_express

    # Reliability
    m = metrics.cluster_metric('ActiveControllerCount')
    findings.extend(analyze_active_controller_count(m) if m else
                    [not_assessed('active_controller', 'ActiveControllerCount', Category.RELIABILITY, 'Active controller',
                                  _missing_reason(metrics, 'ActiveControllerCount'))])
    if _applicable('OfflinePartitionsCount', cluster_info):
        m = metrics.cluster_metric('OfflinePartitionsCount')
        findings.extend(analyze_offline_partitions(m) if m else
                        [not_assessed('offline_partitions', 'OfflinePartitionsCount', Category.RELIABILITY, 'Offline partitions',
                                      _missing_reason(metrics, 'OfflinePartitionsCount'))])
    if _applicable('UnderMinIsrPartitionCount', cluster_info):
        b = _require_brokers(metrics, 'UnderMinIsrPartitionCount', 'under_min_isr', Category.RELIABILITY,
                             'Partitions below min ISR', findings)
        if b:
            findings.extend(analyze_under_min_isr(b))
    if _applicable('UnderReplicatedPartitions', cluster_info):
        b = _require_brokers(metrics, 'UnderReplicatedPartitions', 'under_replicated', Category.RELIABILITY,
                             'Under-replicated partitions', findings)
        if b:
            findings.extend(analyze_under_replicated(b))
    if not express:
        b = _require_brokers(metrics, 'KafkaDataLogsDiskUsed', 'disk_usage', Category.RELIABILITY, 'Data log disk usage', findings)
        if b:
            findings.extend(analyze_disk_usage(b, cluster_info))
    findings.extend(analyze_availability_zones(cluster_info, workload))
    findings.extend(analyze_storage_auto_scaling(cluster_info, workload))
    findings.extend(analyze_kafka_version(cluster_info, allow_network))
    findings.extend(analyze_intelligent_rebalancing(cluster_info))

    # Performance
    cpu_user = metrics.broker_metrics('CpuUser')
    cpu_system = metrics.broker_metrics('CpuSystem')
    if cpu_user and cpu_system:
        findings.extend(analyze_cpu_total(cpu_user, cpu_system))
        totals = cpu_total_series(cpu_user, cpu_system)
        findings.extend(analyze_balance('cpu_balance', 'CpuTotal', {b_: d['stats']['avg'] for b_, d in totals.items()},
                                        'Percent', cluster_info, chart='CpuTotal', title_noun='CPU load'))
    else:
        missing = 'CpuUser' if not cpu_user else 'CpuSystem'
        findings.append(not_assessed('cpu_total', 'CpuTotal', Category.PERFORMANCE, 'CPU utilisation',
                                     _missing_reason(metrics, missing), section='derived'))
    if _applicable('HeapMemoryAfterGC', cluster_info):
        b = _require_brokers(metrics, 'HeapMemoryAfterGC', 'heap_after_gc', Category.PERFORMANCE, 'Heap memory after GC', findings)
        if b:
            findings.extend(analyze_heap_memory(b))
    bytes_in = metrics.broker_metrics('BytesInPerSec')
    bytes_out = metrics.broker_metrics('BytesOutPerSec')
    if bytes_in or bytes_out:
        findings.extend(analyze_throughput(bytes_in, bytes_out, cluster_info))
        if bytes_in:
            findings.extend(analyze_balance('bytes_in_balance', 'BytesInPerSec',
                                            {m_.broker_id: m_.statistics['avg'] for m_ in bytes_in}, 'Bytes/Second',
                                            cluster_info, title_noun='inbound traffic'))
        if bytes_out:
            findings.extend(analyze_balance('bytes_out_balance', 'BytesOutPerSec',
                                            {m_.broker_id: m_.statistics['avg'] for m_ in bytes_out}, 'Bytes/Second',
                                            cluster_info, title_noun='outbound traffic'))
    else:
        findings.append(not_assessed('throughput_in', 'BytesInPerSec', Category.PERFORMANCE, 'Network throughput',
                                     _missing_reason(metrics, 'BytesInPerSec')))
    msgs = metrics.broker_metrics('MessagesInPerSec')
    if msgs:
        findings.extend(analyze_balance('messages_balance', 'MessagesInPerSec',
                                        {m_.broker_id: m_.statistics['avg'] for m_ in msgs}, 'Count/Second', cluster_info,
                                        title_noun='message intake'))
    parts = _require_brokers(metrics, 'PartitionCount', 'partition_capacity', Category.PERFORMANCE, 'Partitions per broker', findings)
    if parts:
        findings.extend(analyze_partition_capacity(parts, cluster_info, metrics.cluster_metric('GlobalPartitionCount')))
        findings.extend(analyze_balance('partition_balance', 'PartitionCount',
                                        {m_.broker_id: (m_.values[-1] if m_.values else m_.statistics['avg']) for m_ in parts},
                                        'Count', cluster_info, title_noun='partition replicas'))
    leaders = metrics.broker_metrics('LeaderCount')
    if leaders:
        findings.extend(analyze_balance('leader_balance', 'LeaderCount',
                                        {m_.broker_id: (m_.values[-1] if m_.values else m_.statistics['avg']) for m_ in leaders},
                                        'Count', cluster_info, title_noun='partition leaders'))
    b = _require_brokers(metrics, 'ClientConnectionCount', 'client_connections', Category.PERFORMANCE, 'Client connections', findings)
    if b:
        findings.extend(analyze_client_connections(b, cluster_info))
    conns = metrics.broker_metrics('ConnectionCount')
    if conns:
        findings.extend(analyze_balance('connection_balance', 'ConnectionCount',
                                        {m_.broker_id: m_.statistics['avg'] for m_ in conns}, 'Count', cluster_info,
                                        title_noun='connections'))
    b = _require_brokers(metrics, 'ConnectionCreationRate', 'connection_creation_rate', Category.PERFORMANCE,
                         'Connection creation rate', findings)
    if b:
        findings.extend(analyze_connection_creation_rate(b, cluster_info, metrics.broker_metrics('IAMTooManyConnections')))
    findings.extend(analyze_enhanced_monitoring(cluster_info, metrics))

    # Security
    findings.extend(analyze_authentication_methods(cluster_info))
    findings.extend(analyze_encryption(cluster_info))
    findings.extend(analyze_logging_configuration(cluster_info, workload))

    # Cost
    findings.extend(analyze_instance_type(cluster_info))
    findings.extend(analyze_right_sizing(cluster_info, findings))

    findings.sort(key=lambda f: (SEVERITY_ORDER[f.severity], f.category.value, f.title))
    category_scores = {c.value: round(s, 1) for c, s in _calculate_category_scores(findings).items()}
    health_score = _calculate_health_score(findings)
    assessed = [f for f in findings if f.severity != Severity.NOT_ASSESSED]
    result = AnalysisResult(
        cluster_info=cluster_info, metrics=metrics, findings=findings, overall_health_score=health_score,
        category_scores=category_scores, overall_status=overall_status(findings), checks_total=len(findings),
        checks_assessed=len(assessed), workload=workload,
        version_reference=next((f.evidence for f in findings if f.check_id == 'kafka_version'), {}),
    )
    logger.info(f"Analysis complete: {len(findings)} checks ({len(assessed)} assessed), status {result.overall_status}, "
                f"score {health_score}")
    return result
