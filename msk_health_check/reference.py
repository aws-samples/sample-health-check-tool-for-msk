"""Reference data used by the analysis: instance limits, thresholds and documentation sources.

Every number the report compares against lives here together with its provenance, so the
PDF can state where a threshold comes from and how much confidence it deserves.

Confidence levels:
    official  - published AWS quota or documented best-practice value
    guideline - value shipped with this tool (derived from AWS sizing guidance, not a quota)
"""

from dataclasses import dataclass
from typing import Dict, Optional

RULES_VERSION = "2026.09"

DOCS: Dict[str, str] = {
    'best_practices': 'https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html',
    'best_practices_express': 'https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices-express.html',
    'quotas': 'https://docs.aws.amazon.com/msk/latest/developerguide/limits.html',
    'metrics_standard': 'https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details.html',
    'metrics_express': 'https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details-express.html',
    'monitoring': 'https://docs.aws.amazon.com/msk/latest/developerguide/monitoring.html',
    'storage_autoscaling': 'https://docs.aws.amazon.com/msk/latest/developerguide/msk-autoexpand.html',
    'storage_manual': 'https://docs.aws.amazon.com/msk/latest/developerguide/manually-expand-storage.html',
    'kafka_versions': 'https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html',
    'version_upgrade': 'https://docs.aws.amazon.com/msk/latest/developerguide/version-upgrades.html',
    'encryption': 'https://docs.aws.amazon.com/msk/latest/developerguide/msk-encryption.html',
    'authentication': 'https://docs.aws.amazon.com/msk/latest/developerguide/msk-authentication.html',
    'iam_access': 'https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html',
    'public_access': 'https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html',
    'logging': 'https://docs.aws.amazon.com/msk/latest/developerguide/msk-logging.html',
    'graviton': 'https://docs.aws.amazon.com/msk/latest/developerguide/graviton.html',
    'broker_sizes': 'https://docs.aws.amazon.com/msk/latest/developerguide/broker-instance-sizes.html',
    'cruise_control': 'https://docs.aws.amazon.com/msk/latest/developerguide/cruise-control.html',
    'update_broker_type': 'https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-broker-type.html',
    'update_broker_count': 'https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-broker-count.html',
    'client_best_practices': 'https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices-kafka-client.html',
    'troubleshooting': 'https://docs.aws.amazon.com/msk/latest/developerguide/troubleshooting.html',
    'pricing': 'https://aws.amazon.com/msk/pricing/',
    'kafka_reassign': 'https://kafka.apache.org/documentation/#basic_ops_cluster_expansion',
}

# Thresholds documented by AWS (value, source key)
CPU_TOTAL_MAX_PCT = 60.0            # CPU User + CPU System, best practices
HEAP_AFTER_GC_MAX_PCT = 60.0        # HeapMemoryAfterGC, best practices
DISK_ACTION_PCT = 85.0              # KafkaDataLogsDiskUsed action threshold, best practices
DISK_WARNING_PCT = 75.0             # Tool guideline: headroom before the documented 85%
IAM_CONNECTION_RATE_WARNING_RATIO = 0.7  # Tool guideline: fraction of the quota that triggers a warning
IAM_CONNECTIONS_WARNING_RATIO = 0.8      # Tool guideline
THROUGHPUT_MAX_WARNING_RATIO = 0.9       # Tool guideline: fraction of the Express throttle quota

# Imbalance thresholds (tool guidelines). Deviation of the most loaded broker from the mean.
IMBALANCE_THRESHOLDS_PCT: Dict[str, float] = {
    'PartitionCount': 10.0,
    'LeaderCount': 10.0,
    'MessagesInPerSec': 20.0,
    'BytesInPerSec': 20.0,
    'BytesOutPerSec': 20.0,
    'CpuTotal': 20.0,
    'ConnectionCount': 25.0,
    'ClientConnectionCount': 25.0,
}
# Below these levels an imbalance is not worth reporting (tool guidelines)
IMBALANCE_MIN_ACTIVITY: Dict[str, float] = {
    'MessagesInPerSec': 100.0,          # msg/s per broker
    'BytesInPerSec': 1.0 * 1024 * 1024,  # 1 MB/s per broker
    'BytesOutPerSec': 1.0 * 1024 * 1024,
    'CpuTotal': 30.0,                    # percent
    'ConnectionCount': 20.0,
    'ClientConnectionCount': 20.0,
    'PartitionCount': 10.0,
    'LeaderCount': 10.0,
}

MONITORING_LEVELS = ['DEFAULT', 'PER_BROKER', 'PER_TOPIC_PER_BROKER', 'PER_TOPIC_PER_PARTITION']


def monitoring_level_rank(level: Optional[str]) -> int:
    """Rank of an enhanced-monitoring level; unknown levels rank as DEFAULT."""
    try:
        return MONITORING_LEVELS.index(level or 'DEFAULT')
    except ValueError:
        return 0


@dataclass(frozen=True)
class InstanceLimits:
    """Per-broker limits for one MSK broker size."""
    instance_type: str
    kind: str                              # 'standard' or 'express'
    partitions_recommended: int            # official (includes replicas)
    partitions_max: int                    # official maximum (update operations / hard quota)
    ingress_sustained_mbps: Optional[float]  # official for Express; guideline for Standard
    egress_sustained_mbps: Optional[float]
    ingress_max_mbps: Optional[float]        # Express throttle quota, None for Standard
    egress_max_mbps: Optional[float]
    throughput_confidence: str             # 'official' or 'guideline'
    iam_connections_per_broker: int = 3000   # official quota (IAM listeners)
    iam_connection_rate_per_sec: float = 100.0  # official quota (IAM), 4/s on kafka.t3.small


def _std(itype: str, rec: int, mx: int, net_mbps: Optional[float], rate: float = 100.0) -> InstanceLimits:
    return InstanceLimits(
        instance_type=itype, kind='standard',
        partitions_recommended=rec, partitions_max=mx,
        ingress_sustained_mbps=net_mbps, egress_sustained_mbps=net_mbps,
        ingress_max_mbps=None, egress_max_mbps=None,
        throughput_confidence='guideline',
        iam_connection_rate_per_sec=rate,
    )


def _exp(itype: str, rec: int, mx: int, in_s: float, in_m: float, out_s: float, out_m: float) -> InstanceLimits:
    return InstanceLimits(
        instance_type=itype, kind='express',
        partitions_recommended=rec, partitions_max=mx,
        ingress_sustained_mbps=in_s, egress_sustained_mbps=out_s,
        ingress_max_mbps=in_m, egress_max_mbps=out_m,
        throughput_confidence='official',
    )


# Partition values: "Right-size your cluster" tables (best practices) and Express partition quota.
# Standard network values are guidelines shipped with this tool (there is no published per-broker
# MB/s quota for Standard brokers); validate them with your own load tests.
INSTANCE_LIMITS: Dict[str, InstanceLimits] = {
    'kafka.t3.small': _std('kafka.t3.small', 300, 300, 5, rate=4.0),
    'kafka.m5.large': _std('kafka.m5.large', 1000, 1500, 9),
    'kafka.m5.xlarge': _std('kafka.m5.xlarge', 1000, 1500, 16),
    'kafka.m5.2xlarge': _std('kafka.m5.2xlarge', 2000, 3000, 31),
    'kafka.m5.4xlarge': _std('kafka.m5.4xlarge', 4000, 6000, 63),
    'kafka.m5.8xlarge': _std('kafka.m5.8xlarge', 4000, 6000, 106),
    'kafka.m5.12xlarge': _std('kafka.m5.12xlarge', 4000, 6000, 125),
    'kafka.m5.16xlarge': _std('kafka.m5.16xlarge', 4000, 6000, 125),
    'kafka.m5.24xlarge': _std('kafka.m5.24xlarge', 4000, 6000, 125),
    'kafka.m7g.large': _std('kafka.m7g.large', 1000, 1500, 10),
    'kafka.m7g.xlarge': _std('kafka.m7g.xlarge', 1000, 1500, 20),
    'kafka.m7g.2xlarge': _std('kafka.m7g.2xlarge', 2000, 3000, 39),
    'kafka.m7g.4xlarge': _std('kafka.m7g.4xlarge', 4000, 6000, 78),
    'kafka.m7g.8xlarge': _std('kafka.m7g.8xlarge', 4000, 6000, 125),
    'kafka.m7g.12xlarge': _std('kafka.m7g.12xlarge', 4000, 6000, 125),
    'kafka.m7g.16xlarge': _std('kafka.m7g.16xlarge', 4000, 6000, 125),
    # Express: sustained (recommended) and maximum quota (throttle) per broker, MBps
    'express.m7g.large': _exp('express.m7g.large', 1000, 1500, 15.6, 23.4, 31.2, 58.5),
    'express.m7g.xlarge': _exp('express.m7g.xlarge', 1000, 2000, 31.2, 46.8, 62.5, 117.0),
    'express.m7g.2xlarge': _exp('express.m7g.2xlarge', 2500, 4000, 62.5, 93.7, 125.0, 234.2),
    'express.m7g.4xlarge': _exp('express.m7g.4xlarge', 6000, 8000, 125.0, 187.5, 250.0, 468.7),
    'express.m7g.8xlarge': _exp('express.m7g.8xlarge', 12000, 16000, 250.0, 375.0, 500.0, 937.5),
    'express.m7g.12xlarge': _exp('express.m7g.12xlarge', 16000, 24000, 375.0, 562.5, 750.0, 1406.2),
    'express.m7g.16xlarge': _exp('express.m7g.16xlarge', 20000, 32000, 500.0, 750.0, 1000.0, 1875.0),
}


def get_instance_limits(instance_type: str) -> Optional[InstanceLimits]:
    """Return the limits for a broker size, or None when the size is not in the catalog."""
    return INSTANCE_LIMITS.get((instance_type or '').lower())


def determine_instance_family(instance_type: str) -> str:
    """Classify a broker size as 'graviton', 'intel' or 'unknown'.

    AWS instance family names end with 'g' for Graviton (m6g, m7g, t4g...). Known x86 families
    are returned as 'intel'; anything else is 'unknown' so that no cost recommendation is made
    on a guess.
    """
    parts = (instance_type or '').lower().split('.')
    family = ''
    for part in parts:
        if part and part[0] in 'mcrt' and any(ch.isdigit() for ch in part) and part not in ('kafka', 'express'):
            family = part
            break
    if not family:
        return 'unknown'
    if family.endswith('g') or family.endswith('gd'):
        return 'graviton'
    if family in {'m5', 'm4', 'c5', 'c4', 'r5', 'r4', 't3', 't2', 'm6i', 'c6i', 'r6i', 'm5a', 'm6a'}:
        return 'intel'
    return 'unknown'


def has_graviton_equivalent(instance_type: str) -> Optional[str]:
    """Return the Graviton broker size AWS offers for an x86 size, if one exists."""
    mapping = {
        'kafka.m5.large': 'kafka.m7g.large',
        'kafka.m5.xlarge': 'kafka.m7g.xlarge',
        'kafka.m5.2xlarge': 'kafka.m7g.2xlarge',
        'kafka.m5.4xlarge': 'kafka.m7g.4xlarge',
        'kafka.m5.8xlarge': 'kafka.m7g.8xlarge',
        'kafka.m5.12xlarge': 'kafka.m7g.12xlarge',
        'kafka.m5.16xlarge': 'kafka.m7g.16xlarge',
        'kafka.m5.24xlarge': 'kafka.m7g.16xlarge',
    }
    return mapping.get((instance_type or '').lower())
