"""Cluster information retrieval module."""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from .reference import determine_instance_family  # re-exported for backwards compatibility

logger = logging.getLogger(__name__)

__all__ = ['ClusterInfo', 'get_cluster_info', 'get_available_kafka_versions', 'determine_instance_family',
           'get_storage_autoscaling', 'parse_version', 'version_sort_key']


@dataclass
class ClusterInfo:
    """MSK cluster configuration information."""
    arn: str
    name: str
    cluster_type: str                      # 'PROVISIONED' (Standard) or 'EXPRESS'
    instance_type: str
    instance_family: str                   # 'intel', 'graviton' or 'unknown'
    broker_count: int
    availability_zones: int                # number of AZs (0 = unknown)
    authentication_methods: List[str]
    encryption_in_transit: bool
    encryption_at_rest: bool
    kafka_version: str
    storage_auto_scaling_enabled: bool     # kept for compatibility; see storage_autoscaling
    logging_enabled: bool
    logging_destinations: List[str]        # ['CloudWatch', 'S3', 'Firehose']
    available_kafka_versions: List[str]    # versions offered by MSK, latest first
    intelligent_rebalancing_enabled: bool  # Express only
    encryption_in_transit_type: str = 'TLS'  # 'TLS', 'PLAINTEXT', 'TLS_PLAINTEXT'
    ebs_volume_size: int = 0               # GB per broker (Standard)
    enhanced_monitoring_level: str = 'DEFAULT'
    cluster_state: str = 'UNKNOWN'
    creation_time: Optional[datetime] = None
    # Added fields
    in_cluster_encryption: Optional[bool] = None       # broker-to-broker TLS
    kms_key_arn: Optional[str] = None
    public_access: str = 'DISABLED'                    # DISABLED | SERVICE_PROVIDED_EIPS
    client_subnets: List[str] = field(default_factory=list)
    security_groups: List[str] = field(default_factory=list)
    kafka_version_status: str = 'unknown'              # ACTIVE | DEPRECATED | unknown (from ListKafkaVersions)
    kafka_versions_catalog: List[Dict[str, str]] = field(default_factory=list)  # [{'version','status'}]
    storage_autoscaling: Optional[bool] = None         # None = could not be assessed
    storage_autoscaling_detail: str = ''
    storage_autoscaling_target_pct: Optional[float] = None
    provisioned_throughput_enabled: bool = False
    provisioned_throughput_mibps: Optional[int] = None
    storage_mode: str = 'LOCAL'                        # LOCAL | TIERED
    metadata_mode: str = 'unknown'                     # ZOOKEEPER | KRAFT | unknown
    rebalancing_status: Optional[str] = None
    current_version: Optional[str] = None              # MSK cluster resource version
    tags: Dict[str, str] = field(default_factory=dict)

    @property
    def region(self) -> str:
        parts = self.arn.split(':')
        return parts[3] if len(parts) > 3 else ''

    @property
    def account_id(self) -> str:
        parts = self.arn.split(':')
        return parts[4] if len(parts) > 4 else ''

    @property
    def is_express(self) -> bool:
        return self.cluster_type == 'EXPRESS'


def parse_version(version: str) -> tuple:
    """Turn '3.8.x', '2.8.2.tiered' or '3.6.0' into a comparable tuple of ints."""
    numbers = re.findall(r'\d+', version or '')
    return tuple(int(n) for n in numbers[:3]) or (0,)


def version_sort_key(version: str) -> tuple:
    key = parse_version(version)
    return key + (0,) * (3 - len(key))


def get_available_kafka_versions(msk_client) -> List[Dict[str, str]]:
    """List the Kafka versions MSK offers, with their status (ACTIVE or DEPRECATED)."""
    try:
        versions: List[Dict[str, str]] = []
        paginator = None
        try:
            paginator = msk_client.get_paginator('list_kafka_versions')
        except Exception:
            paginator = None
        pages = paginator.paginate() if paginator else [msk_client.list_kafka_versions()]
        for page in pages:
            for item in page.get('KafkaVersions', []):
                versions.append({'version': item.get('Version', ''), 'status': item.get('Status', 'unknown')})
        versions.sort(key=lambda v: version_sort_key(v['version']), reverse=True)
        logger.info(f"Available Kafka versions: {[v['version'] for v in versions]}")
        return versions
    except Exception as e:
        logger.warning(f"Could not retrieve available Kafka versions: {e}")
        return []


def get_storage_autoscaling(autoscaling_client, cluster_arn: str) -> Dict[str, Any]:
    """Read the Application Auto Scaling configuration for broker storage.

    Returns a dict with keys enabled (True/False/None when not assessable), detail and target_pct.
    """
    if autoscaling_client is None:
        return {'enabled': None, 'detail': 'Application Auto Scaling client not available', 'target_pct': None}
    try:
        targets = autoscaling_client.describe_scalable_targets(
            ServiceNamespace='kafka', ResourceIds=[cluster_arn],
        ).get('ScalableTargets', [])
        if not targets:
            return {'enabled': False, 'detail': 'no scalable target registered for broker storage', 'target_pct': None}
        policies = autoscaling_client.describe_scaling_policies(
            ServiceNamespace='kafka', ResourceId=cluster_arn,
        ).get('ScalingPolicies', [])
        target_pct = None
        for policy in policies:
            cfg = policy.get('TargetTrackingScalingPolicyConfiguration', {})
            if cfg.get('TargetValue') is not None:
                target_pct = float(cfg['TargetValue'])
        max_capacity = targets[0].get('MaxCapacity')
        detail = f"scalable target registered (max {max_capacity} GiB)"
        if policies:
            detail += f", {len(policies)} scaling polic{'y' if len(policies) == 1 else 'ies'}"
            if target_pct is not None:
                detail += f", target {target_pct:.0f}% disk utilisation"
            return {'enabled': True, 'detail': detail, 'target_pct': target_pct}
        return {'enabled': False, 'detail': detail + ', but no scaling policy attached', 'target_pct': None}
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', 'ClientError')
        logger.warning(f"Storage auto scaling could not be assessed: {code}")
        return {'enabled': None, 'detail': f'not assessed ({code}: application-autoscaling:Describe* permission required)',
                'target_pct': None}
    except Exception as e:  # pragma: no cover - defensive
        logger.warning(f"Storage auto scaling could not be assessed: {e}")
        return {'enabled': None, 'detail': f'not assessed ({e})', 'target_pct': None}


def get_cluster_info(msk_client, cluster_arn: str, autoscaling_client=None) -> ClusterInfo:
    """Retrieve comprehensive cluster configuration.

    Raises:
        ValueError: If cluster type is not supported (Serverless)
    """
    response = msk_client.describe_cluster_v2(ClusterArn=cluster_arn)
    cluster = response['ClusterInfo']

    if 'Serverless' in cluster or cluster.get('ClusterType') == 'SERVERLESS':
        raise ValueError(
            "MSK Serverless clusters are not currently supported. "
            "This tool only supports MSK Provisioned clusters (Standard and Express brokers)."
        )

    name = cluster['ClusterName']
    provisioned = cluster['Provisioned']
    node_group = provisioned.get('BrokerNodeGroupInfo', {})
    instance_type = node_group.get('InstanceType', 'unknown')
    broker_count = provisioned.get('NumberOfBrokerNodes', 0)
    cluster_type = 'EXPRESS' if instance_type.lower().startswith('express.') else 'PROVISIONED'

    kafka_version = provisioned.get('CurrentBrokerSoftwareInfo', {}).get('KafkaVersion', 'unknown')
    instance_family = determine_instance_family(instance_type)

    # MSK requires each client subnet to be in a distinct AZ, so the subnet count is the AZ count.
    client_subnets = list(node_group.get('ClientSubnets', []))
    availability_zones = len(client_subnets)  # 0 means unknown
    security_groups = list(node_group.get('SecurityGroups', []))

    connectivity = node_group.get('ConnectivityInfo', {})
    public_access = connectivity.get('PublicAccess', {}).get('Type', 'DISABLED')

    auth_methods: List[str] = []
    client_auth = provisioned.get('ClientAuthentication', {})
    if client_auth.get('Sasl', {}).get('Iam', {}).get('Enabled'):
        auth_methods.append('IAM')
    if client_auth.get('Sasl', {}).get('Scram', {}).get('Enabled'):
        auth_methods.append('SASL/SCRAM')
    if client_auth.get('Tls', {}).get('Enabled'):
        auth_methods.append('mTLS')
    if client_auth.get('Unauthenticated', {}).get('Enabled'):
        auth_methods.append('unauthenticated')

    encryption = provisioned.get('EncryptionInfo', {})
    in_transit = encryption.get('EncryptionInTransit', {})
    encryption_in_transit_setting = in_transit.get('ClientBroker', 'PLAINTEXT')
    encryption_in_transit = encryption_in_transit_setting != 'PLAINTEXT'
    in_cluster_encryption = in_transit.get('InCluster') if 'InCluster' in in_transit else None
    at_rest = encryption.get('EncryptionAtRest', {})
    encryption_at_rest = 'EncryptionAtRest' in encryption
    kms_key_arn = at_rest.get('DataVolumeKMSKeyId')

    storage_info = node_group.get('StorageInfo', {}).get('EbsStorageInfo', {})
    ebs_volume_size = storage_info.get('VolumeSize', 0)
    provisioned_throughput = storage_info.get('ProvisionedThroughput', {})
    provisioned_throughput_enabled = bool(provisioned_throughput.get('Enabled', False))
    provisioned_throughput_mibps = provisioned_throughput.get('VolumeThroughput')
    storage_mode = provisioned.get('StorageMode', 'LOCAL')

    enhanced_monitoring = provisioned.get('EnhancedMonitoring', 'DEFAULT')
    cluster_state = cluster.get('State', 'UNKNOWN')
    metadata_mode = 'ZOOKEEPER' if provisioned.get('ZookeeperConnectString') else (
        'KRAFT' if cluster_type == 'EXPRESS' or parse_version(kafka_version) >= (3, 7) else 'unknown')

    logging_destinations: List[str] = []
    broker_logs = provisioned.get('LoggingInfo', {}).get('BrokerLogs', {})
    if broker_logs.get('CloudWatchLogs', {}).get('Enabled'):
        logging_destinations.append('CloudWatch')
    if broker_logs.get('S3', {}).get('Enabled'):
        logging_destinations.append('S3')
    if broker_logs.get('Firehose', {}).get('Enabled'):
        logging_destinations.append('Firehose')
    logging_enabled = bool(logging_destinations)

    versions_catalog = get_available_kafka_versions(msk_client)
    available_versions = [v['version'] for v in versions_catalog]
    kafka_version_status = 'unknown'
    for v in versions_catalog:
        if v['version'] == kafka_version:
            kafka_version_status = v.get('status') or 'unknown'
            break

    # Storage auto scaling (Standard brokers only) via Application Auto Scaling
    storage_autoscaling: Optional[bool] = None
    storage_autoscaling_detail = 'not applicable (Express brokers use managed storage)'
    storage_autoscaling_target: Optional[float] = None
    if cluster_type == 'PROVISIONED':
        result = get_storage_autoscaling(autoscaling_client, cluster_arn)
        storage_autoscaling = result['enabled']
        storage_autoscaling_detail = result['detail']
        storage_autoscaling_target = result['target_pct']

    rebalancing_status = None
    intelligent_rebalancing_enabled = False
    if cluster_type == 'EXPRESS':
        rebalancing = provisioned.get('Rebalancing', {})
        rebalancing_status = rebalancing.get('Status')
        intelligent_rebalancing_enabled = rebalancing_status == 'ACTIVE'

    creation_time = cluster.get('CreationTime')
    logger.info(f"Retrieved cluster info: {name}, {instance_type}, {broker_count} brokers, "
                f"{availability_zones} AZs, monitoring {enhanced_monitoring}")

    return ClusterInfo(
        arn=cluster_arn,
        name=name,
        cluster_type=cluster_type,
        instance_type=instance_type,
        instance_family=instance_family,
        broker_count=broker_count,
        availability_zones=availability_zones,
        authentication_methods=auth_methods,
        encryption_in_transit=encryption_in_transit,
        encryption_at_rest=encryption_at_rest,
        kafka_version=kafka_version,
        storage_auto_scaling_enabled=bool(storage_autoscaling),
        logging_enabled=logging_enabled,
        logging_destinations=logging_destinations,
        available_kafka_versions=available_versions,
        intelligent_rebalancing_enabled=intelligent_rebalancing_enabled,
        encryption_in_transit_type=encryption_in_transit_setting,
        ebs_volume_size=ebs_volume_size,
        enhanced_monitoring_level=enhanced_monitoring,
        cluster_state=cluster_state,
        creation_time=creation_time,
        in_cluster_encryption=in_cluster_encryption,
        kms_key_arn=kms_key_arn,
        public_access=public_access,
        client_subnets=client_subnets,
        security_groups=security_groups,
        kafka_version_status=kafka_version_status,
        kafka_versions_catalog=versions_catalog,
        storage_autoscaling=storage_autoscaling,
        storage_autoscaling_detail=storage_autoscaling_detail,
        storage_autoscaling_target_pct=storage_autoscaling_target,
        provisioned_throughput_enabled=provisioned_throughput_enabled,
        provisioned_throughput_mibps=provisioned_throughput_mibps,
        storage_mode=storage_mode,
        metadata_mode=metadata_mode,
        rebalancing_status=rebalancing_status,
        current_version=cluster.get('CurrentVersion'),
        tags=dict(cluster.get('Tags', {}) or {}),
    )
