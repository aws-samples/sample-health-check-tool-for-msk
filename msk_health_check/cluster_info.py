"""MSK cluster information retrieval module."""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ClusterInfo:
    """MSK cluster configuration information."""
    arn: str
    name: str
    cluster_type: str  # 'PROVISIONED' or 'EXPRESS'
    instance_type: str
    instance_family: str  # 'intel' or 'graviton'
    broker_count: int
    availability_zones: int  # Number of AZs
    authentication_methods: List[str]
    encryption_in_transit: bool
    encryption_at_rest: bool
    encryption_in_transit_type: str  # 'TLS', 'PLAINTEXT', 'TLS_PLAINTEXT'
    kafka_version: str
    storage_auto_scaling_enabled: bool
    logging_enabled: bool
    logging_destinations: List[str]  # ['CloudWatch', 'S3', 'Firehose']
    available_kafka_versions: List[str]  # Available versions for upgrade
    intelligent_rebalancing_enabled: bool  # For EXPRESS clusters only
    ebs_volume_size: int  # GB per broker
    enhanced_monitoring_level: str  # 'DEFAULT', 'PER_BROKER', 'PER_TOPIC_PER_BROKER'
    cluster_state: str  # 'ACTIVE', 'CREATING', etc.
    creation_time: Optional[datetime] = None  # When cluster was created


def get_available_kafka_versions(msk_client) -> List[str]:
    """
    Get list of available Kafka versions from AWS MSK.
    
    Args:
        msk_client: Boto3 MSK client
        
    Returns:
        List of available Kafka version strings
    """
    try:
        response = msk_client.list_kafka_versions()
        versions = [v['Version'] for v in response.get('KafkaVersions', [])]
        logger.info(f"Available Kafka versions: {versions}")
        return sorted(versions, reverse=True)  # Latest first
    except Exception as e:
        logger.warning(f"Could not retrieve available Kafka versions: {e}")
        return []


def get_cluster_info(msk_client, cluster_arn: str) -> ClusterInfo:
    """
    Retrieve comprehensive cluster configuration.
    
    Args:
        msk_client: Boto3 MSK client
        cluster_arn: ARN of the cluster
        
    Returns:
        ClusterInfo object with cluster details
        
    Raises:
        ValueError: If cluster is Serverless (not supported)
    """
    response = msk_client.describe_cluster_v2(ClusterArn=cluster_arn)
    cluster = response['ClusterInfo']
    
    # Check if cluster is Serverless (not supported)
    if 'Serverless' in cluster:
        raise ValueError(
            "MSK Serverless clusters are not supported. "
            "This tool only supports MSK Provisioned clusters."
        )
    
    # Extract basic info from Provisioned cluster
    name = cluster['ClusterName']
    provisioned = cluster['Provisioned']
    instance_type = provisioned['BrokerNodeGroupInfo']['InstanceType']
    broker_count = provisioned['NumberOfBrokerNodes']
    
    # Determine cluster type (check if it's Express)
    cluster_type = 'EXPRESS' if 'express' in instance_type.lower() else 'PROVISIONED'
    
    # Get Kafka version from CurrentBrokerSoftwareInfo
    kafka_version = 'unknown'
    if 'CurrentBrokerSoftwareInfo' in provisioned:
        kafka_version = provisioned['CurrentBrokerSoftwareInfo'].get('KafkaVersion', 'unknown')
    
    # Determine instance family
    instance_family = determine_instance_family(instance_type)
    
    # Count availability zones
    connectivity_info = provisioned.get('BrokerNodeGroupInfo', {}).get('ConnectivityInfo', {})
    vpc_connectivity = connectivity_info.get('VpcConnectivity', {})
    client_subnets = provisioned.get('BrokerNodeGroupInfo', {}).get('ClientSubnets', [])
    availability_zones = len(client_subnets) if client_subnets else 2  # Default to 2 if not found
    
    # Extract authentication methods
    auth_methods = []
    client_auth = provisioned.get('ClientAuthentication', {})
    
    if client_auth.get('Sasl', {}).get('Iam', {}).get('Enabled'):
        auth_methods.append('IAM')
    if client_auth.get('Sasl', {}).get('Scram', {}).get('Enabled'):
        auth_methods.append('SASL/SCRAM')
    if client_auth.get('Tls', {}).get('Enabled'):
        auth_methods.append('mTLS')
    if client_auth.get('Unauthenticated', {}).get('Enabled'):
        auth_methods.append('unauthenticated')
    
    # Extract encryption settings
    encryption = provisioned.get('EncryptionInfo', {})
    encryption_in_transit_setting = encryption.get('EncryptionInTransit', {}).get('ClientBroker', 'PLAINTEXT')
    encryption_in_transit = encryption_in_transit_setting != 'PLAINTEXT'
    encryption_at_rest = 'EncryptionAtRest' in encryption
    
    # Get EBS volume size
    storage_info = provisioned.get('BrokerNodeGroupInfo', {}).get('StorageInfo', {})
    ebs_storage = storage_info.get('EbsStorageInfo', {})
    ebs_volume_size = ebs_storage.get('VolumeSize', 0)
    
    # Get enhanced monitoring level
    enhanced_monitoring = provisioned.get('EnhancedMonitoring', 'DEFAULT')
    
    # Get cluster state
    cluster_state = cluster.get('State', 'UNKNOWN')
    
    # Check storage auto-scaling (only for PROVISIONED, not EXPRESS)
    storage_auto_scaling_enabled = False
    if cluster_type == 'PROVISIONED':
        storage_info = provisioned.get('BrokerNodeGroupInfo', {}).get('StorageInfo', {})
        ebs_storage = storage_info.get('EbsStorageInfo', {})
        storage_auto_scaling_enabled = ebs_storage.get('ProvisionedThroughput', {}).get('Enabled', False)
    
    # Check logging configuration
    logging_info = provisioned.get('LoggingInfo', {})
    broker_logs = logging_info.get('BrokerLogs', {})
    logging_destinations = []
    logging_enabled = False
    
    if broker_logs.get('CloudWatchLogs', {}).get('Enabled'):
        logging_destinations.append('CloudWatch')
        logging_enabled = True
    if broker_logs.get('S3', {}).get('Enabled'):
        logging_destinations.append('S3')
        logging_enabled = True
    if broker_logs.get('Firehose', {}).get('Enabled'):
        logging_destinations.append('Firehose')
        logging_enabled = True
    
    # Get available Kafka versions for upgrade
    available_versions = get_available_kafka_versions(msk_client)
    
    # Check intelligent rebalancing (EXPRESS only)
    intelligent_rebalancing_enabled = False
    if cluster_type == 'EXPRESS':
        # Check Provisioned.Rebalancing.Status field
        rebalancing_config = provisioned.get('Rebalancing', {})
        status = rebalancing_config.get('Status', 'ACTIVE')  # Default is ACTIVE if not present
        intelligent_rebalancing_enabled = (status == 'ACTIVE')
        logger.info(f"Intelligent rebalancing status: {status}")
    
    # Get cluster creation time
    creation_time = cluster.get('CreationTime')
    
    logger.info(f"Retrieved cluster info: {name}, {instance_type}, {broker_count} brokers, {availability_zones} AZs")
    if creation_time:
        logger.info(f"Cluster created: {creation_time}")
    
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
        encryption_in_transit_type=encryption_in_transit_setting,
        kafka_version=kafka_version,
        storage_auto_scaling_enabled=storage_auto_scaling_enabled,
        logging_enabled=logging_enabled,
        logging_destinations=logging_destinations,
        available_kafka_versions=available_versions,
        intelligent_rebalancing_enabled=intelligent_rebalancing_enabled,
        ebs_volume_size=ebs_volume_size,
        enhanced_monitoring_level=enhanced_monitoring,
        cluster_state=cluster_state,
        creation_time=creation_time
    )


def determine_instance_family(instance_type: str) -> str:
    """
    Determine if instance is Intel or Graviton based.
    
    Args:
        instance_type: EC2 instance type string
        
    Returns:
        'intel' or 'graviton'
    """
    graviton_families = ['m6g', 'm7g', 'c6g', 'c7g', 'r6g', 'r7g', 't4g']
    
    for family in graviton_families:
        # Check for both kafka.m7g.large and kafka.express.m7g.large
        if f'.{family}.' in instance_type or instance_type.endswith(f'.{family}'):
            return 'graviton'
    
    return 'intel'
