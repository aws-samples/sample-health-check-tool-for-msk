"""CloudWatch metrics collection module."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


@dataclass
class MetricData:
    """Time-series data for a single metric."""
    metric_name: str
    broker_id: Optional[str]  # None for cluster-level metrics, broker ID for per-broker metrics
    timestamps: List[datetime]
    values: List[float]
    unit: str
    statistics: Dict[str, float]  # min, max, avg, p95, p99


@dataclass
class MetricsCollection:
    """Collection of all metrics for a cluster."""
    cluster_arn: str
    start_time: datetime
    end_time: datetime
    metrics: Dict[str, List[MetricData]]  # metric_name -> list of MetricData (one per broker or one for cluster)
    missing_metrics: List[str]
    end_time: datetime
    metrics: Dict[str, MetricData]
    missing_metrics: List[str]


# MSK Metrics - Standard (Provisioned) and Express
# Reference: 
# - Standard: https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details.html
# - Express: https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details-express.html

# Standard (Provisioned) Metrics - ALL DEFAULT (no additional cost)
STANDARD_METRICS = {
    # Cluster-level metrics
    'ActiveControllerCount': {'namespace': 'AWS/Kafka', 'stat': 'Maximum', 'level': 'cluster'},
    'GlobalPartitionCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'cluster'},
    'GlobalTopicCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'cluster'},
    'OfflinePartitionsCount': {'namespace': 'AWS/Kafka', 'stat': 'Sum', 'level': 'cluster'},
    
    # Per-broker metrics (DEFAULT - available without Enhanced Monitoring)
    'CpuUser': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'CpuSystem': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'CpuIdle': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'MemoryUsed': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'MemoryFree': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'HeapMemoryAfterGC': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'KafkaDataLogsDiskUsed': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'LeaderCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'PartitionCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'ClientConnectionCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'ConnectionCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'UnderMinIsrPartitionCount': {'namespace': 'AWS/Kafka', 'stat': 'Sum', 'level': 'broker'},
    'BytesInPerSec': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'BytesOutPerSec': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'MessagesInPerSec': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'ConnectionCreationRate': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
}

# Express Metrics (same as Standard - Express has per-broker metrics!)
# Reference: https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details-express.html
EXPRESS_METRICS = {
    # Cluster-level metrics
    'ActiveControllerCount': {'namespace': 'AWS/Kafka', 'stat': 'Maximum', 'level': 'cluster'},
    'GlobalPartitionCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'cluster'},
    'GlobalTopicCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'cluster'},
    'OfflinePartitionsCount': {'namespace': 'AWS/Kafka', 'stat': 'Sum', 'level': 'cluster'},
    
    # Per-broker metrics (Express HAS per-broker metrics!)
    'BytesInPerSec': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'BytesOutPerSec': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'MessagesInPerSec': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'ClientConnectionCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'ConnectionCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'CpuIdle': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'CpuSystem': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'CpuUser': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'LeaderCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'PartitionCount': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'UnderMinIsrPartitionCount': {'namespace': 'AWS/Kafka', 'stat': 'Sum', 'level': 'broker'},
    'MemoryUsed': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'MemoryFree': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'HeapMemoryAfterGC': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
    'ConnectionCreationRate': {'namespace': 'AWS/Kafka', 'stat': 'Average', 'level': 'broker'},
}


def collect_metrics(
    cloudwatch_client,
    cluster_arn: str,
    broker_count: int,
    cluster_type: str,
    days_back: int = 30
) -> MetricsCollection:
    """
    Collect all MSK metrics from CloudWatch.
    Per-broker metrics are collected individually for each broker.
    Cluster-level metrics are collected once.
    
    Args:
        cloudwatch_client: Boto3 CloudWatch client
        cluster_arn: ARN of the MSK cluster
        broker_count: Number of brokers in the cluster
        cluster_type: Type of cluster ('PROVISIONED' or 'EXPRESS')
        days_back: Number of days to look back (default: 30)
        
    Returns:
        MetricsCollection with all collected metrics
    """
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days_back)
    cluster_name = cluster_arn.split('/')[-2]
    
    # Select metrics based on cluster type
    metric_definitions = EXPRESS_METRICS if cluster_type == 'EXPRESS' else STANDARD_METRICS
    
    logger.info(f"Collecting {cluster_type} metrics from {start_time} to {end_time} ({days_back} days)")
    
    metrics = {}  # metric_name -> List[MetricData]
    missing_metrics = []
    
    # Collect metrics in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        
        for metric_name, metric_def in metric_definitions.items():
            if metric_def['level'] == 'cluster':
                # Cluster-level metric - collect once
                future = executor.submit(
                    query_metric_with_retry,
                    cloudwatch_client,
                    metric_name,
                    cluster_name,
                    None,  # No broker ID
                    start_time,
                    end_time
                )
                futures.append((future, metric_name, None))
            elif metric_def['level'] == 'broker':
                # Per-broker metric - collect for each broker
                for broker_id in range(1, broker_count + 1):
                    future = executor.submit(
                        query_metric_with_retry,
                        cloudwatch_client,
                        metric_name,
                        cluster_name,
                        str(broker_id),
                        start_time,
                        end_time
                    )
                    futures.append((future, metric_name, str(broker_id)))
        
        # Collect results
        for future, metric_name, broker_id in futures:
            try:
                metric_data = future.result()
                if metric_data:
                    if metric_name not in metrics:
                        metrics[metric_name] = []
                    metrics[metric_name].append(metric_data)
                    broker_str = f" (broker {broker_id})" if broker_id else ""
                    logger.info(f"Collected {metric_name}{broker_str}: {len(metric_data.values)} data points")
                else:
                    if broker_id is None:  # Only log missing for cluster-level metrics
                        missing_metrics.append(metric_name)
            except Exception as e:
                logger.error(f"Error collecting {metric_name}: {e}")
                if broker_id is None:
                    missing_metrics.append(metric_name)
    
    logger.info(f"Collected {len(metrics)}/{len(metric_definitions)} metric types")
    
    return MetricsCollection(
        cluster_arn=cluster_arn,
        start_time=start_time,
        end_time=end_time,
        metrics=metrics,
        missing_metrics=list(set(missing_metrics))
    )


def query_metric_with_retry(
    cloudwatch_client,
    metric_name: str,
    cluster_name: str,
    broker_id: Optional[str],
    start_time: datetime,
    end_time: datetime,
    max_retries: int = 3
) -> Optional[MetricData]:
    """
    Query single metric with exponential backoff retry.
    
    Args:
        cloudwatch_client: Boto3 CloudWatch client
        metric_name: Name of the metric to query
        cluster_name: Name of the cluster
        broker_id: Broker ID for per-broker metrics, None for cluster metrics
        start_time: Start of time range
        end_time: End of time range
        max_retries: Maximum number of retry attempts
        
    Returns:
        MetricData if successful, None if all retries exhausted
    """
    # Find metric definition from both Standard and Express
    metric_def = STANDARD_METRICS.get(metric_name) or EXPRESS_METRICS.get(metric_name)
    if not metric_def:
        logger.warning(f"Metric {metric_name} not found in definitions")
        return None
    
    # Build dimensions based on metric level
    if broker_id:
        dimensions = [
            {'Name': 'Cluster Name', 'Value': cluster_name},
            {'Name': 'Broker ID', 'Value': broker_id}
        ]
    else:
        dimensions = [
            {'Name': 'Cluster Name', 'Value': cluster_name}
        ]
    
    for attempt in range(max_retries):
        try:
            response = cloudwatch_client.get_metric_statistics(
                Namespace=metric_def['namespace'],
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hour
                Statistics=[metric_def['stat']]
            )
            
            datapoints = response.get('Datapoints', [])
            if not datapoints:
                return None
            
            # Sort by timestamp
            datapoints.sort(key=lambda x: x['Timestamp'])
            
            timestamps = [dp['Timestamp'] for dp in datapoints]
            values = [dp[metric_def['stat']] for dp in datapoints]
            
            # Calculate statistics
            stats = {
                'min': float(np.min(values)),
                'max': float(np.max(values)),
                'avg': float(np.mean(values)),
                'p95': float(np.percentile(values, 95)),
                'p99': float(np.percentile(values, 99))
            }
            
            return MetricData(
                metric_name=metric_name,
                broker_id=broker_id,
                timestamps=timestamps,
                values=values,
                unit=response.get('Label', ''),
                statistics=stats
            )
            
        except ClientError as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                time.sleep(wait_time)
            else:
                return None
        except Exception as e:
            logger.error(f"Unexpected error querying {metric_name}: {e}")
            return None
    
    return None
