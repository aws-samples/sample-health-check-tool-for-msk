"""AWS client manager for MSK Health Check Report."""

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, ClientError

logger = logging.getLogger(__name__)

_RETRY_CONFIG = Config(retries={'max_attempts': 3, 'mode': 'standard'})


@dataclass
class AWSClients:
    """Container for AWS service clients."""
    msk_client: Any
    cloudwatch_client: Any
    region: str
    _autoscaling_client: Optional[Any] = field(default=None, repr=False)

    @property
    def autoscaling_client(self) -> Optional[Any]:
        """Application Auto Scaling client (broker storage scaling policy), created on first use."""
        if self._autoscaling_client is None:
            try:
                self._autoscaling_client = boto3.client('application-autoscaling', region_name=self.region,
                                                        config=_RETRY_CONFIG)
            except Exception as e:  # the storage auto scaling check degrades to "not assessed"
                logger.warning(f"Application Auto Scaling client unavailable: {e}")
                return None
        return self._autoscaling_client


def create_aws_clients(region: str) -> AWSClients:
    """Create and configure the MSK and CloudWatch clients.

    Raises:
        NoCredentialsError: when AWS credentials are not configured
        ClientError: when a client cannot be created
    """
    try:
        msk_client = boto3.client('kafka', region_name=region, config=_RETRY_CONFIG)
        cloudwatch_client = boto3.client('cloudwatch', region_name=region, config=_RETRY_CONFIG)
        logger.info(f"AWS clients created for region {region}")
        return AWSClients(msk_client=msk_client, cloudwatch_client=cloudwatch_client, region=region)
    except NoCredentialsError:
        logger.warning("AWS credentials not found. Configure credentials with the AWS CLI, environment variables or an IAM role.")
        raise
    except ClientError as e:
        logger.warning(f"Failed to create AWS clients: {e}")
        raise
