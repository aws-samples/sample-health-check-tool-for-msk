"""AWS client manager for MSK Health Check Report."""

import logging
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, ClientError

logger = logging.getLogger(__name__)


@dataclass
class AWSClients:
    """Container for AWS service clients."""
    msk_client: Any
    cloudwatch_client: Any
    region: str


def create_aws_clients(region: str) -> AWSClients:
    """
    Create and configure AWS service clients.
    
    Args:
        region: AWS region for clients
        
    Returns:
        AWSClients object with initialized clients
        
    Raises:
        NoCredentialsError: When AWS credentials are not configured
        ClientError: When authentication fails
    """
    config = Config(
        retries={
            'max_attempts': 3,
            'mode': 'standard'
        }
    )
    
    try:
        msk_client = boto3.client('kafka', region_name=region, config=config)
        cloudwatch_client = boto3.client('cloudwatch', region_name=region, config=config)
        
        logger.info(f"AWS clients created successfully for region: {region}")
        return AWSClients(
            msk_client=msk_client,
            cloudwatch_client=cloudwatch_client,
            region=region
        )
    except NoCredentialsError as e:
        logger.warning("AWS credentials not found. Please configure credentials using "
                    "AWS CLI, environment variables, or IAM role.")
        raise
    except ClientError as e:
        logger.warning(f"Failed to create AWS clients: {e}")
        raise
