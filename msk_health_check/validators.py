"""Input validation module for MSK Health Check Report."""

import re
from dataclasses import dataclass
from typing import Optional

from botocore.exceptions import ClientError


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    is_valid: bool
    error_message: Optional[str] = None


def validate_region(region: str) -> ValidationResult:
    """
    Validate AWS region format.
    
    Args:
        region: AWS region string to validate
        
    Returns:
        ValidationResult indicating if region is valid
    """
    pattern = r'^[a-z]{2}-[a-z]+-\d{1}$'
    if re.match(pattern, region):
        return ValidationResult(is_valid=True)
    return ValidationResult(
        is_valid=False,
        error_message=f"Invalid region format: {region}. Expected format: us-east-1"
    )


def validate_arn(arn: str) -> ValidationResult:
    """
    Validate MSK cluster ARN format.
    
    Args:
        arn: MSK cluster ARN to validate
        
    Returns:
        ValidationResult indicating if ARN is valid
    """
    pattern = r'^arn:aws:kafka:[a-z]{2}-[a-z]+-\d{1}:\d{12}:cluster/[a-zA-Z0-9_-]+/[a-f0-9-]+$'
    if re.match(pattern, arn):
        return ValidationResult(is_valid=True)
    return ValidationResult(
        is_valid=False,
        error_message=f"Invalid MSK cluster ARN format: {arn}"
    )


def verify_cluster_exists(msk_client, arn: str) -> ValidationResult:
    """
    Verify cluster exists in AWS.
    
    Args:
        msk_client: Boto3 MSK client
        arn: MSK cluster ARN to verify
        
    Returns:
        ValidationResult indicating if cluster exists
    """
    try:
        msk_client.describe_cluster_v2(ClusterArn=arn)
        return ValidationResult(is_valid=True)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NotFoundException':
            return ValidationResult(
                is_valid=False,
                error_message=f"Cluster not found: {arn}"
            )
        return ValidationResult(
            is_valid=False,
            error_message=f"Error verifying cluster: {e.response['Error']['Message']}"
        )
