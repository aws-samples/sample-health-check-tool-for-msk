"""Unit tests for validators module."""

import pytest
from botocore.exceptions import ClientError
from msk_health_check.validators import validate_region, validate_arn, verify_cluster_exists


class TestValidateRegion:
    """Tests for validate_region function."""
    
    def test_valid_regions(self):
        """Test valid AWS region formats."""
        valid_regions = ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-south-1']
        for region in valid_regions:
            result = validate_region(region)
            assert result.is_valid  # nosemgrep: is-function-without-parentheses
            assert result.error_message is None
    
    def test_invalid_regions(self):
        """Test invalid region formats."""
        invalid_regions = [
            'invalid',
            'us-east',
            'us-east-1a',
            'US-EAST-1',
            '123-456-7',
            '',
            'us_east_1'
        ]
        for region in invalid_regions:
            result = validate_region(region)
            assert not result.is_valid  # nosemgrep: is-function-without-parentheses
            assert result.error_message is not None


class TestValidateArn:
    """Tests for validate_arn function."""
    
    def test_valid_arn(self):
        """Test valid MSK cluster ARN."""
        arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/abc-123-def'
        result = validate_arn(arn)
        assert result.is_valid  # nosemgrep: is-function-without-parentheses
        assert result.error_message is None
    
    def test_invalid_arns(self):
        """Test invalid ARN formats."""
        invalid_arns = [
            'invalid-arn',
            'arn:aws:ec2:us-east-1:123456789012:instance/i-123',
            'arn:aws:kafka:us-east-1:123:cluster/test/uuid',
            'arn:aws:kafka:invalid-region:123456789012:cluster/test/uuid',
            '',
            'arn:aws:kafka:us-east-1:123456789012:topic/test'
        ]
        for arn in invalid_arns:
            result = validate_arn(arn)
            assert not result.is_valid  # nosemgrep: is-function-without-parentheses
            assert result.error_message is not None


class TestVerifyClusterExists:
    """Tests for verify_cluster_exists function."""
    
    def test_cluster_exists(self):
        """Test when cluster exists."""
        mock_client = MockMSKClient(exists=True)
        arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid'
        result = verify_cluster_exists(mock_client, arn)
        assert result.is_valid  # nosemgrep: is-function-without-parentheses
        assert result.error_message is None
    
    def test_cluster_not_found(self):
        """Test when cluster doesn't exist."""
        mock_client = MockMSKClient(exists=False)
        arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid'
        result = verify_cluster_exists(mock_client, arn)
        assert not result.is_valid  # nosemgrep: is-function-without-parentheses
        assert 'not found' in result.error_message.lower()
    
    def test_other_client_error(self):
        """Test when other AWS error occurs."""
        mock_client = MockMSKClient(error='AccessDenied')
        arn = 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid'
        result = verify_cluster_exists(mock_client, arn)
        assert not result.is_valid  # nosemgrep: is-function-without-parentheses
        assert result.error_message is not None


class MockMSKClient:
    """Mock MSK client for testing."""
    
    def __init__(self, exists=True, error=None):
        self.exists = exists
        self.error = error
    
    def describe_cluster_v2(self, ClusterArn):
        """Mock describe_cluster_v2 call."""
        if self.error:
            raise ClientError(
                {'Error': {'Code': self.error, 'Message': 'Test error'}},
                'DescribeClusterV2'
            )
        if not self.exists:
            raise ClientError(
                {'Error': {'Code': 'NotFoundException', 'Message': 'Cluster not found'}},
                'DescribeClusterV2'
            )
        return {'ClusterInfo': {'ClusterArn': ClusterArn}}
