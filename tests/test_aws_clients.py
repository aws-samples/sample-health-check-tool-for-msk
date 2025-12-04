"""Tests for AWS client manager."""

import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import NoCredentialsError, ClientError

from msk_health_check.aws_clients import create_aws_clients, AWSClients


class TestCreateAWSClients:
    """Tests for create_aws_clients function."""
    
    @patch('msk_health_check.aws_clients.boto3.client')
    def test_successful_client_creation(self, mock_boto_client):
        """Test successful creation of AWS clients."""
        mock_msk = MagicMock()
        mock_cloudwatch = MagicMock()
        mock_boto_client.side_effect = [mock_msk, mock_cloudwatch]
        
        result = create_aws_clients('us-east-1')
        
        assert isinstance(result, AWSClients)
        assert result.msk_client == mock_msk
        assert result.cloudwatch_client == mock_cloudwatch
        assert result.region == 'us-east-1'
        assert mock_boto_client.call_count == 2
    
    @patch('msk_health_check.aws_clients.boto3.client')
    def test_retry_configuration(self, mock_boto_client):
        """Test that retry configuration is applied."""
        mock_msk = MagicMock()
        mock_cloudwatch = MagicMock()
        mock_boto_client.side_effect = [mock_msk, mock_cloudwatch]
        
        create_aws_clients('us-west-2')
        
        # Verify config was passed
        calls = mock_boto_client.call_args_list
        for call in calls:
            assert 'config' in call.kwargs
            config = call.kwargs['config']
            assert config.retries['max_attempts'] == 3
            assert config.retries['mode'] == 'standard'
    
    @patch('msk_health_check.aws_clients.boto3.client')
    def test_no_credentials_error(self, mock_boto_client):
        """Test handling of missing credentials."""
        mock_boto_client.side_effect = NoCredentialsError()
        
        with pytest.raises(NoCredentialsError):
            create_aws_clients('us-east-1')
    
    @patch('msk_health_check.aws_clients.boto3.client')
    def test_client_error(self, mock_boto_client):
        """Test handling of client errors."""
        mock_boto_client.side_effect = ClientError(
            {'Error': {'Code': 'InvalidClientTokenId', 'Message': 'Invalid token'}},
            'CreateClient'
        )
        
        with pytest.raises(ClientError):
            create_aws_clients('us-east-1')
    
    @patch('msk_health_check.aws_clients.boto3.client')
    def test_multiple_regions(self, mock_boto_client):
        """Test client creation for different regions."""
        regions = ['us-east-1', 'eu-west-1', 'ap-south-1']
        
        for region in regions:
            mock_msk = MagicMock()
            mock_cloudwatch = MagicMock()
            mock_boto_client.side_effect = [mock_msk, mock_cloudwatch]
            
            result = create_aws_clients(region)
            assert result.region == region
