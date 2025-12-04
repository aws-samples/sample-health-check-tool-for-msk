"""Tests for cluster info module."""

import pytest
from msk_health_check.cluster_info import get_cluster_info, determine_instance_family, ClusterInfo


class TestDetermineInstanceFamily:
    """Tests for determine_instance_family function."""
    
    def test_graviton_instances(self):
        """Test Graviton instance identification."""
        graviton_types = [
            'kafka.m6g.large',
            'kafka.m7g.xlarge',
            'kafka.c6g.2xlarge',
            'kafka.c7g.large',
            'kafka.r6g.xlarge',
            'kafka.r7g.2xlarge',
            'kafka.t4g.small'
        ]
        for instance_type in graviton_types:
            assert determine_instance_family(instance_type) == 'graviton'
    
    def test_intel_instances(self):
        """Test Intel instance identification."""
        intel_types = [
            'kafka.m5.large',
            'kafka.m5.xlarge',
            'kafka.t3.small',
            'kafka.c5.2xlarge',
            'kafka.r5.xlarge'
        ]
        for instance_type in intel_types:
            assert determine_instance_family(instance_type) == 'intel'


class TestGetClusterInfo:
    """Tests for get_cluster_info function."""
    
    def test_basic_cluster_info(self):
        """Test extraction of basic cluster information."""
        mock_client = MockMSKClient()
        result = get_cluster_info(mock_client, 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid')
        
        assert isinstance(result, ClusterInfo)
        assert result.name == 'test-cluster'
        assert result.instance_type == 'kafka.m5.large'
        assert result.instance_family == 'intel'
        assert result.broker_count == 3
        assert result.kafka_version == '2.8.1'
    
    def test_authentication_methods_extraction(self):
        """Test extraction of authentication methods."""
        mock_client = MockMSKClient(auth_methods=['IAM', 'SASL/SCRAM'])
        result = get_cluster_info(mock_client, 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid')
        
        assert 'IAM' in result.authentication_methods
        assert 'SASL/SCRAM' in result.authentication_methods
    
    def test_unauthenticated_access(self):
        """Test detection of unauthenticated access."""
        mock_client = MockMSKClient(auth_methods=['unauthenticated'])
        result = get_cluster_info(mock_client, 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid')
        
        assert 'unauthenticated' in result.authentication_methods
    
    def test_mtls_authentication(self):
        """Test detection of mTLS authentication."""
        mock_client = MockMSKClient(auth_methods=['mTLS'])
        result = get_cluster_info(mock_client, 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid')
        
        assert 'mTLS' in result.authentication_methods
    
    def test_encryption_settings(self):
        """Test extraction of encryption settings."""
        mock_client = MockMSKClient(encryption_in_transit=True, encryption_at_rest=True)
        result = get_cluster_info(mock_client, 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid')
        
        assert result.encryption_in_transit is True
        assert result.encryption_at_rest is True
    
    def test_graviton_instance_cluster(self):
        """Test cluster with Graviton instances."""
        mock_client = MockMSKClient(instance_type='kafka.m6g.xlarge')
        result = get_cluster_info(mock_client, 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid')
        
        assert result.instance_type == 'kafka.m6g.xlarge'
        assert result.instance_family == 'graviton'


class MockMSKClient:
    """Mock MSK client for testing."""
    
    def __init__(self, instance_type='kafka.m5.large', broker_count=3, 
                 auth_methods=None, encryption_in_transit=True, encryption_at_rest=True):
        self.instance_type = instance_type
        self.broker_count = broker_count
        self.auth_methods = auth_methods or ['IAM']
        self.encryption_in_transit = encryption_in_transit
        self.encryption_at_rest = encryption_at_rest
    
    def list_kafka_versions(self):
        """Mock list_kafka_versions call."""
        return {
            'KafkaVersions': [
                {'Version': '3.8.0'},
                {'Version': '3.7.0'},
                {'Version': '2.8.1'},
                {'Version': '2.7.0'}
            ]
        }
    
    def describe_cluster_v2(self, ClusterArn):
        """Mock describe_cluster_v2 call."""
        client_auth = {}
        
        if 'IAM' in self.auth_methods:
            client_auth.setdefault('Sasl', {})['Iam'] = {'Enabled': True}
        if 'SASL/SCRAM' in self.auth_methods:
            client_auth.setdefault('Sasl', {})['Scram'] = {'Enabled': True}
        if 'mTLS' in self.auth_methods:
            client_auth['Tls'] = {'Enabled': True}
        if 'unauthenticated' in self.auth_methods:
            client_auth['Unauthenticated'] = {'Enabled': True}
        
        encryption_info = {}
        if self.encryption_in_transit:
            encryption_info['EncryptionInTransit'] = {'ClientBroker': 'TLS'}
        else:
            encryption_info['EncryptionInTransit'] = {'ClientBroker': 'PLAINTEXT'}
        
        if self.encryption_at_rest:
            encryption_info['EncryptionAtRest'] = {'DataVolumeKMSKeyId': 'key-123'}
        
        return {
            'ClusterInfo': {
                'ClusterArn': ClusterArn,
                'ClusterName': 'test-cluster',
                'Provisioned': {
                    'BrokerNodeGroupInfo': {
                        'InstanceType': self.instance_type,
                        'ClientSubnets': ['subnet-1', 'subnet-2', 'subnet-3']
                    },
                    'NumberOfBrokerNodes': self.broker_count,
                    'CurrentBrokerSoftwareInfo': {
                        'KafkaVersion': '2.8.1'
                    },
                    'ClientAuthentication': client_auth,
                    'EncryptionInfo': encryption_info,
                    'LoggingInfo': {
                        'BrokerLogs': {
                            'CloudWatchLogs': {'Enabled': True}
                        }
                    }
                }
            }
        }
