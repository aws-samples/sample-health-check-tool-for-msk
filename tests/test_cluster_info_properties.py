"""Property-based tests for cluster info module."""

from hypothesis import given, strategies as st
from msk_health_check.cluster_info import determine_instance_family, get_cluster_info


# Property 42: Instance family identification
@given(st.sampled_from(['m6g', 'm7g', 'c6g', 'c7g', 'r6g', 'r7g', 't4g']))
def test_property_graviton_instance_identification(family):
    """Property: All Graviton instance families should be identified as 'graviton'."""
    instance_type = f'kafka.{family}.large'
    assert determine_instance_family(instance_type) == 'graviton'


@given(st.sampled_from(['m5', 'm4', 'c5', 'c4', 'r5', 'r4', 't3', 't2']))
def test_property_intel_instance_identification(family):
    """Property: All Intel instance families should be identified as 'intel'."""
    instance_type = f'kafka.{family}.large'
    assert determine_instance_family(instance_type) == 'intel'


# Property 36: Authentication method extraction
def test_property_authentication_method_extraction():
    """Property: All configured authentication methods should be extracted."""
    from tests.test_cluster_info import MockMSKClient
    
    # Test all combinations of authentication methods
    auth_combinations = [
        ['IAM'],
        ['SASL/SCRAM'],
        ['mTLS'],
        ['unauthenticated'],
        ['IAM', 'SASL/SCRAM'],
        ['IAM', 'mTLS'],
        ['IAM', 'SASL/SCRAM', 'mTLS'],
    ]
    
    for auth_methods in auth_combinations:
        mock_client = MockMSKClient(auth_methods=auth_methods)
        result = get_cluster_info(mock_client, 'arn:aws:kafka:us-east-1:123456789012:cluster/test/uuid')
        
        # All configured methods should be present
        for method in auth_methods:
            assert method in result.authentication_methods, \
                f"Expected {method} in {result.authentication_methods}"
        
        # No extra methods should be present
        assert len(result.authentication_methods) == len(auth_methods), \
            f"Expected {len(auth_methods)} methods, got {len(result.authentication_methods)}"
