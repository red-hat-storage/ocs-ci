import logging

from ocs_ci.framework.pytest_customization.marks import tier2
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.bucket_utils import setup_base_objects, sync_object_directory
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


def has_required_ipranges(ipPermission, lookup_type="all"):
    """
    A helper function to process Amazon's security group's JSON

    Args:
        ipPermission (list): The ipPermission list from the response's JSON
        lookup_type (str): "all" to check whether all subnets are present in the rules,
                           "any" to check whether any of the subnets are present
    """
    if lookup_type == "all":
        return constants.TEST_NET_BLOCK_SET.issubset(
            {ipRange["CidrIp"] for ipRange in ipPermission.get("IpRanges", [])}
        )
    elif lookup_type == "any":
        return constants.TEST_NET_BLOCK_SET.intersection(
            {ipRange["CidrIp"] for ipRange in ipPermission.get("IpRanges", [])}
        )

def has_required_ippermissions(securityGroup, lookup_type="all"):
    """
    A helper function to process Amazon's security group's JSON

    Args:
        ipPermission (list): The securityGroup list from the response's JSON
        lookup_type (str): "all" to check whether all subnets are present in the rules,
                           "any" to check whether any of the subnets are present
    """
    return any(
        has_required_ipranges(ipPermission, lookup_type=lookup_type)
        for ipPermission in securityGroup.get("IpPermissions", [])
    )

class TestLBSubnetConfig(MCGTest):
    @tier2
    def test_subnet_addition_and_removal(
        self,
        edit_mcg_subnets,
        awscli_pod_session,
        cld_mgr,
        bucket_factory,
        test_directory_setup,
    ):
        """
        Test bucket creation using the S3 SDK, OC command or MCG CLI.
        The factory checks the bucket's health by default.
        """

        # Retrieve all security groups
        filtered_security_groups = (
            cld_mgr.aws_client.ec2_resource.meta.client.describe_security_groups()
        )

        # Verify that the patch propagated to AWS
        assert any(
            has_required_ippermissions(securityGroup)
            for securityGroup in filtered_security_groups["SecurityGroups"]
        ), f"Could not find a security group that contains all \
        the expected IPs. SGs that were found: {filtered_security_groups}"

        # Verify IO still works properly
        setup_base_objects(
            awscli_pod_session, test_directory_setup.origin_dir, amount=3
        )
        bucket = bucket_factory()[0]
        sync_object_directory(
            awscli_pod_session,
            src=test_directory_setup.origin_dir,
            target=f"s3://{bucket.name}",
        )

        # Revert the NooBaa CR patch
        clean_lb_config = '{"spec":{"loadBalancerSourceSubnets":null}}'
        OCP(kind="noobaa", namespace=defaults.ROOK_CLUSTER_NAMESPACE).patch(
            resource_name="noobaa", params=clean_lb_config, format_type="merge"
        )

        # Verify that the CIDR blocks cannot be found in any security group
        assert not any(
            has_required_ippermissions(securityGroup, lookup_type="any")
            for securityGroup in filtered_security_groups["SecurityGroups"]
        ), f"A security group containing reserved CIDR blocks was found: \
            {filtered_security_groups}"
