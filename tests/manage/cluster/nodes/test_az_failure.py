import logging
import pytest
import random

from ocs_ci.framework.testlib import ManageTest, tier4, bugzilla
from ocs_ci.framework import config
from tests import sanity_helpers

logger = logging.getLogger(__name__)


@tier4
@pytest.mark.polarion_id("OCS-1287")
@pytest.mark.skipif(
    condition=config.ENV_DATA['platform'] != 'AWS',
    reason="Tests are not running on AWS deployed cluster"
)
@bugzilla('1754287')
class TestAvailabilityZones(ManageTest):
    """
    test availability zone failure:
    test stages:
    1. Select availability zone
    2. In this availability zone, backup instances original security groups
    3. block availability zone by attaching security group with no permissions
    4. validate - cluster functionality and health
        2a. health check - warning or error
        2b. create cephfs, create rbd, create pvc (validate_cluster)

    5. restore availability zone access
    6. validate - cluster functionality and health
    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        init Sanity() object
        """

        self.sanity_helpers = sanity_helpers.Sanity()

    def test_health_check(self):
        """

        Temp function for unittests. check cluster health before
            main test

        """
        self.check_cluster_health()

    def test_availability_zone_failure(self, aws_obj, ec2_instances, pvc_factory, pod_factory):
        """

        Simulate AWS availability zone failure

        """

        # Select instances in randomly chosen availability zone:
        instances_in_az = self.random_availability_zone_selector(aws_obj, ec2_instances)
        logger.info(f"AZ selected, Instances: {instances_in_az} to be blocked")

        # Storing current security groups for selected instances:
        original_sgs = aws_obj.store_security_groups_for_instances(instances_in_az)
        logger.info(f"Original security groups of selected instances: {original_sgs}")

        # Blocking instances:
        security_group_id = self.block_aws_availability_zone(aws_obj, instances_in_az)
        logger.info("Access Blocked")

        # Check cluster's health, need to be unhealthy at that point
        assert self.check_cluster_health() == 0

        # Create resources
        logger.info("Trying to create resources on un-healthy cluster")
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        logger.info("Resources Created")

        # Delete resources

        # Restore access for blocked instances
        aws_obj.restore_instances_access(security_group_id, original_sgs)
        logger.info(f"Access restores")

        # Check cluster's health, need to be healthy at that point
        assert self.check_cluster_health() == 1

    def random_availability_zone_selector(self, aws_obj, ec2_instances):
        """
        Get all instances within random availability zone

        Args:
            aws_obj (obj): aws.AWS() object
            ec2_instances (dict): cluster ec2 instances objects

        Returns:
            list: instances_in_az

        """
        random_az_selector = random.choice(list(ec2_instances.keys()))
        random_az_selected = aws_obj.get_availability_zone_id_by_instance_id(random_az_selector)
        instances_in_az = list()
        for instance in ec2_instances.keys():
            az = aws_obj.get_availability_zone_id_by_instance_id(instance)
            if random_az_selected == az:
                instances_in_az.append(instance)

        return instances_in_az

    def block_aws_availability_zone(self, aws_obj, instances_in_az):
        """
        1. get vpc_id
        2. create security group in this vpc
        3. block availability zone by using "append_security_group"

        Args:
            aws_obj (obj): aws.AWS() object
            instances_in_az (list): ec2_instances within selected availability zone

        Returns:
            security_group_id (str): Newly created security id without access permissions

        """
        group_name = "TEST_SEC_GROUP"
        dict_permissions = {'IpProtocol': 'tcp',
                            'FromPort': 80,
                            'ToPort': 80,
                            'IpRanges': [{'CidrIp': '1.1.1.1/32'}]}
        vpc_id = aws_obj.get_vpc_id_by_instance_id(instances_in_az[0])
        security_group_id = aws_obj.create_security_group(group_name, dict_permissions, vpc_id)
        aws_obj.block_instances_access(security_group_id, instances_in_az)

        return security_group_id

    def check_cluster_health(self):
        try:
            self.sanity_helpers.health_check()
            return True
        except Exception as e:
            print(e)
            return False
