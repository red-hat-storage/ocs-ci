import logging
import random

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    aws_platform_required,
    brown_squad,
)
from ocs_ci.framework.testlib import ManageTest, tier4, tier4b
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers import sanity_helpers

logger = logging.getLogger(__name__)


@brown_squad
@tier4
@tier4b
@pytest.mark.polarion_id("OCS-1287")
@aws_platform_required
@pytest.mark.skip(reason="az blocking method need to be fixed")
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

    @pytest.fixture()
    def teardown(self, request, ec2_instances, aws_obj):
        def finalizer():
            current_sg = aws_obj.store_security_groups_for_instances(
                self.instances_in_az
            )
            if self.original_sgs != current_sg:
                aws_obj.restore_instances_access(
                    self.security_group_id, self.original_sgs
                )
                logger.info(
                    f"Access to EC2 instances {self.instances_in_az} has been restored"
                )

            if self.security_group_id in aws_obj.get_all_security_groups():
                logger.info(f"Deleting: {self.security_group_id}")
                aws_obj.delete_security_group(self.security_group_id)

        request.addfinalizer(finalizer)

    def test_availability_zone_failure(
        self,
        aws_obj,
        ec2_instances,
        pvc_factory,
        pod_factory,
        teardown,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """

        Simulate AWS availability zone failure

        """

        # Select instances in randomly chosen availability zone:
        self.instances_in_az = self.random_availability_zone_selector(
            aws_obj, ec2_instances
        )
        logger.info(f"AZ selected, Instances: {self.instances_in_az} to be blocked")

        # Storing current security groups for selected instances:
        self.original_sgs = aws_obj.store_security_groups_for_instances(
            self.instances_in_az
        )
        logger.info(
            f"Original security groups of selected instances: {self.original_sgs}"
        )

        # Blocking instances:
        self.security_group_id = self.block_aws_availability_zone(
            aws_obj, self.instances_in_az
        )
        logger.info(f"Access to EC2 instances {self.instances_in_az} has been blocked")

        # Check cluster's health, need to be unhealthy at that point

        assert not self.check_cluster_health(), (
            "Cluster is wrongly reported as healthy."
            "EC2 Instances {self.instances_in_az} are blocked"
        )

        # Create resources
        logger.info("Trying to create resources on un-healthy cluster")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        logger.info("Resources Created")

        # Delete resources
        logger.info("Trying to delete resources on un-healthy cluster")
        self.sanity_helpers.delete_resources()
        logger.info("Resources Deleted")

        # Restore access for blocked instances
        aws_obj.restore_instances_access(self.security_group_id, self.original_sgs)
        logger.info("Access restores")

        # Check cluster's health, need to be healthy at that point

        assert self.check_cluster_health(), "Cluster is unhealthy"

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
        random_az_selected = aws_obj.get_availability_zone_id_by_instance_id(
            random_az_selector
        )
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
        dict_permissions = {
            "IpProtocol": "tcp",
            "FromPort": 80,
            "ToPort": 80,
            "IpRanges": [{"CidrIp": "1.1.1.1/32"}],
        }
        vpc_id = aws_obj.get_vpc_id_by_instance_id(instances_in_az[0])
        security_group_id = aws_obj.create_security_group(
            group_name, dict_permissions, vpc_id
        )
        aws_obj.block_instances_access(security_group_id, instances_in_az)

        return security_group_id

    def check_cluster_health(self):
        try:
            self.sanity_helpers.health_check()
            return True
        except CommandFailed as e:
            if "Unable to connect to the server" in str(e):
                logger.warning(f"{e}, Cluster is not healthy")
                return False
