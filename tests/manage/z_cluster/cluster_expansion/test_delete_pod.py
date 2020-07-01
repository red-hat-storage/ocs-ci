import pytest
import logging
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier4a
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.cluster import get_percent_used_capacity
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler
from tests.disruption_helpers import Disruptions


@ignore_leftovers
@tier4a
class TestAddCapacityWithResourceDelete:
    """
    Test add capacity when one of the resources gets deleted
    in the middle of the process.
    """

    new_pods_in_status_running = False

    def kill_resource_repeatedly(self, resource_name, resource_id):
        """
        The function get the resource name, and id and kill the resource repeatedly
        until the new pods reached status running.
        Args:
            resource_name (str): the name of the resource to kill
            resource_id (int): the id of the resource to kill
        """
        d = Disruptions()
        logging.info("starting function 'kill_resource_repeatedly'")
        try:
            timeout = 150
            for d in TimeoutSampler(
                timeout=timeout, sleep=2, func=d.set_resource, resource=resource_name
            ):
                d.delete_resource(resource_id)
                if self.new_pods_in_status_running:
                    logging.info("New osd pods in status running")
                    break
        except TimeoutExpiredError:
            logging.warning(
                f"New osd pods are not in status running after {timeout} seconds"
            )

    @pytest.mark.parametrize(
        argnames="workload_storageutilization_rbd, resource_name, resource_id, is_kill_resource_repeatedly",
        argvalues=[
            pytest.param(
                *[(0.11, True, 120), constants.ROOK_OPERATOR, 0, False],
                marks=pytest.mark.polarion_id("OCS-1206")
            ),
            pytest.param(
                *[(0.11, True, 120), constants.MON_DAEMON, 0, True],
                marks=pytest.mark.polarion_id("OCS-1207")
            ),
        ],
        indirect=["workload_storageutilization_rbd"],
    )
    def test_add_capacity_with_resource_delete(self, workload_storageutilization_rbd, resource_name,
                                               resource_id, is_kill_resource_repeatedly):
        """
        The function get the resource name, and id.
        The function adds capacity to the cluster, and then delete the resource while
        storage capacity is getting increased.
        Args:
            resource_name (str): the name of the resource to delete
            resource_id (int): the id of the resource to delete
            is_kill_resource_repeatedly (bool): If True then kill the resource repeatedly. Else, if False
                delete the resource only once.
        """
        used_percentage = get_percent_used_capacity()
        logging.info(f"storageutilization is completed. used capacity = {used_percentage}")

        max_osds = 15
        osd_pods_before = pod_helpers.get_osd_pods()
        number_of_osd_pods_before = len(osd_pods_before)
        if number_of_osd_pods_before >= max_osds:
            pytest.skip("We have maximum of osd's in the cluster")

        d = Disruptions()
        d.set_resource(resource_name)

        self.new_pods_in_status_running = False

        osd_size = storage_cluster.get_osd_size()
        logging.info(f"Adding one new set of OSDs. osd size = {osd_size}")
        storagedeviceset_count = storage_cluster.add_capacity(osd_size)
        logging.info("Adding one new set of OSDs was issued without problems")

        # Wait for new osd's to come up. After the first new osd in status Init - delete the resource.
        # After deleting the resource we expect that all the new osd's will be in status running,
        # and the delete resource will be also in status running.
        pod_helpers.wait_for_new_osd_pods_to_come_up(number_of_osd_pods_before)
        logging.info(f"Delete a {resource_name} pod while storage capacity is getting increased")
        if is_kill_resource_repeatedly:
            logging.info("kill resource repeatedly chosen")
            with ThreadPoolExecutor() as executor:
                executor.submit(self.kill_resource_repeatedly, resource_name, resource_id)
        else:
            d.delete_resource(resource_id)

        pod = OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )

        pod.wait_for_resource(
            timeout=420,
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-osd',
            resource_count=storagedeviceset_count * 3
        )

        self.new_pods_in_status_running = True
        logging.info("Finished verifying add capacity when one of the osd pods gets deleted")
        logging.info("Waiting for ceph health check to finished...")
        ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace'], tries=80
        )
