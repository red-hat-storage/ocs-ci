# import random
import logging

# from ocs_ci.ocs import constants
# from ocs_ci.ocs.resources.pv import get_pv_in_status
# from ocs_ci.ocs.resources.deployment import get_osd_deployments
# from ocs_ci.helpers.helpers import modify_deployment_replica_count
# from ocs_ci.ocs.resources.pod import (
#     wait_for_pods_by_label_count,
#     delete_all_osd_removal_jobs,
#     run_osd_removal_job,
#     verify_osd_removal_job_completed_successfully,
# )
from ocs_ci.ocs.osd_operations import osd_device_replacement

logger = logging.getLogger(__name__)


class TestDeviceReplacementInStretchCluster:
    def test_device_replacement(self, nodes):
        """
        Test device replacement in stretch cluster

        """
        osd_device_replacement(nodes)

        # # Choose the OSD that needs to be replaced
        # all_osd_deployments = get_osd_deployments()
        # osd_deployment = random.choice(all_osd_deployments)
        # osd_id = osd_deployment.get()["metadata"]["labels"]["ceph-osd-id"]
        # logger.info(f"osd-{osd_id} needs to be removed")
        #
        # # Scale down the osd deployment
        # logger.info(f"scaling down {osd_deployment.name} now...")
        # modify_deployment_replica_count(
        #     osd_deployment.name,
        #     replica_count=0,
        # )
        #
        # # Verify that osd pods are terminated
        # wait_for_pods_by_label_count(
        #     f"ceph-osd-id={osd_id}",
        #     exptected_count=0,
        # )
        # logger.info("osd pods are terminated successfully")
        #
        # # Remove the osd from the cluster
        # # to add new osd
        # delete_all_osd_removal_jobs()
        # run_osd_removal_job(osd_ids=[osd_id])
        #
        # # Verify that OSDs are removed successfully
        # verify_osd_removal_job_completed_successfully(osd_id)
        #
        # # Find the persistent volume (PV) that need to be deleted and delete it
        # pvs = get_pv_in_status(
        #     storage_class=constants.LOCALSTORAGE_SC, status=constants.STATUS_RELEASED
        # )
        # for pv in pvs:
        #     pv.delete()

        # Track the provisioning of PVs for the devices that match the deviceInclusionSpec

        # Delete the ocs-osd-removal job(s).
