import logging

from ocs_ci.framework import config
from ocs_ci.utility import utils
from ocs_ci.framework.pytest_customization.marks import (
    vsphere_platform_required,
    skipif_mcg_only,
    tier2,
    red_squad,
    mcg,
)
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.resources.pvc import get_pvc_objs, get_pvc_size
from ocs_ci.ocs.exceptions import CommandFailed, CephToolBoxNotFoundException
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@mcg
@red_squad
@skipif_mcg_only
@vsphere_platform_required
class TestNoobaaDbPgExpansion:
    """
    Test noobaa DB PG expansion
    Cleanup operation is not applicable due to shrink volume size on PVC is not supported in kubernetes
    """

    @tier2
    def test_noobaa_db_pg_expansion(self, scale_noobaa_db_pod_pv_size):
        """
        Test to check noobaa db pg PVC is getting expanded using ceph block pool.
        """

        # Check ceph health is reporting HEALTH_OK or not
        utils.ceph_health_check(tries=20)

        # Record MAX avail size of ceph block pool before expanding capacity

        try:
            ceph_toolbox = get_ceph_tools_pod(
                namespace=config.ENV_DATA["cluster_namespace"]
            )
        except (AssertionError, CephToolBoxNotFoundException) as ex:
            raise CommandFailed(ex)

        size_in_byte = ceph_toolbox.exec_ceph_cmd(
            ceph_cmd="ceph df",
            timeout=120,
        )
        for pool in size_in_byte["pools"]:
            if pool.get("name") == constants.DEFAULT_BLOCKPOOL:
                max_avai_bytes = pool["stats"]["max_avail"]
                avail_size = int(max_avai_bytes) / constants.GB
                break
        logger.info(f"Max avail size on ceph block pool is {avail_size} GB")

        # record PVC capacity before expanding capacity
        noobaa_db_pvc_obj = get_pvc_objs(pvc_names=[constants.NOOBAA_DB_PVC_NAME])[0]
        pvc_capacity = get_pvc_size(noobaa_db_pvc_obj)
        logger.info(f"Current PVC capacity is {pvc_capacity} GB")

        # Validate PVC expansion against available RBD size and current PVC size
        assert (
            pvc_capacity <= avail_size
        ), f"PVC expand operation is not valid, current PVC capacity {pvc_capacity} >= ceph RBD AVAIL size {avail_size}"

        # Calculate new PVC size by adding half capacity in the current PVC capacity
        new_pvc_size = round(pvc_capacity + (pvc_capacity / 2))

        # Change db-noobaa-db-pg-0 PVC size to new PVC size and ensure PVC size is changed to new size
        scale_noobaa_db_pod_pv_size(pv_size=new_pvc_size)

        # Verify default backingstore is in ready state or not
        default_bs = OCP(
            kind=constants.BACKINGSTORE, namespace=config.ENV_DATA["cluster_namespace"]
        ).get(resource_name=constants.DEFAULT_NOOBAA_BACKINGSTORE)
        assert (
            default_bs["status"]["phase"] == constants.STATUS_READY
        ), "Default backingstore is not in ready state"

        # Ensure PVC size is changed to new size
        noobaa_db_pvc_obj = get_pvc_objs(pvc_names=[constants.NOOBAA_DB_PVC_NAME])[0]
        new_pvc_capacity = get_pvc_size(noobaa_db_pvc_obj)
        assert (
            new_pvc_size == new_pvc_capacity
        ), f"Failed to expand PVC size to {new_pvc_size}"
