import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import validate_num_of_pgs
from ocs_ci.framework.testlib import ui, skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    bugzilla,
    green_squad,
)
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import run_cmd


logger = logging.getLogger(__name__)


@ui
@tier1
@green_squad
@bugzilla("2253013")
@skipif_ocs_version("<4.16")
@pytest.mark.parametrize(
    argnames=["replica", "compression"],
    argvalues=[
        pytest.param(*[2, False], marks=pytest.mark.polarion_id("OCS-6255")),
    ],
)
class TestScAndPoolUserInterface(object):
    """
    Test to create storageclass and cephblockpool via user interface and validate pg_num

    """

    def test_sc_and_pool_ui_and_validate_pg_num(
        self,
        storageclass_factory_ui,
        pod_factory,
        pvc_factory,
        setup_ui,
        replica,
        compression,
    ):
        """
        Test steps
        1. Create storageclass and pool via UI
        2. Check the values of pg_num , it should be equal to osd_pool_default_pg_num
        3. Check PG autoscale is ON
        4. New pool is having non-blank deviceclass
        5. Create PVC and pod using the new storageclass created
        6. Run IOs in the PVCs
        """

        # Creating SC and pool from UI
        sc_obj = storageclass_factory_ui(
            replica=replica,
            compression=compression,
            create_new_pool=True,
            default_pool=constants.DEFAULT_BLOCKPOOL,
        )
        sc_name = sc_obj.get()["metadata"]["name"]
        logger.info(f"The storageclass {sc_name}")
        pool_name = sc_obj.get()["parameters"]["pool"]

        # Check pg_num and osd_pool_default_pg_num matches
        ct_pod = get_ceph_tools_pod()
        osd_pool_default_pg_num = ct_pod.exec_ceph_cmd(
            ceph_cmd="ceph config get mon osd_pool_default_pg_num"
        )
        logger.info(f"The osd pool default pg num value is {osd_pool_default_pg_num}")
        expected_pgs = {
            f"{pool_name}": osd_pool_default_pg_num,
        }
        assert validate_num_of_pgs(
            expected_pgs
        ), "pg_num is not equal to the osd pool default pg num"
        logger.info(
            f"pg_num of the new pool {pool_name} "
            f"is equal to the osd pool default pg num {osd_pool_default_pg_num}"
        )

        # Check if the pg-autoscale is ON
        pool_autoscale_status = ct_pod.exec_ceph_cmd(
            ceph_cmd="ceph osd pool autoscale-status"
        )
        for pool in pool_autoscale_status:
            if pool["pool_name"] == pool_name:
                assert pool["pg_autoscale_mode"] == "on", "PG autoscale mode is off"
        logger.info(f"{pool_name} autoscale mode is on")

        # Check the pool is not none
        oc_obj = OCP(kind=constants.CEPHBLOCKPOOL)
        cbp_output = run_cmd(
            cmd=f"oc get cephblockpool/{pool_name} -n {constants.OPENSHIFT_STORAGE_NAMESPACE} -o yaml"
        )
        cbp_output = oc_obj.exec_oc_cmd(
            command=f"get cephblockpool/{pool_name} -n {constants.OPENSHIFT_STORAGE_NAMESPACE} -o yaml"
        )
        assert cbp_output["spec"]["deviceClass"] is not None, "The Deviceclass is none"
        logger.info(
            f"The deviceClass of the pool {pool_name} is {cbp_output['spec']['deviceClass']}"
        )

        # Create new pvc and pod with the newly created storageclass
        pvc_obj = pvc_factory(
            storageclass=sc_obj,
            interface=constants.CEPHFILESYSTEM,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
            size=200,
        )

        # Create a pod and run IOs
        pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )
        pod_obj.run_io(
            storage_type=constants.CEPHFILESYSTEM,
            size="100M",
            io_direction="write",
            runtime=10,
        )
