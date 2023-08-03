import logging
import subprocess
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    tier1,
    skipif_external_mode,
    skipif_ocs_version,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import get_percent_used_capacity, CephCluster
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


class TestDeleteRbdPool(ManageTest):
    @tier1
    @bugzilla("2228555")
    @skipif_external_mode
    @skipif_ocs_version("<4.6")
    @pytest.mark.parametrize(
        argnames=["replica", "compression", "volume_binding_mode", "pvc_status"],
        argvalues=[
            pytest.param(
                *[
                    2,
                    "aggressive",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_PENDING,
                ],
                marks=pytest.mark.polarion_id("OCS-5134"),
            ),
            pytest.param(
                *[
                    3,
                    "aggressive",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                ],
                marks=pytest.mark.polarion_id("OCS-5135"),
            ),
            pytest.param(
                *[
                    2,
                    "none",
                    constants.WFFC_VOLUMEBINDINGMODE,
                    constants.STATUS_PENDING,
                ],
                marks=pytest.mark.polarion_id("OCS-5136"),
            ),
            pytest.param(
                *[
                    3,
                    "none",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                ],
                marks=pytest.mark.polarion_id("OCS-5137"),
            ),
        ],
    )
    def test_delete_rbd_pool_associated_with_sc(
        self,
        replica,
        compression,
        volume_binding_mode,
        pvc_status,
        storageclass_factory_class,
        pvc_factory,
        pod_factory,
    ):

        """
        1. Create storageclass with the pool.
        2. Check that in pool list and page the storageclass is there.
        3. Try to delete the pool while it is attached to the storageclass.
        4. Verify pool is Ready.

        """

        interface_type = constants.CEPHBLOCKPOOL
        sc_obj = storageclass_factory_class(
            interface=interface_type,
            new_rbd_pool=True,
            replica=replica,
            compression=compression,
            volume_binding_mode=volume_binding_mode,
            pool_name="test-pool",
        )

        logger.info(f"Creating a PVC using {sc_obj.name}")
        pvc_obj = pvc_factory(
            interface=interface_type,
            storageclass=sc_obj,
            size=10,
            status=pvc_status,
        )
        logger.info(f"PVC: {pvc_obj.name} created successfully using " f"{sc_obj.name}")

        logger.info(f"Creating an app pod and mount {pvc_obj.name}")
        pod_obj = pod_factory(interface=interface_type, pvc=pvc_obj)
        logger.info(f"{pod_obj.name} created successfully and mounted {pvc_obj.name}")

        # Run IO on each app pod for sometime
        logger.info(f"Running FIO on {pod_obj.name}")
        pod_obj.run_io(
            "fs",
            size="1G",
            rate="1500m",
            runtime=60,
            buffer_compress_percentage=60,
            buffer_pattern="0xdeadface",
            bs="8K",
            jobs=5,
            readwrite="readwrite",
        )
        cluster_used_space = get_percent_used_capacity()
        logger.info(
            f"Cluster used space with replica size {replica}, "
            f"compression mode {compression}={cluster_used_space}"
        )
        cbp_name = sc_obj.get().get("parameters").get("pool")
        logger.info(f"cephblockpool name is {cbp_name}. Deleting it now")

        try:
            OCP().exec_oc_cmd(
                f"delete cephblockpool {cbp_name} -n {config.ENV_DATA.get('cluster_namespace')}",
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            logger.info(
                f"cephblockpool {cbp_name} deletion failed as expected as it is referenced by storageclass "
                "and data loss may happen"
            )

        ceph_cluster = CephCluster()
        res = ceph_cluster.get_blockpool_status(cbp_name)
        if not res:
            pytest.fail(
                f"cephblockpool '{cbp_name}' state is not ready after deletion. "
                "cephblockpool deletion should fail if referenced by storageclass"
            )
